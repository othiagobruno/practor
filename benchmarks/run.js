#!/usr/bin/env node

const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { performance } = require("node:perf_hooks");
const { spawnSync } = require("node:child_process");

const { getBenchmarkConfig } = require("./config");
const { ensureBenchmarkArtifacts } = require("./artifacts");

function chunk(array, size) {
  const result = [];
  for (let i = 0; i < array.length; i += size) {
    result.push(array.slice(i, i + size));
  }
  return result;
}

function percentile(sorted, p) {
  if (sorted.length === 0) {
    return 0;
  }
  const index = Math.min(
    sorted.length - 1,
    Math.max(0, Math.ceil(sorted.length * p) - 1),
  );
  return sorted[index];
}

function formatMs(value) {
  return `${value.toFixed(2)} ms`;
}

function formatOps(value) {
  return value.toFixed(2);
}

function uniqueEmail(prefix, runId, index) {
  return `${prefix}-${runId}-${index}@bench.practor.local`;
}

function runPrepare(config) {
  const preparePath = path.join(config.benchmarkDir, "prepare.js");
  const result = spawnSync(process.execPath, [preparePath], {
    cwd: config.rootDir,
    encoding: "utf-8",
    env: {
      ...process.env,
      DATABASE_URL: config.databaseUrl,
      PRACTOR_ENGINE_PATH: config.enginePath,
      PRACTOR_BENCHMARK_AUTO_BUILD: "false",
    },
    timeout: 180_000,
  });

  if (result.stdout) {
    process.stdout.write(result.stdout);
  }
  if (result.stderr) {
    process.stderr.write(result.stderr);
  }

  if (result.error) {
    throw result.error;
  }
  if (result.status !== 0) {
    throw new Error(`Benchmark prepare exited with code ${result.status}`);
  }
}

async function createManyInChunks(delegate, rows, batchSize) {
  for (const batch of chunk(rows, batchSize)) {
    await delegate.createMany({ data: batch });
  }
}

async function assertBenchmarkTablesExist(client) {
  const rows = await client.$queryRawUnsafe(
    "SELECT to_regclass('public.\"user\"') AS user_table, to_regclass('public.profile') AS profile_table, to_regclass('public.post') AS post_table",
  );
  const tableCheck = rows[0] || {};

  if (!tableCheck.user_table || !tableCheck.profile_table || !tableCheck.post_table) {
    throw new Error(
      `benchmark schema is missing required tables (user=${tableCheck.user_table ?? "null"}, profile=${tableCheck.profile_table ?? "null"}, post=${tableCheck.post_table ?? "null"})`,
    );
  }
}

async function resetDatabase(client) {
  await client.$executeRawUnsafe(
    'TRUNCATE TABLE "profile", "post", "user" RESTART IDENTITY CASCADE',
  );
}

async function seedDatabase(client, config, runId) {
  await resetDatabase(client);

  const users = [];
  for (let userId = 1; userId <= config.seedUsers; userId++) {
    users.push({
      email: uniqueEmail("seed-user", runId, userId),
      name: `Seed User ${userId}`,
      role: userId % 10 === 0 ? "ADMIN" : "USER",
    });
  }
  await createManyInChunks(client.user, users, config.batchSize);

  const profiles = [];
  for (let userId = 1; userId <= config.seedUsers; userId++) {
    profiles.push({
      userId,
      bio: `Bio ${userId}`,
      avatarUrl: `https://cdn.practor.local/avatar/${userId}.png`,
    });
  }
  await createManyInChunks(client.profile, profiles, config.batchSize);

  const posts = [];
  for (let userId = 1; userId <= config.seedUsers; userId++) {
    for (let index = 1; index <= config.postsPerUser; index++) {
      posts.push({
        authorId: userId,
        title: `Post ${userId}-${index}`,
        content: `Benchmark content ${userId}-${index}`,
        published: index % 2 === 0,
      });
    }
  }
  await createManyInChunks(client.post, posts, config.batchSize);

  return {
    userCount: config.seedUsers,
    profileCount: config.seedUsers,
    postCount: config.seedUsers * config.postsPerUser,
    pageSize: config.pageSize,
    maxUserPage: Math.max(1, Math.ceil(config.seedUsers / config.pageSize)),
    maxPostCursor: Math.max(1, config.seedUsers * config.postsPerUser - config.pageSize),
  };
}

async function getCurrentMaxUserId(client) {
  const rows = await client.$queryRawUnsafe(
    'SELECT COALESCE(MAX(id), 0) AS id FROM "user"',
  );
  return Number(rows[0]?.id ?? 0);
}

async function runBenchmarkCase(client, benchmarkCase, config, seedState, runId) {
  const totalOperations = config.iterations * config.concurrency;
  const totalInvocations = config.warmup + totalOperations;
  const localState = benchmarkCase.setup
    ? await benchmarkCase.setup({
        client,
        config,
        seedState,
        totalOperations,
        totalInvocations,
        runId,
      })
    : {};

  for (let i = 0; i < config.warmup; i++) {
    await benchmarkCase.run({
      client,
      config,
      seedState,
      state: localState,
      operationIndex: i,
      measurementIndex: -1,
      isWarmup: true,
      runId,
    });
  }

  const latencies = [];
  let nextOperation = 0;
  const start = performance.now();

  await Promise.all(
    Array.from({ length: config.concurrency }, async () => {
      while (true) {
        const operationIndex = nextOperation;
        nextOperation += 1;
        if (operationIndex >= totalOperations) {
          return;
        }

        const opStart = performance.now();
        await benchmarkCase.run({
          client,
          config,
          seedState,
          state: localState,
          operationIndex: config.warmup + operationIndex,
          measurementIndex: operationIndex,
          isWarmup: false,
          runId,
        });
        latencies.push(performance.now() - opStart);
      }
    }),
  );

  const totalMs = performance.now() - start;
  const sorted = latencies.slice().sort((a, b) => a - b);

  if (benchmarkCase.teardown) {
    await benchmarkCase.teardown({ client, config, seedState, state: localState, runId });
  }

  return {
    name: benchmarkCase.name,
    description: benchmarkCase.description,
    totalOperations,
    totalMs,
    opsPerSecond: totalOperations / (totalMs / 1000),
    averageMs: latencies.reduce((sum, value) => sum + value, 0) / latencies.length,
    minMs: sorted[0] ?? 0,
    p50Ms: percentile(sorted, 0.5),
    p95Ms: percentile(sorted, 0.95),
    maxMs: sorted[sorted.length - 1] ?? 0,
  };
}

function createBenchmarkCases() {
  return [
    {
      name: "findUnique:userById",
      description: "Primary key lookup on the User model",
      run: async ({ client, seedState, operationIndex }) =>
        client.user.findUnique({
          where: { id: (operationIndex % seedState.userCount) + 1 },
        }),
    },
    {
      name: "findMany:publishedPosts",
      description: "Filtered list query with ordering and take",
      run: async ({ client, seedState, operationIndex }) =>
        client.post.findMany({
          where: {
            authorId: (operationIndex % seedState.userCount) + 1,
            published: true,
          },
          orderBy: { id: "desc" },
          take: 20,
        }),
    },
    {
      name: "include:postsWithAuthor",
      description: "Relation loading via include",
      run: async ({ client }) =>
        client.post.findMany({
          take: 20,
          orderBy: { id: "asc" },
          include: { author: true },
        }),
    },
    {
      name: "paginate:users",
      description: "Offset pagination on users",
      run: async ({ client, seedState, operationIndex }) =>
        client.user.paginate({
          page: (operationIndex % seedState.maxUserPage) + 1,
          limit: seedState.pageSize,
          orderBy: { id: "asc" },
        }),
    },
    {
      name: "cursorPaginate:posts",
      description: "Cursor pagination on posts",
      run: async ({ client, seedState, operationIndex }) =>
        client.post.cursorPaginate({
          cursor: { id: (operationIndex % seedState.maxPostCursor) + 1 },
          take: seedState.pageSize,
          orderBy: { id: "asc" },
        }),
    },
    {
      name: "count:publishedPosts",
      description: "Count query with where filter",
      run: async ({ client, seedState, operationIndex }) =>
        client.post.count({
          where: {
            authorId: (operationIndex % seedState.userCount) + 1,
            published: true,
          },
        }),
    },
    {
      name: "rawQuery:safe",
      description: "Tagged-template raw SQL query",
      run: async ({ client, seedState, operationIndex }) =>
        client.$queryRaw`SELECT id, email FROM "user" WHERE id = ${(
          operationIndex % seedState.userCount
        ) + 1}`,
    },
    {
      name: "update:user",
      description: "Single-row update by primary key",
      run: async ({ client, seedState, operationIndex, runId }) =>
        client.user.update({
          where: { id: (operationIndex % seedState.userCount) + 1 },
          data: { name: `Updated ${runId}-${operationIndex}` },
        }),
    },
    {
      name: "create:user",
      description: "Single-row insert",
      run: async ({ client, operationIndex, runId }) =>
        client.user.create({
          data: {
            email: uniqueEmail("create-user", runId, operationIndex),
            name: `Create ${operationIndex}`,
            role: "USER",
          },
        }),
    },
    {
      name: "delete:user",
      description: "Single-row delete with dedicated fixture rows",
      setup: async ({ client, config, totalInvocations, runId }) => {
        const startId = (await getCurrentMaxUserId(client)) + 1;
        const rows = [];
        for (let i = 0; i < totalInvocations; i++) {
          rows.push({
            email: uniqueEmail("delete-user", runId, i),
            name: `Delete ${i}`,
            role: "USER",
          });
        }
        await createManyInChunks(client.user, rows, config.batchSize);
        return { startId };
      },
      run: async ({ client, state, operationIndex }) =>
        client.user.delete({
          where: { id: state.startId + operationIndex },
        }),
    },
    {
      name: "transaction:interactive",
      description: "Interactive transaction with three dependent writes",
      run: async ({ client, operationIndex, runId }) =>
        client.$transaction(async (tx) => {
          const user = await tx.user.create({
            data: {
              email: uniqueEmail("tx-interactive-user", runId, operationIndex),
              name: `TX User ${operationIndex}`,
              role: operationIndex % 2 === 0 ? "USER" : "ADMIN",
            },
          });

          await tx.profile.create({
            data: {
              userId: user.id,
              bio: `TX Bio ${operationIndex}`,
            },
          });

          return tx.post.create({
            data: {
              authorId: user.id,
              title: `TX Post ${operationIndex}`,
              content: "Interactive transaction benchmark",
              published: operationIndex % 2 === 0,
            },
          });
        }),
    },
    {
      name: "transaction:batch",
      description: "Batch transaction with multiple inserts",
      run: async ({ client, operationIndex, runId }) =>
        client.$transaction([
          client.user.create({
            data: {
              email: uniqueEmail("tx-batch-a", runId, operationIndex),
              name: `Batch A ${operationIndex}`,
              role: "USER",
            },
          }),
          client.user.create({
            data: {
              email: uniqueEmail("tx-batch-b", runId, operationIndex),
              name: `Batch B ${operationIndex}`,
              role: "ADMIN",
            },
          }),
        ]),
    },
  ];
}

function printSummary(results, outputPath) {
  const lines = [
    "",
    "| Test | Ops | Total | Ops/s | Avg | P50 | P95 | Max |",
    "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
  ];

  for (const result of results) {
    lines.push(
      `| ${result.name} | ${result.totalOperations} | ${formatMs(result.totalMs)} | ${formatOps(result.opsPerSecond)} | ${formatMs(result.averageMs)} | ${formatMs(result.p50Ms)} | ${formatMs(result.p95Ms)} | ${formatMs(result.maxMs)} |`,
    );
  }

  console.log(lines.join("\n"));
  console.log(`\nBenchmark results written to ${outputPath}`);
}

async function main() {
  const config = getBenchmarkConfig();
  const runId = `${Date.now()}`;

  ensureBenchmarkArtifacts(config);

  console.log("Ensuring benchmark schema is prepared...");
  runPrepare(config);

  const { PractorClient } = require("../packages/client/dist");

  const client = new PractorClient({
    enginePath: config.enginePath,
    schemaPath: config.schemaPath,
    datasourceUrl: config.databaseUrl,
    pool: {
      maxOpenConns: Math.max(20, config.concurrency * 2),
      maxIdleConns: Math.max(10, config.concurrency),
      connMaxLifetimeMs: 300_000,
      connMaxIdleTimeMs: 60_000,
    },
  });

  console.log("Connecting benchmark client...");
  await client.$connect();

  try {
    console.log("Verifying benchmark tables...");
    await assertBenchmarkTablesExist(client);

    console.log("Resetting and seeding benchmark database...");
    const seedState = await seedDatabase(client, config, runId);

    console.log(
      `Seed complete: ${seedState.userCount} users, ${seedState.profileCount} profiles, ${seedState.postCount} posts`,
    );
    console.log(
      `Running ${config.iterations * config.concurrency} ops per test with concurrency ${config.concurrency}`,
    );

    const benchmarkCases = createBenchmarkCases();
    const results = [];

    for (const benchmarkCase of benchmarkCases) {
      console.log(`\n[${benchmarkCase.name}] ${benchmarkCase.description}`);
      const result = await runBenchmarkCase(
        client,
        benchmarkCase,
        config,
        seedState,
        runId,
      );
      results.push(result);
      console.log(
        `Completed ${benchmarkCase.name}: ${formatOps(result.opsPerSecond)} ops/s, p95 ${formatMs(result.p95Ms)}`,
      );
    }

    fs.mkdirSync(config.resultsDir, { recursive: true });
    const outputPath = path.join(config.resultsDir, "latest.json");
    const payload = {
      generatedAt: new Date().toISOString(),
      system: {
        hostname: os.hostname(),
        platform: process.platform,
        arch: process.arch,
        node: process.version,
        cpus: os.cpus().length,
      },
      config: {
        databaseUrl: config.databaseUrl,
        iterations: config.iterations,
        concurrency: config.concurrency,
        warmup: config.warmup,
        seedUsers: config.seedUsers,
        postsPerUser: config.postsPerUser,
        batchSize: config.batchSize,
        pageSize: config.pageSize,
      },
      seed: seedState,
      results,
    };
    fs.writeFileSync(outputPath, `${JSON.stringify(payload, null, 2)}\n`, "utf-8");

    printSummary(results, outputPath);
  } finally {
    await client.$disconnect().catch(() => {});
  }
}

main().catch((error) => {
  console.error(`Benchmark run failed: ${error.message}`);
  if (
    String(error.message).includes('relation "profile" does not exist') ||
    String(error.message).includes("benchmark schema is missing required tables")
  ) {
    console.error(
      "Benchmark tables are missing. The runner now invokes benchmark prepare automatically; rebuild and rerun the benchmark.",
    );
  }
  process.exit(1);
});
