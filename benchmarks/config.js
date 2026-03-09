const path = require("node:path");

const benchmarkDir = __dirname;
const rootDir = path.resolve(benchmarkDir, "..");

const DEFAULT_DATABASE_URL =
  "postgresql://practor:practor@127.0.0.1:54329/practor_benchmark?sslmode=disable";

function readInt(name, fallback) {
  const raw = process.env[name];
  if (!raw) {
    return fallback;
  }

  const parsed = Number.parseInt(raw, 10);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw new Error(`Invalid integer for ${name}: ${raw}`);
  }

  return parsed;
}

function getBenchmarkConfig() {
  return {
    benchmarkDir,
    rootDir,
    schemaPath: path.join(benchmarkDir, "schema.practor"),
    resultsDir: path.join(benchmarkDir, "results"),
    cliPath: path.join(rootDir, "packages", "cli", "dist", "index.js"),
    clientPath: path.join(rootDir, "packages", "client", "dist", "index.js"),
    enginePath:
      process.env.PRACTOR_ENGINE_PATH ||
      path.join(rootDir, "bin", process.platform === "win32" ? "practor-engine.exe" : "practor-engine"),
    databaseUrl: process.env.DATABASE_URL || DEFAULT_DATABASE_URL,
    resetSchemaBeforePush: process.env.PRACTOR_BENCHMARK_RESET_SCHEMA !== "false",
    iterations: readInt("PRACTOR_BENCHMARK_ITERATIONS", 40),
    concurrency: readInt("PRACTOR_BENCHMARK_CONCURRENCY", 8),
    warmup: readInt("PRACTOR_BENCHMARK_WARMUP", 10),
    seedUsers: readInt("PRACTOR_BENCHMARK_SEED_USERS", 1000),
    postsPerUser: readInt("PRACTOR_BENCHMARK_POSTS_PER_USER", 5),
    batchSize: readInt("PRACTOR_BENCHMARK_BATCH_SIZE", 250),
    pageSize: readInt("PRACTOR_BENCHMARK_PAGE_SIZE", 25),
    prepareRetries: readInt("PRACTOR_BENCHMARK_PREPARE_RETRIES", 20),
    prepareRetryDelayMs: readInt("PRACTOR_BENCHMARK_PREPARE_RETRY_DELAY_MS", 2000),
  };
}

function parseDatabaseTarget(databaseUrl) {
  const parsed = new URL(databaseUrl);
  return {
    host: parsed.hostname,
    port: Number.parseInt(parsed.port || "5432", 10),
  };
}

module.exports = {
  DEFAULT_DATABASE_URL,
  getBenchmarkConfig,
  parseDatabaseTarget,
};
