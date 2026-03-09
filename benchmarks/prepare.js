#!/usr/bin/env node

const fs = require("node:fs");
const net = require("node:net");
const path = require("node:path");
const { spawnSync } = require("node:child_process");
const { getBenchmarkConfig, parseDatabaseTarget } = require("./config");
const { ensureBenchmarkArtifacts } = require("./artifacts");

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function assertFileExists(filePath, label) {
  if (!fs.existsSync(filePath)) {
    throw new Error(`${label} not found: ${filePath}`);
  }
}

function waitForPort(host, port, timeoutMs) {
  return new Promise((resolve, reject) => {
    const socket = new net.Socket();
    const timer = setTimeout(() => {
      socket.destroy();
      reject(new Error(`Timed out waiting for ${host}:${port}`));
    }, timeoutMs);

    socket.once("error", (error) => {
      clearTimeout(timer);
      socket.destroy();
      reject(error);
    });

    socket.connect(port, host, () => {
      clearTimeout(timer);
      socket.end();
      resolve();
    });
  });
}

async function waitForDatabasePort(config) {
  const dbTarget = parseDatabaseTarget(config.databaseUrl);
  for (let attempt = 1; attempt <= config.prepareRetries; attempt++) {
    try {
      console.log(
        `Waiting for PostgreSQL on ${dbTarget.host}:${dbTarget.port} (attempt ${attempt}/${config.prepareRetries})...`,
      );
      await waitForPort(dbTarget.host, dbTarget.port, 5_000);
      return;
    } catch (error) {
      if (attempt === config.prepareRetries) {
        throw error;
      }
      await sleep(config.prepareRetryDelayMs);
    }
  }
}

function runCli(args, config) {
  const result = spawnSync(process.execPath, [config.cliPath, ...args], {
    cwd: config.benchmarkDir,
    encoding: "utf-8",
    env: {
      ...process.env,
      DATABASE_URL: config.databaseUrl,
      PRACTOR_SCHEMA_PATH: config.schemaPath,
      PRACTOR_ENGINE_PATH: config.enginePath,
    },
    timeout: 120_000,
  });

  if (result.error) {
    throw result.error;
  }

  if (result.status !== 0) {
    const stderr = (result.stderr || "").trim();
    const stdout = (result.stdout || "").trim();
    throw new Error(stderr || stdout || `CLI exited with code ${result.status}`);
  }

  if (result.stdout) {
    process.stdout.write(result.stdout);
  }
}

async function main() {
  const config = getBenchmarkConfig();
  ensureBenchmarkArtifacts(config);

  assertFileExists(config.cliPath, "Practor CLI build");
  assertFileExists(config.clientPath, "Practor client build");
  assertFileExists(config.enginePath, "Practor engine binary");
  assertFileExists(config.schemaPath, "Benchmark schema");

  await waitForDatabasePort(config);

  if (config.resetSchemaBeforePush) {
    console.log("Resetting benchmark schema...");
    const { PractorClient } = require("../packages/client/dist");
    const client = new PractorClient({
      enginePath: config.enginePath,
      schemaPath: config.schemaPath,
      datasourceUrl: config.databaseUrl,
    });

    await client.$connect();
    try {
      await client.$executeRawUnsafe("DROP SCHEMA IF EXISTS public CASCADE");
      await client.$executeRawUnsafe("CREATE SCHEMA public");
    } finally {
      await client.$disconnect().catch(() => {});
    }
  }

  console.log(`Validating benchmark schema ${path.relative(config.rootDir, config.schemaPath)}...`);
  runCli(["validate"], config);

  let lastError = null;
  for (let attempt = 1; attempt <= config.prepareRetries; attempt++) {
    try {
      console.log(`Applying benchmark schema (attempt ${attempt}/${config.prepareRetries})...`);
      runCli(["db", "push"], config);
      console.log("Benchmark schema is ready.");
      return;
    } catch (error) {
      lastError = error;
      if (attempt === config.prepareRetries) {
        break;
      }
      console.log(`db push failed: ${error.message}`);
      await sleep(config.prepareRetryDelayMs);
    }
  }

  throw lastError;
}

main().catch((error) => {
  console.error(`Benchmark prepare failed: ${error.message}`);
  process.exit(1);
});
