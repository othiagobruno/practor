#!/usr/bin/env node

/**
 * @practor/cli — Practor ORM Command Line Interface
 *
 * Commands:
 *   practor init         — Initialize a new Practor project
 *   practor generate     — Generate the TypeScript client from schema
 *   practor db push      — Push schema changes to the database
 *   practor migrate dev  — Create and apply a migration
 *   practor validate     — Validate the schema file
 *   practor studio       — Open Practor Studio (planned)
 */

import * as fs from "fs";
import * as path from "path";
import { execFileSync, execSync } from "child_process";

// ============================================================================
// CLI Entry
// ============================================================================

const VERSION = "0.4.0";
const SCHEMA_FILE = "schema.practor";

/** Resolves the Go engine binary path. */
function resolveEnginePath(): string {
  const envPath = process.env.PRACTOR_ENGINE_PATH;
  if (envPath && fs.existsSync(envPath)) return envPath;

  const platform = process.platform;
  const arch = process.arch;
  const isWindows = platform === "win32";
  const binaryName = isWindows ? "practor-engine.exe" : "practor-engine";
  const packageName = `@practor/engine-${platform}-${arch}`;

  try {
    const packageDir = path.dirname(
      require.resolve(`${packageName}/package.json`),
    );
    return path.join(packageDir, "bin", binaryName);
  } catch {
    // Package not installed — fall through to local fallbacks
  }

  const paths = [
    path.resolve(process.cwd(), "bin", binaryName),
    path.resolve(process.cwd(), "node_modules", ".practor", binaryName),
    path.resolve(__dirname, "..", "..", "..", "bin", binaryName),
  ];

  for (const p of paths) {
    if (fs.existsSync(p)) return p;
  }

  // Try building the engine
  console.log("⚙️  Building Practor engine...");
  try {
    const engineDir = findEngineDir();
    if (engineDir) {
      const outPath = path.resolve(process.cwd(), "bin", "practor-engine");
      fs.mkdirSync(path.dirname(outPath), { recursive: true });
      execSync(`go build -o ${outPath} ./cmd/practor`, {
        cwd: engineDir,
        stdio: "inherit",
      });
      return outPath;
    }
  } catch (err) {
    // Fall through
  }

  console.error(
    "❌ Practor engine binary not found. Run `npm run build:engine` first.",
  );
  process.exit(1);
}

/** Finds the engine source directory. */
function findEngineDir(): string | null {
  const candidates = [
    path.resolve(process.cwd(), "engine"),
    path.resolve(process.cwd(), "..", "engine"),
  ];
  for (const dir of candidates) {
    if (fs.existsSync(path.join(dir, "go.mod"))) return dir;
  }
  return null;
}

/** Finds the schema file path. */
function findSchemaPath(): string {
  const envPath = process.env.PRACTOR_SCHEMA_PATH;
  if (envPath && fs.existsSync(envPath)) return envPath;

  const candidates = [
    path.resolve(process.cwd(), SCHEMA_FILE),
    path.resolve(process.cwd(), "prisma", SCHEMA_FILE),
    path.resolve(process.cwd(), "schema", SCHEMA_FILE),
  ];

  for (const p of candidates) {
    if (fs.existsSync(p)) return p;
  }

  return path.resolve(process.cwd(), SCHEMA_FILE);
}

// ============================================================================
// Commands
// ============================================================================

/** `practor init` — Initialize a new Practor project. */
function cmdInit(): void {
  console.log("🚀 Initializing Practor project...\n");

  const schemaPath = path.resolve(process.cwd(), SCHEMA_FILE);
  const envPath = path.resolve(process.cwd(), ".env");

  // Create schema file
  if (!fs.existsSync(schemaPath)) {
    const defaultSchema = `// Practor Schema File
// Documentation: https://practor.dev/docs/schema

datasource db {
  provider = "postgresql"
  url      = env("DATABASE_URL")
}

generator client {
  provider = "practor-client"
  output   = "./generated/client"
}

model User {
  id        Int      @id @default(autoincrement())
  email     String   @unique
  name      String?
  createdAt DateTime @default(now())
  updatedAt DateTime @updatedAt
}
`;
    fs.writeFileSync(schemaPath, defaultSchema, "utf-8");
    console.log(`  ✅ Created ${SCHEMA_FILE}`);
  } else {
    console.log(`  ⏭️  ${SCHEMA_FILE} already exists`);
  }

  // Create .env file
  if (!fs.existsSync(envPath)) {
    fs.writeFileSync(
      envPath,
      'DATABASE_URL="postgresql://user:password@localhost:5432/mydb?schema=public"\n',
      "utf-8",
    );
    console.log("  ✅ Created .env");
  } else {
    console.log("  ⏭️  .env already exists");
  }

  // Create .gitignore entries
  const gitignorePath = path.resolve(process.cwd(), ".gitignore");
  const gitignoreEntries = ["generated/", "bin/", "node_modules/", ".env"];
  if (fs.existsSync(gitignorePath)) {
    const existing = fs.readFileSync(gitignorePath, "utf-8");
    const toAdd = gitignoreEntries.filter((e) => !existing.includes(e));
    if (toAdd.length > 0) {
      fs.appendFileSync(
        gitignorePath,
        "\n# Practor\n" + toAdd.join("\n") + "\n",
      );
      console.log("  ✅ Updated .gitignore");
    }
  } else {
    fs.writeFileSync(
      gitignorePath,
      "# Practor\n" + gitignoreEntries.join("\n") + "\n",
      "utf-8",
    );
    console.log("  ✅ Created .gitignore");
  }

  console.log("\n✨ Practor project initialized!\n");
  console.log("Next steps:");
  console.log("  1. Set your DATABASE_URL in .env");
  console.log(`  2. Edit ${SCHEMA_FILE} to define your models`);
  console.log("  3. Run `npx practor generate` to generate the client");
  console.log("  4. Run `npx practor db push` to sync the database\n");
}

/** `practor generate` — Generate the TypeScript client. */
function cmdGenerate(): void {
  console.log("🔄 Generating Practor client...\n");

  const schemaPath = findSchemaPath();
  if (!fs.existsSync(schemaPath)) {
    console.error(`❌ Schema file not found: ${schemaPath}`);
    console.error("   Run `npx practor init` to create one.");
    process.exit(1);
  }

  const enginePath = resolveEnginePath();

  // Parse schema via Go engine
  console.log(`  📖 Parsing ${path.basename(schemaPath)}...`);
  let schemaJSON: string;
  try {
    schemaJSON = execFileSync(enginePath, ["parse", schemaPath], {
      encoding: "utf-8",
      timeout: 10_000,
    });
  } catch (err: any) {
    console.error("❌ Schema parse error:", err.stderr || err.message);
    process.exit(1);
  }

  const schema = JSON.parse(schemaJSON);

  // Determine output directory
  let outputDir = path.resolve(process.cwd(), "generated", "client");
  if (schema.generators?.length > 0 && schema.generators[0].output) {
    outputDir = path.resolve(process.cwd(), schema.generators[0].output);
  }

  // Generate TypeScript client
  console.log(
    `  ⚡ Generating types to ${path.relative(process.cwd(), outputDir)}/`,
  );

  // Dynamic import of generator
  const { generate } = require("@practor/generator");
  generate(schema, outputDir);

  const modelCount = schema.models?.length ?? 0;
  const enumCount = schema.enums?.length ?? 0;

  console.log(
    `\n✅ Generated Practor client with ${modelCount} models and ${enumCount} enums`,
  );
  console.log(
    `   Output: ${path.relative(process.cwd(), outputDir)}/index.ts\n`,
  );
}

/** `practor validate` — Validate the schema file. */
function cmdValidate(): void {
  const schemaPath = findSchemaPath();
  if (!fs.existsSync(schemaPath)) {
    console.error(`❌ Schema file not found: ${schemaPath}`);
    process.exit(1);
  }

  const enginePath = resolveEnginePath();

  try {
    execFileSync(enginePath, ["validate", schemaPath], {
      encoding: "utf-8",
      stdio: "inherit",
      timeout: 10_000,
    });
  } catch {
    process.exit(1);
  }
}

/** `practor db push` — Push schema to database. */
function cmdDbPush(): void {
  console.log("🔄 Pushing schema to database...\n");

  const schemaPath = findSchemaPath();
  if (!fs.existsSync(schemaPath)) {
    console.error(`❌ Schema file not found: ${schemaPath}`);
    process.exit(1);
  }

  // Load .env if available
  loadEnv();

  const enginePath = resolveEnginePath();

  // Use the engine in JSON-RPC mode for db push
  const { spawnSync } = require("child_process");
  const env = { ...process.env, PRACTOR_SCHEMA_PATH: schemaPath };

  // Send a db.push request
  const request =
    JSON.stringify({
      jsonrpc: "2.0",
      id: 1,
      method: "db.push",
      params: { schemaPath, acceptDataLoss: false, forceReset: false },
    }) + "\n";

  const result = spawnSync(enginePath, [], {
    input: request,
    encoding: "utf-8",
    timeout: 30_000,
    env,
  });

  if (result.error) {
    console.error("❌ Engine error:", result.error.message);
    process.exit(1);
  }

  if (typeof result.status === "number" && result.status !== 0) {
    if (result.stderr) {
      console.error(result.stderr.trim());
    }
    process.exit(result.status);
  }

  if (typeof result.status === "number" && result.status !== 0) {
    if (result.stderr) {
      console.error(result.stderr.trim());
    }
    process.exit(result.status);
  }

  // Parse responses (skip the ready message, get the actual response)
  const lines = (result.stdout || "").trim().split("\n").filter(Boolean);
  for (const line of lines) {
    try {
      const response = JSON.parse(line);
      if (response.id === 1) {
        if (response.error) {
          console.error("❌ Push error:", response.error.message);
          process.exit(1);
        }
        console.log(
          "✅",
          response.result?.message || "Schema pushed successfully",
        );
        return;
      }
    } catch {
      // Skip non-JSON lines
    }
  }

  if (result.stderr) {
    console.error(result.stderr.trim());
  }

  console.error("❌ Push error: engine returned no db.push response");
  process.exit(1);
}

/** `practor migrate dev` — Create and apply a migration. */
function cmdMigrateDev(name?: string): void {
  console.log("🔄 Creating migration...\n");

  const schemaPath = findSchemaPath();
  if (!fs.existsSync(schemaPath)) {
    console.error(`❌ Schema file not found: ${schemaPath}`);
    console.error("   Run `npx practor init` to create one.");
    process.exit(1);
  }

  loadEnv();

  const enginePath = resolveEnginePath();
  const migrationsDir = path.resolve(process.cwd(), "migrations");

  const { spawnSync } = require("child_process");
  const env = { ...process.env, PRACTOR_SCHEMA_PATH: schemaPath };

  const request =
    JSON.stringify({
      jsonrpc: "2.0",
      id: 1,
      method: "migrate.dev",
      params: {
        schemaPath,
        migrationsDir,
        name: name || "migration",
      },
    }) + "\n";

  const result = spawnSync(enginePath, [], {
    input: request,
    encoding: "utf-8",
    timeout: 60_000,
    env,
  });

  if (result.error) {
    console.error("❌ Engine error:", result.error.message);
    process.exit(1);
  }

  const lines = (result.stdout || "").trim().split("\n").filter(Boolean);
  for (const line of lines) {
    try {
      const response = JSON.parse(line);
      if (response.id === 1) {
        if (response.error) {
          console.error("❌ Migration error:", response.error.message);
          process.exit(1);
        }
        const r = response.result;
        console.log(`  ✅ ${r?.message || "Migration created and applied"}`);
        if (r?.filePath) {
          console.log(`  📁 File: ${path.relative(process.cwd(), r.filePath)}`);
        }
        if (r?.migrationId) {
          console.log(`  🏷️  ID: ${r.migrationId}\n`);
        }
        return;
      }
    } catch {
      // Skip non-JSON lines
    }
  }

  if (result.stderr) {
    console.error(result.stderr.trim());
  }

  console.error("❌ Migration error: engine returned no migrate.dev response");
  process.exit(1);
}

/** `practor migrate deploy` — Apply pending migrations in production. */
function cmdMigrateDeploy(): void {
  console.log("🚀 Deploying migrations...\n");

  const schemaPath = findSchemaPath();
  loadEnv();

  const enginePath = resolveEnginePath();
  const migrationsDir = path.resolve(process.cwd(), "migrations");

  if (!fs.existsSync(migrationsDir)) {
    console.log("  ℹ️  No migrations directory found. Nothing to deploy.");
    return;
  }

  const { spawnSync } = require("child_process");
  const env = { ...process.env, PRACTOR_SCHEMA_PATH: schemaPath };

  const request =
    JSON.stringify({
      jsonrpc: "2.0",
      id: 1,
      method: "migrate.deploy",
      params: {
        migrationsDir,
      },
    }) + "\n";

  const result = spawnSync(enginePath, [], {
    input: request,
    encoding: "utf-8",
    timeout: 120_000,
    env,
  });

  if (result.error) {
    console.error("❌ Engine error:", result.error.message);
    process.exit(1);
  }

  if (typeof result.status === "number" && result.status !== 0) {
    if (result.stderr) {
      console.error(result.stderr.trim());
    }
    process.exit(result.status);
  }

  const lines = (result.stdout || "").trim().split("\n").filter(Boolean);
  for (const line of lines) {
    try {
      const response = JSON.parse(line);
      if (response.id === 1) {
        if (response.error) {
          console.error("❌ Deploy error:", response.error.message);
          process.exit(1);
        }
        const r = response.result;
        console.log(`  ✅ ${r?.message || "Deploy completed"}`);
        if (r?.applied?.length > 0) {
          console.log(`\n  Applied migrations:`);
          for (const migId of r.applied) {
            console.log(`    • ${migId}`);
          }
        }
        console.log("");
        return;
      }
    } catch {
      // Skip non-JSON lines
    }
  }

  if (result.stderr) {
    console.error(result.stderr.trim());
  }

  console.error("❌ Deploy error: engine returned no migrate.deploy response");
  process.exit(1);
}

// ============================================================================
// Utilities
// ============================================================================

/** Load .env file into process.env. */
function loadEnv(): void {
  const envPath = path.resolve(process.cwd(), ".env");
  if (fs.existsSync(envPath)) {
    const content = fs.readFileSync(envPath, "utf-8");
    for (const line of content.split("\n")) {
      const trimmed = line.trim();
      if (!trimmed || trimmed.startsWith("#")) continue;
      const eqIdx = trimmed.indexOf("=");
      if (eqIdx < 0) continue;
      const key = trimmed.slice(0, eqIdx).trim();
      let value = trimmed.slice(eqIdx + 1).trim();
      // Strip surrounding quotes
      if (
        (value.startsWith('"') && value.endsWith('"')) ||
        (value.startsWith("'") && value.endsWith("'"))
      ) {
        value = value.slice(1, -1);
      }
      process.env[key] = value;
    }
  }
}

/** Print usage help. */
function printHelp(): void {
  console.log(`
Practor ORM v${VERSION}

Usage: practor <command> [options]

Commands:
  init              Initialize a new Practor project
  generate          Generate the TypeScript client from schema
  validate          Validate the schema file
  db push           Push schema changes to the database (no migration files)
  migrate dev       Create and apply a migration (development)
  migrate deploy    Apply pending migrations (production)

Options:
  --help, -h        Show this help
  --version, -v     Show version

Examples:
  npx practor init
  npx practor generate
  npx practor db push
  npx practor migrate dev --name add_users_table
  npx practor migrate deploy
`);
}

// ============================================================================
// Main
// ============================================================================

function main(): void {
  const args = process.argv.slice(2);

  if (args.length === 0 || args.includes("--help") || args.includes("-h")) {
    printHelp();
    process.exit(0);
  }

  if (args.includes("--version") || args.includes("-v")) {
    console.log(`practor v${VERSION}`);
    process.exit(0);
  }

  const command = args[0];
  const subcommand = args[1];

  switch (command) {
    case "init":
      cmdInit();
      break;

    case "generate":
    case "gen":
      cmdGenerate();
      break;

    case "validate":
      cmdValidate();
      break;

    case "db":
      switch (subcommand) {
        case "push":
          cmdDbPush();
          break;
        case "pull":
          console.log("ℹ️  Database introspection is not yet available.");
          break;
        default:
          console.error(`Unknown db command: ${subcommand}`);
          printHelp();
          process.exit(1);
      }
      break;

    case "migrate":
      switch (subcommand) {
        case "dev":
          const nameIdx = args.indexOf("--name");
          const name = nameIdx >= 0 ? args[nameIdx + 1] : undefined;
          cmdMigrateDev(name);
          break;
        case "deploy":
          cmdMigrateDeploy();
          break;
        case "reset":
          console.log("ℹ️  Migrate reset is not yet available.");
          break;
        case "status":
          console.log("ℹ️  Migrate status is not yet available.");
          break;
        default:
          console.error(`Unknown migrate command: ${subcommand}`);
          printHelp();
          process.exit(1);
      }
      break;

    default:
      console.error(`Unknown command: ${command}`);
      printHelp();
      process.exit(1);
  }
}

main();
