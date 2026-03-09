<p align="center">
  <img src="https://img.shields.io/npm/v/@practor/client?style=flat-square&color=00C853&label=client" alt="client version" />
  <img src="https://img.shields.io/npm/v/@practor/cli?style=flat-square&color=00C853&label=cli" alt="cli version" />
  <img src="https://img.shields.io/npm/v/@practor/generator?style=flat-square&color=00C853&label=generator" alt="generator version" />
  <img src="https://img.shields.io/github/license/othiagobruno/practor?style=flat-square" alt="license" />
  <img src="https://img.shields.io/github/stars/othiagobruno/practor?style=flat-square" alt="stars" />
</p>

<h1 align="center">⚡ Practor ORM</h1>

<p align="center">
  <strong>A high-performance, Prisma-compatible ORM for Node.js — powered by a Go query engine.</strong>
</p>

<p align="center">
  <a href="#-quick-start">Quick Start</a> ·
  <a href="#-features">Features</a> ·
  <a href="#-documentation">Docs</a> ·
  <a href="#-contributing">Contributing</a> ·
  <a href="https://github.com/othiagobruno/practor/issues">Issues</a>
</p>

---

## Why Practor?

Practor brings together the **developer experience of Prisma** with the **raw speed of Go**. The query engine runs as a compiled Go binary sidecar, communicating with your Node.js application over a JSON-RPC 2.0 protocol via stdin/stdout — giving you process isolation, type safety, and compiled-language performance in a single package.

```
┌──────────────────┐      JSON-RPC 2.0      ┌──────────────────┐       SQL        ┌──────────────┐
│   Your Node.js   │ ◄──── stdin/stdout ───► │  Go Query Engine │ ◄──────────────► │   Database   │
│   Application    │                         │    (sidecar)     │                  │  (Postgres)  │
└──────────────────┘                         └──────────────────┘                  └──────────────┘
```

---

## ✨ Features

| Feature                      | Description                                                    |
| ---------------------------- | -------------------------------------------------------------- |
| 🚀 **Go Query Engine**       | Compiled binary for maximum throughput and low-latency queries |
| 🔒 **Type-safe Client**      | Full TypeScript generation from your `.practor` schema         |
| 🔄 **Prisma-compatible API** | Familiar `findMany`, `create`, `update`, `delete`, `upsert`    |
| 💳 **Transactions**          | Interactive callbacks and batch transaction support            |
| 📄 **Pagination**            | Offset-based `paginate()` and cursor-based `cursorPaginate()`  |
| 🔗 **Connection Pooling**    | Configurable pool with runtime stats via `$pool()`             |
| 📡 **JSON-RPC IPC**          | Clean process isolation via stdin/stdout communication         |
| 🛠 **CLI Tooling**           | Schema validation, code generation, DB push, and migrations    |
| 🧩 **PSL Schema**            | Prisma Schema Language compatible syntax                       |

---

## 📦 Packages

Practor is organized as a monorepo with three scoped packages:

| Package                                      | Description                                               | npm                                                                                                                           |
| -------------------------------------------- | --------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------- |
| [`@practor/client`](./packages/client)       | Runtime client — engine process manager + Prisma-like API | [![npm](https://img.shields.io/npm/v/@practor/client?style=flat-square)](https://www.npmjs.com/package/@practor/client)       |
| [`@practor/generator`](./packages/generator) | TypeScript client code generator from `.practor` schemas  | [![npm](https://img.shields.io/npm/v/@practor/generator?style=flat-square)](https://www.npmjs.com/package/@practor/generator) |
| [`@practor/cli`](./packages/cli)             | CLI for init, generate, validate, db push, and migrations | [![npm](https://img.shields.io/npm/v/@practor/cli?style=flat-square)](https://www.npmjs.com/package/@practor/cli)             |

---

## 🚀 Quick Start

### Prerequisites

- **Node.js** >= 18.0.0
- **Go** >= 1.21 (for building the engine)
- **PostgreSQL** (or compatible database)

### 1. Install the CLI

```bash
npm install -g @practor/cli
# or use npx directly
npx @practor/cli init
```

### 2. Initialize your project

```bash
practor init
```

This creates a `schema.practor` file in your project root.

### 3. Define your schema

Edit `schema.practor` using Prisma Schema Language (PSL):

```prisma
// schema.practor

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
  role      Role     @default(USER)
  posts     Post[]
  createdAt DateTime @default(now())
  updatedAt DateTime @updatedAt
}

model Post {
  id        Int      @id @default(autoincrement())
  title     String
  content   String?
  published Boolean  @default(false)
  authorId  Int
  author    User     @relation(fields: [authorId], references: [id])
  createdAt DateTime @default(now())
}

enum Role {
  USER
  ADMIN
}
```

### 4. Generate the TypeScript client

```bash
npx practor generate
```

### 5. Push schema to database

```bash
npx practor db push
```

### 6. Use the client

```typescript
import { PractorClient } from "@practor/client";

const practor = new PractorClient({
  datasourceUrl: process.env.DATABASE_URL,
});

await practor.$connect();

// Create
const user = await practor.user.create({
  data: { email: "alice@practor.dev", name: "Alice" },
});

// Find many with filtering & sorting
const users = await practor.user.findMany({
  where: { role: "USER" },
  orderBy: { createdAt: "desc" },
  take: 10,
});

// Update
await practor.user.update({
  where: { id: 1 },
  data: { name: "Alice Updated" },
});

// Delete
await practor.user.delete({ where: { id: 1 } });

await practor.$disconnect();
```

---

## 📖 Documentation

### CRUD Operations

```typescript
// Find unique
const user = await practor.user.findUnique({ where: { id: 1 } });

// Find first
const first = await practor.user.findFirst({ where: { name: "Alice" } });

// Upsert
await practor.user.upsert({
  where: { id: 1 },
  update: { name: "Alice Updated" },
  create: { id: 1, email: "alice@practor.dev", name: "Alice" },
});
```

### Transactions

Practor supports both **interactive callbacks** and **batch arrays**.

#### Interactive Transactions

```typescript
await practor.$transaction(async (tx) => {
  const account = await tx.account.update({
    where: { id: 1 },
    data: { balance: { decrement: 100 } },
  });

  await tx.transfer.create({
    data: { amount: 100, accountId: account.id },
  });
});
```

#### Batch Transactions

```typescript
const [user, post] = await practor.$transaction([
  practor.user.create({ data: { email: "alice@practor.dev" } }),
  practor.post.create({ data: { title: "Hello World", authorId: 1 } }),
]);
```

### Pagination

```typescript
const result = await practor.user.paginate({
  where: { active: true },
  page: 1,
  limit: 10,
});

// result.data     → User[]
// result.page     → number
// result.limit    → number
// result.total    → number
// result.has_next → boolean
```

### Cursor-Based Pagination

For large datasets and real-time feeds, use `cursorPaginate()` — it avoids the skip-scan problem and stays consistent under concurrent writes.

```typescript
// First page (no cursor needed)
const page1 = await practor.user.cursorPaginate({
  take: 20,
  orderBy: { id: "asc" },
  where: { active: true },
});

// Next page — pass the cursor from the previous result
const page2 = await practor.user.cursorPaginate({
  cursor: { id: page1.nextCursor },
  take: 20,
  orderBy: { id: "asc" },
  where: { active: true },
});

// page2.data        → User[]
// page2.nextCursor  → value | null (null = last page)
// page2.hasNextPage → boolean
```

> **How it works:** The engine uses `WHERE id > $cursor LIMIT take+1` — the extra row detects `hasNextPage` without a separate `COUNT(*)` query.

### Raw SQL

```typescript
import { PractorClient, sql } from "@practor/client";

// Query raw (returns rows)
const users = await practor.$queryRaw`SELECT * FROM "user" WHERE id = ${1}`;

// Execute raw (returns affected row count)
const count =
  await practor.$executeRaw`DELETE FROM "user" WHERE active = ${false}`;

// Reusable safe SQL fragments
const onlyActive = sql`WHERE active = ${true}`;
const activeUsers = await practor.$queryRaw(
  sql`SELECT * FROM "user" ${onlyActive} ORDER BY id DESC`,
);

// Explicitly unsafe string SQL
const rows = await practor.$queryRawUnsafe(
  'SELECT * FROM "user" WHERE email = $1',
  userEmail,
);
```

### Middleware / Hooks (`$use`)

Practor supports Prisma-compatible middleware that intercepts all model operations. Middleware runs in FIFO order and can inspect/mutate both params and results.

```typescript
// Logging middleware
practor.$use(async (params, next) => {
  const start = Date.now();
  console.log(`Query: ${params.model}.${params.action}`);
  const result = await next(params);
  console.log(`Completed in ${Date.now() - start}ms`);
  return result;
});
```

```typescript
// Soft-delete middleware
practor.$use(async (params, next) => {
  if (params.action === "delete") {
    params.action = "update";
    params.args = { ...params.args, data: { deletedAt: new Date() } };
  }
  if (params.action === "findMany" || params.action === "findFirst") {
    params.args.where = { ...params.args.where, deletedAt: null };
  }
  return next(params);
});
```

```typescript
// Access control middleware
practor.$use(async (params, next) => {
  if (params.model === "Post" && params.action === "delete") {
    throw new Error("Deleting posts is not allowed");
  }
  return next(params);
});
```

> Middleware also runs inside `$transaction` — both interactive and batch modes.

### Relation Queries (`include` & `select`)

Eager-load related models with `include` or pick specific fields with `select`. The Go engine uses batched `WHERE IN` queries to avoid N+1 performance issues.

#### Include — Eager Loading

```typescript
// Simple: load all related posts and profile
const users = await practor.user.findMany({
  include: { posts: true, profile: true },
});
// users[0].posts → Post[]
// users[0].profile → Profile | null
```

```typescript
// Nested: filter, sort, and limit included relations
const users = await practor.user.findMany({
  include: {
    posts: {
      where: { published: true },
      orderBy: { createdAt: "desc" },
      take: 5,
    },
  },
});
```

```typescript
// Deep nesting: include relations of relations
const users = await practor.user.findMany({
  include: {
    posts: {
      include: { categories: true },
    },
  },
});
```

#### Select — Field Picking with Relations

```typescript
// Pick specific scalar fields + load relations
const users = await practor.user.findMany({
  select: {
    name: true,
    email: true,
    posts: { select: { title: true, published: true } },
  },
});
// users[0] → { name, email, posts: [{ title, published }] }
```

> **Note:** `select` and `include` are mutually exclusive at the same level — use one or the other.

> **How it works:** After the main query, the engine collects all parent IDs and runs a single `SELECT ... WHERE fk IN ($1, $2, ...)` per relation, then groups and attaches results. Nested includes are resolved recursively.

### Connection Pooling

Practor uses a configurable connection pool managed by the Go engine. Customize pool behavior via client options or environment variables.

```typescript
const practor = new PractorClient({
  datasourceUrl: process.env.DATABASE_URL,
  pool: {
    maxOpenConns: 50, // Max open connections (default: 20)
    maxIdleConns: 10, // Max idle connections (default: 5)
    connMaxLifetimeMs: 600_000, // Max connection lifetime (default: 5 min)
    connMaxIdleTimeMs: 120_000, // Max idle time per connection (default: 1 min)
  },
});
```

Alternatively, configure via environment variables:

```bash
PRACTOR_POOL_MAX_OPEN_CONNS=50
PRACTOR_POOL_MAX_IDLE_CONNS=10
PRACTOR_POOL_CONN_MAX_LIFETIME_MS=600000
PRACTOR_POOL_CONN_MAX_IDLE_TIME_MS=120000
```

#### Runtime Observability

Monitor pool health in real time with `$pool()`:

```typescript
const stats = await practor.$pool();
console.log(stats);
// {
//   maxOpenConnections: 50,
//   openConnections: 12,
//   inUse: 8,
//   idle: 4,
//   waitCount: 0,
//   waitDurationMs: 0,
//   maxIdleClosed: 3,
//   maxIdleTimeClosed: 1,
//   maxLifetimeClosed: 0
// }
```

### CLI Commands

| Command                  | Description                                            |
| ------------------------ | ------------------------------------------------------ |
| `practor init`           | Initialize a new Practor project with a starter schema |
| `practor generate`       | Generate the TypeScript client from `schema.practor`   |
| `practor validate`       | Validate the schema file for syntax errors             |
| `practor db push`        | Push schema changes directly to the database           |
| `practor migrate dev`    | Create and apply a new migration (development)         |
| `practor migrate deploy` | Apply pending migrations in order (production/CI)      |

---

## 🧪 Benchmarking

Practor includes a reproducible PostgreSQL benchmark harness in [`benchmarks/`](./benchmarks).

### Start the benchmark database

```bash
npm run docker:test:up
```

This starts PostgreSQL 16 on `127.0.0.1:54329` with an isolated `practor_benchmark` database.

### Build, prepare, and run

```bash
npm run benchmark:build
npm run benchmark:run
```

`benchmark:run` prepares the benchmark schema automatically before it seeds data and starts measuring.
By default this resets the `public` schema in the dedicated benchmark database, so reruns do not inherit stale table definitions.
It also rebuilds the local packages and Go engine by default, so running `node ./benchmarks/run.js` does not depend on stale artifacts.

Or run the full flow in one command:

```bash
npm run benchmark:fresh
```

### Included benchmark cases

- primary-key lookups
- filtered `findMany`
- relation loading with `include`
- offset pagination
- cursor pagination
- count queries
- safe raw SQL
- inserts, updates, and deletes
- interactive transactions
- batch transactions

Each run seeds a dedicated fixture set and writes machine-readable results to `benchmarks/results/latest.json`.

---

## 🏗 Architecture

```

practor/
├── engine/ # Go Query Engine
│ ├── cmd/practor/main.go # Engine entrypoint & JSON-RPC server
│ └── internal/
│ ├── schema/ # PSL Parser
│ ├── query/ # Query execution
│ ├── connector/ # Database connectors
│ ├── migration/ # Migration engine
│ └── protocol/ # JSON-RPC communication bridge
├── packages/
│ ├── client/ # @practor/client — Runtime + API
│ ├── generator/ # @practor/generator — Code generation
│ └── cli/ # @practor/cli — CLI tooling
├── bin/ # Compiled engine binary output
├── schema.practor # Example schema
├── tsconfig.base.json # Shared TypeScript config
└── package.json # Monorepo root (npm workspaces)

```

---

## 🔧 Running Locally (Development)

### 1. Clone the repository

```bash
git clone https://github.com/othiagobruno/practor.git
cd practor
```

### 2. Install Node.js dependencies

```bash
npm install
```

### 3. Build the Go engine

```bash
npm run build:engine
```

> This compiles the engine binary to `bin/practor-engine`.

### 4. Build all TypeScript packages

```bash
npm run build
```

### 5. Set up your database

Create a `.env` file at the root:

```env
DATABASE_URL="postgresql://user:password@localhost:5432/practor_dev?sslmode=disable"
```

### 6. Run the CLI locally

```bash
# Use the local CLI package
node packages/cli/dist/index.js generate
```

---

## 🤝 Contributing

Contributions are welcome! Whether it's a bug fix, feature, or documentation improvement — every contribution makes Practor better.

### Getting Started

1. **Fork** the repository on GitHub
2. **Clone** your fork locally:
   ```bash
   git clone https://github.com/<your-username>/practor.git
   cd practor
   ```
3. **Install dependencies**:
   ```bash
   npm install
   ```
4. **Create a branch** for your work:
   ```bash
   git checkout -b feat/my-new-feature
   ```
5. **Make your changes** and commit with [Conventional Commits](https://www.conventionalcommits.org/):
   ```bash
   git commit -m "feat: add cursor-based pagination support"
   ```
6. **Push** your branch and open a **Pull Request**

### Branch Naming Convention

| Prefix      | Use Case                 |
| ----------- | ------------------------ |
| `feat/`     | New feature              |
| `fix/`      | Bug fix                  |
| `docs/`     | Documentation changes    |
| `refactor/` | Code refactoring         |
| `test/`     | Adding or updating tests |
| `chore/`    | Maintenance tasks        |

### Commit Message Format

We follow [Conventional Commits](https://www.conventionalcommits.org/):

```
<type>(<scope>): <short description>

[optional body]
[optional footer]
```

**Examples:**

```
feat(client): add cursor-based pagination method
fix(engine): resolve connection pool leak on disconnect
docs(readme): add contributing guidelines
test(generator): add unit tests for enum generation
```

---

## 🐛 Reporting Issues

Found a bug or have a feature request? [Open an issue](https://github.com/othiagobruno/practor/issues/new) on GitHub.

### Bug Report Template

When reporting a bug, please include:

1. **Description** — A clear and concise description of the bug
2. **Steps to Reproduce** — Minimal steps to reproduce the behavior
3. **Expected Behavior** — What you expected to happen
4. **Actual Behavior** — What actually happened
5. **Environment** — OS, Node.js version, Go version, database
6. **Schema** — Relevant portions of your `schema.practor` (if applicable)
7. **Logs** — Any error messages or stack traces

### Feature Request Template

1. **Summary** — Describe the feature you'd like
2. **Motivation** — Why is this feature important?
3. **Proposed API** — How would the API look from a user perspective?
4. **Alternatives** — Any alternative approaches you've considered

---

## 📬 Creating a Pull Request

### PR Checklist

Before submitting a PR, make sure:

- [ ] Code compiles — `npm run build` passes
- [ ] Go engine compiles — `npm run build:engine` passes
- [ ] Tests pass — `npm run test` (when applicable)
- [ ] Commit messages follow [Conventional Commits](https://www.conventionalcommits.org/)
- [ ] Documentation is updated (if the change affects public API)
- [ ] No hardcoded credentials or secrets

### PR Process

1. Open a PR against the `main` branch
2. Fill in the PR description with:
   - **What** — What does this PR do?
   - **Why** — Why is this change needed?
   - **How** — How was it implemented?
   - **Testing** — How was it tested?
3. Request a review from a maintainer
4. Address any review feedback
5. Once approved, a maintainer will merge your PR

### Code Style

- **TypeScript**: Strict mode, ES2022 target, functional style preferred
- **Go**: Standard `gofmt` formatting, idiomatic Go patterns
- **Comments**: Explain the _why_, not the _what_
- **Exported functions**: Must include JSDoc/TSDoc documentation

---

## 🗺 Roadmap

- [ ] MySQL and SQLite connector support
- [x] Cursor-based pagination
- [x] Relation queries (`include` and `select`)
- [x] Middleware / hooks (`$use`)
- [x] Connection pooling
- [x] `migrate deploy` for production
- [ ] Plugin system for custom generators
- [ ] Dashboard UI for schema visualization

---

## 📄 License

This project is licensed under the **MIT License** — see the [LICENSE](./LICENSE) file for details.

---

<p align="center">
  Built with 💚 and Go-powered performance.<br/>
  <a href="https://github.com/othiagobruno/practor">github.com/othiagobruno/practor</a>
</p>
