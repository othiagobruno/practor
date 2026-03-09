# Benchmarks

This directory contains a reproducible PostgreSQL benchmark environment for Practor.

## What it provisions

- `docker-compose.yml`: isolated PostgreSQL 16 instance on `127.0.0.1:54329`
- `schema.practor`: benchmark-only schema used for setup and runtime
- `prepare.js`: validates the schema and applies it with `practor db push`
- `run.js`: seeds the database and runs a multi-case benchmark suite

## Default database URL

```bash
postgresql://practor:practor@127.0.0.1:54329/practor_benchmark?sslmode=disable
```

Override it with `DATABASE_URL` if needed.

## Scripts

From the repository root:

```bash
npm run docker:test:up
npm run benchmark:build
npm run benchmark:run
```

`benchmark:run` now prepares the benchmark schema automatically before seeding and measuring.
By default, prepare drops and recreates the `public` schema in the benchmark database so old tables do not leak stale DDL into new runs.
It also rebuilds the TypeScript packages and engine binary automatically unless you set `PRACTOR_BENCHMARK_AUTO_BUILD=false`.

Or run the full fresh flow:

```bash
npm run benchmark:fresh
```

## Benchmarked operations

- `findUnique:userById`
- `findMany:publishedPosts`
- `include:postsWithAuthor`
- `paginate:users`
- `cursorPaginate:posts`
- `count:publishedPosts`
- `rawQuery:safe`
- `update:user`
- `create:user`
- `delete:user`
- `transaction:interactive`
- `transaction:batch`

## Tunable environment variables

```bash
PRACTOR_BENCHMARK_ITERATIONS=40
PRACTOR_BENCHMARK_CONCURRENCY=8
PRACTOR_BENCHMARK_WARMUP=10
PRACTOR_BENCHMARK_SEED_USERS=1000
PRACTOR_BENCHMARK_POSTS_PER_USER=5
PRACTOR_BENCHMARK_BATCH_SIZE=250
PRACTOR_BENCHMARK_PAGE_SIZE=25
PRACTOR_BENCHMARK_RESET_SCHEMA=false
PRACTOR_BENCHMARK_AUTO_BUILD=false
```

Results are written to `benchmarks/results/latest.json`.
