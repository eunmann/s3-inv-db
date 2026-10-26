---
name: repo-research
description: "Pre-implementation codebase research sized to the change: locate what it touches, read that, synthesize, plan. Use for new features, refactors, cross-cutting bug fixes and issue writing."
---

# Repo Research

Understand the blast radius of a change before making it, reading only what the change reaches. Skip this for typo fixes, one-line changes and mechanical edits; the calling command decides.

Every turn re-reads your whole context, so cost grows with what you load. Locate before you read, read in parallel batches, and delegate a subsystem only when its files would otherwise fill your context.

## 1. Orient

Read the parts of `docs/` that cover the task, not all of them (`how-it-works.md`, `index-format.md`, `operations.md`, `configuration.md`, `cli.md`, `http-api.md`). The package map:

- **Build pipeline:** `pkg/s3fetch` (manifest + object download), `pkg/s3inventory` (CSV/Parquet row readers), `pkg/extsort` (aggregation, spill runs, k-way merge, `IndexBuilder`, `Pipeline.Run`), `pkg/extsort/events` (progress bus), `pkg/sysmem` (memory limit), `pkg/tiers` (storage classes)
- **On-disk format:** `pkg/format` — writers used by the builder and mmap readers: core stats, depth index, MPHF + fingerprints, prefix dictionary, tier stats, manifest; `Version` in `format.go`
- **Read path:** `pkg/indexread` (the public library: `Lookup`, `Stats`, `DescendantsAtDepth`, `Top*`, `TierBreakdown`), `pkg/pricing`, `pkg/humanfmt`
- **Server:** `internal/server` (routes, middleware, bootstrap), `internal/inventory` (catalog of loaded indexes, discovery snapshot, browse/compare domain logic, SQLite store), `internal/jobs` (scheduler, job store, progress recorder), `internal/autoload` + `internal/budget` + `internal/loadcontrol` (auto-load, retention, disk cap), `internal/s3disco` (S3 inventory discovery), `internal/loader` (S3 → index build for the server), `internal/handlers` (HTTP/HTMX/JSON + SSE), `internal/templates` (html/template + Tailwind), `internal/migrate` (SQLite migrations), `internal/metrics`
- **Binaries and config:** `cmd/s3-inv-db` → `internal/cli`, `cmd/s3-inv-db-server`, `cmd/s3-inv-db-seeder` → `internal/seeder`; `internal/appconfig` (JSON config shared by CLI and server)
- **Tests and tooling:** unit tests beside each package; `*_s3_test.go` behind the `integration` tag with `internal/s3test` (local S3 via compose or CI); `internal/benchutil` (seeded synthetic data); `bench/` + `scripts/bench-ab.sh` (A/B gate: `bench/suite/ingest.txt`, `bench/suite/query.txt`)

## 2. Locate

Grep for the symbols, flags, config keys, JSON fields, SQL columns, file names and routes the task touches, and list each affected layer with one line on why. A change usually crosses a seam — index writer ↔ reader (`pkg/extsort`/`pkg/format` builders ↔ `pkg/format` readers/`pkg/indexread`), catalog ↔ handlers ↔ templates, Go structs ↔ `internal/migrate/migrations/*.sql`, `internal/appconfig` ↔ `cmd/*` flags ↔ `docs/` — so find both sides.

Check, without reading them in full, whether the change needs: a format `Version` bump (any on-disk layout change), a new migration (never edit an applied one), a `make bench-ingest` / `make bench-query` run or a new suite entry (ingestion or query code only), an `integration`-tagged test (S3 behaviour), `make css` (new Tailwind classes in templates), or a docs update (`docs/*.md`; config keys and flags live in `docs/configuration.md`).

## 3. Read

Read the affected code directly: the functions the change touches, their callers and their tests. Batch independent reads and greps into one turn. When the question is what the index or a response looks like, build a small index with the seeder or a test helper and inspect it rather than reasoning from the generator.

Spawn an `Explore` subagent only for a subsystem whose files you would otherwise load just to answer one question: at most three, in parallel, each with that question and the instruction to return conclusions with `file:line`, not file contents.

## 4. Synthesize

- What does the code do today, as evidenced by `file:line`, against what the task assumes?
- Which existing pattern should the change follow, and where do features diverge?
- Is the stated approach right? Present a simpler or more robust alternative with its tradeoffs when there is one. Prefer the clean replacement over a compatible patch; keep the public `pkg/indexread` API stable unless the task says otherwise.

## 5. Plan

List the commits in dependency order (format writer before reader, migration before the code that uses it, domain logic before handlers and templates), each with its files, change and tests. Mark which are independent and which need `make bench-ingest` / `make bench-query`.

For research-only commands (`/create-issue`), stop here.

## 6. Implement

Implement directly, or hand independent tasks to `implementer` subagents in parallel, each with the files, the behaviour, the tests and the commit split. Their output is verified by the gate and `/review-loop`, not by re-reading every diff.
