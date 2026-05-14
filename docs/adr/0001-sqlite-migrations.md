# ADR 0001 — SQLite schema migrations via golang-migrate

Status: accepted (2026-05-14)

## Context

Two domains persist to a shared SQLite file: `internal/inventory` (the
`inventories` table) and `internal/jobs` (the `jobs` table + a FK
cascade to inventories). Both packages run `CREATE TABLE IF NOT EXISTS`
inline at `NewStore` time. That's enough for greenfield deploys but
leaves no room for schema evolution: an `ALTER TABLE` would have to be
hand-applied per environment, with no tracking of which DBs are caught
up.

We want a migration mechanism that:

- ships with the binary (no external `migrate` CLI required to deploy)
- runs before any store constructor touches its tables
- tracks applied versions in the same database
- supports forward + emergency-reverse migrations
- works with the project's existing pure-Go SQLite driver
  (`modernc.org/sqlite`) so we keep CGO-free builds

## Decision

Adopt `github.com/golang-migrate/migrate/v4`:

- **Source driver:** `iofs`, fed an `embed.FS` rooted at
  `internal/migrate/migrations`. Migrations are part of the binary.
- **Database driver:** `database/sqlite` (the modernc-backed driver),
  not `sqlite3` (which is CGO-only via `mattn/go-sqlite3`).
- **Migration files:** classic `NNNN_<slug>.up.sql` and
  `NNNN_<slug>.down.sql` pairs, four-digit zero-padded version numbers.
- **Versioning table:** the default `schema_migrations` table, owned
  by golang-migrate.
- **One migration set across both domains** — same DB, single sequence.
  The opening migrations are essentially the existing DDL split into
  per-table files (`0001_inventories`, `0002_jobs`).
- **Apply at startup**, before either `NewStore` is called. `Bootstrap`
  runs migrations after `OpenStateDB` succeeds and before constructing
  the inventory or jobs stores. Migration failure aborts startup.
- **Down policy:** down files exist so tests and emergency rollbacks
  can call `Down(n)`. The binary never runs `Down` automatically — only
  `Up`. Operators who need to revert do it explicitly.

## Consequences

Positive:

- Schema changes become reviewable diffs against `.sql` files instead
  of inline string literals split across packages.
- Test setup can spin up a fresh DB and `Up()` once instead of
  duplicating DDL.
- The schema version is observable: `SELECT version FROM schema_migrations`.

Trade-offs:

- One more transitive dependency tree. golang-migrate is widely used
  (~15k stars, maintained) so the risk is acceptable.
- Migrations are a single sequence — coordinating two domains in the
  same DB means version numbers interleave. Acceptable while there are
  only two domains; if a third lands, we revisit.
- Backwards-incompatible schema changes still need careful design;
  the migration tool only enforces ordering, not safety.

## Alternatives considered

- **Keep `CREATE TABLE IF NOT EXISTS` and add `ALTER TABLE` as needed.**
  Works for one change; falls apart on the second when half the dev
  databases are on the old schema. Rejected.
- **goose** (`github.com/pressly/goose`). Similar feature set, slightly
  smaller. Rejected because golang-migrate has cleaner integration with
  `embed.FS` via `iofs` and a wider track record across CI tooling.
- **Hand-rolled migrate runner** over `embed.FS`. Tempting (only a few
  hundred lines) but reinvents the wheel and skips the corner cases
  golang-migrate has already absorbed (dirty migrations, version
  detection, atomic application within a single statement).

## Rollout

1. Add the dependency and `internal/migrate` package.
2. Move the existing DDL into `0001_inventories.up.sql` and
   `0002_jobs.up.sql`. Write the matching `down.sql` files.
3. Wire `migrate.Apply(db)` into `Bootstrap` ahead of store construction.
4. Drop the inline `CREATE TABLE` from `internal/inventory/store.go`
   and `internal/jobs/store.go` — schema is now the migrations'
   responsibility.
5. Update tests to call `migrate.Apply` (or use a shared helper) when
   they open ad-hoc in-memory DBs.

## See also

- golang-migrate documentation: <https://github.com/golang-migrate/migrate>
- modernc.org/sqlite driver: <https://pkg.go.dev/modernc.org/sqlite>
- iofs source: <https://pkg.go.dev/github.com/golang-migrate/migrate/v4/source/iofs>
