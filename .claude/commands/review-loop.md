---
description: Iterative review-fix loop that finds and fixes bugs, gaps, and quality issues until the branch is clean. Use after implementing a feature, before creating a PR, or whenever you want to polish work to completion. This is the primary review command — use it instead of manually reviewing code.
---

# Review Loop

> **Mindset:** Be skeptical, challenge assumptions, and use first principles. Do not stop at the first match. Do not shortcut. Verify behavior by reading code — names are claims, not proof.

Repeatedly review, fix, and verify until the branch is clean. Stop when zero issues remain.

## Current State

- **Branch:** `$!git branch --show-current`
- **Commits since main:**
```
$!git log --oneline main..HEAD 2>/dev/null || echo "(on main — reviewing working tree)"
```

## What this repo uses (and you must respect)

The first thing every review pass does is reconcile the branch with the project's actual tooling and conventions. Don't propose patterns that fight these — and don't reinvent what's already wired up.

### Build / dev tools

- **Go 1.26+**, single module under `github.com/eunmann/s3-inv-db`.
- **Linter:** `golangci-lint v2` via `make lint`. Settings live in `.golangci.yml` — read it before flagging style.
- **Hot reload:** `Air` (`.air.toml`). Air watches `go`, `html`, `tmpl` and rebuilds the server binary. **There is no in-process devMode template reload** — if you see one, it's redundant with Air and should be removed.
- **Local dev stack:** `docker compose -f infra/docker-compose.yml` with profiles `dev` / `prod` / `seed`. Make targets: `make dev`, `make docker-prod`, `make docker-seed`, `make docker-down`.
- **Three binaries**, all named after the project:
  - `cmd/s3-inv-db` — CLI (`build` / `query`)
  - `cmd/s3-inv-db-server` — HTTP server
  - `cmd/s3-inv-db-seeder` — synthetic-data generator
  - Mismatch (e.g. binary named `s3inv-foo` while project is `s3-inv-db`) is a bug.

### Libraries the repo already uses — use them, don't reinvent

- **chi v5** (`github.com/go-chi/chi/v5`) for routing, plus its built-in middleware: `RequestID`, `RealIP`, `Recoverer`, `WrapResponseWriter`, etc. If you write a custom `responseWriter` wrapper or a custom request-ID middleware, you've reinvented chi.
- **zerolog** for logging, with **rs/zerolog/hlog** for HTTP integration. The idioms are:
  - `logger.WithContext(ctx)` to put a logger into ctx
  - `zerolog.Ctx(ctx)` to retrieve it
  - `hlog.NewHandler(logger)` to wire a base logger into the HTTP request ctx
  - `hlog.FromRequest(r)` inside handlers
  - `hlog.AccessHandler` for access logs
  - **If you see a custom `logctx` / `WithLogger` / `FromContext` package, delete it** — it's reinventing zerolog's own ctx integration.
- **html/template** with `embed.FS` for templates. Air handles hot-reload via rebuilds; no need for a dev-mode disk-reload path.
- **HTMX** for SSR partials. Action buttons (`hx-post` / `hx-delete`) should target `closest tr` and `hx-swap="outerHTML"`. Empty body = row removed. No `fetch()` + `window.location.reload()` patterns — those are pre-HTMX anti-patterns.
- **Tailwind CSS** via CDN (acceptable for an internal tool).
- **MinIO** (via docker compose) for S3-backed integration tests. Tests under `internal/s3disco`, `internal/seeder`, and `pkg/s3fetch` `t.Skip` when `AWS_ENDPOINT_URL_S3` is unset. **Plain `go test ./...` skips them.** If you're auditing S3-related code, either run the dev stack or call out that the path is unexercised.

### Architecture contract: HTTP → Domain → Storage

| Layer | Owns | Files |
|---|---|---|
| HTTP | Parse requests, render JSON/HTML, content negotiation | `internal/server`, `internal/handlers`, `internal/templates` |
| Domain | Use cases, in-memory state machine, orchestration | `internal/inventory`, `internal/s3disco`, `internal/loader` |
| Storage / Index | mmap-backed on-disk format, build pipeline, S3 fetch | `pkg/indexread`, `pkg/format`, `pkg/extsort`, `pkg/triebuild`, `pkg/s3fetch` |

Layer leaks are real bugs: handler code that builds breadcrumbs/sorts/paginates is domain logic in the wrong place; domain code that imports `net/http` is upside-down.

## Pass 0: Deep Research

Before line-by-line review, orient to the full blast radius of the branch's changes.

1. List the changed files and packages: `git diff --name-only $(git merge-base main HEAD)..HEAD` and group by directory.
2. Apply the `deep-research` skill, steps 1-4, scoped to those packages:
   - Broad repo scan of packages that import or consume the changed ones.
   - Feature inventory covering every affected subsystem. If the index pipeline (`extsort`/`triebuild`/`indexread`) changed, check the others. If a handler changed, check the template it renders and the manager method it calls.
   - Parallel sub-agent deep-dives — one per affected feature — looking for: invariant violations, consumer breakage, missing tests, on-disk-format drift, and assumptions that differ from current behavior.
   - Synthesis — produce a list of integration risks that line-by-line review would miss.

Scale with diff size: a one-file change skips Pass 0 and goes straight to Pass 1. A cross-package refactor does the full Pass 0.

## The Loop

### Pass N (start at 1, after Pass 0)

#### 1. Lint

Run `make lint`. If it auto-fixed files, stage and commit with message `style: apply lint auto-fixes`.

The linter already catches function length, magic numbers, formatting, method ordering, and doc comment issues. Trust it for those — focus your review on what the linter cannot catch.

#### 2. Review

Identify changed files: `git diff --name-only $(git merge-base main HEAD)..HEAD`

Read every changed file. The list below is a comprehensive checklist — work through it. Items 1–6 are the standard correctness review; items 7–14 catch the meta-level mistakes that line-by-line review misses.

1. **Logic bugs:** off-by-one, nil dereference, race conditions, incorrect return values, wrong comparison operators.
2. **Missing tests:** new or changed behavior without a corresponding test. Both positive and negative paths. Empty-string / boundary inputs. **A new package with zero `_test.go` files is a finding.**
3. **Error semantics:** errors swallowed silently, wrapped without context (`%w`), or wrong sentinel errors used. Generic 500s that should be typed.
4. **API design:** interfaces defined at producer instead of consumer, parameter order inviting swaps, leaking internal types. HTML-partial routes returning JSON.
5. **Concurrency:** goroutines without shutdown paths, shared state without synchronization, unbounded channels. Lock dropped around I/O — what happens if state changes during the unlocked window?
6. **On-disk format:** changes to one of `extsort`/`triebuild`/`indexread` that aren't mirrored in the others.

7. **Reinvented stdlib / library features.** Before accepting a custom wrapper, search for the upstream equivalent:
   - Logging context plumbing? → `zerolog.Ctx` + `logger.WithContext` + `rs/zerolog/hlog` already do this. Custom `logctx` is a smell.
   - HTTP middleware for request IDs, access logs, response status, recovery? → chi/v5 + hlog supply these. Hand-rolled `responseWriter` wrappers are a smell.
   - Sets, ordered maps, common file walks? → Check stdlib (`maps`, `slices`, `io/fs`) before writing a helper.
   - Template hot-reload? → Air watches HTML; the in-process reload path is redundant.
   - **Rule: if you find a custom thing, ask "is there an upstream that does this?" before accepting it. Delete custom reinventions unless they add a real capability.**

8. **Dev-tooling alignment.** Does the change account for `.air.toml`, `Makefile`, `infra/docker-compose.yml`, `.golangci.yml`? Adding a new directory that Air doesn't watch is a real problem. Adding a binary without a Makefile target is too.

9. **Naming consistency with the project.** The project is `s3-inv-db`. Binaries, packages, Docker images, READMEs should match that base. A binary named `myapp-foo` in a `myapp-bar` project is a finding.

10. **Documentation sync.** Does README mention old binary names? Old API patterns? Old architecture? Run `grep` for renamed symbols / files. If the README hasn't been touched in N passes of refactoring, it's almost certainly stale.

11. **3-layer leaks.** Apply the table above:
   - Handlers calling `idx.Lookup` etc directly = domain inside HTTP. Acceptable inside a `manager.WithIndex` closure if scoped to one request.
   - Domain importing `net/http` = inverted dependency.
   - HTTP response types living in domain pkg, or domain types with JSON tags they don't need.

12. **Test reality vs claims.** When a test "covers" an integration (S3, MinIO, real index), confirm it actually runs in `make test`. `t.Skip` guards on env vars are a giant asterisk — call them out in the report and document how to run them in README.

13. **Bad wrappers / dead code.** grep-verify zero callers before claiming dead. Wrapper functions that just cast through (e.g. `BytesUint64(b uint64) string { return Bytes(int64(b)) }`) underflow above 2^63 — flag both the deadness and the bug.

14. **HTMX / SSR best practices** (when templates are touched):
   - Mutating routes return HTML partials, not JSON-and-reload.
   - Buttons have `type="button"` (default is `submit`).
   - Clickable rows are `<a href>` or have `tabindex` + `role="button"` (keyboard a11y).
   - Initial page render is server-rendered, not a JS bootstrap that fires `htmx.ajax` on load.
   - `<form>`s have `action` + `method` so they degrade without JS.
   - `hx-vals` JSON values are escaped via the project's `hxVals` helper, not raw `{{...}}`.
   - Same-origin / CSRF middleware on mutators.

Do not re-check things the linter handles (function length, magic numbers, formatting, doc comments).

#### 3. Triage

If no issues found, report "Branch is clean" and stop.

Otherwise, list issues:

```
# | File                       | Line | Category     | Severity  | Description
--|----------------------------|------|--------------|-----------|------------
1 | internal/handlers/stats.go | 67   | logic-bug    | must-fix  | Empty prefix rejected as missing
2 | internal/server/server.go  | 93   | error        | must-fix  | Graceful shutdown returns error
```

#### 4. Fix

Fix issues in priority order (must-fix, then should-fix, then nits):

1. Fix the issue.
2. Run `make lint`. Stage any auto-fixes.
3. Commit with a clear message.

When you delete or rename something user-visible (a binary, an exported API), **don't keep a backwards-compat shim** unless the user asks. This is a single-developer repo; clean rename wins.

#### 5. Test and Loop

1. Run `make test` (and `make test-race` if concurrency was touched).
2. **If you touched S3-related code, also note whether the MinIO tests would have caught the bug** — they skip silently without `AWS_ENDPOINT_URL_S3`.
3. If all pass and no new issues, report results and stop.
4. If tests fail or fixes introduced new issues, increment pass number and go to step 1.

## Output

```
Review Loop Complete — N passes

Pass 1: 5 issues found, 5 fixed
Pass 2: 1 regression from fix, 1 fixed
Pass 3: 0 issues — clean

Total: 6 issues fixed across 2 passes
```

## After-loop checklist (don't skip)

Before declaring the branch done, verify:

- [ ] `make lint` clean
- [ ] `make test` clean
- [ ] `make test-race` clean if concurrency was touched
- [ ] `make dev` boots; manual smoke test of one HTML route and one JSON route
- [ ] README reflects current binaries, routes, dev workflow
- [ ] No reinvented stdlib / library features introduced (see item 7)
- [ ] No layer leaks (see item 11)
- [ ] Every new package has at least one `_test.go` file
- [ ] MinIO-backed test paths are documented (or run)
