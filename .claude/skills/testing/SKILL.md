---
name: testing
description: "Reference: Testing conventions, lint rules, and make targets. Consult when writing tests or debugging failures, not as an action to run."
---

# Testing Standards

## Conventions

- Every behavior change requires tests. Cover success-path and error-path.
- Read existing test files in the package to learn the project's testing conventions before writing new tests. `internal/handlers/integration_test.go` and `internal/inventory/manager_test.go` are good examples of the style we use here.
- Use Go's standard `testing` package only. No third-party assertion libraries (`testify/assert`, etc.). Use `if` + `t.Errorf`/`t.Fatalf` directly.
- Use table-driven tests when multiple cases share the same test logic. Name every subtest clearly: `t.Run("missing_prefix", ...)`.
- Separate success-path and error-path tests into distinct test functions for clarity.
- Cover the positive space (valid inputs → correct outputs) and the negative space (invalid inputs → correct errors). Test edge cases: zero values, nil, boundaries, empty inputs (the empty-prefix bug shipped because the empty case was untested).
- Compare structs with `google/go-cmp` (`cmp.Diff`), not `reflect.DeepEqual`.
- Do not test internal implementation details. Test the public API contract.
- Test files live next to the code they test: `parser.go` and `parser_test.go` in the same directory.
- No test should be skipped or commented out.

## Concurrency

- For anything touching shared state (the `Manager`, in-flight `Load`, channel-based code), add a `-race`-targeted stress test. See `TestManagerConcurrent_LoadRemoveRace` in `internal/inventory/manager_test.go`.
- Run `make test-race` before declaring concurrency-related work done.

## HTTP handlers

- For success paths that need a real index on disk, use the `seeder` package to build a synthetic index in `t.TempDir()`. `internal/handlers/integration_test.go:buildLoadedTestHandlers` is the helper pattern.
- For HTML partial routes, assert the response **Content-Type** is `text/html` (or `text/plain` for errors) — not `application/json`. HTMX swaps the body directly into the DOM, so JSON shows through.
- For URL params that distinguish "missing" from "empty", use `r.URL.Query().Has("name")` rather than checking the value.

## Lint Rules That Affect Tests

The authoritative source is `.golangci.yml`. Notable rules:

- `wrapcheck` — wrap returned errors with `fmt.Errorf("...: %w", err)`. The `_test\.go` exclusion does **not** turn this off; tests should still wrap.
- `errorlint` — use `errors.Is` / `errors.As`, not `==` or string matching.
- `intrange` — use Go 1.22's `for i := range N` instead of `for i := 0; i < N; i++`.
- `prealloc` — pre-allocate slices when the final size is known.
- `funlen` is set to 120 lines / 80 statements; tests are exempted from most complexity checks (`funlen`, `gocognit`, `gocyclo`, `cyclop`, `maintidx`, `nestif`).

## Make Targets

- `make lint` — runs golangci-lint v2.1.2 across the module. Run first; commit any auto-fixes.
- `make lint-fix` — same with `--fix`.
- `make test` — fast unit tests.
- `make test-race` — same with the race detector. Always use for concurrency changes.

## See Also

- `go-standards` skill — full code quality rules
- `tdd` skill — the red-green-refactor cycle
