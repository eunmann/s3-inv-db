---
name: go-standards
description: "Reference: Go code quality standards — control flow, error handling, functions, concurrency, naming, architecture. Consult when writing or reviewing Go code, not as an action to run."
---

# Go Standards

## Read First, Write Second

- Read neighboring code before writing. Match the existing style, error handling, naming, and structure.
- Evaluate whether existing code near your changes should be refactored for clarity, maintainability, or correctness.
- Run `make` with no args (or read the `Makefile`) to see available targets.

## Control Flow

- Use simple, explicit control flow. No `goto`. No recursion unless a provable depth bound is documented.
- Prefer early returns for errors to keep the happy path flat and un-indented.
- Centralize branching in parent functions. Helpers compute and return values — they do not make high-level control decisions.

## Bounds and Allocation

- Every loop over dynamic data must have a fixed upper bound. Unbounded retries, unbounded channel buffers, and open-ended polling are prohibited.
- Every retry must have a max attempt count and bounded backoff. Use `context.Context` for cancellation.
- Pre-allocate slices and maps with known capacity: `make([]T, 0, n)`, `make(map[K]V, n)`. Avoid `append` that triggers reallocation in hot paths.

## Functions

- Maximum 120 lines per function (matches `funlen` config). One job per function — if describing it requires "and", split it.
- Validate all inputs at the top of the function. Return errors for bad input; `panic` only for invariant violations that indicate programmer bugs.
- If a function takes more than 3-4 parameters of the same type, use an options struct to prevent argument swaps.
- Keep helpers pure (no side effects) when possible.

## Error Handling

- Check every returned error. Never discard an error silently.
- Wrap errors with context: `fmt.Errorf("operation: %w", err)`. `wrapcheck` is enabled — it will flag bare returns of foreign errors.
- Use `errors.Is` / `errors.As` for inspection; never string-match error messages. `errorlint` enforces this.
- Define sentinel errors as package-level vars: `var ErrNotFound = errors.New("not found")`.
- Do not use `panic` for normal error handling. `panic` in a library is a bug.
- If a return value is intentionally unused, assign to `_` with a comment explaining why.

## Types and Interfaces

- Accept interfaces, return structs. Define interfaces at the consumer, not the producer.
- Do not define an interface alongside its only implementation — that is premature abstraction.
- Keep interfaces small and focused: 1-3 methods. The ideal Go interface is one method.
- Return concrete types from constructors so new methods can be added without breaking consumers.
- Prefer functions over methods when no state is needed. Do not create a struct to hold a single method.

## Scope and State

- Declare variables at the smallest possible scope. Use `:=` and `if` initializers to confine variables.
- Do not use package-level mutable variables. Inject dependencies through struct fields or function parameters.
- Do not reuse a variable for multiple unrelated purposes.
- Group a mutex and the state it protects into a struct. Do not expose the mutex — provide methods that lock internally. See `internal/inventory/manager.go` for the canonical local example.

## Concurrency

- Every goroutine must have a clear owner and a clear shutdown path.
- Never fire-and-forget goroutines. Use `sync.WaitGroup` or `errgroup.Group` to wait for completion.
- Pass `context.Context` as the first parameter. Never store it in a struct field (`containedctx` flags this).
- Every channel must have a declared buffer size, or the goroutine lifecycle must guarantee no blocking.
- If you can't diagram the concurrency on a napkin, simplify it.
- Always run `make test-race` for changes touching concurrency.

## Pointers and Unsafe

- Do not use `unsafe` unless absolutely necessary, isolated, and commented with justification. The mmap-backed `pkg/indexread` is the only place this is currently justified.
- Do not use `reflect` in hot paths.
- Avoid pointer-to-pointer indirection. If a function accesses its argument only as `*x`, pass the value directly.
- Use pointer receivers when the method mutates the receiver or the struct is large; value receivers for small immutable types. Do not mix receiver types on a single struct.

## Naming

- `MixedCaps` (exported), `mixedCaps` (unexported). Never `snake_case` for Go identifiers.
- Short receiver names: one or two letters abbreviating the type (`m` for `Manager`, `idx` for `Index`). No `self`, `this`, or `me`.
- No stutter: `inventory.NewManager`, not `inventory.NewInventoryManager`. The package name is context.
- Standard initialisms in all-caps: `ID`, `URL`, `HTTP`, `API`.
- Add units to bare numeric names: `timeoutMs`, `sizeBytes`, `retryDelaySeconds`.
- Package names describe what the package provides, not what it contains. Never `util`, `common`, `helpers`, or `misc`.

## Documentation

- Doc comments on every exported symbol. Start with the symbol name: `// ParseAge parses...`. `revive` enforces this.
- Comments are sentences: capital letter, full stop (`godot`).

### What comments are for (and what they're not)

A comment exists to surface what the code *can't* — a constraint imposed
by something outside the file, a non-obvious invariant the surrounding
code is honoring, or a specific past bug the shape of the code is
preventing. Default to **no comment**; only add one when removing it
would mislead a future reader who has the code in front of them.

Keep:

- Doc comments on exported symbols. `// ParseAge parses an ISO duration string.`
- External constraints. `// AWS S3 Inventory uses minute-granularity folders (YYYY-MM-DDTHH-MMZ).`
- Non-obvious invariants. `// Caller must hold m.mu before invoking.`
- Specific past bugs with evidence. `// Regression for #142: index.Clone shares namespace after Execute.`

Delete:

- Narration of what well-named code already shows: `// We then aggregate the views by config.` — call out the function name instead.
- Justifications of the chosen approach: `// Matching that here keeps the synthetic layouts indistinguishable from real ones.` — the commit message is the home for "why I picked this".
- Restating the test name in prose: `// TestFoo_BarHappens pins that bar happens.` — the name is the documentation.
- References to "the current task / this fix / the new behavior / the recent change". Code outlives the PR; the comment will read like fossilised PR description.
- Multi-line laundry lists enumerating each branch or field a function inspects — those belong in the function name or in extracted helpers.

If reasoning about *why this approach over alternatives* would help, write it in the commit message or the PR description, not the source tree.

## Architecture

- Every package has a single, clear purpose. Dependency graph must be a DAG — no import cycles.
- Keep `main` thin: parse config, wire dependencies, call `Run`. See `cmd/s3-inv-db-server/main.go` for the pattern.
- Justify every external dependency. Prefer the standard library. Do not vendor a library for a single utility function.
- Pin dependencies to exact versions. Review dependency updates before merging.

## See Also

- `testing` skill — test conventions and lint rules
- `.golangci.yml` — the authoritative linter config
