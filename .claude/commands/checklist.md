---
description: Quick pre-commit sanity check for items the linter cannot catch. Run before finishing a change.
---

# Pre-Commit Checklist

The linter catches formatting, function length, magic numbers, doc comments, and method ordering. This checklist covers what it cannot:

```
[ ] Every error is wrapped with context: fmt.Errorf("operation: %w", err)
[ ] No errors silently discarded (assign to _ only with a comment explaining why)
[ ] Tests cover both valid and invalid inputs, plus edge cases (nil, zero, empty, boundaries)
[ ] No global mutable state — dependencies injected via struct fields or parameters
[ ] Interfaces defined at the consumer, not the producer. Return concrete types.
[ ] context.Context is the first parameter for anything with I/O or cancellation
[ ] New code follows existing patterns in the package (read neighboring code first)
[ ] No hacks, workarounds, or compatibility shims — replaced code is deleted
[ ] No comments that narrate the edit or restate the code — only a non-obvious why
[ ] No TODOs (unless explicitly approved)
[ ] Both sides of every seam agree: index writer ↔ reader (and format Version bumped on layout changes), Go structs ↔ SQL migrations, handlers ↔ templates, config keys ↔ flags ↔ docs
[ ] Nothing derived from S3 keys or names reaches a file path, shell, SQL or HTML unvalidated/unescaped
[ ] A failure or cancellation never deletes or corrupts data the user already had
[ ] Memory stays bounded by the documented rules; no per-object or per-child allocation that grows with input on a hot path
[ ] Performance-relevant changes gated with `make bench-ab BASE=main` and the result cited
[ ] Concurrency-touching changes run with -race
```

Run `make lint`, then `make test-unit` (`make test` for S3-facing code). Both clean before committing.
