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
[ ] Hot-path changes have benchmarks before and after (CLAUDE.md rule 4)
[ ] Concurrency-touching changes verified with `make test-race`
```

Run `make lint`, then `make test` (or `make test-race` for concurrency work). Both clean before committing.
