---
name: tdd
description: Red-green-refactor test-driven development cycle. Use when implementing new behavior or fixing bugs outside the task system.
---

# TDD — Red-Green-Refactor

> **Mindset:** Be skeptical, challenge assumptions, and use first principles. Do not stop at the first match. Do not shortcut. Verify behavior by reading code — names are claims, not proof.

## Current State

- **Branch:** `$!git branch --show-current`
- **Status:**
```
$!git status --short
```

## Cycle

For each behavior to add or change:

### Red — Write a Failing Test

Before writing the test, challenge the assumption that you understand the behavior. Trace a call path and verify — a test that encodes a misread of the spec will pass for the wrong reasons.

1. Write the smallest test that expresses the next behavior.
2. Table-driven with `t.Run`, standard `testing` package, `cmp.Diff` for structs.
3. Run `make test`. Confirm it **fails for the expected reason**.
4. If the failure is unexpected (compile error, wrong assertion), fix the test first.

### Green — Make It Pass

1. Write the **minimum** code to make the failing test pass.
2. Do not add extra logic, optimizations, or unrelated fixes.
3. Run `make test`. All tests pass.

### Refactor — Clean Up

1. Improve naming, extract helpers, remove duplication — only in code you just touched.
2. Run `make lint`. Stage any auto-fixes.
3. Run `make test` (and `make test-race` if you touched concurrency). Still green.
4. Commit with a clear message describing the behavior added.

## Repeat

Pick the next behavior. Keep cycles small — one test + its implementation per commit.
