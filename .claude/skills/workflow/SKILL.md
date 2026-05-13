---
name: workflow
description: Branch creation, incremental change process, and PR steps. Use when starting new work, planning a feature, or preparing to submit a PR.
---

# Workflow

> **Mindset:** Be skeptical, challenge assumptions, and use first principles. Do not stop at the first match. Do not shortcut. Verify behavior by reading code — names are claims, not proof.

> Always follow `CLAUDE.md`'s workflow: feature branch from `main`, small atomic commits, lint + tests pass, merge then delete the branch.

## Current State

- **Branch:** `$!git branch --show-current`
- **Status:**
```
$!git status --short
```
- **Recent commits:**
```
$!git log --oneline -5
```

## Branch Creation

Before branching, challenge whether this is the right scope — a single branch may be too big (should be split) or too small (should be folded into existing work). Cite the evidence: what does the code actually require?

- **New feature:** `git checkout main && git pull origin main && git checkout -b feature/<short-name>`
- **Extending a feature:** branch off the parent feature branch, not main, when the work depends on unmerged code.

## Implementation

Use the `tdd` skill for changes that benefit from test-driven development. Touch hot paths in `pkg/extsort`, `pkg/indexread`, `pkg/triebuild`, or anything aggregated over many objects? Run benchmarks before and after per `CLAUDE.md` rule 4.

### Atomic Commits

- One logical change per commit: a test + its implementation, a refactor, or a bug fix.
- Each commit compiles and passes tests independently.
- Messages describe behavior, not files: `fix: graceful shutdown exit code` not `update server.go`.

## Review

Run `/review-loop` to find and fix bugs, gaps, and quality issues iteratively until clean.

## Finishing Up

1. Run `make lint`. Commit any auto-fixes. Run `make test` (or `make test-race` for concurrency-touching work). All green.
2. Summarize what changed and why. Ask: "Ready to create a draft PR?"
3. After merge, delete the local and remote branches:
   ```
   git branch -d feature/<short-name>
   git push origin --delete feature/<short-name>
   ```
