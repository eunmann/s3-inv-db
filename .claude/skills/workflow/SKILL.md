---
name: workflow
description: Branch creation, incremental change process, and PR steps. Use when starting new work, planning a feature, or preparing to submit a PR.
---

# Workflow

> **Mindset:** Be skeptical, challenge assumptions — the user's, the repo's, your own — with evidence from code. Names are claims, not proof: verify by reading. Read the related features before changing anything. Present options with tradeoffs; never commentary on size or difficulty. No hacks, no workarounds, no compatibility shims.

## Current State

- **Branch:** !`git branch --show-current`
- **Status:** !`git status --short`
- **Recent commits:** !`git log --oneline -5`

## Branch Creation

Before branching, challenge whether this is the right scope: what is independently mergeable? Large PRs are fine when the work is cohesive.

- **New feature:** `git checkout main && git pull origin main && git checkout -b feature/<short-name>`
- **Extending a feature:** branch off the parent feature branch when the work depends on unmerged code.

## Implementation

1. **Read first.** Read the related features, consumers, and call paths the change touches. For non-trivial work, apply the `repo-research` skill.
2. **Implement directly.** Write code and its tests together. Use the `tdd` skill for bug fixes, where the failing test proves the bug.
3. **Refactor where warranted**, in its own commit.
4. **Performance.** Only ingestion changes (`make bench-ingest BASE=main`) and index data-structure / query changes (`make bench-query BASE=main`) are benchmarked, with only that area's suite — see bench/README.md "When to run". Nothing else is benchmarked: it takes a long time. Report the result in the PR's **Performance** section; a regression needs an explicit justification.

### Atomic Commits

- One logical change per commit: a feature + its tests, a refactor, or a bug fix.
- Each commit compiles and passes tests independently.

### Commit Messages

[Conventional Commits](https://www.conventionalcommits.org), plus a `Task:` label saying which job produced the commit. PR titles use the same header, because the squash merge makes the PR title the commit on `main`.

```
fix(extsort): propagate the final spill error from each worker

Why the change was needed and what it does, in a few lines.

Task: impl #18
Co-Authored-By: ...
```

- **Header:** `type(scope): summary`, at most 72 characters, imperative, lowercase, no trailing period. Describe behaviour, not files.
- **Type:** `feat` · `fix` · `refactor` (no behaviour change) · `perf` · `test` · `docs` · `build` (Makefile, deps, Docker) · `ci` · `chore`.
- **Scope:** one short word for the area: a package (`extsort`, `format`, `indexread`, `server`, `inventory`, `handlers`, `autoload`, …) or `bench`, `infra`, `claude`. Omit it when the change spans many areas.
- **`Task:` label** (a footer, before trailers): `impl #<issue>` (several: `impl #18 #20`), `review R<round>`, `pr #<number>` for PR-comment fixes, or `manual`.

## Verification (end of change set, not mid-development)

Locally, run the fast checks; PR Validation is the full gate.

1. `make lint`, and commit any auto-fixes (the pre-commit hook does this for agent commits).
2. `make test-unit`; `make test` when S3-facing code changed (`pkg/s3fetch`, `internal/s3disco`, `internal/loader`, `internal/seeder`).
3. `make bench-ingest` or `make bench-query` (`BASE=main`) only if the branch changed ingestion or data-structure / query code.

PR Validation runs lint, tidy, govulncheck, deadcode, actionlint, build, CSS freshness, the full suite with `-race` against a local S3 server and the coverage floor. When a run fails, read only the failing job's log tail (`gh run view <id> --log-failed | tail -n 100`).

## Review

Run `/review-loop`: one full review of the branch, then checks of its own fixes, at most three rounds.

## Finishing Up

1. Summarize what changed and why — no commentary on effort or scale.
2. Open the PR (`/create-pr`). Its description includes a **Performance** section with the measured before → after numbers for every benchmarked change, so reviewers know what to expect.
3. **Before merging**, in this order:
   1. `/review-loop` has finished on the branch.
   2. Every open review thread is addressed with `/address-pr-comments`.
   3. PR Validation is green on the final head (its `Gate` job is the required check). Update the branch after another PR lands.
   4. Merge with `gh pr merge --squash --delete-branch`. Never push directly to `main`.
4. After merge: `git checkout main && git pull`, delete the local branch (`git branch -D feature/<short-name>`; squash merges aren't ancestors, so `-d` refuses), and remove any worktree.

## Working Multiple Branches at Once

- Give every branch its own git worktree (`git worktree add ../wt-<short-name> -b feature/<short-name> main`) and do all edits, builds and tests for that branch inside it.
- Put changes that touch the same files on one branch (or a stack); disjoint changes can proceed in parallel worktrees.
- Run lint in a worktree as `GOLANGCI_LINT_CACHE=$PWD/.golangci-cache make lint` and remove the cache directory afterwards, so the shared cache never holds another checkout's paths.
- Run one heavy phase at a time (`make test`, a benchmark run, a review fan-out): benchmarks in particular need a quiet machine.
