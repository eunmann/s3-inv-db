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

Read every changed file. Focus on:

- **Logic bugs:** Off-by-one, nil dereference, race conditions, incorrect return values, wrong comparison operators.
- **Missing tests:** New or changed behavior without a corresponding test. Both positive and negative paths. Empty-string / boundary inputs.
- **Error semantics:** Errors swallowed silently, wrapped without context (`%w`), or wrong sentinel errors used.
- **API design:** Interfaces defined at producer instead of consumer, parameter order inviting swaps, leaking internal types. HTML-partial routes returning JSON.
- **Concurrency:** Goroutines without shutdown paths, shared state without synchronization, unbounded channels. Lock dropped around I/O — what happens if state changes during the unlocked window?
- **On-disk format:** Changes to one of `extsort`/`triebuild`/`indexread` that aren't mirrored in the others.

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

#### 5. Test and Loop

1. Run `make test` (and `make test-race` if concurrency was touched).
2. If all pass and no new issues, report results and stop.
3. If tests fail or fixes introduced new issues, increment pass number and go to step 1.

## Output

```
Review Loop Complete — N passes

Pass 1: 5 issues found, 5 fixed
Pass 2: 1 regression from fix, 1 fixed
Pass 3: 0 issues — clean

Total: 6 issues fixed across 2 passes
```
