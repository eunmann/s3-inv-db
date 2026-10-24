---
description: Bounded review-fix loop for the current branch — one full review, then short checks of the fixes, at most three rounds. Use after implementing a change and before creating a PR. This is the primary review command — use it instead of manually reviewing code or the built-in /code-review.
---

# Review Loop

Find and fix what a careful reviewer would block the merge on, at a bounded cost.

**What counts.** The bar is CLAUDE.md's North star: exact per-prefix totals, build memory bounded by `GOMEMLIMIT` and scratch disk by a documented multiple, query cost independent of index size, and builds that never destroy or corrupt existing data. A finding names who hits it (a CLI user, an operator running the server, an API or UI user, a library importer) on a reachable path, and how they notice. Theoretical findings are refuted, not fixed. Every fix preserves the intended behaviour of the code it touches. When the intended behaviour is unclear (docs, tests and the issue disagree or are silent), the fixer skips the finding and states the concrete alternatives, and the coordinator asks the user.

**Why it is bounded.** Repeated full reviews of unchanged code don't converge: each pass surfaces new low-severity nits while most of the tokens go to finders re-reading the same code. So the loop reviews the branch once, then reviews only what its own fixes changed, and low findings never buy another round.

## Rounds

| Round | Range | Finders | Mode |
|---|---|---|---|
| 1 — full | `$(git merge-base main HEAD)..HEAD` | 1 per ~1500 changed code lines (tests not counted) from `review-scope.sh`, at most 3 | full |
| 2, 3 — check | the previous round's reviewed tip`..HEAD` (that round's fixes) | 1 | check |

- **Continue** only if the round confirmed a `high` or `medium` finding. Low findings are fixed in the same round and end it.
- **Stop** after round 3 regardless. Anything still open goes to the user with its evidence; a fourth round runs only if the user asks for it.
- Never re-review code an earlier round already reviewed and no fix has touched since.

## Round procedure

State lives in `$(git rev-parse --git-dir)/review-loop/` (inside the git dir, so per worktree and never committed): `reviewed` holds the tip the last round reviewed.

1. **Preflight.** `git status --porcelain` empty.
2. **Scope.** The base is the merge-base in round 1 and `$(cat "$(git rev-parse --git-dir)/review-loop/reviewed")` after it; then `mkdir -p "$(git rev-parse --git-dir)/review-loop" && git rev-parse HEAD > "$(git rev-parse --git-dir)/review-loop/reviewed"`. Round 1 only: `.claude/scripts/review-scope.sh <base>` prints changed code and test lines per directory (`go.sum`, the generated `tailwind.css` and `testdata/` excluded). Group the directories into finder groups along package boundaries, keeping a package's tests with it (≤ 1500 code lines → one group). Natural seams: the index build (`pkg/extsort`, `pkg/format` writers, `pkg/s3inventory`, `pkg/s3fetch`), the read path (`pkg/format` readers, `pkg/indexread`), the server (`internal/server`, `internal/inventory`, `internal/jobs`, `internal/autoload`, `internal/budget`, `internal/handlers`, `internal/templates`), and tooling/docs.
3. **Run the round as one Workflow**: `.claude/workflows/review-round.js` with `args` `{round, mode: "full"|"check", range: "<base>..<tip>", groups: [{name, paths}], trailer, checkout}` (`checkout` is the absolute path of the branch's worktree). For a check round, one group with `paths: ["."]`. The finders return findings; the script deduplicates them; one `review-fixer` verifies and fixes every finding in that checkout. The coordinator reads only the workflow's result.
4. **Gate**, once: `make lint-check`, and `make test-unit` only if step 6 continues the loop (the final gate covers the last round). A failure is fixed before anything else.
5. **Ask** any skipped finding's question to the user (`AskUserQuestion`).
6. **Next.** Apply the continue/stop rule above. Print the round line.

## Final gate

Only the fast checks run locally. PR Validation on GitHub is the full gate: lint, tidy, govulncheck, deadcode, actionlint, build, CSS freshness, the full suite with `-race` against a local S3 server, and the coverage floor.

1. `make test-unit`, plus `make test` when the branch touched S3-facing code (`pkg/s3fetch`, `internal/s3disco`, `internal/loader`, `internal/seeder`).
2. Performance-relevant changes (and any fix the fixer flagged): `make bench-ab BASE=main` (bench/README.md). A regression beyond the thresholds blocks the merge unless the commit message justifies it.
3. Push; update the PR body if the loop changed what the PR does; delete `$(git rev-parse --git-dir)/review-loop/`.
4. Merge only on a green PR Validation run on the final head. On a failure, read only the failing job's log tail (`gh run view <id> --log-failed | tail -n 100`).

## Commits

The fixer commits one logical change at a time (related findings together), in the `workflow` skill's Commit Messages format, labelled `Task: review R<N>`. The PR is squash-merged, so branch commits exist for review, not for `main`'s history; don't split a fix finer than a reviewer needs.

## Output

Per round, one line:

```
Round N (full|check, range X..Y): findings F (high H, medium M, low L) -> fixed K, refuted R, asked Q | next: check|stop
```

At the end: rounds run, totals by severity, and the final gate's local results.
