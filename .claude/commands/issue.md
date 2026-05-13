---
description: "Implement a GitHub issue end-to-end: understand, plan, branch, implement with TDD, review, and open a PR. Use when given a GitHub issue number or URL to work on."
---

# Implement Issue

> **Mindset:** Be skeptical, challenge assumptions, and use first principles. Do not stop at the first match. Do not shortcut. Verify behavior by reading code — names are claims, not proof.

Take a GitHub issue from understanding through to a ready-to-merge PR.

**Issue:** $ARGUMENTS

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

## 1. Preflight

Ensure a clean starting point:

1. If there are uncommitted changes, warn the user and stop.
2. Switch to `main` and pull latest:
   ```
   git checkout main && git pull origin main
   ```

## 2. Understand the Issue

Fetch the issue: `gh issue view <number> --json title,body,labels,state,comments,assignees`.

Read the title, body, labels, and comments. Summarize:

- **Goal:** What does this issue ask for?
- **Motivation:** Why is it needed?
- **Constraints:** Requirements, edge cases, or non-goals mentioned.

### Still needed?

Check whether the issue is still relevant:

- Is it still open?
- Has it been partially addressed by recent commits? (`git log --oneline -20 --all`)
- Does the codebase already have the requested behavior? Search for key terms.

If the issue appears resolved or outdated, explain why and ask the user before continuing.

## 3. Plan

Apply the `deep-research` skill to this issue. Work through all six steps: broad repo scan, exhaustive feature inventory, parallel sub-agent deep-dives, synthesis, a parallelizable task plan, and — once approved — spawn implementation subagents.

During steps 4-5 (synthesis and task plan), also:

1. **Assess scope** — bug fix, new feature, refactor, or something else?
2. **Consider alternatives** — if there is a simpler or more robust approach than what the issue describes, explain clearly: what it is, why it is better, and the tradeoffs. The user should understand the options before committing to an approach.
3. **Break into atomic commits** — each step in the implementation plan should produce one atomic commit that compiles and passes tests independently.

Present the plan as a numbered list. Each item must name the file path, the change, and the rationale. Then write exactly:

> Approve this plan? Reply "approved" to proceed, or describe changes to adjust.

Do not proceed to branching or implementation until the user responds with explicit approval. Interpret ambiguous replies as "not yet" and ask a clarifying question.

## 4. Branch

After the plan is approved, create the feature branch:

```
git checkout -b feature/<short-name-from-issue>
```

Use a concise, descriptive branch name derived from the issue title.

## 5. Implement

For each step in the plan, follow the TDD cycle (see the `tdd` skill):

1. **Write a failing test** — smallest test expressing the behavior. Table-driven with `t.Run`, standard `testing` package, `cmp.Diff` for structs.
2. **Make it pass** — minimum code only.
3. **Lint** — `make lint`. Stage any auto-fixes.
4. **Test** — `make test` (and `make test-race` if concurrency was touched).
5. **Commit** — one atomic commit per behavior. Message describes the what and why.

If the change touches a hot path (`pkg/extsort`, `pkg/indexread`, `pkg/triebuild`, or anything aggregated per object), run benchmarks before and after per `CLAUDE.md` rule 4.

Keep commits focused: one logical change each, independently compilable and passing.

## 6. Review

Run `/review-loop` to find and fix bugs, gaps, and quality issues iteratively until clean.

## 7. Final Verification

Before creating the PR:

1. `make lint` — zero warnings.
2. `make test-race` — all green.
3. If the change touched docker or `.air.toml`, smoke-test `make dev` and/or `make docker-prod`.

Do not proceed to the PR until these pass.

## 8. Pull Request

1. Push the branch: `git push -u origin feature/<short-name>`.
2. Create the PR:

```
gh pr create --title "<concise title>" --body "$(cat <<'EOF'
Closes #<issue-number>

## Summary
- <what changed and why, 1-3 bullets>

## Test Plan
- <how to verify the changes>
EOF
)"
```

3. Request Copilot review: `gh pr edit --add-reviewer @copilot`.
4. Report the PR URL to the user.
