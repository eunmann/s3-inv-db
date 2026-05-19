---
description: "Deeply review a pull request — fetch, invoke deep-research scoped to the PR's changed areas, and post a structured review. Use when reviewing PRs with significant scope."
allowed-tools: Bash(gh pr view:*), Bash(gh pr diff:*), Bash(gh pr list:*), Bash(gh pr review:*), Bash(gh api:*), Bash(git log:*), Bash(git diff:*), Bash(git merge-base:*), Read, Grep, Glob
---

# Review Pull Request

> **Mindset:** Be skeptical, challenge assumptions, and use first principles. Do not stop at the first match. Do not shortcut. Verify behavior by reading code — names are claims, not proof.

Review a pull request end-to-end using the `deep-research` process. Scope: any PR (your own or someone else's). The goal is to surface issues a line-by-line review would miss — cross-cutting breakage, missing consumers, invariant violations — and post a structured review after explicit approval.

**PR:** $ARGUMENTS

## 1. Fetch the PR

If a PR number or URL was provided, use it. Otherwise, use the PR for the current branch.

```bash
gh pr view <n> --json number,title,body,headRefName,baseRefName,state,files,commits,author,url
gh pr diff <n>
```

Summarize for the user:

- **Title & author**
- **What changed** — list of files grouped by package/area
- **Commit count** and notable commit messages
- **Description intent** — what the PR author claims it does

If the PR is closed or merged, stop and ask the user whether to proceed.

## 2. Deep Research Scoped to the Diff

Apply the `deep-research` skill steps 1-4, scoped to the PR's changed files. Remap the steps:

- **Step 1 (Broad scan)** → "Which packages does this PR touch? Which packages import or consume them?"
- **Step 2 (Feature inventory)** → "List every consumer, caller, and related subsystem that could break." For index-pipeline changes (`extsort`/`triebuild`/`indexread`), list the other two. For handler changes, list the template and the manager method called. For tier/price changes (`pkg/tiers`/`pkg/pricing`), list every consumer.
- **Step 3 (Sub-agent deep-dives)** → Spawn one `Explore` subagent per affected feature. Each agent looks for:
  - **Logic bugs** — off-by-one, nil deref, races, wrong operator, wrong return value.
  - **Missing tests** — new or changed behavior without a corresponding test (positive AND negative paths, empty-string/boundary inputs).
  - **Error semantics** — errors swallowed, wrapped without context, wrong sentinel, lost stack.
  - **API design** — interfaces defined producer-side, parameter swaps, leaked internal types, breaking changes, HTML-partial routes returning JSON.
  - **Concurrency** — goroutines without shutdown, shared state without sync, unbounded channels, locks dropped around I/O.
  - **Consumer breakage** — call sites that pass the old shape to the new code.
  - **On-disk format drift** — index format changes not mirrored across writer and reader.
  - **Doc / Makefile / Docker drift** — `docs/`, `Makefile`, `infra/`, `.air.toml` references that the PR invalidates.
  Instruct each agent explicitly: *"Do not stop at the first match. Trace every call path. Be skeptical of naming — verify behavior by reading code."*
  Wait for all agents to complete before synthesizing.
- **Step 4 (Synthesize)** → Consolidate findings into a prioritized list.

Do not run steps 5-6 — `/review-pr` produces a review artifact, not code.

### When fixes are requested after the review lands

If the user then asks you to fix any of the findings (in this conversation or a follow-up), default to **TDD red-green-refactor** (use the `tdd` skill) for anything where the bug is observable from a test:

- **Bug fixes** → write a failing regression test that reproduces the finding first, watch it fail, then fix.
- **Missing-test findings** → the test you write IS the deliverable. Add it; if it passes immediately the behavior was already correct and the gap was just coverage.
- **Pure style / rename / template-only nits** → TDD doesn't apply; just make the change.

Surface this preference up front when transitioning from "review posted" to "now fix it" so the user can opt out for a single batch fix.

## 3. Synthesize the Review

Build a prioritized issue list:

```
# | File                       | Line | Category       | Severity   | Description
--|----------------------------|------|----------------|------------|------------
1 | internal/server/server.go  | 93   | logic-bug      | must-fix   | <one line>
2 | pkg/indexread/index.go     | 188  | missing-test   | should-fix | <one line>
3 | internal/handlers/stats.go | —    | api-design     | nit        | <one line>
```

Severity:

- **must-fix** — bug, security issue, breaking change, missing test on new behavior.
- **should-fix** — quality issue that will cause problems later (bad error handling, hidden coupling).
- **nit** — style or minor improvement, take it or leave it.

Add a **summary verdict**: approve / request changes / comment-only. Back it with evidence, not vibes.

If zero issues, report "Clean — no findings." and stop before posting.

## 4. Explicit Approval Gate

Present the review to the user:

- The verdict
- The full issue list with file:line references
- The draft review body (what will be posted)

Then write exactly:

> Approve this review? Reply "approved" to post, or describe changes to adjust.

Do not post until the user responds with explicit approval. Interpret ambiguous replies as "not yet" and ask a clarifying question.

## 5. Post the Review

Post per-line comments for issues that map to specific lines. Use a body comment for the verdict and general notes.

**Per-line comments** (use `gh api` to target exact lines):

```bash
gh api --method POST repos/{owner}/{repo}/pulls/<n>/comments \
  -f body='<comment>' \
  -f commit_id='<head_sha>' \
  -f path='<file>' \
  -F line=<line> \
  -f side='RIGHT'
```

**Summary review** with verdict:

```bash
gh pr review <n> --request-changes --body "$(cat <<'EOF'
## Summary
<verdict and high-level notes>

## Findings
- **must-fix** (N): ...
- **should-fix** (N): ...
- **nit** (N): ...

See per-line comments for specifics.
EOF
)"
```

Use `--approve` for approvals, `--request-changes` for must-fix findings, `--comment` for comment-only reviews.

## 6. Report

Output to the user:

- PR URL
- Verdict
- Issue count by severity (must-fix / should-fix / nit)
- Number of per-line comments posted

## Review Layers

Structure your review across three layers, spending proportional effort on each:

### Layer 1: Mechanical (defer to tooling)

Skip items that automated tooling catches — linters, CI/CD, pre-commit hooks. Only flag if CI is misconfigured or missing a check.

### Layer 2: Structural (majority of review time)

These require understanding the codebase architecture:

- **Security:** Widened attack surfaces, input validation at boundaries, path-traversal risks (the inventory `Path` is passed straight to `indexread.Open`).
- **Architecture:** Separation of concerns, configurable parameters, on-disk format invariants.
- **Best Practices:** Single Responsibility, DRY, minimal nesting, no magic values.
- **Maintainability:** Readability, no duplication, appropriate method length.
- **Testing:** Edge cases and negative paths tested, not just happy paths. Empty-string boundary cases. Concurrency tests with `-race`.

### Layer 3: Narrative (the "why")

Assess using the PR description and broader context:

- **Requirements:** Does the PR meet the original ticket or issue requirements?
- **Context:** Does the description explain reasoning, alternatives considered, and migration paths?
- **Documentation:** Are non-obvious decisions explained via comments that describe "why" not "what"?
- **Impact:** Is the change breaking? How does it affect existing integrations? Are performance implications addressed? Hot-path changes need benchmarks per `CLAUDE.md` rule 4.
