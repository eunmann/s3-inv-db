---
description: Create a detailed GitHub Issue by deeply researching the entire codebase, challenging the user's assumptions, and filing the issue with full context. Use when you want to file a well-researched issue for a feature, bug, or improvement.
---

# Create GitHub Issue

> **Mindset:** Be skeptical, challenge assumptions, and use first principles. Do not stop at the first match. Do not shortcut. Verify behavior by reading code — names are claims, not proof.

Your job is to deeply understand what the user wants, challenge their assumptions with evidence from the codebase, ensure the design is sound, and only then file a well-structured GitHub Issue.

You are a collaborator, not an order-taker. Push back when the user's framing misses important context. Ask hard questions. Surface trade-offs. The goal is an issue that the implementer can trust.

## User's Request

$ARGUMENTS

## Current State

- Branch: `$!git branch --show-current`
- Recent commits:
```
$!git log --oneline -10
```

## Step 1: Understand Intent Before Searching

Before touching the codebase, make sure you understand what the user actually wants — not just what they said.

Ask yourself:
- What problem is this solving? Is there a simpler framing?
- What assumptions is the user making about how the system works?
- Are there ambiguous terms that could mean different things in different parts of the codebase (e.g., "prefix" — root vs leaf, "tier" — storage class vs aggregation bucket)?

If anything is unclear, ask the user to clarify before proceeding. Do not guess.

## Step 2: Deep Research

Apply the `deep-research` skill scoped to this request. Work through steps 1-5: broad repo scan, exhaustive feature inventory, parallel sub-agent deep-dives (one per feature), synthesis, and a parallelizable task plan.

Stop before step 6 (spawn implementation subagents) — the output of `/create-issue` is an issue body, not code.

Scoping reminders for this repo:

- **Index pipeline changes** — touch any of `pkg/extsort`, `pkg/triebuild`, `pkg/indexread`? Deep-dive all three; the on-disk format is a shared contract.
- **Server changes** — touch handlers, server routes, templates, or the inventory `Manager`? Deep-dive every consumer of the changed types.
- **Tier / price changes** — `pkg/tiers` or `pkg/pricing`? Trace every caller (CLI, handlers, cost-estimate code paths).
- **Always include** `Makefile`, `.golangci.yml`, `.air.toml`, `infra/Dockerfile*`, `infra/docker-compose.yml`, `docs/`, `cmd/*`.

The goal is to leave no stone unturned. A good issue captures the full blast radius of a change so the implementer isn't surprised.

## Step 3: Challenge the User's Design

This is the most important step. Before writing the issue, present your findings to the user and challenge their assumptions. Use AskUserQuestion or direct conversation to drive alignment.

### What to challenge

**Does the request match reality?**
- "You asked for X, but the codebase already does Y. Did you mean to extend Y, or replace it?"
- "This feature assumes Z exists, but I found that Z was removed in commit abc123."

**Are there simpler alternatives?**
- "You described building a new package, but `pkg/foo` already handles 80% of the use case. Would extending it be better?"

**What are the trade-offs they haven't considered?**
- "This approach requires a new index column, but it widens the on-disk format. The alternative is computing it at query time."
- "Adding this field to the API response is a breaking change for existing clients."

**Is the scope right?**
- "This touches 15 files across 6 packages. Should we break it into smaller issues?"
- "This is listed as a bug fix, but the behavior is actually by design — see the comment in handler.go:42. Is this a feature request instead?"

**What's missing from their thinking?**
- "You didn't mention tests, but this area has integration tests that will need updating."
- "Touching the index format requires updating both writer and reader in lock-step."

### How to challenge

- Be direct and specific. Reference file paths, function names, and line numbers.
- Present evidence, not opinions. "The code does X" beats "I think X might be an issue."
- Offer alternatives with clear trade-offs. Don't just say "this is wrong" — say "here's a better approach and why."
- Ask for decisions when there are genuine forks: "Do you want A (simpler but limited) or B (more complex but extensible)?"

Wait for the user to respond before proceeding. Iterate if needed — go back to `deep-research` if the conversation reveals new areas to explore.

## Step 4: Write the Solution Plan

Once aligned with the user, write a concrete, ordered implementation plan:

- Break the work into sequential steps that each result in a working state
- For each step, name the specific files that change and describe the modification
- Call out new files or packages that need to be created
- Specify required tests — both unit and (where applicable) integration via the seeder
- Flag decisions that the implementer needs to make
- Estimate complexity: **small** (< 1 hour), **medium** (1-4 hours), **large** (4+ hours)

Present the plan as a numbered list. Each item must name the file path, the change, and the rationale. Then write exactly:

> Approve this plan? Reply "approved" to proceed, or describe changes to adjust.

Do not proceed to filing until the user responds with explicit approval. Interpret ambiguous replies as "not yet" and ask a clarifying question.

## Step 5: Labels

Every issue gets exactly one **type** label. Add area / priority labels only if the repo's label set supports them — check with `gh label list` first; do not invent labels.

### Type (required — pick exactly one)

| Label | When to use |
|-------|-------------|
| `bug` | Something is broken or produces wrong results |
| `enhancement` | New feature or improvement to existing behavior |
| `documentation` | Docs-only change (no code) |
| `dead-code` | Unused code, fields, or declared-not-wired features |
| `test-gap` | Missing test coverage for important behavior |

## Step 6: Scope Check — Single Issue or Parent + Children

Before filing, decide whether this is one issue or a parent with sub-issues:

- **Single issue:** The work can be done in one PR with a clear scope. File one issue.
- **Parent issue with children:** The work spans multiple independent PRs or has distinct phases. File a parent issue first, then create child issues and link them as sub-issues.

**When to create a parent issue:**
- The implementation plan has 3+ steps that are independently mergeable
- Different steps touch unrelated subsystems (e.g., index format + handler + docs)
- The user explicitly asks for it to be broken up

**Workflow for parent + children:**

1. Create the parent issue first (using the Step 7 template). Its body should describe the overall goal and list the planned sub-issues in a summary section.
2. Create each child issue. Each child's body should reference the parent: `Part of #<parent-number>` at the top.
3. Link each child as a sub-issue of the parent using the GitHub GraphQL API:

```bash
PARENT_ID=$(gh issue view <parent-number> --json id --jq '.id')
CHILD_ID=$(gh issue view <child-number> --json id --jq '.id')

gh api graphql -f query='
  mutation($parentId: ID!, $childId: ID!) {
    addSubIssue(input: { issueId: $parentId, subIssueId: $childId }) {
      issue { number }
      subIssue { number }
    }
  }
' -f parentId="$PARENT_ID" -f childId="$CHILD_ID"
```

Repeat for each child. This creates proper tracked sub-issue relationships in GitHub's UI — not just text references.

## Step 7: Create the Issue

Create the GitHub Issue using `gh issue create`. Always include the `--label` flag.

```
gh issue create \
  --title "<concise title, under 70 chars>" \
  --label "<type>" \
  --body "$(cat <<'EOF'
## Summary

<1-3 sentences: what this is and why it matters>

## What to Build

<Detailed description of what needs to be built, organized by component.>

## Implementation Plan

### Step 1: <Step name>
- Files: `path/to/file.go`
- <What changes and why>

### Step 2: <Step name>
- Files: `path/to/file.go`, `path/to/other.go`
- <What changes and why>

<Continue for all steps...>

## Key Files

| Area | Path |
|------|------|
| Core | `pkg/...` / `internal/...` |
| Tests | `.../..._test.go` |
| Docs | `docs/...` |

## Testing

- [ ] <Specific test: what it covers and which file>
- [ ] <Another test>

## Verification

- [ ] `make lint` passes with zero warnings
- [ ] `make test` passes
- [ ] `make test-race` passes (if concurrency touched)
- [ ] Benchmarks run before/after for hot-path changes (per CLAUDE.md rule 4)
- [ ] <Manual verification steps>

## Risks

- <Any risks, unknowns, or decisions needing input>

## Complexity

**<small/medium/large>** — <brief justification>

EOF
)"
```

After creating the issue, display the issue URL, the labels applied, and a brief summary of what was filed.
