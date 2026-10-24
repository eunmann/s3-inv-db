---
description: "Review a pull request with the review-loop finders and post a structured review. Use when reviewing a PR, your own or someone else's."
allowed-tools: Bash(gh pr view:*), Bash(gh pr diff:*), Bash(gh pr list:*), Bash(gh pr review:*), Bash(gh api:*), Bash(git fetch:*), Bash(git log:*), Bash(git diff:*), Bash(git merge-base:*), Bash(.claude/scripts/review-scope.sh:*), Read, Grep, Glob, Agent
---

# Review Pull Request

Find what should block the merge — cross-file breakage, missing consumers, broken invariants, untested behaviour — and post it as a review. The description, names and tests are claims; verdicts rest on `file:line`. Review your own PRs as hard as anyone's.

**PR:** $ARGUMENTS

## 1. Fetch

Use the given PR, or the current branch's. `gh pr view <n> --json number,title,body,author,baseRefName,headRefOid,state,url`, then `git fetch origin pull/<n>/head:refs/review/pr-<n> <baseRefName>:refs/review/base-<n>` (the pull ref works for fork PRs too). If the PR is closed or merged, ask the user whether to proceed.

## 2. Find

With `base=$(git merge-base refs/review/base-<n> refs/review/pr-<n>)`, run `.claude/scripts/review-scope.sh $base refs/review/pr-<n>`. Group the directories as `/review-loop` round 1 does: one group per ~1500 changed code lines, at most three. Spawn one `review-finder` agent per group in parallel, mode `full`, with the range `$base..refs/review/pr-<n>` and the group's paths. Also check the PR against its description and linked issue: does it do what it claims? For a performance claim, is a `make bench-ab` result cited?

## 3. Synthesize

One table, most severe first, using the finder's severities:

```
# | File:Line                       | Severity | Kind     | Description
1 | pkg/extsort/pipeline.go:552     | high     | bug      | <one line>
```

Verdict: **request changes** for any high or medium, **comment** for low only, **approve** for none. GitHub rejects approving or requesting changes on your own PR, so when the PR's author is the current `gh` user, post the verdict as a `--comment` review.

## 4. Post

Post without an approval prompt. Per-line comments for findings that map to a line:

```bash
gh api --method POST repos/{owner}/{repo}/pulls/<n>/comments \
  -f body='<comment>' -f commit_id='<headRefOid>' -f path='<file>' -F line=<line> -f side='RIGHT'
```

Then the summary review (`--request-changes`, `--comment` or `--approve`):

```bash
gh pr review <n> --request-changes --body "$(cat <<'EOF'
## Summary
<verdict and the evidence for it>

## Findings
- high (N) · medium (N) · low (N) — see the line comments.
EOF
)"
```

Report the PR URL, the verdict and the counts.
