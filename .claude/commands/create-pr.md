---
description: Push branch, create a pull request, and request Copilot review. Use when your work is committed and ready for PR.
allowed-tools: Bash(git push:*), Bash(gh pr create:*), Bash(gh pr edit:*), Bash(git log:*), Bash(git diff:*), Bash(git branch:*), Bash(git merge-base:*), Bash(gh pr list:*), Bash(gh pr view:*), Bash(git status:*), Bash(git add:*), Bash(git commit:*)
---

# Create Pull Request

Push the current branch, open a PR, and request a Copilot review.

## Current State

- **Branch:** `$!git branch --show-current`
- **Status:**
```
$!git status --short
```
- **Commits since main:**
```
$!git log --oneline main..HEAD 2>/dev/null || echo "(on main — nothing to PR)"
```

## Preflight Checks

Before creating anything, validate:

1. **Not on main.** If the current branch is `main`, stop and tell the user.
2. **No existing PR.** Run `gh pr list --head <branch> --state all --json number,state,url` to check for existing PRs:
   - If a **merged** PR exists for this branch, stop and tell the user the branch already has a merged PR.
   - If an **open** PR exists, stop and tell the user — offer to update it instead.
3. **Uncommitted changes.** If there are staged or unstaged changes, commit them first:
   - Review the diff to write an appropriate commit message.
   - Stage and commit all relevant changes (exclude secrets, .env files).
4. **Has commits ahead of main.** If `git log main..HEAD` is empty after handling uncommitted changes, stop and tell the user there is nothing to PR.

If a hard check fails (on main, existing PR, no commits), report the problem clearly and stop. Do not create a PR.

## Steps

If all preflight checks pass, execute all tool calls in a single message. After the tool calls, report the PR URL.

1. **Push** the branch: `git push -u origin $(git branch --show-current)`.

2. **Create the PR** using `gh pr create`. Analyze all commits since main (not just the latest) to write the title and body:

```
gh pr create --title "<concise title, under 70 chars>" --body "$(cat <<'EOF'
## Summary
- <what changed and why, 1-3 bullets>

## Test Plan
- [ ] <how to verify the changes>

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

3. **Request Copilot review**: `gh pr edit --add-reviewer @copilot`.

4. **Report** the PR URL to the user.
