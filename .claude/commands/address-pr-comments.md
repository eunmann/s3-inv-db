---
description: "Review, address, and resolve PR comments. Use when you want to handle feedback on a pull request — reads comments, fixes code, replies, and resolves threads."
allowed-tools: Bash(gh api:*), Bash(gh pr view:*), Bash(gh pr list:*), Bash(git push:*), Bash(git add:*), Bash(git commit:*), Bash(git diff:*), Bash(git log:*), Bash(git status:*), Bash(git branch:*), Read, Edit, Grep, Glob
---

# Address PR Comments

Review all comments on a pull request, fix the code, reply to each comment, and resolve the threads.

**PR:** $ARGUMENTS

## Step 1: Identify the PR

- If a PR number or URL was provided, use it.
- Otherwise, get the PR for the current branch: `gh pr view --json number,url,headRefName`.
- If no PR exists for the current branch, stop and tell the user.

## Step 2: Fetch all review comments in one call

Use a single GraphQL query to get everything at once — threads, comments, file paths, line numbers, and resolution status:

```bash
gh api graphql -f query='
query($owner: String!, $repo: String!, $pr: Int!) {
  repository(owner: $owner, name: $repo) {
    pullRequest(number: $pr) {
      reviewThreads(first: 100) {
        nodes {
          id
          isResolved
          isOutdated
          path
          line
          startLine
          comments(first: 20) {
            nodes {
              id
              databaseId
              author { login }
              body
              createdAt
            }
          }
        }
      }
    }
  }
}' -f owner='{owner}' -f repo='{repo}' -F pr=<number>
```

## Step 3: Triage

Skip threads that are already resolved or outdated. For remaining threads, categorize each:

| # | File | Line | Comment | Action |
|---|------|------|---------|--------|
| 1 | path | line | summary | fix / acknowledge / dismiss |

- **fix**: The comment identifies a real issue — change the code.
- **acknowledge**: The comment is valid feedback but no code change needed (e.g., style preference already handled).
- **dismiss**: The comment is incorrect or not applicable — explain why.

Present this triage table to the user and wait for confirmation before proceeding. The user may override any categorization.

## Step 4: Address each comment

For each thread marked **fix**:

1. Read the file at the relevant line.
2. Make the fix.
3. Reply to the review comment explaining what was changed:
   ```bash
   gh api --method POST repos/{owner}/{repo}/pulls/<pr>/comments/<comment_id>/replies -f body='<reply>'
   ```
4. Resolve the thread:
   ```bash
   gh api graphql -f query='mutation { resolveReviewThread(input: { threadId: "<thread_id>" }) { thread { isResolved } } }'
   ```

For each thread marked **acknowledge**:
1. Reply explaining how it's already handled or why no change is needed.
2. Resolve the thread.

For each thread marked **dismiss**:
1. Reply explaining why the comment doesn't apply.
2. Resolve the thread.

Keep replies concise — one or two sentences. No filler.

## Step 5: Commit and push

If any code changes were made:

1. Stage the changed files.
2. Commit with a message like: `fix: address PR review comments`.
3. Push to the branch.

## Step 6: Summary

Report what was done:
- How many threads addressed
- How many code changes made
- How many threads resolved
- Link to the PR
