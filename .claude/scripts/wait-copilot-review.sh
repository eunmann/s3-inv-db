#!/usr/bin/env bash
# Usage: wait-copilot-review.sh <pr> [timeout-minutes, default 30]
# Requests a Copilot review of the PR's current head unless one exists, then
# waits for it, polling once a minute. Run it in the background: it exits once,
# printing the review's state, its unresolved thread count and the first line
# of its body (which says so when Copilot could not review). Exits 1 on timeout.
set -euo pipefail
pr=$1
deadline=$(($(date +%s) + ${2:-30} * 60))
repo=$(gh repo view --json nameWithOwner -q .nameWithOwner)
bot='copilot-pull-request-reviewer[bot]'
head=$(gh pr view "$pr" --json headRefOid -q .headRefOid)

latest() {
	gh api "repos/$repo/pulls/$pr/reviews" --paginate \
		-q "[.[] | select(.user.login == \"$bot\" and .commit_id == \"$head\")] | last | select(. != null) | \"\(.state)\t\([.body | split(\"\n\")[] | select(test(\"^\\\\s*(<!--.*)?$\") | not)] | first // \"\")\""
}

[ -n "$(latest)" ] || gh pr edit "$pr" --add-reviewer @copilot >/dev/null

while [ "$(date +%s)" -lt "$deadline" ]; do
	review=$(latest || true)
	if [ -n "$review" ]; then
		owner=${repo%/*} name=${repo#*/}
		open=$(gh api graphql -F owner="$owner" -F name="$name" -F pr="$pr" -f query='
			query($owner: String!, $name: String!, $pr: Int!) {
			  repository(owner: $owner, name: $name) { pullRequest(number: $pr) {
			    reviewThreads(first: 100) { nodes { isResolved } } } } }' \
			-q '[.data.repository.pullRequest.reviewThreads.nodes[] | select(.isResolved | not)] | length')
		printf 'PR #%s copilot review on %s: %s | unresolved threads: %s\n' "$pr" "${head:0:9}" "$review" "$open"
		exit 0
	fi
	sleep 60
done
echo "PR #$pr: no copilot review of ${head:0:9} after ${2:-30} minutes" >&2
exit 1
