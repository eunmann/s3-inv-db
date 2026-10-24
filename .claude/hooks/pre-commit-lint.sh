#!/usr/bin/env bash
# PreToolUse hook (Bash): before any `git commit` the agent runs, lint the
# packages of the staged Go files with --fix, re-stage what the linter
# rewrote, and block the commit (exit 2) on remaining findings. Uses the
# golangci-lint version pinned in the Makefile; build tags come from
# .golangci.yml (run.build-tags), so tag-gated tests are linted too.
#
# The commit may sit anywhere in a compound command and target another
# checkout (a worktree): the hook lints the repository the commit runs in,
# taken from `git -C <dir> commit`, else a preceding `cd <dir>`, else the
# tool call's working directory.
set -euo pipefail

INPUT=$(cat)
COMMAND=$(jq -r '.tool_input.command // empty' <<<"$INPUT" 2>/dev/null || true)
CWD=$(jq -r '.cwd // empty' <<<"$INPUT" 2>/dev/null || true)
CWD=${CWD:-${CLAUDE_PROJECT_DIR:-.}}

SEP=$'(^|[;&|(\n][[:space:]]*)'
COMMIT_RE="${SEP}git([[:space:]]+-C[[:space:]]+([^[:space:];&|]+))?[[:space:]]+commit([[:space:];&|)]|$)"
CD_RE="${SEP}cd[[:space:]]+([^[:space:];&|]+)[[:space:]]*(&&|;|"$'\n'")"
# resolve BASE PATH: PATH unquoted, ~-expanded and made absolute against BASE.
resolve() {
    local p=${2#[\"\']}
    p=${p%[\"\']}
    p=${p/#\~/$HOME}
    [[ $p == /* ]] || p=$1/$p
    echo "$p"
}

[[ $COMMAND =~ $COMMIT_RE ]] || exit 0
GIT_C=${BASH_REMATCH[3]}
REST=${COMMAND%%"${BASH_REMATCH[0]}"*}${BASH_REMATCH[1]}
DIR=$CWD
while [[ $REST =~ $CD_RE ]]; do
    DIR=$(resolve "$DIR" "${BASH_REMATCH[2]}")
    REST=${BASH_REMATCH[3]}${REST#*"${BASH_REMATCH[0]}"}
done
[[ -z $GIT_C ]] || DIR=$(resolve "$DIR" "$GIT_C")

# A directory that doesn't exist or isn't a repository fails the commit
# itself; there is nothing to lint.
cd "$DIR" 2>/dev/null || exit 0
TOP=$(git rev-parse --show-toplevel 2>/dev/null) || exit 0
cd "$TOP"

STAGED=$(git diff --cached --name-only --diff-filter=ACM -- '*.go' 2>/dev/null || true)
if [[ -z "$STAGED" ]]; then
    exit 0
fi

VERSION=$(sed -n 's/^GOLANGCI_LINT_VERSION := //p' Makefile)
PACKAGES=$(xargs -n1 dirname <<<"$STAGED" | sort -u | sed 's|^|./|; s|$|/...|')

declare -A BEFORE
while IFS= read -r f; do
    [[ -f "$f" ]] && BEFORE["$f"]=$(md5sum "$f" | awk '{print $1}')
done <<<"$STAGED"

LINT_EXIT=0
LINT_OUT=$(mktemp)
# shellcheck disable=SC2086 # PACKAGES is a newline-separated list of patterns
go run "github.com/golangci/golangci-lint/v2/cmd/golangci-lint@${VERSION}" run --fix $PACKAGES >"$LINT_OUT" 2>&1 || LINT_EXIT=$?
cat "$LINT_OUT"

# Exit 5 means "no Go files to analyze" (e.g. every staged file is
# excluded) — nothing to lint is not a failure. `go run` launders the
# child's exit code to 1 and prints "exit status 5", so match that too,
# but only when golangci reported no findings.
if { [[ $LINT_EXIT -eq 5 ]] || { [[ $LINT_EXIT -ne 0 ]] && grep -qx 'exit status 5' "$LINT_OUT"; }; } \
        && ! grep -qE '^[0-9]+ issues:' "$LINT_OUT"; then
    LINT_EXIT=0
fi
rm -f "$LINT_OUT"

RESTAGED=0
while IFS= read -r f; do
    if [[ -f "$f" && "${BEFORE[$f]:-}" != "$(md5sum "$f" | awk '{print $1}')" ]]; then
        git add "$f"
        RESTAGED=$((RESTAGED + 1))
    fi
done <<<"$STAGED"
if [[ $RESTAGED -gt 0 ]]; then
    echo "Auto-fixed and re-staged $RESTAGED file(s)"
fi

if [[ $LINT_EXIT -ne 0 ]]; then
    echo "Lint errors found. Fix them before committing." >&2
    exit 2
fi
