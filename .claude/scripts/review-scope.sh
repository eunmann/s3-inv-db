#!/usr/bin/env bash
# Usage: review-scope.sh <base> [tip]
# Prints changed lines per directory (up to three levels deep) for <base>..<tip> (default HEAD)
# as "code tests dir", largest first, then the totals. go.sum, the generated tailwind.css
# and testdata/ are excluded; tests are _test.go files and testdata/.
set -euo pipefail
EXCLUDE=(
	':!go.sum' ':!internal/templates/styles/tailwind.css' ':!**/testdata/**'
)
git diff --numstat --no-renames "$1..${2:-HEAD}" -- . "${EXCLUDE[@]}" |
	awk -F'\t' '{
		n = split($3, p, "/"); d = p[1]
		for (i = 2; i < n && i <= 3; i++) d = d "/" p[i]
		if (n == 1) d = "."
		lines = $1 + $2
		if ($3 ~ /_test\.go$|(^|\/)testdata\//) { t[d] += lines; tt += lines } else { c[d] += lines; ct += lines }
		seen[d] = 1
	} END {
		for (d in seen) printf "%6d %6d %s\n", c[d], t[d], d | "sort -rn"
		close("sort -rn")
		printf "%6d %6d total\n", ct, tt
	}'
