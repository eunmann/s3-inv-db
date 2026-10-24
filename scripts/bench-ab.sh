#!/usr/bin/env bash
# Usage: scripts/bench-ab.sh [-n rounds] [-s suite] BASE_REF
#
# A/B benchmark gate for performance changes. Builds the test binaries
# for BASE_REF (in a temporary worktree) and for the current working tree
# ("head"), then runs every suite entry ROUNDS times, alternating which
# side goes first each round so machine drift lands on both sides
# equally. benchstat compares the two; the script exits 1 when a
# statistically significant (p < 0.05) regression exceeds its threshold:
#
#   sec/op                       > BENCH_TIME_PCT (default 5)
#   B/op, allocs/op, disk_B      > BENCH_MEM_PCT  (default 5)
#   peak_heap_B (sampled, noisy) > BENCH_HEAP_PCT (default 15)
#
# Suite lines (bench/suite.txt): `<tags> <package> <bench regex> <benchtime>`,
# tags `-` for none. Entries tagged `integration` need a local S3 server;
# when AWS_ENDPOINT_URL_S3 is unset the script starts one in Docker.
# Results land in bench/ab/<timestamp>/ (gitignored).
set -euo pipefail

rounds=8
suite=bench/suite.txt
while getopts "n:s:" opt; do
	case $opt in
		n) rounds=$OPTARG ;;
		s) suite=$OPTARG ;;
		*) sed -n '2,21p' "$0"; exit 2 ;;
	esac
done
shift $((OPTIND - 1))
[[ $# -eq 1 ]] || { sed -n '2,21p' "$0"; exit 2; }
base_ref=$1
time_pct=${BENCH_TIME_PCT:-5}
mem_pct=${BENCH_MEM_PCT:-5}
heap_pct=${BENCH_HEAP_PCT:-15}

root=$(git rev-parse --show-toplevel)
cd "$root"
base_sha=$(git rev-parse --verify "$base_ref^{commit}")
out="bench/ab/$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$out/bin"
work=$(mktemp -d)
s3_container=""
cleanup() {
	git worktree remove --force "$work/base" >/dev/null 2>&1 || true
	rm -rf "$work"
	[[ -n $s3_container ]] && docker rm -f "$s3_container" >/dev/null 2>&1 || true
}
trap cleanup EXIT

git worktree add --detach "$work/base" "$base_sha" >/dev/null
echo "base $base_ref ($base_sha) vs head (working tree $(git rev-parse --short HEAD)); $rounds rounds; results in $out"

mapfile -t entries < <(grep -vE '^\s*(#|$)' "$suite")

if grep -q '^integration' <<<"$(printf '%s\n' "${entries[@]}")" && [[ -z ${AWS_ENDPOINT_URL_S3:-} ]]; then
	image=$(sed -n 's/^ *image: &s3-image //p' infra/docker-compose.yml)
	s3_container="s3inv-bench-$$"
	docker run -d --name "$s3_container" -p 127.0.0.1::9000 --tmpfs /data "$image" \
		--port :9000 --access devaccess --secret devsecret posix /data >/dev/null
	port=$(docker port "$s3_container" 9000 | head -1 | sed 's/.*://')
	export AWS_ENDPOINT_URL_S3="http://127.0.0.1:$port" AWS_ACCESS_KEY_ID=devaccess AWS_SECRET_ACCESS_KEY=devsecret AWS_REGION=us-east-1
	for _ in $(seq 1 30); do
		[[ $(curl -s -o /dev/null -w '%{http_code}' "$AWS_ENDPOINT_URL_S3/" || true) != 000 ]] && break
		sleep 1
	done
fi

# Compile once per side and package so build time never lands in a run.
for side in base head; do
	src=$root
	[[ $side == base ]] && src=$work/base
	for e in "${entries[@]}"; do
		read -r tags pkg _ _ <<<"$e"
		bin="$out/bin/$side-$(tr '/.' '__' <<<"$pkg")-$tags.test"
		[[ -x $bin ]] && continue
		tagflag=()
		[[ $tags != - ]] && tagflag=(-tags "$tags")
		(cd "$src" && go test -c "${tagflag[@]}" -o "$root/$bin" "./$pkg")
	done
done

run_side() {
	local side=$1 src=$root e tags pkg re bt
	[[ $side == base ]] && src=$work/base
	for e in "${entries[@]}"; do
		read -r tags pkg re bt <<<"$e"
		(cd "$src/$pkg" && "$root/$out/bin/$side-$(tr '/.' '__' <<<"$pkg")-$tags.test" \
			-test.run '^$' -test.bench "$re" -test.benchtime "$bt" -test.count 1 -test.benchmem) \
			2>/dev/null | { grep -E '^(Benchmark|goos|goarch|pkg|cpu)' || true; } >>"$out/$side.txt"
	done
}

for r in $(seq 1 "$rounds"); do
	if ((r % 2)); then order=(base head); else order=(head base); fi
	for side in "${order[@]}"; do run_side "$side"; done
	echo "round $r/$rounds done"
done

for side in base head; do
	grep -q '^Benchmark' "$out/$side.txt" 2>/dev/null || {
		echo "bench-ab: no benchmark results for $side; does $side contain the suite's benchmarks?" >&2
		exit 1
	}
done

benchstat=(go run golang.org/x/perf/cmd/benchstat@v0.0.0-20261009192801-be2c69fb417e)
(cd "$out" && "${benchstat[@]}" base=base.txt head=head.txt) | tee "$out/report.txt"
(cd "$out" && "${benchstat[@]}" -format csv base=base.txt head=head.txt 2>/dev/null) >"$out/report.csv"

# Walk the CSV: a `,<unit>,CI,...` line starts a metric section; data rows
# carry `<name>,<base>,<ci>,<head>,<ci>,<delta%|~>,p=<p> n=<n>`.
awk -F, -v tp="$time_pct" -v mp="$mem_pct" -v hp="$heap_pct" '
	$1 == "" && $3 == "CI" { unit = $2; next }
	$1 == "" || $1 == "geomean" || $6 == "~" || $6 == "" { next }
	{
		delta = $6; sub(/%$/, "", delta); delta += 0
		split($7, pv, /[= ]/); p = pv[2] + 0
		limit = (unit == "sec/op") ? tp : (unit == "peak_heap_B") ? hp : mp
		if (unit !~ /^(sec\/op|B\/op|allocs\/op|peak_heap_B|disk_B)$/) next
		if (p < 0.05 && delta > limit) {
			printf "REGRESSION %s %s: %+.2f%% (limit %s%%, p=%s)\n", $1, unit, delta, limit, p
			bad = 1
		}
	}
	END { exit bad }
' "$out/report.csv" && echo "bench-ab: no significant regression beyond thresholds"
