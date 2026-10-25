#!/usr/bin/env bash
# Usage: scripts/bench-ab.sh [-n rounds] -s suite BASE_REF
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
#   peak_heap_B (sampled, noisy),
#   retained_heap_B, gc_scan_heap_B > BENCH_HEAP_PCT (default 15)
#
# It exits 2 when it cannot compare: bad usage, a build or benchmark
# failure, a suite entry with no results on a side, or no S3 server.
#
# Suite lines (bench/suite/*.txt): `<tags> <package> <bench regex> <benchtime>`,
# tags `-` for none. Entries tagged `integration` need a local S3 server;
# when AWS_ENDPOINT_URL_S3 is unset the script starts one in Docker.
# BASE_GOTOOLCHAIN / HEAD_GOTOOLCHAIN (e.g. go1.26.10) build that side
# with a given Go toolchain, to gate a toolchain upgrade; by default both
# sides use the toolchain `go` resolves for their go.mod.
# BENCH_COOLDOWN (seconds, default 0) sleeps before every run, so a
# laptop's power or thermal limits can't settle on one side; use it when
# sec/op moves with no matching change in B/op or allocs/op.
# Results land in bench/ab/<timestamp>/ (gitignored); each side's full
# benchmark output is in <side>.log.
set -euo pipefail

die() {
	echo "bench-ab: $*" >&2
	exit 2
}

usage() {
	sed -n '2,/^set -e/{s/^# \{0,1\}//p}' "$0" >&2
	exit 2
}

rounds=8
suite=
while getopts "n:s:" opt; do
	case $opt in
		n) rounds=$OPTARG ;;
		s) suite=$OPTARG ;;
		*) usage ;;
	esac
done
shift $((OPTIND - 1))
[[ $# -eq 1 && -n $suite ]] || usage
base_ref=$1
time_pct=${BENCH_TIME_PCT:-5}
mem_pct=${BENCH_MEM_PCT:-5}
heap_pct=${BENCH_HEAP_PCT:-15}
cooldown=${BENCH_COOLDOWN:-0}
[[ $cooldown =~ ^[0-9]+$ ]] || die "BENCH_COOLDOWN must be whole seconds, got '$cooldown'"

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
		--port :9000 --access devaccess --secret devsecret posix /data >/dev/null ||
		die "cannot start S3 server ($image)"
	port=$(docker port "$s3_container" 9000 | head -1 | sed 's/.*://')
	export AWS_ENDPOINT_URL_S3="http://127.0.0.1:$port" AWS_ACCESS_KEY_ID=devaccess AWS_SECRET_ACCESS_KEY=devsecret AWS_REGION=us-east-1
	s3_up=false
	for _ in $(seq 1 30); do
		if [[ $(curl -s -o /dev/null -w '%{http_code}' "$AWS_ENDPOINT_URL_S3/" || true) != 000 ]]; then
			s3_up=true
			break
		fi
		sleep 1
	done
	if ! $s3_up; then
		docker logs --tail 20 "$s3_container" >&2 || true
		die "S3 server at $AWS_ENDPOINT_URL_S3 not up after 30s"
	fi
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
		toolchain=BASE_GOTOOLCHAIN
		[[ $side == head ]] && toolchain=HEAD_GOTOOLCHAIN
		toolchain=${!toolchain:-}
		(cd "$src" && GOTOOLCHAIN=${toolchain:-${GOTOOLCHAIN:-auto}} go test -c "${tagflag[@]}" -o "$root/$bin" "./$pkg") ||
			die "cannot build $side $pkg"
	done
done

# Every entry must produce results on both sides in every round: an entry
# whose regex matches nothing would otherwise drop out of the comparison
# and the gate would pass without measuring it.
run_side() {
	local side=$1 round=$2 src=$root e tags pkg re bt status run=$out/run.log errlog=$out/run.stderr
	[[ $side == base ]] && src=$work/base
	for e in "${entries[@]}"; do
		read -r tags pkg re bt <<<"$e"
		status=0
		((cooldown > 0)) && sleep "$cooldown"
		# Results go to stdout; logs go to stderr, kept apart so a log
		# line written mid-benchmark cannot split a result line.
		(cd "$src/$pkg" && "$root/$out/bin/$side-$(tr '/.' '__' <<<"$pkg")-$tags.test" \
			-test.run '^$' -test.bench "$re" -test.benchtime "$bt" -test.count 1 -test.benchmem) \
			>"$run" 2>"$errlog" || status=$?
		cat "$run" "$errlog" >>"$out/$side.log"
		if ((status != 0)); then
			tail -n 30 "$run" "$errlog" >&2
			die "$side round $round: entry '$e' failed (exit $status); full output in $out/$side.log"
		fi
		grep -q '^Benchmark' "$run" ||
			die "$side round $round: entry '$e' produced no benchmark results; does $side contain them?"
		grep -E '^(Benchmark|goos|goarch|pkg|cpu)' "$run" >>"$out/$side.txt"
	done
}

for r in $(seq 1 "$rounds"); do
	if ((r % 2)); then order=(base head); else order=(head base); fi
	for side in "${order[@]}"; do run_side "$side" "$r"; done
	echo "round $r/$rounds done"
done

benchstat=(go run golang.org/x/perf/cmd/benchstat@v0.0.0-20261009192801-be2c69fb417e)
(cd "$out" && "${benchstat[@]}" base=base.txt head=head.txt) | tee "$out/report.txt" || die "benchstat failed"
(cd "$out" && "${benchstat[@]}" -format csv base=base.txt head=head.txt 2>/dev/null) >"$out/report.csv" ||
	die "benchstat -format csv failed"

# Walk the CSV: a `,<unit>,CI,...` line starts a metric section; data rows
# carry `<name>,<base>,<ci>,<head>,<ci>,<delta%|~>,p=<p> n=<n>`.
awk -F, -v tp="$time_pct" -v mp="$mem_pct" -v hp="$heap_pct" '
	$1 == "" && $3 == "CI" { unit = $2; next }
	$1 == "" || $1 == "geomean" || $6 == "~" || $6 == "" { next }
	{
		delta = $6; sub(/%$/, "", delta); delta += 0
		split($7, pv, /[= ]/); p = pv[2] + 0
		limit = (unit == "sec/op") ? tp : (unit ~ /^(peak_heap_B|retained_heap_B|gc_scan_heap_B)$/) ? hp : mp
		if (unit !~ /^(sec\/op|B\/op|allocs\/op|peak_heap_B|retained_heap_B|gc_scan_heap_B|disk_B)$/) next
		if (p < 0.05 && delta > limit) {
			printf "REGRESSION %s %s: %+.2f%% (limit %s%%, p=%s)\n", $1, unit, delta, limit, p
			bad = 1
		}
	}
	END { exit bad }
' "$out/report.csv" && echo "bench-ab: no significant regression beyond thresholds"
