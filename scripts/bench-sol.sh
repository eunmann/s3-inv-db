#!/usr/bin/env bash
# Speed-of-light report (#280): the machine's ceilings, measured with
# off-the-shelf tools, next to the build's own numbers on the same CPUs.
# Reported, not gated. See bench/README.md "Speed of light".
#
#   scripts/bench-sol.sh            # 10M objects on CPUs 0-3
#   N=1000000 CPUS=0-7 scripts/bench-sol.sh
#
# Needs lmbench, fio, hyperfine, pigz and the AWS CLI; perf is optional
# (top-down metrics need kernel.perf_event_paranoid <= 2). Uses the local
# S3 server at AWS_ENDPOINT_URL_S3, or starts one in Docker.
set -euo pipefail

N=${N:-10000000}
CPUS=${CPUS:-0-3}
SHAPES=${SHAPES:-realistic realistic_sorted}

die() {
	echo "bench-sol: $*" >&2
	exit 2
}

lmbench=/usr/lib/lmbench/bin/$(uname -m)-linux-gnu
[[ -x $lmbench/bw_mem ]] || lmbench=$(dirname "$(command -v bw_mem 2>/dev/null || echo /missing/bw_mem)")
[[ -x $lmbench/bw_mem ]] || die "lmbench not found (apt install lmbench)"
for tool in fio hyperfine pigz gzip aws go; do
	command -v "$tool" >/dev/null || die "$tool not found"
done

root=$(git rev-parse --show-toplevel)
cd "$root"
out=bench/sol/$(date -u +%Y%m%dT%H%M%SZ)
work=$out/work # on the repo's filesystem, like a build's scratch
mkdir -p "$work"
pin=(taskset -c "$CPUS")
cores=$("${pin[@]}" nproc)

s3_container=
cleanup() {
	rm -rf "$work"
	[[ -n $s3_container ]] && docker rm -f "$s3_container" >/dev/null 2>&1 || true
}
trap cleanup EXIT

if [[ -z ${AWS_ENDPOINT_URL_S3:-} ]]; then
	image=$(sed -n 's/^ *image: &s3-image //p' infra/docker-compose.yml)
	s3_container=s3inv-sol-$$
	docker run -d --name "$s3_container" -p 127.0.0.1::9000 --tmpfs /data "$image" \
		--access devaccess --secret devsecret posix /data >/dev/null
	port=$(docker port "$s3_container" 9000 | head -1 | sed 's/.*://')
	export AWS_ENDPOINT_URL_S3="http://127.0.0.1:$port" AWS_ACCESS_KEY_ID=devaccess AWS_SECRET_ACCESS_KEY=devsecret AWS_REGION=us-east-1
	for _ in $(seq 30); do
		[[ $(curl -s -o /dev/null -w '%{http_code}' "$AWS_ENDPOINT_URL_S3/" || true) != 000 ]] && break
		sleep 1
	done
fi
export S3INV_LONG_BENCH=1

# fixture SHAPE: the bucket BenchmarkPipeline seeded for SHAPE on these
# CPUs (internal/loader chunkCountFor: four data files per CPU).
chunks=$((4 * cores < N / 10000 ? 4 * cores : N / 10000))
fixture() {
	aws s3 ls | awk '{print $3}' |
		grep -E "^fixture-${1//_/-}-$N-$chunks-[0-9a-f]+${S3INV_FIXTURE_TAG:+-$S3INV_FIXTURE_TAG}\$" | head -1
}

echo "== building" >&2
go test -c -tags integration -o "$work/loader.test" ./internal/loader
go build -o "$work/s3-inv-db" ./cmd/s3-inv-db

# The build: BenchmarkPipeline, which also seeds the fixtures.
echo "== build (BenchmarkPipeline)" >&2
for shape in $SHAPES; do
	(cd internal/loader && "${pin[@]}" "$root/$work/loader.test" -test.run '^$' \
		-test.bench "^BenchmarkPipeline\$/^shape=$shape\$/^n=$N\$" -test.benchtime 1x -test.count 3) |
		grep '^Benchmark' >>"$out/pipeline.txt"
done

# Native gunzip on the fixture's own data files: the decode floor. On
# the fixture, Go's klauspost gunzip runs within ~15% of zlib (#280).
first=${SHAPES%% *}
bucket=$(fixture "$first")
[[ -n $bucket ]] || die "no fixture bucket for $first at $N"
aws s3 cp --quiet --recursive "s3://$bucket/bench-inv/bench-src/" "$work/gz/" --exclude '*' --include '*.csv.gz'
mapfile -t gzfiles < <(find "$work/gz" -name '*.csv.gz' | sort)
printf '%s\n' "${gzfiles[@]}" >"$work/gz.list"
echo "gz_B $(du -cb "${gzfiles[@]}" | tail -1 | cut -f1)" >"$out/fixture.txt"
echo "csv_B $(cat "${gzfiles[@]}" | gzip -dc | wc -c)" >>"$out/fixture.txt"
echo "== native gunzip (hyperfine)" >&2
hyperfine -N -w 1 -r 3 --export-json "$out/gunzip.json" \
	-n "gzip 1 core" "taskset -c ${CPUS%%[-,]*} sh -c 'xargs -a $work/gz.list cat | gzip -dc >/dev/null'" \
	-n "gzip $cores cores" "taskset -c $CPUS xargs -a $work/gz.list -P $cores -n 1 gzip -dc -q -k -c" \
	-n "pigz $cores cores" "taskset -c $CPUS xargs -a $work/gz.list -P $cores -n 1 pigz -dc" \
	>/dev/null

# Memory: lmbench bandwidth on one core and all, random dependent-load
# latency, and how many misses one core keeps in flight.
echo "== memory (lmbench)" >&2
{
	for op in rd cp; do
		echo "bw_${op}_1 $(taskset -c "${CPUS%%[-,]*}" "$lmbench/bw_mem" -N 3 512m "$op" 2>&1 | awk '{print $2}')"
		echo "bw_${op}_all $("${pin[@]}" "$lmbench/bw_mem" -P "$cores" -N 3 512m "$op" 2>&1 | awk '{print $2}')"
	done
	taskset -c "${CPUS%%[-,]*}" "$lmbench/lat_mem_rd" -t -N 1 1024 128 2>&1 |
		awk 'NF == 2 && $1 + 0 > 0 { last = $2; if ($1 >= 64 && !m) m = $2 } END { print "lat_64m", m; print "lat_1g", last }'
	taskset -c "${CPUS%%[-,]*}" "$lmbench/par_mem" -M 1024m 2>&1 | awk 'NF == 2 { last = $2 } END { print "mlp_1g", last }'
} >"$out/memory.txt"

# Storage: the build's scratch writes as one sequential write and fsync,
# then read back past the page cache (O_DIRECT, no root needed).
echo "== storage (fio)" >&2
scratch=$(awk '{ for (i = 3; i < NF; i += 2) if ($(i + 1) == "write_B" && $i > m) m = $i } END { printf "%d", m }' "$out/pipeline.txt")
size=$(((scratch >> 20) + 1))M
"${pin[@]}" fio --name=write --directory="$work" --rw=write --bs=1M --size="$size" --end_fsync=1 \
	--output-format=json --output="$out/fio-write.json"
"${pin[@]}" fio --name=write --directory="$work" --rw=read --bs=1M --size="$size" --direct=1 \
	--output-format=json --output="$out/fio-read.json"
rm -f "$work"/write.*

# Where the build's cycles go: perf's top-down level 1 on the real CLI.
if command -v perf >/dev/null && perf stat -M TopdownL1 -- true >/dev/null 2>&1; then
	echo "== top-down (perf stat)" >&2
	for shape in $SHAPES; do
		b=$(fixture "$shape")
		manifest=$(aws s3 cp "s3://$b/fixture.json" - | sed -n 's/.*"path": *"\([^"]*\)".*/\1/p')
		rm -rf "$work/index"
		perf stat -M TopdownL1 -o "$out/topdown-$shape.txt" -- \
			"${pin[@]}" "$work/s3-inv-db" build --s3-manifest "$manifest" --out "$work/index" >/dev/null 2>&1
	done
	rm -rf "$work/index"
else
	echo "perf unavailable or kernel.perf_event_paranoid > 2: top-down skipped" >"$out/topdown-skipped.txt"
fi

scripts/bench-sol-report.sh "$out" "$CPUS" | tee "$out/report.md"
