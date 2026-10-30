#!/usr/bin/env bash
# Prints bench-sol.sh's report from the tool outputs in DIR, measured on
# the CPU list CPUS: scripts/bench-sol-report.sh DIR CPUS
set -euo pipefail

dir=$1
cpus=$2
cores=$(taskset -c "$cpus" nproc)

# mean NAME_REGEX UNIT FILE: the mean of UNIT over the benchmark lines
# whose name matches NAME_REGEX.
mean() {
	awk -v re="$1" -v unit="$2" '$1 ~ re {
		for (i = 3; i < NF; i += 2) if ($(i + 1) == unit) { s += $i; n++ }
	} END { if (n) printf "%.6g", s / n }' "$3"
}
kv() { awk -v k="$1" '$1 == k { print $2 }' "$2"; }
calc() { awk "BEGIN { printf \"%.${2:-2}f\", ($1) }"; }
hf() { jq -r --arg n "$1" '.results[] | select(.command == $n) | .mean' "$dir/gunzip.json"; }

gz_b=$(kv gz_B "$dir/fixture.txt")
csv_b=$(kv csv_B "$dir/fixture.txt")
gzip1=$(hf "gzip 1 core")
gzipn=$(hf "gzip $cores cores")
pigzn=$(hf "pigz $cores cores")
# The CPUs' parallel speedup, from native gunzip: below the CPU count
# when they share physical cores.
speedup=$(calc "$gzip1 / $gzipn")
# Seconds the CSV.gz input needs at best: the faster native decoder on
# all CPUs.
floor_gz=$(calc "$pigzn < $gzipn ? $pigzn : $gzipn")
fio_w=$(jq '.jobs[0].write.bw_bytes' "$dir/fio-write.json")
fio_r=$(jq '.jobs[0].read.bw_bytes' "$dir/fio-read.json")

cat <<EOF
# Speed of light

$(lscpu | sed -n 's/^Model name: *//p'), CPUs $cpus ($cores), $(uname -r)

## Machine ceilings

| Resource | Ceiling | Tool |
|---|---|---|
| Memory read, 1 CPU / all | $(kv bw_rd_1 "$dir/memory.txt") / $(kv bw_rd_all "$dir/memory.txt") MB/s | lmbench \`bw_mem rd\` |
| Memory copy, 1 CPU / all | $(kv bw_cp_1 "$dir/memory.txt") / $(kv bw_cp_all "$dir/memory.txt") MB/s | lmbench \`bw_mem cp\` |
| Random dependent load, 64 MiB / 1 GiB | $(kv lat_64m "$dir/memory.txt") / $(kv lat_1g "$dir/memory.txt") ns | lmbench \`lat_mem_rd -t\` |
| Misses one CPU keeps in flight, 1 GiB | $(kv mlp_1g "$dir/memory.txt") | lmbench \`par_mem\` |
| Random loads per CPU (in flight / latency) | $(calc "$(kv mlp_1g "$dir/memory.txt") / $(kv lat_1g "$dir/memory.txt") * 1000" 0) M/s | derived |
| Storage write + fsync / read past the cache | $(calc "$fio_w / 1e6" 0) / $(calc "$fio_r / 1e6" 0) MB/s | fio (\`direct=1\` read) |
| Parallel speedup of the CPUs | ${speedup}× | gzip, 1 CPU vs all |

## Input: $(calc "$gz_b / 1e6" 0) MB of CSV.gz, $(calc "$csv_b / 1e6" 0) MB of CSV

| Decoder | Seconds | Compressed MB/s |
|---|---|---|
| gzip (zlib), 1 CPU | $(calc "$gzip1") | $(calc "$gz_b / $gzip1 / 1e6" 0) |
| gzip (zlib), all CPUs | $(calc "$gzipn") | $(calc "$gz_b / $gzipn / 1e6" 0) |
| pigz, all CPUs | $(calc "$pigzn") | $(calc "$gz_b / $pigzn / 1e6" 0) |

The faster all-CPU decoder is the gunzip floor below. The build is
pure Go; on the fixture, klauspost's gunzip runs within ~15% of zlib.

## The build against its floors

| Shape | Wall s | Ingest s | Ingest cores | Ingest over gunzip floor | Build phase s | Scratch written | Storage floor s | Wall floor s | Wall over floor |
|---|---|---|---|---|---|---|---|---|---|
EOF
for shape in $(awk '{ sub(/^BenchmarkPipeline\/shape=/, "", $1); sub(/\/.*/, "", $1); print $1 }' "$dir/pipeline.txt" | sort -u); do
	re="^BenchmarkPipeline/shape=$shape/"
	wall=$(calc "$(mean "$re" ns/op "$dir/pipeline.txt") / 1e9")
	ingest=$(mean "$re" phase_downloading_s "$dir/pipeline.txt")
	icores=$(mean "$re" phase_downloading_cores "$dir/pipeline.txt")
	build=$(mean "$re" phase_building_s "$dir/pipeline.txt")
	written=$(mean "$re" write_B "$dir/pipeline.txt")
	storage=$(calc "${written:-0} / $fio_w")
	floor=$(calc "$floor_gz > $storage ? $floor_gz : $storage")
	echo "| $shape | $wall | $(calc "$ingest") | $(calc "$icores") | $(calc "$ingest / $floor_gz") × | $(calc "${build:-0}") | $(calc "${written:-0} / 1e6" 0) MB | $storage | $floor | $(calc "$wall / $floor") × |"
done

shopt -s nullglob
tops=("$dir"/topdown-*.txt)
if ((${#tops[@]})); then
	printf '\n## Where the cycles go (perf top-down level 1, P-cores)\n\n| Shape | Retiring | Bad speculation | Front-end bound | Back-end bound |\n|---|---|---|---|---|\n'
	for f in "${tops[@]}"; do
		shape=${f##*/topdown-}
		# The first value per metric: on a hybrid CPU, the P-core PMU's.
		awk -v shape="${shape%.txt}" '/%  tma_/ {
			for (i = 1; i <= NF; i++) if ($i ~ /^tma_/ && !($i in v)) v[$i] = $(i - 2)
		} END { printf "| %s | %s%% | %s%% | %s%% | %s%% |\n", shape, v["tma_retiring"], v["tma_bad_speculation"], v["tma_frontend_bound"], v["tma_backend_bound"] }' "$f"
	done
fi
