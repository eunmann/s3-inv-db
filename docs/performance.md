# Performance

This page is the canonical home for memory rules, build/query
costs, on-disk size, and benchmarks.

## Query

Lookup is constant-time in index size: hash the prefix (FNV-1a),
probe the MPHF, verify a second-hash fingerprint, then read one mmap
slot. Tested up to ~10M prefixes with consistent sub-microsecond
warm-cache lookups.

| Operation | Complexity | Typical warm latency |
|-----------|------------|---------------------|
| `Lookup` | O(1) | low single-digit µs |
| `StatsForPrefix` | O(1) | low single-digit µs |
| `TierBreakdown` | O(tiers) | low single-digit µs |
| `DescendantsAtDepth` | O(log n + k) | µs–tens of µs (depends on k) |

Numbers come from `BenchmarkGridQuery` on a warm mmap; cold-cache
numbers are higher by roughly one page-fault per file touched. Run
the suite yourself:

```bash
go test -bench=BenchmarkGridQuery -benchtime=2s -run=^$ ./pkg/indexread/
```

Reads are lock-free; throughput scales linearly with cores up to
memory-bandwidth limits.

## Memory

The process memory ceiling is set at startup via
`runtime/debug.SetMemoryLimit`:

- If `GOMEMLIMIT` is set explicitly, it wins (capped only by the
  cgroup `memory.max`).
- Otherwise the limit is `min(cgroup memory.max, 0.6 × detected RAM)`.

This lets an operator opt into more than 60% of RAM on a dedicated
host while still being bounded by a tighter container cap.

The build aggregator spills when **any** of three thresholds fires:

1. The combined per-worker aggregator footprint exceeds `0.15 × GOMEMLIMIT`
   (or 512 MiB if no limit is configured), divided across N chunk workers.
2. Overall `HeapInuse` crosses 85% of the limit — a safety valve for
   when the per-worker estimate undercounts runtime reality.
3. A mid-chunk check every 50K rows — so a single very large chunk
   can't grow past the cap before its first spill opportunity.

After all inventory files are processed, a k-way merge combines run
files into the final sorted stream. Run-file write buffers
(zstd-compressed by default), the merge heap, and the index-build
streaming arrays each take bounded space outside the aggregator cap.

```bash
GOMEMLIMIT=4GiB s3-inv-db build ...   # cap at 4 GiB
s3-inv-db build --max-depth 5 ...     # limit prefix count
```

Worker counts and S3 part concurrency derive from `runtime.NumCPU()`:
parser + download workers scale to `min(NumCPU, manifest_files)`,
SDK part concurrency is `max(NumCPU/4, 2)`. The same binary scales
from a laptop to an ingest node without flags.

### Bottleneck checklist

| Symptom | Likely cause | Fix |
|---------|--------------|-----|
| Low CPU during build | S3 bandwidth limited | Run on a host with more vCPUs |
| Slow build, lots of temp files | Aggregator spilling under a tight `GOMEMLIMIT` | Raise the limit or run on a fatter host |
| Slow final build phase | Large MPHF build | Expected for >5M prefixes |
| First query slow | Cold page cache | Expected; subsequent queries fast |
| Descendant queries slow | Large subtrees | Use `DescendantsAtDepthFiltered` with `MinCount` / `MinBytes` |

Query-time RAM is just the OS page cache plus iterator stack frames;
"high memory usage" on a warm process is page cache and is correct.

## Index size

Each prefix occupies a fixed stride per file regardless of how full
its data is, so total size scales linearly with prefix count:

- `core_stats.bin`: **28 B/prefix** (`object_count + total_bytes + subtree_end + depth + max_depth_in_subtree`).
- `tier_stats/tier_stats_row.bin`: **`presentTiers × 16` B/prefix** —
  e.g. an index with 5 of 13 tiers populated is 80 B/prefix (vs the
  208 B/prefix upper bound at all 13). The build writes a dense
  208 B/prefix intermediate and finalize repacks it to the packed
  stride.
- Prefix dictionary (`prefix_dict.*`): dictionary-encoded, dominated
  by the unique-segment blob plus 4 B per segment ID per prefix plus
  8 B per offset.
- MPHF (`mph.bin` + `mph_fp_pos.u64`): ~24 B/prefix (BBHash +
  interleaved fingerprint/position pair).
- Depth index: ~8 B/prefix.

A 1M-prefix sample comes out to roughly **300 B/prefix on disk**
end-to-end. Per-file layout: [index-format.md](index-format.md).

## Server: sizing the disk budget

When auto-load is enabled, `--max-index-disk` caps the total bytes
the server will hold for materialised indexes.
`BenchmarkLoadDiskPeak` in `pkg/extsort` measures the final index
size and scratch peak for synthetic loads:

| Synthetic objects | Final index bytes | Scratch peak |
|---|---|---|
| 10 K | ~5 MB | ~7 MB |
| 100 K | ~46 MB | ~69 MB |
| 500 K | ~213 MB | ~318 MB |

These scale near-linearly with prefix count, not object count. The
planner applies a fixed `0.30` multiplier to a manifest's compressed
CSV total when estimating final index bytes — conservative, but
measure your own corpus if disk budgets are tight. Builds use
`os.TempDir()` for intermediate run files; keep that volume sized
to at least 2× the largest manifest-compressed-size you intend to
load.

## Running benchmarks

```bash
go test -bench=. -benchmem ./pkg/indexread/                          # all indexread benches
go test -bench=BenchmarkMPHFBuild -benchmem ./pkg/format/             # MPHF build
go test -bench=BenchmarkMPHFQuery -benchmem ./pkg/format/             # MPHF query
go test -bench=BenchmarkBBHashScaling -benchmem ./pkg/format/         # isolated bbhash.New
S3INV_LONG_BENCH=1 go test -bench=. -benchmem ./pkg/indexread/        # large dataset

# Memory profile
go test -bench=BenchmarkGridQuery -benchmem -memprofile=mem.out ./pkg/indexread/
go tool pprof mem.out
```
