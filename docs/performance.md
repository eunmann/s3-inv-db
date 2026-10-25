# Performance

This page is the canonical home for memory rules, build/query
costs, on-disk size, and benchmarks.

## Query

Lookup is constant-time in index size: hash the prefix (FNV-1a),
probe the MPHF, check a second-hash fingerprint, then read one mmap
slot. Tested up to ~10M
prefixes with consistent sub-microsecond warm-cache lookups.

| Operation | Complexity | Typical warm latency |
|-----------|------------|---------------------|
| `Lookup` | O(1) | sub-µs |
| `StatsForPrefix` | O(1) | sub-µs (~160 ns) |
| `TierBreakdown` | O(tiers) | sub-µs (~70 ns) |
| `DescendantsAtDepth` | O(log n + k) | µs–tens of µs (depends on k) |

The figures in parentheses come from `BenchmarkGateStatsForPrefix` and
`BenchmarkGateTierBreakdown` on the 1M-object gate fixture (2.37M
prefixes) on a warm mmap; `BenchmarkGridQuery` sweeps index sizes.
Cold-cache numbers are higher by roughly one page-fault per file
touched. Run the suite yourself:

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
- Otherwise the limit is `min(0.85 × cgroup memory.max, 0.6 × detected RAM)`:
  the cgroup also charges page cache, mmap'd pages and stacks the Go
  limit doesn't count, so a soft limit at the hard cap invites the OOM
  killer.

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
files (zstd-compressed) into the final sorted stream, which the index
builder consumes directly. The final merge reads up to 64 runs at once.
Only past that do disk rounds run, merging just the smallest runs needed
in groups of up to 16, and each run is deleted as soon as it has been
merged. The merge decodes a few 4096-row batches ahead of the index
build on its own goroutine. Run-file buffers, the merge heap and the
index-build streaming arrays each take bounded space outside the
aggregator cap.

```bash
GOMEMLIMIT=4GiB s3-inv-db build ...   # cap at 4 GiB
s3-inv-db build --max-depth 5 ...     # limit prefix count
```

Worker counts and S3 concurrency derive from `runtime.NumCPU()`:
parser + download workers scale to `min(NumCPU, manifest_files)` (at
least 2), each
download fetches up to `min(max(NumCPU, 4), 16)` 16 MiB parts at once,
and GETs in flight across all downloads are capped at
`min(max(4 × NumCPU, 32), 256)`. The same binary scales from a laptop
to an ingest node without flags.

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

Every per-prefix file has a fixed stride per index, except the
multi-tier slots, which only prefixes with two or more tiers store, so
total size scales linearly with prefix count. Strides adapt to the
index: each `core_stats.bin` field, tier slot and offset or position
array is stored at the narrowest byte width its largest value needs.
Tier stats store only what a prefix's totals and tier mask don't give,
in whichever of two layouts is smaller for the index. Layouts:
[index-format.md](index-format.md).

Measured on the S3-realistic 1M-object fixture
(`benchutil.S3RealisticConfig(1_000_000)`, built through
`extsort.NewAggregator` → `Drain` → `SortPrefixRows` → `IndexBuilder` the way
`pkg/indexread`'s `BenchmarkGate*` fixture is): 2,374,251 prefixes,
max depth 7, 11 of 13 tiers present, 95% of prefixes holding one tier.

| Component | Files | B/prefix |
|---|---|---|
| Core stats | `core_stats.bin` (16-byte rows: 3 + 7 + 3 + 1 + 2) | 16.0 |
| Tier stats | `tier_stats/tier_multi.*` (multi layout) | 1.4 |
| MPHF | `mph_fp_pos.u64` (16) + `mph.bin` (BBHash, ~3.3 bits) | 16.4 |
| Prefix dictionary | `prefix_dict.ids.u32` (~5 IDs × 2 B) + `prefix_dict.prefix_off.u64` (3 B) + segment blob | 13.2 |
| Depth index | `depth_positions.u64` (3 B) + `depth_offsets.u64` | 3.0 |
| **Total** | including `manifest.json` and `tiers.json` | **49.9** |

The 16 B fingerprint/position slot is the same for every index. Tier
stats depend on how many tiers each prefix holds: nothing for an index
with one tier, up to (T − 1) slots per prefix when every prefix holds
all T tiers. The other components grow by a byte per field as counts,
sizes and prefix totals cross powers of 256. `s3-inv-db stats --profile`
shows those distributions for an index.

## Server: sizing the disk budget

When auto-load is enabled, `--max-index-disk` caps the total bytes
the server will hold for materialised indexes.
`BenchmarkLoadDiskPeak` in `pkg/extsort` measures the final index
size and scratch peak for synthetic loads:

| Synthetic objects | Final index bytes | Run-file scratch |
|---|---|---|
| 10 K | ~2.1 MB | ~0.6 MB |
| 100 K | ~18.6 MB | ~5.6 MB |
| 500 K | ~86.7 MB | ~26 MB |

The scratch column is the compressed run files only, sampled before
the merge. The index build also keeps temporary files in the same
temp directory until it finishes (the MPHF builder alone holds three
8-byte arrays per prefix), which the table does not count.

These scale near-linearly with prefix count, not object count. The
planner applies a fixed `0.30` multiplier to a manifest's compressed
CSV total when estimating final index bytes — conservative, but
measure your own corpus if disk budgets are tight. Builds use
`os.TempDir()` for intermediate run files; keep that volume sized
to at least 2× the largest manifest-compressed-size you intend to
load.

## Running benchmarks

Ingestion and query changes are gated by an A/B comparison against the
base commit on the same machine — `make bench-ingest` / `make
bench-query`; see [bench/README.md](../bench/README.md) for when to run
which. The commands below are for
exploration.

```bash
go test -bench=. -benchmem ./pkg/indexread/                          # all indexread benches
go test -bench=BenchmarkMPHFBuild -benchmem ./pkg/format/             # MPHF build
go test -bench=BenchmarkMPHFQuery -benchmem ./pkg/format/             # MPHF query
go test -bench=BenchmarkBBHash -benchmem ./pkg/format/               # BBHash build + find alone
S3INV_LONG_BENCH=1 go test -bench=. -benchmem ./pkg/indexread/        # large dataset

# Memory profile
go test -bench=BenchmarkGridQuery -benchmem -memprofile=mem.out ./pkg/indexread/
go tool pprof mem.out
```
