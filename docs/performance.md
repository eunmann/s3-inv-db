# Performance

## Query Performance

### Lookup Latency

Numbers below come from `BenchmarkLookup`, `BenchmarkStats`,
`BenchmarkTierBreakdown`, `BenchmarkDescendantsAtDepth` on a warm
mmap with a 1M-prefix fixture index. Cold-cache numbers are higher
by roughly one page-fault per file touched; the cold-query suite
(`pkg/indexread/cold_*_bench_test.go`) measures those separately.

| Operation | Complexity | Typical warm latency |
|-----------|------------|---------------------|
| `Lookup` | O(1) | low single-digit µs |
| `StatsForPrefix` | O(1) | low single-digit µs |
| `TierBreakdown` | O(tiers) | low single-digit µs |
| `DescendantsAtDepth` | O(log n + k) | µs–tens of µs (depends on k) |

Run the benches yourself for current numbers on your machine:

```bash
go test -bench='Lookup|Stats|TierBreakdown|DescendantsAtDepth' -benchtime=2s -run=^$ ./pkg/indexread/
```

Lookup performance is dominated by:
1. Two hash computations (FNV-1a + FNV-1)
2. BBHash query
3. Fingerprint comparison
4. Memory-mapped array access

### Scaling

Query latency is largely independent of index size due to O(1) MPHF lookups. The memory-mapped design means working set depends on access patterns, not total index size.

Tested up to 10M prefixes with consistent sub-microsecond lookups.

### Concurrent Access

The `Index` type is lock-free for reads. Concurrent query throughput scales linearly with CPU cores up to memory bandwidth limits.

## Build Performance

Build performance depends on:
- S3 download bandwidth
- CSV/Parquet parsing throughput
- Memory budget (affects flush frequency)
- Number of unique prefixes

### Memory limit

The process memory ceiling is set via `runtime/debug.SetMemoryLimit`
at startup. Resolution is `min(GOMEMLIMIT env, cgroup v2 memory.max,
0.6 × detected RAM)` — whichever candidate is smallest binds, so a
permissive `GOMEMLIMIT` can't override a tighter container cap.

Override the limit by exporting `GOMEMLIMIT=4GiB` (etc.) or running
under a constrained cgroup. The aggregator's spill threshold is
`min(512 MiB, 0.15 × GOMEMLIMIT)`; it also spills when overall
heap-in-use exceeds 85% of the limit.

### Tuning

```bash
# Cap a build at 4 GiB
GOMEMLIMIT=4GiB s3-inv-db build ...

# Limit depth to reduce prefix count
s3-inv-db build --max-depth 5 ...
```

Worker counts and S3 part concurrency derive from `runtime.NumCPU()`:
parser + download workers each scale to `min(NumCPU, manifest_files)`,
SDK part concurrency is `max(NumCPU/4, 2)`. The same binary scales
linearly from a laptop to an ingest node without flags.

### Build Phases

1. **Download & Parse**: Bottlenecked by S3 bandwidth and CPU for CSV parsing
2. **Aggregation**: Bounded by memory budget, flushes when threshold reached
3. **Merge**: I/O bound, reads/writes temporary files
4. **Index Build**: CPU bound for MPHF construction

## Running Benchmarks

### Query Benchmarks

```bash
# Run all indexread benchmarks
go test -bench=. -benchmem ./pkg/indexread/

# Run specific benchmark
go test -bench=BenchmarkLookup -benchmem ./pkg/indexread/

# Run with larger dataset (requires S3INV_LONG_BENCH=1)
S3INV_LONG_BENCH=1 go test -bench=. -benchmem ./pkg/indexread/
```

### MPHF Benchmarks

```bash
# Build performance at different scales
go test -bench=BenchmarkMPHFBuild -benchmem ./pkg/format/

# Query performance
go test -bench=BenchmarkMPHFQuery -benchmem ./pkg/format/

# Full pipeline benchmark
go test -bench=BenchmarkFingerprintPipeline -benchmem ./pkg/format/
```

### Memory Profiling

```bash
# Profile memory allocations
go test -bench=BenchmarkLookup -benchmem -memprofile=mem.out ./pkg/indexread/
go tool pprof mem.out
```

## Index Size

Index size scales approximately linearly with prefix count. The
row-major formats use a fixed stride per prefix regardless of
how many tier slots actually carry data, so size is predictable:

- `core_stats.bin`: **28 B/prefix** (object_count + total_bytes + subtree_end + depth + max_depth_in_subtree).
- `tier_stats/tier_stats_row.bin`: **`NumTiers × 16` B/prefix** — 208 B/prefix at the current 13 tiers — even when most slots are zero.
- `prefix_blob.bin` + `prefix_offsets.u64`: variable, dominated by prefix string lengths (avg ~30 B in typical workloads) + 8 B per offset.
- MPHF (`mph.bin` + `mph_fp_pos.u64`): ~24 B/prefix (BBHash + interleaved fingerprint/position pair).
- Depth index (`depth_offsets.u64` + `depth_positions.u64`): ~8 B/prefix.

At a 1M-prefix sample workload this comes out to roughly **300 B/prefix on disk** end-to-end. See `docs/index-format.md` for the per-file layout.

## Memory Usage

### Build Phase

Aggregators are sized off the process memory limit
(`min(GOMEMLIMIT env, cgroup memory.max, 0.6 × detected RAM)` —
applied at startup via `runtime/debug.SetMemoryLimit`). The cap on
the combined worker aggregator footprint is
`min(512 MiB, 0.15 × GOMEMLIMIT)` and is split evenly across N
chunk workers. Any worker also force-spills when overall
HeapInuse crosses 85% of the limit — the safety valve for cases
where the aggregator footprint estimate undercounts runtime
reality.

Run-file write buffers (zstd-compressed by default), the K-way
merge heap, and the index-build streaming arrays each take
bounded space outside the aggregator cap; they collectively fit
under the remaining headroom even at multi-GiB limits.

### Query Phase

Query-time memory is minimal:
- File descriptors for mmap'd files
- OS page cache for accessed regions
- Stack allocations for iterators

The OS manages page cache automatically. Frequently accessed index regions stay resident; cold regions are paged out.

## Bottleneck Analysis

### Build Bottlenecks

| Symptom | Likely Cause | Fix |
|---------|--------------|-----|
| Low CPU usage | S3 bandwidth limited | Run on a host with more vCPUs (workers scale with `NumCPU`) |
| High memory, slow | Too many unique prefixes | Use `--max-depth` |
| Lots of temp files | Aggregator spilling often under a tight `GOMEMLIMIT` | Raise the limit or run on a fatter host |
| Slow final phase | Large MPHF build | Expected for >5M prefixes |

### Query Bottlenecks

| Symptom | Likely Cause | Fix |
|---------|--------------|-----|
| First query slow | Cold page cache | Expected, subsequent queries fast |
| Descendant queries slow | Large subtrees | Use iterator API |
| High memory usage | OS caching full index | Expected behavior, safe |

## Best Practices

1. **Size memory budget appropriately**: 50% of RAM works well for dedicated build servers
2. **Use `--max-depth` for large buckets**: Limits prefix explosion from deep hierarchies
3. **Pre-warm for latency-sensitive queries**: Read index files sequentially to populate page cache
4. **Use iterators for large result sets**: Avoids allocating million-element slices
5. **Monitor temp disk usage**: External sort needs 2-3x index size in temp space

## Server: sizing the disk budget

When auto-load is enabled, `--max-index-disk` caps the total bytes the
server will hold for materialised indexes. `BenchmarkLoadDiskPeak` in
`pkg/extsort` measures the final index size and scratch peak for a
range of object counts; representative numbers from a single run:

| Synthetic objects | Final index bytes | Scratch peak |
|---|---|---|
| 10 K | ~5 MB | ~7 MB |
| 100 K | ~46 MB | ~69 MB |
| 500 K | ~213 MB | ~318 MB |

These scale near-linearly with prefix count, not object count, so
realistic billion-object inventories with deep paths land closer to
~430–510 bytes per object on disk. Use `--index-ratio` to refine the
multiplier the planner applies to a manifest's compressed CSV total
when estimating final index bytes (default `0.30` is a conservative
seed — measure your own corpus). Keep `--scratch-dir` on a volume
with at least 2× the expected manifest-compressed-size of the largest
inventory you intend to load.
