# Inventory Pipeline Benchmark Sweep — Summary

Each finding lives on its own branch off `bench/baseline`. Pull a
branch to inspect or reproduce; raw `go test -bench` output is in
`bench-results/<branch>.txt`.

Hardware: AMD Ryzen 9 5950X (32 logical cores), Linux 6.17, Go 1.x.

Priority order (per CLAUDE.md guidance):
1. Correctness above everything
2. Faster ingestion
3. Smaller on disk

## Correctness checks (no perf delta — they're pin tests)

Committed on `bench/baseline` itself in `pkg/extsort/correctness_test.go`:

| Test | Outcome |
|---|---|
| `TestTierWriterBackfillAlignment` | PASSES — refutes the audit's "off-by-one" claim. `posCount-1` is correct because `posCount++` runs before `writeTierStats`. |
| `TestRunFileHeaderRewriteCrashSafety` | PASSES, but pins a known weakness: a crash mid-`Close()` orphans the body (header count stays 0, 2336 body bytes unreadable). |
| `TestAggregatorRetainsSourceKey` | PASSES — confirms the aggregator's prefix map pins the source key's backing array (4 prefixes from one 1062-byte key all reference it). |

## Ingestion-speed findings

| # | Branch | Effect on ingest | Notes |
|---|---|---|---|
| I1 | `bench/clone-on-miss` | Cold path: **-17% wall** (358→297 ns/op). Steady-state unchanged. Lets source key buffers be GC'd in production. | Recommended ship. |
| I2 | `bench/typed-merge-heap` | Merge heap push+pop: **-24% wall** at every k (8/32/64), **-2 allocs/op**. | Recommended ship. |
| I3 | `bench/dedicated-flush` | Flush-loop wall time **-30%** (213→150 ms) at the cost of **+58% peak heap** and **+78% allocs**. | Production rollout needs pipeline.go refactor. |
| I4 | `bench/pooled-zstd` | Compressed write **-68% B/op** (13.7 MB→4.4 MB), **-6% wall**. Compressed read **-24% B/op**, **-6% wall**. | Recommended ship. |
| I5 | `bench/madvise-hints` | Warm-cache: **no measurable change** (kernel readahead doesn't matter once pages are resident). | API added; ship behind cold-cache verification. |
| I6 | `bench/header-alignment` | **Slight regression** (+9% on sequential u64 reads). amd64 handles unaligned u64 transparently. | **Do not ship.** |
| I7 | `bench/single-pass-drain` | Within run-to-run noise. Drain time dominated by sort, not iteration. | Ship for code clarity only — not perf. |
| I8 | `bench/sort-jobs-by-size` | LPT scheduling cuts wall time **-14%** (35→30 ms) on tail-heavy job mix. | Recommended ship — one-line `sort.Slice` in `setupIngestConfig`. |
| I9 | `bench/csv-handrolled` | CSV parsing **-66% wall** (6.1→2.1 ms), **0 allocs vs 50K**. | Ship; specialise for unquoted S3 inventory rows. |
| I10 | `bench/pool-prefixrow-merge` | Run-file read **-33% wall**, **-83% B/op** when reusing a caller-owned `PrefixRow`. | Recommended ship; thread reused row through merge iterator. |

## Disk-size findings

| # | Branch | Effect on size | Notes |
|---|---|---|---|
| D1 | `bench/tier-bitmap-runfile` | Sparse-tier run-file format: 95 B/row vs 285 B/row dense (**-67%**), and 1.77× faster to write than dense uncompressed. Read is half the cost of zstd-decoded dense. | Sweet spot for single-tier buckets. Combining with zstd would beat both axes. |

## Stack-rank for shipping

Order optimised for the user's priority (correctness > ingest > disk):

1. **Ship I1** (clone-on-miss) — one-line aggregator fix, releases source-key retention in production.
2. **Ship I2** (typed merge heap) — pure perf, removes interface boxing on a hot path.
3. **Ship I9** (hand-rolled CSV) — 3× faster, zero alloc on the inventory parse hot path.
4. **Ship I10** (reused PrefixRow) — 33% faster run-file read; pair with I2 for compounding wins on merge.
5. **Ship I4** (pooled zstd) — biggest memory win, no behaviour change.
6. **Ship I8** (sort jobs by size) — one-line scheduling fix, 14% tail-wall improvement on uneven manifests.
7. **Ship D1** (sparse run-file) — disk savings + write throughput; compatible with I4 zstd if both layered.
8. **Ship I3** (dedicated flush) — requires pipeline.go refactor; defer until the simpler wins are in.
9. **Don't ship I6** (header alignment) — regression on amd64.
10. **I5** (madvise) — ship behind a real cold-cache measurement; warm-cache bench can't justify it.
11. **I7** (single-pass drain) — code clarity only, not perf.

## Files I added

```
bench-results/                    # captured raw bench output per branch
pkg/extsort/aggregator_alloc_bench_test.go   # AddObject diverse / cold / retention / drain
pkg/extsort/correctness_test.go              # tier-backfill, header crash, retention pins
pkg/extsort/merger_bench_test.go             # merge-heap push/pop
pkg/extsort/runfile_bench_test.go            # uncompressed + compressed write/read
pkg/format/header_align_bench_test.go        # u64 read + mmap open/access patterns
```

Each variant branch adds 1–2 files (implementation + bench file) and
captures its own results in `bench-results/<branch>.txt`.

## Reproducing

```bash
# Baseline numbers
git checkout bench/baseline
go test -run=^$ -bench=. -benchmem -count=3 ./pkg/extsort/ ./pkg/format/

# One variant
git checkout bench/typed-merge-heap
go test -run=^$ -bench=BenchmarkMergeHeap -benchmem -count=3 ./pkg/extsort/

# Compare files with benchstat
go run golang.org/x/perf/cmd/benchstat@latest \
  bench-results/baseline.txt bench-results/typed-merge-heap.txt
```
