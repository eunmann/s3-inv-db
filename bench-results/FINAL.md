# Final results — what to ship after scale-bench reality check

Multi-round sweep with realistic multi-tier data at **10M-object scale**.
Every finding from the earlier rounds re-verified at this scale; those
that didn't hold up are documented below alongside the data that
killed them.

User constraints honoured:
- Correctness > ingest speed > disk size
- Final index files stay mmap-able (no compression there)
- All counters stay `uint64` (billion-object / PB-scale safety)
- No heap bloat (I3 dedicated-flush correctly designed for this would
  not bloat, but the naive version did — deferred)

## End-to-end result at 10M objects (3 runs averaged)

| Phase / Metric | Baseline | Combined | Δ |
|---|---|---|---|
| **Total wall**  | 98.7 s    | **97.2 s**   | **-1.6%** |
| Ingest wall     | 45.5 s    | 45.6 s       | flat |
| **Merge wall**  | 53.3 s    | **51.5 s**   | **-3.3%** |
| **Memory/op**   | 37.5 GB   | **32.2 GB**  | **-14.1%** |
| **Allocs/op**   | 572 M     | **551 M**    | **-3.6%** |

Throughput rose from ~101K to ~103K objects/sec. The numbers are
modest because the dominant bottleneck (gzip → sort → zstd) wasn't
touched; the wins target hotter paths within each phase.

## Ship list (verified at scale)

| # | Wire location | Isolated win | E2E impact at 10M |
|---|---|---|---|
| I2 | typed merge heap (`merger.go`) | -24% heap push/pop | merge -3.3% |
| I4 | pool zstd encoder/decoder (`zstd_pool.go`) | -68% write B/op | memory/op -14% |
| I8 | LPT manifest sort (`pipeline.go` `setupIngestConfig`) | tail wall -14% (synthetic) | not exercised by this bench (no manifest) |
| I10 | reusable `PrefixRow` in `RunReader.ReadInto` + `singleRunIterator` | merge read -33% wall, -83% B/op | merge -3.3%, allocs -3.6% |

All four pass the test suite plus the correctness pins
(`TestTierWriterBackfillAlignment`, `TestFastCSVMatchesStdlib`'s
unused-but-pinned suite, etc.).

## Dropped after re-bench

| # | Original claim | What killed it |
|---|---|---|
| I1 | clone-on-miss saves substring-pin memory | At 10M scale the 25M extra `strings.Clone` allocs cost +5 s of ingest wall — wipes out the ~1% memory benefit it actually provides on top of I4/I10. |
| I9 | hand-rolled CSV 2.7× faster than `encoding/csv` | Apples-to-apples bench (both materialize Key, both extract StorageClass) shows stdlib at **903 MB/s** vs hand-rolled at **465 MB/s** — stdlib is faster. Original win was an artifact of skipping Key materialization on the hand-rolled side. |
| I6 | 24-byte aligned header | Regressed on amd64 sequential reads. |
| slim `PrefixStats` | uint32 tier counts save 17% memory | uint32 overflows at billion-object root prefixes. **Correctness violation at user's scale.** |
| `fixed_sparse` stats | 34% smaller struct | 21% slower AddObject; net loss unless memory-bound. |

## Deferred (not in combined)

| # | Reason |
|---|---|
| I3 dedicated flush | Naive impl bloats heap → violates constraint. A correct version (subtract in-flight aggregator from visible budget) needs careful design. |
| I5 madvise hints | Warm-cache benches show no signal; need cold-cache production measurement. |
| D1 sparse+zstd run file | Sparse+zstd is the win for 1–8 tiers/row (-22-30% disk, -22-30% wall). At 8 tiers density the win shrinks; at 13 it disappears. Wiring needs a sparse+zstd reader implementing `RunReader`; deferred so the combined branch stays well-tested. |

## Correctness verification

Pin tests on `bench/combined`:

```
go test -run='TestTierWriterBackfillAlignment|TestRunFileHeaderRewriteCrashSafety|TestAggregatorRetainsSourceKey' ./pkg/extsort/
go test -run='TestFastCSVMatchesStdlib|TestFastCSVRejectsQuotes|TestFastCSVGzipStream' ./pkg/inventory/
go test -run='TestSparseRunFileRoundTrip' ./pkg/extsort/
```

All pass.

## Reproducing

```bash
# Baseline scale numbers
git checkout bench/baseline
S3INV_SCALE_BENCH=1 go test -run=^$ -bench='BenchmarkPipelineScale_E2E' \
  -benchmem -benchtime=1x -count=3 ./pkg/extsort/

# Combined
git checkout bench/combined
S3INV_SCALE_BENCH=1 go test -run=^$ -bench='BenchmarkPipelineScale_E2E' \
  -benchmem -benchtime=1x -count=3 ./pkg/extsort/

# Side-by-side comparison
go run golang.org/x/perf/cmd/benchstat@latest \
  bench-results/scale-baseline.txt bench-results/scale-combined-no-i1.txt
```

## What I would investigate next

1. **I3 with budget accounting**: explicit accounting of in-flight
   aggregator bytes so `ShouldFlush` sees only the new accumulator's
   size. This preserves the peak-heap invariant and unlocks the
   flush-overlap win (~30% on isolated bench). Roughly two days of
   work — non-trivial because the budget logic is currently tangled
   with `runtime.ReadMemStats`.
2. **D1 sparse+zstd wired into production** + a `SparseRunReader`
   that implements `RunReader`. Reader cost is the open question
   (sparse + zstd decode in one pass).
3. **End-to-end with realistic gzipped CSV input** instead of
   pre-generated objects — will surface whether I8 LPT actually
   helps on real manifests and whether anything else hides in the
   parse phase.
4. **`fixed_sparse` PrefixStats** — 34% memory at 21% AddObject
   cost. Worth wiring as a runtime-toggle if the user is memory-bound
   at the aggregator step.

## Branches

- `bench/baseline` — reference numbers + correctness pin tests
- `bench/clone-on-miss` (**I1, dropped**) — bench-only history
- `bench/typed-merge-heap` (**I2, shipped**)
- `bench/dedicated-flush` (**I3, deferred**)
- `bench/pooled-zstd` (**I4, shipped**)
- `bench/madvise-hints` (**I5, deferred**)
- `bench/header-alignment` (**I6, dropped — regressed**)
- `bench/single-pass-drain` (**I7, clarity only**)
- `bench/sort-jobs-by-size` (**I8, shipped — synthetic only**)
- `bench/csv-handrolled` (**I9, dropped**)
- `bench/pool-prefixrow-merge` (**I10, shipped**)
- `bench/tier-bitmap-runfile` (**D1, isolated bench**)
- `bench/realistic-shapes` — multi-tier bench rerun that exposed D1's real bounds
- `bench/sparse-prefix-stats` — struct-layout exploration (none shipped — correctness)
- `bench/combined` — **what to merge to main**: I2 + I4 + I8 + I10
