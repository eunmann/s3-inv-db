# s3-inv-db Pipeline Plan

Forward-looking plan for the ingest + query pipeline. Restructured
2026-05-16 around the canonical algorithm + user-stated priorities.
Historical wave-based structure is in git history (see commits
prior to this rewrite).

---

## Purpose

Ingest S3 inventory files (CSV / Parquet, potentially billions of
objects) and produce a queryable on-disk index. Two queries must be
as fast as possible:

A. **Prefix + stats** — `StatsForPrefix(prefix string)` → object
   count + total bytes (+ tier breakdown on demand).
B. **Children + their stats** — Browse a parent prefix; for each
   direct child return its name, object count, total bytes, and
   (typically) tier breakdown.

---

## Canonical Pipeline

```
                                                   N independent workers
                                                 ┌──────────────────────┐
        S3                ┌─[chunk 1]──parse──agg│  (per-worker map)    │──spill──→ run files
inventory ──manifest──→  ─┼─[chunk 2]──parse──agg│  (per-worker map)    │──spill──→ run files
                          └─[chunk N]──parse──agg│  (per-worker map)    │──spill──→ run files
                                                 └──────────────────────┘
                                                            │
                                                  K-way parallel merge
                                                  (round-based fan-in)
                                                            │
                                                  one sorted prefix stream
                                                            │
                                              streaming MPHF + columnar indexes
                                                            │
                                                  on-disk, mmap-friendly
                                                            │
                                              read-side O(1) mmap queries
```

Invariants:
- Per-worker aggregation; nothing shared across workers
- Worker spills to disk when its in-memory budget is exceeded
- Final merge step produces ONE sorted stream that feeds the index build
- MPHF + index file writes are streamed (no big in-memory accumulators)
- Files may be grouped for locality but stay columnar + mmap-friendly
- A pub-sub event bus emits structured events at every stage so
  observability and benchmarking are trivial
- No worker should sit idle while there is work to do upstream

---

## Optimization Priorities (immutable)

1. **Query time** — A and B above
2. **Ingestion wall time** — end-to-end Pipeline.Run
3. **Memory during ingestion** — must scale; mmap is preferred so the
   OS can page out under pressure
4. **On-disk size** — least important; never trade query latency
   for disk bytes

## Hard Constraints

- Counts AND total bytes per prefix MUST stay uint64 (root-prefix
  aggregates exceed uint32 in production).
- No combining files in a way that loses semantic info. Grouping
  for cache locality is fine; conflating fields is not.
- No backward compatibility needed. Free license to remove legacy
  paths, change formats, drop shims. Simplification is a goal.
- Iterative progress > big-bang rewrites.

---

## Pipeline State (canonical vs current)

| # | Stage | Canonical intent | Current state | Status |
|---|---|---|---|---|
| 1 | Stream chunks from S3 | streamed | ✅ `reader.Next()` row-by-row | ✅ done |
| 2 | Per-worker aggregation | per-worker, nothing shared | ✅ post-I1, each worker owns its `Aggregator` | ✅ done |
| 3 | Worker spills on budget | per-worker budget | ⚠️ Spill triggered by **global** `HeapAlloc` check (`aggregator.go:171-190`). One worker's growth makes ALL workers flush | ❌ gap (I5) |
| 4a | K-way parallel merge | parallelized | ✅ round-based fan-in (`parallel_merge.go:214-277`) | ⚠️ not compared to alternatives (I7) |
| 4b | Merge → Build seam | streamed | ⚠️ **Full disk round-trip on final merged file.** `MergeAll` writes the final run file; `OpenRunFileAuto` reads it back (`pipeline.go:850`) | ❌ gap (I3) |
| 5a | MPHF input streamed from disk | streamed | ✅ hashes / preorderPos / fingerprints on disk via `u64DiskArray` | ✅ done |
| 5b | Index file writes streamed | streamed | ✅ core_stats, tier_stats_row, prefix_blob streamed. ❌ **DepthIndex built in-memory** (`depthindex.go:10-31` `buckets map[uint32][]uint64`, est. 1-2 GiB heap at 1B prefixes) | ❌ gap (I4) |
| 6 | Read-side O(1) mmap | O(1) | ✅ core_stats / tier_stats / fp+pos. ⚠️ Lookup gated on MPHF; DepthIndex range queries are O(log N) within range (acceptable) | acceptable |
| 7 | Pub-sub event model | structured events | ❌ Only `OnProgress(phase, done, total)` + atomic counters + periodic logger | ❌ gap (E1-E3) |
| 8 | All workers always working | no avoidable idle | ⚠️ Workers can block on `jobs` channel when producer can't keep up. **No utilization measurement exists** | ❌ gap (audit) |

---

## Forward Queue

Ranked by priority order (query → ingest → memory → disk). Each
item lists: change, where, expected impact, measurement.

### Q — Query path (priority #1)

| ID | Status | Result |
|---|---|---|
| Q5 ✅ | shipped (`BenchmarkBrowse_Matrix`) | Browse warm @ n=1M = ~294 µs / 515 KB / 2168 allocs per call |
| Q6 ✅ | shipped (`BenchmarkPrefixString_Matrix`) | Raw warm @ n=1M = ~90 ns/call. Used by Q7 |
| Q7 ✅ | **decision shipped — segmented dropped entirely.** ~850 LOC removed | Raw blob is the only prefix encoding. -1 dispatch branch from every read. Tests + lint clean |
| Q8 | open (research only) | At 1B prefixes Lookup working set won't fit in RAM. Investigation pending |

### I — Ingestion wall time (priority #2)

| ID | Status | Result |
|---|---|---|
| I3 ✅ | shipped — `MergeAllToIterator` pipes K-way merge into IndexBuilder | Merge→Build seam @ n=1M, 8 runs: **4.00 s → 2.95 s (-26%)**; eliminates final-file disk round-trip (~150 GB I/O saved at 1B) |
| I4 ✅ | shipped — `DepthIndexBuilder` per-depth `u64DiskArray` | Peak heap @ n=1M: **1.613 GB → 1.533 GB (-80 MB, -5%)**; linear to **-80 GB at 1B prefixes** |
| I5 ✅ | shipped — `ShouldWorkerFlush` (per-worker budget) | Replaces global HeapAlloc trigger; each worker decides independently |
| I6 = E1 ✅ | shipped (event bus is foundational, not its own perf item) | See E1 below |
| I7 | open (research) | K-way merge strategy comparison not yet performed |

### E — Instrumentation (cross-cutting, enables benchmarking)

| ID | Status | Result |
|---|---|---|
| E1 ✅ | shipped (`pkg/extsort/events/`) | Pub-sub bus, never blocks publishers, zero-cost when nobody subscribes. Wired into Pipeline.Run / chunkWorker / streamChunkIntoAggregator / flushAggregator / ParallelMerger. Tests + concurrency stress validated |
| E2 ✅ | shipped (via E1 worker_idle/busy events) | Workers emit state transitions on jobs-channel wait + chunk processing; subscribers can compute utilization in real time |
| E3 ✅ | shipped (via E1 batch_committed events) | Each stage emits row+byte commits; subscribers compute throughput per stage |

### M — Memory during ingestion (priority #3)

| ID | Change | Where | Expected impact | Measurement |
|---|---|---|---|---|
| M3 | Audit `PrefixStats` struct shape (~288 B/entry per comment). Consider sparse-tier representation in-memory (most prefixes won't touch all 13 tier slots even though the on-disk row format is dense) | `pkg/extsort/aggregator.go` | Bigger per-worker aggregator capacity at the same heap budget → fewer flushes | `BenchmarkAggregator_AddObject` + heap delta |
| M4 | Per-worker memory metrics emitted via E1 | aggregator instrumentation | Visibility into spill timing | Read from event stream |

### D — Disk (priority #4, mostly closed)

Counts AND bytes stay uint64 by constraint. The remaining ideas
that don't violate constraints:

| ID | Change | Status |
|---|---|---|
| D-old1 | Per-column variable-width tier_stats | ❌ dropped — violates "counts/bytes uint64" |
| D-old2 | Frame-of-reference block encoding per column | ❌ dropped — adds query-time decode cost; violates priority order |
| D-old3 | Default segmented prefix encoding | ⚠️ tied to Q5/Q6/Q7 outcome (query cost vs disk saving) |

### S — Simplification (per "no back-compat needed")

| ID | Status | Notes |
|---|---|---|
| S0 = Q7 ✅ | shipped (segmented removed) | ~850 LOC + an entire dispatch dimension gone |
| S1 | open — legacy per-tier columnar `TierStatsReader` shim still present | Reader probes for row file; falls back. Removal pending |
| S2 | open — legacy per-column core stats readers still in `pkg/indexread/index.go` | Same probe-and-fallback shape |
| S3 | open — legacy MPHF non-combined fp/pos branch still in `pkg/format/mphf.go` | Same |
| S4 | open — `LookupWithVerify` retained; need to grep callers to confirm dead |
| S5 | open — `MPHFBuilder` + `StreamingMPHFBuilder` duplication | Keep streaming, drop the in-memory variant |
| S6 | open — W0.2/.5/.6/.9/.10 correctness items | Each small, can bundle |

### Cleanups

| ID | Change |
|---|---|
| C1 | Delete `objectRecord` type if only used by the test stub (post-I2) — verify by grep |
| C2 | Consolidate cleanup patterns across `Aggregator`, `IndexBuilder`, `StreamingMPHFBuilder` (currently 3 different error-path shapes) |

---

## Bench Matrix (your explicit ask)

Every measurement-driven enhancement above must run against a
benchmark that covers the configuration matrix. Current benches
cover individual dimensions; nothing covers them together.

### Configuration dimensions

| Dimension | Values | Configurable via |
|---|---|---|
| Worker count | 1, 4, 16, `NumCPU` | `ingestConfig.numWorkers` |
| Prefix encoding | raw, segmented | `IndexBuilder useSegmentEncoding bool` |
| Run-file compression | off, on | `Config.UseCompressedRuns` |
| Compression level | `CompressionFastest`, default | `CompressedRunWriterOptions.CompressionLevel` |
| Object count | 500K, 1M, 10M (gated 100M / 1B) | `S3INV_HARNESS_SIZES`, `S3INV_LONG_BENCH` |
| Merge fan-in | 4, 8, 16 | `Config.MaxMergeFanIn` |
| Merge workers | 1, 4, NumCPU | `Config.NumMergeWorkers` |
| GOMEMLIMIT | 2 GiB, 8 GiB, off | env / `debug.SetMemoryLimit` |
| Cache state | cold, warm | `dropPageCache` before measure |
| Tier distribution | realistic 11-tier (default) | `benchutil.S3RealisticConfig` |

### New bench targets

| Bench | What it measures | Matrix axes |
|---|---|---|
| `BenchmarkPipelineMatrix` | End-to-end Pipeline.Run wall time | workers × encoding × compression × size |
| `BenchmarkBrowseMatrix` | Browse(parent) end-to-end | encoding × cold/warm × depth × child_count |
| `BenchmarkPrefixString` | Per-call PrefixString latency | encoding × cold/warm × depth |
| `BenchmarkPhaseTimings` | Per-phase ms via E1 event bus | size × encoding |
| `BenchmarkWorkerSpillIndependence` | Verify per-worker spill (post-I5) | workers × budget |
| `BenchmarkMergeStrategy` | Round-fan-in vs alternatives | strategy × workers × fan-in |
| `BenchmarkEventBus_Overhead` | Cost of E1 events | events/sec × subscriber count |

### Existing benches to keep (no rewrite needed)

`BenchmarkBuildHarness`, `BenchmarkAggregationConcurrency_SharedVsPerWorker`,
`BenchmarkQueryScale_{Lookup,StatsForPrefix,StatsByPos,Depth}`,
`BenchmarkQueryScale_{StatsForPrefix_Cold,TierBreakdown_Cold,ChildrenIterate_Cold}`,
`BenchmarkCompressedVsUncompressed`, `BenchmarkExtsortEndToEnd_Scaling`,
`BenchmarkFingerprintPipeline_1M/5M/10M`.

### Existing benches to retire

| Bench | Reason |
|---|---|
| Any bench that hardcodes a single worker count, encoding, or compression mode without parametrization | Doesn't honour the matrix |
| `BenchmarkTierStats_Density` (W2.5 era) | W2.5 dropped |

(Audit + retire as part of the matrix work.)

---

## Implementation Methodology

A systematic, reproducible approach. Every enhancement follows the
same shape so results compare cleanly across commits.

### Phase 0 — Instrumentation foundation (one-time)

Before any optimization work, land **E1 (event bus)**. Every later
phase depends on it for phase-timing measurements. Without it, we
can't honestly attribute wall-time changes to specific stages.

### Phase 1 — Benchmark matrix (one-time)

Land the new bench targets above with their matrix harness. Each
bench:
- Accepts size / config via env vars (`S3INV_*`) so CI and local
  runs share a single source of truth
- Uses fixed RNG seeds for input generation (already true in
  `benchutil`)
- Reports the same metrics across all axes (ingest_ns, peak_heap_B,
  disk_B, per_query_ns) for direct comparability
- Writes JSON artifacts to a known location so `benchstat` / custom
  diff tooling can post-process

### Phase 2 — Implementation cycles

For each enhancement (Q5, I3, I4, I5, etc.):

1. **Pre-bench**: run the relevant bench at the current state. Save
   output to `bench/before/<id>.txt` (or JSON). Tag commit.
2. **Stated success criterion**: written before implementation.
   E.g. "I3 must reduce 1M-object Pipeline.Run wall time by ≥15%
   with no regression in disk size or peak heap." If you don't
   know the threshold, write the bench first and use its noise
   floor as the threshold.
3. **Implement** in one atomic commit (or small contiguous set if
   the change spans multiple files atomically). Include the bench
   delta in the commit message.
4. **Post-bench**: re-run the same bench. Save to `bench/after/<id>.txt`.
   Run `benchstat before/<id>.txt after/<id>.txt`.
5. **Verify**: tests green, lint clean (`make test && make lint`).
6. **Decide**:
   - If success criterion met: ship and update the plan with the
     measured delta.
   - If not met: revert and write a NEGATIVE RESULT entry below.
7. **Update the plan**: mark the item ✅ done with measured impact.

### Phase 3 — Verification

After landing a cluster of changes, run the full bench matrix and
diff against an earlier-tagged baseline. Confirms no cross-axis
regression (e.g., a query-time win that secretly costs ingest).

### Reproducibility checklist

- [ ] Bench RNG seeded (use `benchutil` defaults)
- [ ] Bench takes size via env var (`S3INV_HARNESS_SIZES`, etc.)
- [ ] Bench reports metrics that comparable across runs
- [ ] Bench output captured to file artifact
- [ ] Commit references both before/after artifacts
- [ ] `benchstat` (or equivalent) diff in commit message
- [ ] No commit ships without quantitative delta

---

## Shipped (this branch, condensed)

Status as of HEAD `74e32f6`. Detailed commit messages in git history.

### Query path
- ✅ **Q1** row-major tier_stats — TierBreakdown cold 1.36 ms → 62 µs at n=1M (9× faster; 22× fewer page faults). Ingest -29% bonus.
- ✅ **Q2** row-major core_stats — 5 per-column files → 1 file; cold StatsForPrefix single-shot 227 µs → 146 µs.

### Ingest path
- ✅ **I1** per-worker aggregators — 7.1× aggregation-phase speedup at 32 workers (1040 ms → 146 ms @ 500K).
- ✅ **I2** stream chunk into aggregator — eliminates intermediate `[]objectRecord` slice (~1 GB transient heap saved at 32 workers × 1M-row chunks).

### Memory (mmap-backed scratch arrays)
- ✅ **W2.3a** MPHF Add arrays disk-backed — 24 GiB heap moved to page cache at 1B prefixes.
- ✅ **W2.3b** parallelSort lookup + mmap pair scratch — 12 GiB at 1B.
- ✅ **W2.3c** mmap-direct MPHF output write — 16 GiB at 1B.
- ✅ IndexBuilder subtree scratch arrays (`U64`/`U16RandomDiskArray`) — 10 GiB at 1B.

### Format / read-side
- ✅ **W2.6** interleave MPHF fp+pos into one file — single page touch covers both.
- ✅ **D6** BlobReader `UnsafeBytesNoCopy` — zero-copy hot path.
- ✅ **D4** Compare uses O(n+m) sorted-merge join.

### Bench harness
- ✅ `BenchmarkBuildHarness` — unified ingest/disk/heap/lookup row.
- ✅ Realistic 11-tier S3 storage class distribution in generator.
- ✅ Cold-cache query bench suite (with amortised dropPageCache).

### Wave 0/1 (older work)
- ✅ W0.1 fsync intermediate run files (`commit 548ecf0`)
- ✅ W0.4 unique merger filenames (`commit 548ecf0`)
- ✅ W1.6/W1.7 depth + max_depth_in_subtree shrunk to uint16
- ✅ W1.1-W1.5, W1.8-W1.12 (results channel sizing, GC tuning, madvise hints, etc.)
- ✅ W2.4 pool PrefixRow in merge

---

## Negative Results (institutional memory)

| Item | Why dropped |
|---|---|
| **W2.5 sparse tier stats** | Premise (most prefixes use 1–2 tiers) is false. Realistic enterprise buckets populate ~10 of 11 tier columns at aggregated levels. Sparse encoding would add per-entry overhead and grow the file. |
| **W3.1 trie aggregator** | Implemented + reverted. Tied on time, ×3 allocs, +20% bytes vs the map. Go's mapaccess is too well-optimized for a path-segment trie to beat it. |
| **Q3 MADV_WILLNEED on subtree iteration** | Implemented + reverted. 142 µs cold gain at 1M (-45%) but: zero warm gain, kernel adaptive readahead does the same job within a few pages, <1% of HTTP request budget, adds API surface callers must remember. |
| **Q4 sync.Pool for `TierBreakdown` slice** | Dropped without implementing. ~50 ns warm gain vs sync.Pool footgun (Put-back semantics, no escape across goroutines). |
| **Per-column variable-width tier_stats** | Violates "counts/bytes stay uint64" constraint. |
| **Frame-of-reference block encoding** | Adds query-time decode cost; violates priority order (disk < query). |

---

## How to use this plan

- **Adding work**: pick the lowest unstamped ID in the highest-priority
  section that fits the next session. Write the bench before the
  implementation per Phase 2 above.
- **Closing work**: edit the row in-place. Move shipped items to
  the "Shipped" appendix with a one-line measured impact.
- **Killing work**: move to "Negative Results" with the honest
  reason. Don't silently delete.
- **New ideas**: add a new row in the relevant priority section with
  expected impact + measurement plan. Don't implement until that
  plan is written.
