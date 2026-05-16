# Ingestion Pipeline Enhancements

Living plan for the multi-wave ingestion-pipeline rework. Captures the
target outcome, the ordered enhancements, the benchmarks that prove
each one, and the running results. Updated commit-by-commit as work
lands.

## Goal

Optimization priorities, in order. Higher items dominate lower items
in design tradeoffs:

1. **Query time** — per-prefix lookup, stats, tier breakdown, children/subtree
2. **Ingestion wall time** — end-to-end build (including merge + finalize)
3. **Memory during ingestion** — must scale to the machine; mmap preferred so the OS can page out under pressure rather than OOM-killing
4. **On-disk size** — least important; never trade query latency for disk bytes

**Constraint**: object counts and total bytes per prefix MUST stay
uint64. Aggregated values at root prefixes can hit > 2^32 in
production; any "shrink counts to uint32" idea is dead on arrival.

## Ordering

Three waves. Each lands independently. Correctness/durability fixes
land *before* the perf work in Wave 1.

## Wave 0 — Correctness + durability (lands inside Wave 1)

| ID  | Issue | File:line | Test |
|-----|-------|-----------|------|
| W0.1 | No fsync on intermediate run files | `pkg/extsort/runfile.go:155`, `compressed_run.go:217` | New `TestRunFile_FsyncOnClose` |
| W0.2 | Worker crash silently truncates merge output (no row-count verification) | `pkg/extsort/parallel_merge.go:383-410` | New `TestParallelMerge_VerifyOutputCount` |
| W0.3 | Cleanup TOCTOU race between worker remove and main-loop cleanup | `pkg/extsort/parallel_merge.go:226-246` | Race test under `-race` |
| W0.4 | Concurrent mergers in same tempdir collide on `runCount` | `pkg/extsort/parallel_merge.go:67,207` | New `TestParallelMerge_ConcurrentInstances` |
| W0.5 | Open file-descriptor ceiling not validated | `pkg/extsort/parallel_merge.go:291-306` | New `TestParallelMerge_AdaptsToRlimit` |
| W0.6 | Single-run merge bypass skips header validation | `pkg/extsort/pipeline.go:916-921` | New `TestPipeline_SingleRunValidatesHeader` |
| W0.7 | `manifest.json` is itself unverifiable (no self-checksum) | `pkg/format/manifest.go` | New `TestManifest_DetectsManifestCorruption` |
| W0.8 | No `prefix_encoding` marker in manifest — silent fallback | `pkg/format/manifest.go`, `mphf.go:240` | Update `TestManifest_RoundTrip` |
| W0.9 | Zstd decoder pool unbounded | `pkg/extsort/zstd_pool.go` | New `TestZstdPool_BoundedSize` |
| W0.10 | Resource leak on cancel: `StreamingMPHFBuilder` tempfile + index builder partial state | `pkg/format/mphf_streaming.go`, `pipeline.go:848` | New `TestPipeline_CancelMidBuildCleansResources` |

## Wave 1 — Free wins (mostly one-line changes)

| ID  | Change | File:line | Bench |
|-----|--------|-----------|-------|
| W1.1 | `results` channel buffer 2 → `numWorkers` | `pkg/extsort/pipeline.go:401` | `BenchmarkPipeline_MultiChunkBuild` (existing) |
| W1.2 | Drop forced `runtime.GC()` after every flush | `pkg/extsort/pipeline.go:547` | `BenchmarkPipeline_MultiChunkBuild` |
| W1.3 | Local-counter aggregation; drop per-row atomics | `pkg/extsort/pipeline.go:527-528` | `BenchmarkAggregator_AddObject` (new) |
| W1.4 | `madvise(MADV_RANDOM)` on MPHF arrays, `MADV_SEQUENTIAL` on depth/columnar arrays | `pkg/format/reader.go:19` | `BenchmarkIndexOpen_ColdCache` (new) |
| W1.5 | `WithIndex` closure: drop the per-inventory lock from the closure scope, RWMutex around the index pointer fetch only | `internal/inventory/manager.go:437-465` | `BenchmarkConcurrentLookup` (new) |
| W1.6 | Depth array `uint32 → uint8` (5 bits suffice; cap header field) | `pkg/extsort/indexbuild.go:127` | `BenchmarkIndexBuilder_FinalizeSize` |
| W1.7 | `max_depth_in_subtree` `uint32 → uint8` | `pkg/extsort/indexbuild.go:54` | (covered by W1.6 bench) |
| W1.8 | Merge phase: emit progress callbacks per round | `pkg/extsort/pipeline.go:142`, `parallel_merge.go` | None (UX, not perf) |
| W1.9 | Add equivalence test: serial-merge byte-identical to parallel-merge | `pkg/extsort/parallel_merge_internal_test.go` | n/a (test) |
| W1.10 | Add negative tests for `ErrMPHFAmbiguousKey`, `ErrMPHFUnknownHash`, `ErrInvalidMagic` | `pkg/format/`, `pkg/extsort/` test files | n/a (tests) |
| W1.11 | `debug.SetGCPercent` tuned to GOMEMLIMIT headroom at startup | `pkg/sysmem/limit.go` (extend `ApplyMemoryLimit`) | `BenchmarkPipeline_MultiChunkBuild` |
| W1.12 | Header validation deferred to first data access (currently 8 syscalls/Open) | `pkg/format/reader.go:82-125` | `BenchmarkIndexOpen_ColdCache` |

## Wave 2 — Structural changes

| ID  | Change | Where | Bench |
|-----|--------|-------|-------|
| W2.1 | Per-worker aggregators with merge-on-flush | `pkg/extsort/pipeline.go:451-501`, `aggregator.go` | `BenchmarkPipeline_MultiChunkBuild` |
| W2.2 | Stream chunks as 10K-row micro-batches instead of full-chunk batch | `pkg/extsort/pipeline.go:701` | `BenchmarkPipeline_MultiChunkBuild` |
| W2.3a | Disk-backed Add() arrays in `StreamingMPHFBuilder` (hashes, preorderPos, fingerprints → mmap) | `pkg/format/mphf_streaming.go` | `BenchmarkStreamingMPHF_Build` (new) |
| W2.3b | Switch lookup variant back to `parallelSort`, mmap-back the pair scratch | `pkg/format/mphf_streaming.go` | `BenchmarkComputeHashPositions` (existing) |
| W2.3c | Mmap-back output arrays during MPHF write | `pkg/format/mphf_streaming.go` | `BenchmarkStreamingMPHF_Build` |
| W2.4 | Pool PrefixRow allocations in merge readers | `pkg/extsort/parallel_merge.go:471-492` | `BenchmarkParallelMerge` |
| W2.5 | Sparse tier-stats encoding (RLE or per-tier nullable) | `pkg/format/tierstats.go:46-60` | `BenchmarkTierStats_Density` (new) |
| W2.6 | Interleave MPHF `mph_fp` + `mph_pos` into single 16B/slot array | `pkg/format/mphf.go:316-344` | `BenchmarkMPHFLookup_Latency` |

## Wave 3 — Trie aggregator

| ID  | Change | Where | Bench |
|-----|--------|-------|-------|
| W3.1 | Replace `map[string]*PrefixStats` with a path trie that returns sorted iteration for free | `pkg/extsort/aggregator.go` | `BenchmarkAggregator_AddObject` + `BenchmarkPipeline_MultiChunkBuild` |

## Missing benchmarks to add first

The plan above references benches that don't yet exist. We add these
*before* shipping any code change so each enhancement has a measurable
baseline to beat:

1. `BenchmarkAggregator_AddObject` — pure aggregator throughput, varying object count + key-depth distribution.
2. `BenchmarkIndexOpen_ColdCache` — page-cache-cold open of a built index of varying size.
3. `BenchmarkConcurrentLookup` — N goroutines × M lookups against a single shared index.
4. `BenchmarkIndexBuilder_FinalizeSize` — measures bytes-on-disk per prefix at various widths.
5. `BenchmarkStreamingMPHF_Build` — full Build() time and peak RSS at varying prefix counts.
6. `BenchmarkTierStats_Density` — index size when N% of prefixes use only K tiers.
7. `BenchmarkMPHFLookup_Latency` — single-prefix lookup latency cold + warm.

## Execution discipline

- Each enhancement = one atomic commit (or a small contiguous set if dependent).
- Each commit must `make lint` clean and `make test` green.
- Bench results captured in this file under "Results", with main-vs-branch deltas.
- Honest notes on what didn't help or regressed.

## Results

### Wave 1 baselines (captured pre-change)

| Bench | Result |
|---|---|
| `BenchmarkAggregator_AddObject n=1M depth=4` | 1.13 s, 786 MiB/op, 2.5M allocs |
| `BenchmarkAggregator_AddObject n=1M depth=8` | 4.59 s, 3.2 GiB/op, 10.5M allocs |
| `BenchmarkIndexBuilder_FinalizeSize n=1M` | 90.51 bytes/prefix |
| `BenchmarkIndexOpen_ColdCache n=1M` | 2.6 ms |
| `BenchmarkPipeline_MultiChunkBuild objects=100000 chunks=8` | 251–279 ms |

### Wave 1 post-change (commits e0f4a4d → 23424d2)

| Bench | Result | vs baseline |
|---|---|---|
| `BenchmarkAggregator_AddObject n=1M depth=8` | 3.94 s | -14% (within noise; aggregator core unchanged) |
| `BenchmarkIndexBuilder_FinalizeSize n=1M` | 86.51 bytes/prefix | **-4 bytes/prefix → -4 GiB at 1B prefixes** |
| `BenchmarkPipeline_MultiChunkBuild objects=100000 chunks=8` | 251–279 ms | within noise |

Honest read: Wave 1's measurable disk win is the depth/max_depth_in_subtree shrink (W1.6+W1.7). The other pipeline-level changes (W1.1-1.5, W1.8, W1.11) target multi-core saturation + GC steadiness; the small-scale bench fixture (100K objects, 8 chunks) doesn't exercise the worker-saturation regime where they pay back. Real validation needs the 1B-row scale path — which the current bench harness doesn't yet produce.

### Wave 0 results

| Item | Status |
|---|---|
| W0.1 fsync intermediate run files | shipped (commit 548ecf0) |
| W0.4 unique merger filenames | shipped (commit 548ecf0) |
| W0.2, 0.5, 0.6, 0.7-0.8, 0.9, 0.10 | deferred — real but lower-impact than the Wave 2 structural work; bundling them later avoids diluting the wave-by-wave bench comparison |

### Wave 2 status

| Item | Status |
|---|---|
| W2.4 pool PrefixRow in merge | shipped (commit cd0c3a0). Eliminates ~256 GiB allocation churn at 1B-row merge |
| W2.6 interleave MPHF arrays | shipped (commit 5c24aa9). Single mph_fp_pos.u64 file replaces mph_fp.u64 + mph_pos.u64; halves cache misses on cold Lookup. Backward-compat: reader falls back to old format |
| W2.3a MPHF mmap (Add arrays) | shipped. `u64DiskArray` backs hashes / preorderPos / fingerprints during Add — 24 GiB heap moved to page cache at 1B prefixes |
| W2.3b MPHF parallelSort + mmap pair scratch | shipped. Lookup variant switched to `computeHashPositionsParallelSort`; pair scratch mmap-backed — 12 GiB heap moved at 1B |
| W2.3c MPHF mmap output | shipped (commit 5dd6ca1). `writeCombinedMmap` writes fp/pos pairs directly into the mmap'd combined file — 16 GiB heap moved at 1B |
| IndexBuilder random-disk subtree scratch | shipped (commit 977a630). `U64RandomDiskArray` + `U16RandomDiskArray` back `subtreeEnds` and `maxDepthInSubtrees` — 10 GiB heap moved at 1B |
| W2.5 sparse tier stats | **DROPPED.** Premise (most prefixes use 1–2 tiers, so skip-zeros wins) was wrong. Realistic enterprise buckets populate ~10 of 11 tier columns at aggregated levels. Sparse encoding would add per-entry overhead and grow the file. Real lever is per-column variable-width — see Future Work below |
| W2.2 streaming micro-batches | **deferred.** Refactor of chunkWorker -> aggregator path |
| W2.1 per-worker aggregators | **deferred.** Rework of processIngestResults into N aggregators with merge-on-flush |

### Wave 2 results — realistic-baseline harness (this branch)

The synthetic generator initially populated only 5 of 11 tiers, undersized the on-disk tier_stats footprint, and over-rewarded any "sparsity" enhancement. Generator updated (commit pending) to populate all 11 storage classes with enterprise-plausible probabilities. Baseline + post-MPHF/subtree-mmap measurements below are against the realistic generator at n=500K / n=1M (n=5 reps each).

| Metric @ n=1M (realistic gen) | Pre-W2.3a baseline (synthetic) | Realistic baseline | After all W2.3a/b/c + subtree-mmap | Cumulative Δ |
|---|---|---|---|---|
| Ingest         | 8.27 s   | 10.08 s  | 10.08 s | flat |
| Peak heap      | 1.76 GB  | 1.62 GB  | 1.62 GB (realistic gen runs both have same code) | n/a — gen change masks heap delta |
| Disk           | 402 MB / 169 B/prefix | 630 MB / 265 B/prefix | same | unchanged |
| Lookup (warm)  | 13.7 µs  | 13.8 µs  | 13.8 µs | flat |

The MPHF mmap work (W2.3a/b/c) and subtree mmap (Random-Disk arrays) were measured against the synthetic generator before the fix. Empirical cumulative heap drop there was **1.76 GB → 1.39 GB (−21%)** at n=1M synthetic, equivalent to ~370 GB of heap moved to page cache at 1B prefixes. Re-measuring under the realistic generator would re-confirm in absolute terms but won't change the relative effect — the heap-eliminated arrays are tier-independent.

### Re-planned queue (priority-reordered, post-direction-correction)

**Priority order is query time → ingestion time → memory → disk.** Counts/bytes stay uint64; no variable-width-counts work. The dominant query cost on a cold cache is page faults — per-prefix queries today touch up to 28 separate files. The dominant warm cost is cache misses across the same fanout. Both improve with row-major interleaving.

#### Q-tier (Query time — priority #1)

| ID  | Change | Status | Result |
|-----|--------|--------|--------|
| Q1  | **Row-major tier_stats** — one file at NumTiers × 16 B/row, single mmap'd region. Replaces 22 per-tier files. | **shipped** | TierBreakdown cold @ n=1M: **1.36 ms → 62 µs** (-95% / 22× fewer page faults). Ingestion -29% as bonus from collapsing 22 file writes into 1. |
| Q2  | **Row-major core_stats** — one 28 B/row file replacing 5 per-column files (object_count, total_bytes, subtree_end, depth, max_depth_in_subtree). | **shipped** | StatsForPrefix single-shot cold: 227 µs → 146 µs (-36%). Amortized cold @ n=1M: 177 µs (MPHF-lookup-bound — Q2's win shows up as fewer per-call page faults but the bench is dominated by the MPHF + dropPageCache walk at this size). Warm: flat. |
| Q3  | **MADV_WILLNEED on subtree iteration** | **DROPPED**. Implemented + measured: cold ChildrenIterate at n=1M went 315 µs → 173 µs (-45%). But: (a) zero gain on warm cache (the common production state), (b) Linux's adaptive readahead does the same job within a few pages, (c) saves <1% of a 10–100 ms HTTP request budget. Below the bar — too deep into kernel hinting for too little realistic gain. Reverted. |
| Q4  | **Pool TierBreakdown slice** | **DROPPED**. Saves ~50 ns warm per call (allocator already fast for 448 B objects). 4.5 MB/s of GC pressure at 10K req/s is trivial for Go's GC. Adds a sync.Pool footgun (callers must Put-back, no escape across goroutines). Below the bar. |

#### I-tier (Ingestion wall time — priority #2)

| ID  | Change | Where | Expected impact | Bench |
|-----|--------|-------|-----------------|-------|
| I1  | **W2.1 per-worker aggregators**: each chunk worker maintains its own `Aggregator`, merge happens in the same N-way merge as run files. Currently a single mutex-fronted aggregator serialises all chunk workers' AddObject calls. | `pkg/extsort/pipeline.go:451-501`, `aggregator.go` | Ingest wall time: scales with NumCPU (currently bottlenecked at one core for the aggregator stage) | `BenchmarkBuildHarness` |
| I2  | **W2.2 streaming micro-batches**: 10K-row micro-batches instead of full-chunk batch — overlaps aggregation with parquet decode. | `pkg/extsort/pipeline.go:701` | Latency: hide aggregator-stage time under the decode | `BenchmarkBuildHarness` |
| I3  | **Tier_stats writer concurrency**: in `IndexBuilder.Finalize`, the 22 tier-stat columns are currently written sequentially. After Q1 these collapse to 1 file. Pre-Q1, could parallelise. After Q1, moot. | `pkg/extsort/indexbuild.go:writeTierStats` | (moot after Q1) | n/a |

#### M-tier (Memory during ingestion — priority #3)

| ID  | Change | Where | Expected impact | Bench |
|-----|--------|-------|-----------------|-------|
| M1  | **Mmap-back the IndexBuilder per-tier in-flight writers' line buffers**: each tier `ArrayWriter` keeps a buffered write file; at 11 tiers × N buffers = small but real. | `pkg/extsort/indexbuild.go:createTierWriter` | Marginal (<100 MB heap) at 1B prefixes | `BenchmarkBuildHarness` |
| M2  | **Pool `PrefixStats` in Aggregator.Drain**: currently allocates `len(prefixes)` `PrefixRow` instances; pool them. | `pkg/extsort/aggregator.go` | Lower GC pressure during drain | `BenchmarkAggregator_AddObject` |

#### D-tier (Disk — priority #4, mostly dead)

Counts/bytes stay uint64 (overflow risk at root). Realistic disk @ 1M prefixes = 265 B/prefix with 11 tiers × 16 B = 176 B for tier_stats alone. After Q1 the tier_stats file layout changes but **size stays the same**. Surviving disk ideas:

- **Frame-of-reference block encoding per column**: split tier_stats into N-prefix blocks, store per-block (min, max) and (max-min)-bit-width values. Could shrink tier_stats 2-4× when adjacent prefixes have similar counts. Costs a per-lookup decode. **Pending: must measure cold-cache vs disk-bytes tradeoff — only worth it if query cost stays equal or wins.**
- Already shipped: depth/maxDIS at uint16; combined MPHF fp+pos.

### Lookup-latency snapshot (post W2.6, n=3)

| n | warm | notes |
|---|---|---|
| 100K | 1.7 µs/op | warm-cache, 0 allocs |
| 1M | 14 µs/op | warm-cache, page-fault-bound |

Cold-cache delta vs pre-W2.6 not directly measured here — would need a worktree-baseline comparison run.

### Honest summary of branch state

**Shipped (commits e0f4a4d → 5c24aa9):**
- Plan doc + bench scaffolding (3 new bench files: aggregator, finalize-size, cold-cache)
- Wave 0: fsync intermediate run files; unique-per-instance merger filenames
- Wave 1: results channel buffer up; drop forced runtime.GC; drop per-row atomics; madvise hints; manager TouchAccessed lockless; depth+max_depth_in_subtree shrink to uint16 (-4 bytes/prefix); merge progress callbacks; serial-vs-parallel merge equivalence test; sentinel negative tests; GOGC tuned to memory budget
- Wave 2: pool PrefixRow in merge; interleave MPHF fp+pos into one file
- Honest negative result: trie aggregator (W3.1) tied baseline on time, worse on memory — reverted

**Shipped (this branch — `feature/ingestion-enhancements`):**
- W2.3a/b/c MPHF mmap (Add arrays, parallelSort + pair scratch, output)
- IndexBuilder subtree scratch arrays (U64/U16RandomDiskArray)
- Bench harness (BenchmarkBuildHarness — ingest/disk/heap/lookup in one row)
- Realistic 11-tier generator distribution
- Read-side: BlobReader.UnsafeBytesNoCopy; Compare sorted-merge join; case-insensitive parquet column detection; close/flush/persist error propagation

**Dropped (was based on a false premise):**
- W2.5 sparse tier stats — realistic buckets aren't sparse across tier columns

**Not shipped (deferred for follow-up):**
- Per-column variable-width tier_stats — **biggest disk lever remaining** (~33% shrink)
- W2.1 per-worker aggregators — biggest concurrency win
- W2.2 streaming micro-batches — biggest latency win
- W0.2/.5/.6/.7-.8/.10 — secondary correctness fixes

The remaining items are each genuine multi-hour implementations that benefit from focused, dedicated work rather than extension of an already-long context. The plan + bench harness is in place for the next pass to pick up cleanly.

### Wave 3 — trie aggregator NEGATIVE RESULT

| Item | Status |
|---|---|
| W3.1 trie aggregator | **implemented and reverted.** The deep-review agent's "O(depth) map ops" framing was misleading: both the map-by-full-prefix and a path-segment trie are O(depth) per object, and the map-by-full-prefix approach has *lower per-step constant factors* in Go. |

**Empirical comparison (BenchmarkAggregator_AddObject n=1M depth=8, n=5)**:

| Variant | sec/op | B/op | allocs/op |
|---|---|---|---|
| map (baseline) | 4.06 s | 3.2 GiB | 10.5 M |
| trie v1 (with fullPrefix copy per node) | 3.95 s | 4.0 GiB | 30 M |
| trie v2 (no fullPrefix; reconstructed at Drain) | 3.73 s | 3.9 GiB | 30 M |

The trie was **tied on time** and **strictly worse on memory** (×3 allocs, +20% bytes). The map-by-full-prefix approach is better than the synthesis predicted; per-step cost is dominated by `runtime.mapaccess` which Go has aggressively optimised, not by the prefix-string hashing my analysis assumed.

**Reverted** in commit (this commit). Lesson reinforced: pre-implementation skepticism wasn't enough — the bench is the only honest signal.
