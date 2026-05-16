# Ingestion Pipeline Enhancements

Living plan for the multi-wave ingestion-pipeline rework. Captures the
target outcome, the ordered enhancements, the benchmarks that prove
each one, and the running results. Updated commit-by-commit as work
lands.

## Goal

Two headline metrics, both at billion-object scale:
- **Fast loading times**: build wall-clock + index Open time + per-lookup latency.
- **Small on-disk size**: bytes per prefix in the materialised index.

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

### Wave 2 in progress

| Item | Status |
|---|---|
| W2.4 pool PrefixRow in merge | shipped (commit cd0c3a0) |
| W2.6 interleave MPHF arrays | next |
| W2.5 sparse tier stats | format change, requires writer/reader/back-compat |
| W2.3a/b/c MPHF mmap | the disk-spill plan from the user; multi-step implementation |
| W2.2 streaming micro-batches | refactor of chunk worker → aggregator path |
| W2.1 per-worker aggregators | rework of `processIngestResults` |

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
