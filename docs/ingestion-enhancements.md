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

## Results (filled in as work lands)

(empty — populated commit by commit)
