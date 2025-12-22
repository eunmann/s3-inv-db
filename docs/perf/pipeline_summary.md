# Pipeline Performance Summary

This document summarizes the key findings from the per-stage performance analysis and provides a prioritized optimization backlog.

## Pipeline-Wide Bottlenecks

### 1. Memory Pressure During Aggregation

**Stages affected:** 2, 3, 4

The aggregation stage (Stage 4) is the primary memory consumer, using ~288 bytes per unique prefix. Combined with inaccurate memory accounting in the download stage (estimates 8MB per chunk, actual usage is 128MB+), the system can experience memory pressure earlier than expected.

**Impact:** Premature flushes create more run files, increasing merge I/O and total processing time.

**Root causes:**
- Fixed-size tier arrays (192 bytes) even when tier tracking is disabled
- Download memory underestimation by 16x
- No backpressure from aggregator to download workers

### 2. Single-Threaded MPHF Construction

**Stages affected:** 6

The bbhash library used for MPHF construction is single-threaded, taking ~0.5-1 second per 1M prefixes. For large inventories (50M+ prefixes), this becomes a significant bottleneck.

**Impact:** CPU cores sit idle while MPHF builds.

**Root cause:** Library limitation in `github.com/relab/bbhash`

### 3. Intermediate File I/O

**Stages affected:** 5, 6

Multi-round merge writes and reads intermediate files. The final merged run file is written to disk and then immediately read back for index building.

**Impact:** Unnecessary disk I/O, especially on slower storage.

**Root causes:**
- Fixed 8-way merge fan-in regardless of memory availability
- No streaming path from merge to index build

### 4. Gzip Decompression for CSV

**Stages affected:** 3

Standard library gzip is single-threaded, often becoming the bottleneck for CSV parsing.

**Impact:** Underutilized CPU during CSV chunk processing.

**Root cause:** Using `compress/gzip` instead of parallel alternatives

---

## Inter-Stage Issues

### Download → Aggregator Memory Mismatch

**Issue:** Stage 2 (Download) estimates 8MB per chunk for memory budgeting. Actual memory usage per in-flight chunk is ~128MB (S3 Download Manager buffers + temp file).

**Consequence:** The pipeline may allow 16x more concurrent downloads than intended, causing memory spikes before the aggregator's heap threshold triggers a flush.

**Fix:** Accurate memory accounting in `pipeline.go:268-271` and `pipeline.go:296-300`.

### Parse → Aggregator String Copying

**Issue:** Stage 3 (Parse) produces `InventoryRow` with string fields. Stage 4 (Aggregator) uses these strings as map keys, requiring the strings to be retained.

**Consequence:** Cannot pool or reuse string allocations across rows.

**Observation:** This is inherent to Go's string semantics. Using `[]byte` keys would require `map[string]*Stats` to become `map[unsafeString]*Stats` or similar, adding complexity.

### Aggregator → Merge Run File Volume

**Issue:** More frequent flushes in Stage 4 create more run files for Stage 5 to merge.

**Consequence:** Each run file adds overhead (open, buffer, heap entry). Many small runs are less efficient than fewer large runs.

**Mitigation:** Rec-4 in Stage 5 (pre-merge small runs) addresses this.

### Merge → Index Build File Pass-Through

**Issue:** Stage 5 writes a final merged run file, then Stage 6 reads it back.

**Consequence:** For single-round merges, this is pure overhead (~2x I/O for merged data).

**Fix:** Rec-5 in Stage 5 (streaming merge to index) eliminates this for single-round cases.

### Parquet Double Temp File

**Issue:** Stage 2 downloads to temp file. Stage 3 (Parquet) copies to a second temp file for random access.

**Consequence:** ~100MB extra I/O per Parquet chunk, plus doubled temp disk usage.

**Fix:** Rec-1 in Stage 3 (use ReaderAt interface already implemented in `tempFileReader`).

---

## End-to-End Suggestions

### E2E-1: Memory Budget Coordination

**Current state:** Each stage has independent memory controls:
- Download: `maxConcurrentChunks` (but memory per chunk is underestimated)
- Aggregator: `heapThreshold` (but doesn't account for in-flight downloads)
- Merge: `MaxFanIn` and `BufferSize`

**Suggestion:** Implement a global memory budget that coordinates across stages:

```go
type MemoryBudget struct {
    total       int64
    reserved    int64
    mu          sync.Mutex
}

func (b *MemoryBudget) Reserve(bytes int64) bool {
    b.mu.Lock()
    defer b.mu.Unlock()
    if b.reserved + bytes > b.total {
        return false
    }
    b.reserved += bytes
    return true
}
```

This allows:
- Download workers to wait when memory is tight
- Aggregator to flush proactively when merge needs memory
- Merge fan-in to adapt to available memory

**Priority:** Medium
**Complexity:** High (coordination across stages)

### E2E-2: Streaming Pipeline Mode

**Current state:** Pipeline has clear stage boundaries with intermediate files.

**Suggestion:** For "fast path" scenarios (few chunks, single-round merge possible), implement streaming:

```
Download → Parse → Aggregate → [Direct Iterator] → Index Build
```

This eliminates:
- Run file writes entirely (if memory permits full aggregation)
- Merged run file write/read (if single-round merge)

**Priority:** Low (complex, limited use cases)
**Complexity:** High

### E2E-3: Progress and Metrics Unification

**Current state:** Each stage has its own progress reporting (callbacks, metrics).

**Suggestion:** Unified metrics collector that tracks:
- Bytes read/written per stage
- Time in each stage
- Memory high-water marks
- Bottleneck identification

This aids in:
- Performance debugging
- Capacity planning
- Automated tuning

**Priority:** Low
**Complexity:** Low

---

## Optimization Backlog

### High Priority

| ID | Stage | Title | Impact | Risk | Area |
|----|-------|-------|--------|------|------|
| S2-R1 | Download | Accurate memory accounting | Fix 16x underestimation | Low | Memory |
| S3-R1 | Parse | Avoid double temp file (Parquet) | 10-20% faster Parquet chunks | Low | I/O |
| S4-R4 | Aggregate | Reduce tier array when unused | 8x memory reduction | Medium | Memory |
| S6-R1 | Build Index | Parallel MPHF construction | 2-4x faster MPHF | High | CPU |

### Medium Priority

| ID | Stage | Title | Impact | Risk | Area |
|----|-------|-------|--------|------|------|
| S3-R3 | Parse | Parallel gzip (pgzip) for CSV | 2-4x faster gzip | Low | CPU |
| S4-R1 | Aggregate | Parallel sorting during drain | 2-4x faster flush | Medium | CPU |
| S5-R3 | Merge | Larger fan-in to reduce rounds | Fewer merge rounds | Low | I/O |
| S5-R5 | Merge | Streaming merge to index build | Eliminate final file I/O | Medium | I/O |
| S6-R3 | Build Index | Batch array writes in Add() | Reduce write overhead | Low | I/O |

### Low Priority

| ID | Stage | Title | Impact | Risk | Area |
|----|-------|-------|--------|------|------|
| S1-R1 | Manifest | Cache column indices | Marginal CPU | Low | CPU |
| S1-R2 | Manifest | Cache format detection | Marginal CPU | Low | CPU |
| S2-R3 | Download | Configurable part size | Tuning flexibility | Low | I/O |
| S2-R4 | Download | Pre-warm S3 connections | Faster first chunks | Low | I/O |
| S2-R5 | Download | Per-chunk retries | Better error handling | Low | I/O |
| S3-R2 | Parse | Optimize Parquet column lookup | Minor CPU | Medium | CPU |
| S3-R4 | Parse | Larger Parquet buffer | Reduce call overhead | Low | I/O |
| S4-R2 | Aggregate | Pre-size map from estimate | 5-10% faster aggregation | Low | Memory |
| S4-R3 | Aggregate | Batch slash position finding | 1-2% faster | Low | CPU |
| S4-R6 | Aggregate | Verify Add() is inlined | Marginal | Low | CPU |
| S5-R1 | Merge | Adaptive fan-in from memory | Better memory use | Low | Memory |
| S5-R4 | Merge | Pre-merge small run files | Reduce per-file overhead | Low | I/O |
| S6-R2 | Build Index | Reduce subtree array memory | Memory savings | Medium | Memory |
| S6-R4 | Build Index | mmap hash arrays for large sets | Support 100M+ prefixes | Medium | Memory |
| S6-R5 | Build Index | Skip unused tier writers | Minor per-row savings | Low | CPU |
| S6-R6 | Build Index | Parallel prefix blob write | Minor I/O improvement | Medium | I/O |
| S7-R1 | Query | Warm index on startup | Eliminate cold-start | Low | I/O |
| S7-R2 | Query | Cache frequent lookups | App-level optimization | Low | Memory |
| S7-R3 | Query | Batch lookup API | Better bulk throughput | Low | CPU |
| S7-R4 | Query | Prefix bloom filter | Faster negative lookups | Medium | Memory |

### Skipped (Not Recommended)

| ID | Stage | Title | Reason |
|----|-------|-------|--------|
| S2-R2 | Download | Streaming CSV without temp file | Too complex, marginal benefit |
| S3-R5 | Parse | Pool InventoryRow allocations | Strings copied to aggregator anyway |
| S4-R5 | Aggregate | Async run file write | Complex synchronization |
| S5-R2 | Merge | Use standard heap | Custom heap is 15% faster |
| S5-R6 | Merge | Batch duplicate merging | Duplicates are rare |
| S6-R7 | Build Index | Direct hash indexing | Too complex for minor gain |
| S7-R5 | Query | Smaller fingerprints | Breaking format change |
| S7-R6 | Query | Direct hash indexing | Complexity not worth it |

---

## Recommended Implementation Order

Based on impact, risk, and dependencies:

### Phase 1: Quick Wins (Low Risk, Clear Benefit)

1. **S2-R1:** Fix memory accounting (accurate 128MB per chunk estimate)
2. **S3-R1:** Avoid Parquet double temp file (use existing ReaderAt)
3. **S3-R3:** Switch to pgzip for parallel gzip decompression

### Phase 2: Memory Optimization

4. **S4-R4:** Conditional tier arrays (nil pointers when tracking disabled)
5. **S5-R3:** Increase default MaxFanIn to 16 when memory permits

### Phase 3: I/O Reduction

6. **S5-R5:** Streaming merge to index build (single-round case)
7. **S6-R3:** Batch array writes in index builder

### Phase 4: CPU Parallelism

8. **S4-R1:** Parallel sort during flush (for large drains)
9. **S6-R1:** Investigate parallel MPHF alternatives (highest impact, highest risk)

---

## Metrics for Validation

For each optimization, collect before/after metrics:

| Metric | How to Measure |
|--------|----------------|
| Peak heap memory | `runtime.ReadMemStats()` during run |
| Total run time | Wall clock time for full pipeline |
| Stage timing | Per-stage duration breakdown |
| Disk I/O | Total bytes written to temp/run files |
| CPU utilization | `time` command or profiler |
| Throughput | Objects processed per second |

Use the existing benchmark infrastructure:
- `go test -bench=. -benchmem` for microbenchmarks
- `./scripts/bench_pipeline.sh` for end-to-end benchmarks (if exists)
- Memory profiling: `go test -memprofile=mem.out -bench=...`
- CPU profiling: `go test -cpuprofile=cpu.out -bench=...`
