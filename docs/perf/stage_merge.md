# Stage 5: K-Way Merge (`stage_merge`)

## Stage Summary

**What this stage does:** Merges multiple sorted run files into a single globally-sorted stream. Uses parallel multi-round merge when many run files exist. Automatically merges duplicate prefixes (same prefix from different runs).

**Inputs:**
- List of sorted run file paths (`.bin` or `.crun`)
- Merge configuration (workers, fan-in, buffer sizes)

**Outputs:**
- Single merged run file (or iterator for streaming to index build)
- Globally sorted by prefix, duplicates merged

---

## Control Flow and Data Flow

### Single-File Case (No Merge)

```
1 run file → Return directly → Index Build
```

### Multi-File Case (Parallel Merge)

```
N run files
    │
    ├── Round 1: Partition into groups of MaxFanIn
    │       │
    │       ├── Worker 1: K-way merge group 1 → intermediate run
    │       ├── Worker 2: K-way merge group 2 → intermediate run
    │       └── Worker M: K-way merge group M → intermediate run
    │
    ├── Round 2: Merge intermediate runs
    │       └── ... repeat until single file
    │
    └── Final merged run file
```

### K-Way Merge Algorithm (per worker)

```
Open K input readers
    │
    ▼
Initialize min-heap with first row from each reader
    │
    ▼
while heap not empty:
    │
    ├── Pop minimum row
    │
    ├── Merge duplicate prefixes (peek heap, merge if same)
    │
    ├── Write merged row to output
    │
    └── Advance the reader that provided the row → push to heap
```

### Key Files and Line References

| File | Function | Lines | Purpose |
|------|----------|-------|---------|
| `parallel_merge.go` | `MergeAll()` | 122-188 | Orchestrate multi-round merge |
| `parallel_merge.go` | `mergeRound()` | 192-253 | One round of parallel merge |
| `parallel_merge.go` | `executeMerge()` | 287-409 | Single K-way merge job |
| `parallel_merge.go` | `runReaderMergeIterator.Next()` | 442-471 | Heap-based merge iteration |
| `merger.go` | `MergeIterator.Next()` | 104-140 | Legacy heap-based merger |
| `parallel_merge.go` | `heapPush/heapPop/heapDown` | 506-548 | Custom heap operations |

---

## Performance Analysis

### CPU Hotspots

1. **Heap Operations:**
   ```go
   heapPop(h *mergeHeap) mergeItem  // O(log K)
   heapPush(h *mergeHeap, item)     // O(log K)
   ```
   - Called once per row read
   - K = number of input runs (up to MaxFanIn)

2. **String Comparison (for duplicate detection):**
   ```go
   for m.heap.Len() > 0 && m.heap.items[0].row.Prefix == result.Prefix {
       // Merge duplicates
   }
   ```
   - O(k) where k = prefix length
   - Only when duplicates exist (rare after first flush)

3. **Binary Encoding/Decoding:**
   - Read: `binary.LittleEndian.Uint64(buf[offset:])`
   - Write: `binary.LittleEndian.PutUint64(buf[offset:], value)`
   - Very fast, inlined by compiler

4. **Compression/Decompression (if enabled):**
   - zstd fastest level: ~500MB/s decode, ~200MB/s encode
   - Often faster than uncompressed due to reduced I/O

### Complexity

- **Per row:** O(log K) for heap operations
- **Per merge job:** O(N log K) where N = total rows in inputs
- **Multi-round:** O(N log K * log_K(total_runs))

### Typical Performance

- **Merge rate:** 500K-2M rows/second per worker
- **Bottleneck:** Usually I/O (reading/writing run files)
- **Parallelism benefit:** Near-linear scaling up to 4-8 workers

---

## Memory Analysis

### Per-Worker Memory

| Component | Size |
|-----------|------|
| Input readers (K) | K * bufferSize (~64KB-8MB each) |
| Merge heap | K * sizeof(mergeItem) (~48 bytes each) |
| Output writer buffer | bufferSize (~4MB) |
| Compression state | ~1MB for zstd encoder/decoder |

**Per-worker total:** K * bufferSize + 5MB overhead

### Example: 8-way merge with 1MB buffers

- 8 * 1MB read buffers = 8MB
- Output buffer = 4MB
- Compression = 2MB
- Heap and overhead = 1MB
- **Total per worker:** ~15MB

### With N Workers

- Total memory: N * 15MB + run file sizes on disk
- Default: N = CPU/2, so 4-8 workers = 60-120MB

---

## Concurrency & Parallelism

### Current Model

```go
// From parallel_merge.go:200-208
for range min(m.config.NumWorkers, len(groups)) {
    wg.Add(1)
    go func() {
        defer wg.Done()
        m.mergeWorker(ctx, jobs, results)
    }()
}
```

**Worker pool:** Fixed number of goroutines process merge jobs from channel.

**Job distribution:** Each job is a group of up to `MaxFanIn` run files.

### Configuration

```go
// Defaults from parallel_merge.go:47-65
NumWorkers:       workers (CPU/2, max 8)
MaxFanIn:         8
BufferSize:       1 * 1024 * 1024 (1MB)
UseCompression:   true
CompressionLevel: CompressionFastest
```

### Observations

1. **Work balancing:** Jobs are statically sized (MaxFanIn files each).
   - Issue: Last group may have fewer files, underutilizing workers.

2. **Memory scaling:** NumWorkers * K * BufferSize can be significant.
   - With 8 workers, 8-way merge, 1MB buffers: 64MB for reads alone.

---

## I/O Patterns

### Read Pattern

1. **Buffered sequential:** Each run file read sequentially with large buffer
2. **Parallel across files:** K files read concurrently per worker
3. **Decompression on-the-fly:** zstd decodes as data is read

### Write Pattern

1. **Buffered sequential:** Output written with large buffer
2. **Compression on-the-fly:** zstd encodes as data is written
3. **Header update:** Seek back to write final count

### Disk I/O

**Total data transferred:**
- Input: Sum of all run file sizes
- Output: One merged run file (slightly smaller due to no duplicate prefixes)
- Multi-round: Additional I/O for intermediate files

**Typical I/O:**
- 10 run files * 100MB each = 1GB read
- 1 merged file ~ 1GB write
- With 3 rounds: ~4GB total I/O

---

## Concrete Recommendations

### Rec-1: Adaptive Fan-In Based on Memory Budget

**Location:** `parallel_merge.go:19-45` and `pipeline.go:700-720`

**Current behavior:**
```go
MaxFanIn: 8  // Fixed
```

**Issue:** 8-way merge with large buffers can exceed memory budget.

**Proposed change:**
```go
func optimalFanIn(numRuns int, memoryBudget int64, bufferSize int) int {
    // Memory per input: bufferSize (read buffer)
    // Plus output buffer and compression overhead
    overheadPerRun := int64(bufferSize) + 1024*1024 // 1MB overhead
    maxFanIn := memoryBudget / overheadPerRun
    if maxFanIn < 2 {
        maxFanIn = 2
    }
    if maxFanIn > 16 {
        maxFanIn = 16  // Diminishing returns beyond this
    }
    return int(maxFanIn)
}
```

**Expected benefit:** Better memory control, avoid OOM with many runs.

**Risk level:** Low

**How to test:** Profile memory usage with various fan-in values.

---

### Rec-2: Avoid Custom Heap, Use Standard Library

**Location:** `parallel_merge.go:499-548`

**Current behavior:**
```go
// Custom heap operations to avoid interface{} conversions
func heapInit(h *mergeHeap) { ... }
func heapPush(h *mergeHeap, item mergeItem) { ... }
func heapPop(h *mergeHeap) mergeItem { ... }
```

**Issue:** Code duplication with `merger.go`. The standard library heap is well-optimized.

**Proposed change:**
```go
import "container/heap"

// Use standard heap with concrete type
type mergeHeap []mergeItem

func (h mergeHeap) Len() int           { return len(h) }
func (h mergeHeap) Less(i, j int) bool { return h[i].row.Prefix < h[j].row.Prefix }
func (h mergeHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *mergeHeap) Push(x any)        { *h = append(*h, x.(mergeItem)) }
func (h *mergeHeap) Pop() any          { old := *h; n := len(old); x := old[n-1]; *h = old[:n-1]; return x }
```

**Expected benefit:** Simplified code, same performance (Go 1.18+ generics could help).

**Risk level:** Low (previous attempt showed custom is slightly faster - may skip)

**Note:** Earlier optimization attempt showed custom heap was 15% faster. Keep custom.

---

### Rec-3: Reduce Intermediate File Count with Larger Fan-In

**Location:** Configuration

**Current behavior:**
- Default MaxFanIn = 8
- 20 runs → Round 1: 5 outputs → Round 2: 2 outputs → Round 3: 1 output

**Issue:** More rounds = more disk I/O.

**Proposed change:**
- Increase MaxFanIn to 16 or 32 when memory permits
- Example: 20 runs with MaxFanIn=20 → 1 round, no intermediate files

**Expected benefit:** Reduce total I/O by eliminating intermediate writes.

**Risk level:** Low (only increase fan-in if memory budget allows)

**How to test:** Benchmark with 10, 20, 50 run files; measure total time.

---

### Rec-4: Pre-Merge Small Run Files

**Location:** `parallel_merge.go:122-188`

**Current behavior:** All run files treated equally regardless of size.

**Issue:** Many small run files (from frequent flushes) create overhead.

**Proposed change:**
```go
// Before parallel merge, combine small files
func consolidateSmallRuns(paths []string, threshold int64, tempDir string) ([]string, error) {
    small := []string{}
    large := []string{}

    for _, p := range paths {
        info, _ := os.Stat(p)
        if info.Size() < threshold {
            small = append(small, p)
        } else {
            large = append(large, p)
        }
    }

    if len(small) > 1 {
        // Merge all small into one
        merged, _ := mergeFiles(small, tempDir)
        large = append(large, merged)
    }

    return large, nil
}
```

**Expected benefit:** Reduce per-file overhead, more balanced merge jobs.

**Risk level:** Low

**How to test:** Profile with many small run files.

---

### Rec-5: Streaming Merge to Index Build

**Location:** `pipeline.go:731-796`

**Current behavior:**
```go
// Merge all to single file, then read for index build
finalRunPath, mergeErr = parallelMerger.MergeAll(ctx, p.runFiles)
// ...
reader, err := OpenRunFileAuto(finalRunPath, ...)
```

**Issue:** Writes final merged file to disk, then reads it back.

**Proposed change:**
```go
// For single-round merge, stream directly to index builder
if canSingleRound(numRuns, maxFanIn) {
    // Create merge iterator directly
    readers := openAllRunFiles(p.runFiles)
    mergeIter := newMergeIteratorFromRunReaders(readers)

    // Stream to index builder (no intermediate file)
    builder.AddAllWithContext(ctx, mergeIter)
}
```

**Expected benefit:** Eliminate final file write/read cycle.

**Risk level:** Medium (need to handle single-round case specially)

**How to test:** Compare with/without streaming for single-round scenarios.

---

### Rec-6: Batch Duplicate Merging

**Location:** `parallel_merge.go:459-468`

**Current behavior:**
```go
// Merge duplicates one at a time
for m.heap.Len() > 0 && m.heap.items[0].row.Prefix == result.Prefix {
    dup := heapPop(m.heap)
    result.Merge(dup.row)
    // Advance reader
}
```

**Issue:** Each duplicate requires heap pop + reader advance.

**Proposed change:**
```go
// Batch collect all duplicates, then merge
duplicates := []*PrefixRow{result}
for m.heap.Len() > 0 && m.heap.items[0].row.Prefix == result.Prefix {
    dup := heapPop(m.heap)
    duplicates = append(duplicates, dup.row)
    m.advanceReader(dup.readerIdx)
}

// Merge all at once (could be SIMD optimized)
for _, dup := range duplicates[1:] {
    result.Merge(dup)
}
```

**Expected benefit:** Minimal (current code is already efficient).

**Risk level:** Low

**Recommendation:** Skip - duplicates are rare after first flush.

---

## Summary

Stage 5 is well-implemented with parallel K-way merge. The main concerns are:

1. **Multi-round I/O:** Each round writes intermediate files
2. **Fixed fan-in:** May not optimally use available memory
3. **Final file write:** Could stream directly to index build

**Priority of recommendations:**

| ID | Title | Priority | Impact |
|----|-------|----------|--------|
| Rec-5 | Streaming merge to index | Medium | Eliminate final write/read |
| Rec-3 | Larger fan-in | Medium | Reduce rounds and I/O |
| Rec-1 | Adaptive fan-in | Low | Better memory utilization |
| Rec-4 | Pre-merge small runs | Low | Reduce overhead |
| Rec-2 | Standard heap | Skip | Custom is faster |
| Rec-6 | Batch duplicates | Skip | Minimal benefit |

**Overall assessment:** The parallel merge is efficient. Main opportunity is streaming directly to index build (Rec-5) to eliminate final file I/O.
