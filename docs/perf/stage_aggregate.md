# Stage 4: Aggregation & Run File Creation (`stage_aggregate`)

## Stage Summary

**What this stage does:** Aggregates object statistics by prefix in memory. When heap usage exceeds a threshold, drains the aggregator, sorts prefixes, and writes a sorted run file to disk.

**Inputs:**
- Stream of `objectRecord{key, size, tierID}` from parsed chunks
- Memory budget threshold for triggering flush

**Outputs:**
- Multiple sorted run files (`.bin` uncompressed or `.crun` zstd-compressed)
- Each run file contains all prefixes from one aggregation window, sorted lexicographically

---

## Control Flow and Data Flow

### Main Flow

```
objectRecord{key, size, tierID}
    │
    ▼
Aggregator.AddObject(key, size, tierID)
    │
    ├── accumulate("", 0, size, tierID)  [root prefix]
    │
    ├── for each '/' in key:
    │       accumulate(prefix, depth, size, tierID)
    │
    ▼
map[string]*PrefixStats updated
    │
    ├── [if ShouldFlush(heapThreshold)]
    │       │
    │       ▼
    │   Aggregator.Drain()
    │       │
    │       ▼
    │   []*PrefixRow (unsorted)
    │       │
    │       ▼
    │   slices.SortFunc(rows, ...)
    │       │
    │       ▼
    │   RunFileWriter.WriteSorted(rows)
    │       │
    │       ▼
    │   .bin or .crun file on disk
    │
    └── [continue until all objects processed]
```

### Key Files and Line References

| File | Function | Lines | Purpose |
|------|----------|-------|---------|
| `extsort/aggregator.go` | `AddObject()` | 42-61 | Add object to all ancestor prefixes |
| `extsort/aggregator.go` | `accumulate()` | 64-76 | Update stats for single prefix |
| `extsort/aggregator.go` | `Drain()` | 138-159 | Extract sorted prefix rows |
| `extsort/aggregator.go` | `ShouldFlush()` | 126-130 | Check heap threshold |
| `extsort/pipeline.go` | `flushAggregator()` | 594-675 | Sort and write run file |
| `extsort/runfile.go` | `Write()` | 80-120 | Write single record |
| `extsort/runfile.go` | `WriteSorted()` | 133-138 | Sort and write all |

---

## Performance Analysis

### CPU Hotspots

1. **Prefix Extraction (`AddObject`):**
   ```go
   for i := range len(key) {
       if key[i] == '/' {
           prefix := key[:i+1]  // String slice (no alloc)
           a.accumulate(prefix, depth, size, tierID)
       }
   }
   ```
   - O(n) where n = key length
   - Map lookup for each prefix (average 4-10 prefixes per key)

2. **Map Operations:**
   - `map[string]*PrefixStats` lookup: O(1) amortized
   - String key hashing: O(k) where k = prefix length
   - Map growth: O(n) amortized when resizing

3. **Sorting (`Drain`):**
   ```go
   slices.SortFunc(rows, func(a, b *PrefixRow) int {
       return strings.Compare(a.Prefix, b.Prefix)
   })
   ```
   - O(n log n) where n = unique prefixes
   - String comparison: O(k) where k = common prefix length

4. **Run File Write:**
   - Binary encoding: O(n) where n = record count
   - Buffered I/O: amortized O(1) per write

**Complexity Summary:**
- AddObject: O(d * k) where d = depth, k = average prefix length
- Drain + Sort: O(n log n * k)
- Write: O(n * record_size)

### Typical Performance

- **Aggregation rate:** 1-5M objects/second (map update bound)
- **Sort rate:** ~10M prefixes/second for ~30-char prefixes
- **Write rate:** ~500K-1M records/second (I/O bound)

---

## Memory Analysis

### Data Structures

**Aggregator Map:**
```go
prefixes map[string]*PrefixStats
```

**PrefixStats (210 bytes):**
```go
type PrefixStats struct {
    Depth      uint16                    // 2 bytes
    Count      uint64                    // 8 bytes
    TotalBytes uint64                    // 8 bytes
    TierCounts [MaxTiers]uint64          // 12 * 8 = 96 bytes
    TierBytes  [MaxTiers]uint64          // 12 * 8 = 96 bytes
}
```

**PrefixRow (similar size + Prefix string):**
```go
type PrefixRow struct {
    Prefix     string                    // 16 bytes (header) + len(prefix)
    Depth      uint16                    // 2 bytes
    Count      uint64                    // 8 bytes
    TotalBytes uint64                    // 8 bytes
    TierCounts [MaxTiers]uint64          // 96 bytes
    TierBytes  [MaxTiers]uint64          // 96 bytes
}
```

### Memory Per Prefix

| Component | Size |
|-----------|------|
| Map bucket overhead | ~48 bytes |
| String key (avg 30 chars) | ~30 bytes (shared with PrefixRow.Prefix on flush) |
| PrefixStats | ~210 bytes |
| **Total per unique prefix** | **~288 bytes** |

### Peak Memory Usage

For 10M unique prefixes:
- Map: 10M * 288 bytes = ~2.88 GB

**Memory Budget Enforcement:**
```go
// From aggregator.go:126-130
func ShouldFlush(heapThreshold uint64) bool {
    current := HeapAllocBytes()
    return current > (heapThreshold * 80 / 100)  // 80% threshold
}
```

### Pool Usage

```go
// From aggregator.go:33-36
statsPool: sync.Pool{
    New: func() interface{} {
        return &PrefixStats{}
    },
},
```

- PrefixStats are pooled for reuse across flushes
- Reduces allocation pressure during drain/refill cycles

---

## Concurrency & Parallelism

### Current Model

**Single-threaded aggregation.** The aggregator is not thread-safe.

From `aggregator.go:11-14`:
```go
// The aggregator is NOT safe for concurrent use. For concurrent access,
// use multiple aggregators or external synchronization.
```

**Pipeline Coordination:**
```go
// From pipeline.go:387-417
for batch := range results {
    for _, obj := range batch.objects {
        agg.AddObject(obj.key, obj.size, obj.tierID)  // Single-threaded
    }
}
```

### Why Single-Threaded?

1. **Map updates are not atomic:** Concurrent map writes would require locks
2. **Memory budget is global:** Hard to split budget across multiple aggregators
3. **Prefix overlap:** Same prefix updated by objects from different chunks

### Potential for Parallelism

1. **Sharded Aggregation:**
   - Hash key to N aggregator shards
   - Each shard is single-threaded
   - Merge shards at flush time
   - Benefit: N-way parallelism
   - Cost: Merge overhead, more complex memory management

2. **Parallel Flush:**
   - Sorting and writing can be parallelized
   - Current implementation is sequential

---

## I/O Patterns

### Run File Format

```
Header (16 bytes):
    Magic:   4 bytes  (0x45585453 = "EXTS")
    Version: 4 bytes  (1)
    Count:   8 bytes

Records (variable length each):
    PrefixLen:  4 bytes
    Prefix:     N bytes
    Depth:      2 bytes
    Count:      8 bytes
    TotalBytes: 8 bytes
    TierCounts: 12 * 8 = 96 bytes
    TierBytes:  12 * 8 = 96 bytes

Total per record: 214 + len(prefix) bytes
```

### Write Pattern

1. **Buffered writes:** 4-16 MB buffer
2. **Sequential:** One record after another
3. **Single flush:** Buffer flushed at close
4. **Header update:** Seek back to write final count

### Compression (if enabled)

- Uses zstd with "fastest" compression level
- Typical compression ratio: 2-4x
- Reduces disk I/O significantly

---

## Concrete Recommendations

### Rec-1: Parallel Sorting During Drain

**Location:** `extsort/pipeline.go:594-638` and `extsort/aggregator.go:138-159`

**Current behavior:**
```go
rows := agg.Drain()  // Extracts unsorted
// In flushAggregator:
slices.SortFunc(rows, ...)  // Sequential sort
```

**Proposed change:**
```go
// Use parallel sort for large drains
import "golang.org/x/exp/slices"

if len(rows) > 100000 {
    // Parallel merge sort for large datasets
    parallelSort(rows, runtime.NumCPU())
} else {
    slices.SortFunc(rows, ...)
}
```

**Expected benefit:** 2-4x faster sort for large flushes (> 100K prefixes)

**Risk level:** Medium (need to implement or import parallel sort)

**How to test:** Benchmark sort phase with various sizes

---

### Rec-2: Pre-size Map Based on Estimate

**Location:** `extsort/aggregator.go:25-40` and `extsort/pipeline.go:383`

**Current behavior:**
```go
agg := NewAggregator(10000, p.config.MaxDepth)  // Fixed 10K capacity
```

**Issue:** Map grows dynamically, causing rehashes.

**Proposed change:**
```go
// Estimate unique prefixes from first chunk
func estimateUniquePrefixes(objectCount int, avgKeyDepth float64) int {
    // Each object contributes ~avgKeyDepth prefixes
    // But many are shared. Typical ratio: 1 unique prefix per 10-50 objects.
    return max(objectCount / 20, 10000)
}

// In runIngestPhase, after processing first chunk:
estimatedPrefixes := estimateUniquePrefixes(chunkObjectCount, avgDepth)
agg := NewAggregator(estimatedPrefixes, p.config.MaxDepth)
```

**Expected benefit:** Reduce map rehashing; ~5-10% faster aggregation

**Risk level:** Low

**How to test:** Benchmark with profiler showing map growth

---

### Rec-3: Batch String Slice Operations

**Location:** `extsort/aggregator.go:44-61`

**Current behavior:**
```go
for i := range len(key) {
    if key[i] == '/' {
        prefix := key[:i+1]  // Creates string slice each time
        a.accumulate(prefix, depth, size, tierID)
    }
}
```

**Issue:** While `key[:i+1]` doesn't allocate (shares backing array), the map lookup still hashes the full prefix.

**Proposed change:**
```go
// Pre-compute all slash positions
slashes := make([]int, 0, 10)
for i := 0; i < len(key); i++ {
    if key[i] == '/' {
        slashes = append(slashes, i)
    }
}

// Single pass with cached positions
a.accumulate("", 0, size, tierID)
for depth, slashPos := range slashes {
    if a.maxDepth > 0 && depth+1 > a.maxDepth {
        break
    }
    a.accumulate(key[:slashPos+1], uint16(depth+1), size, tierID)
}
```

**Expected benefit:** Minor (~1-2% faster) - reduces branch mispredictions

**Risk level:** Low

**How to test:** Microbenchmark AddObject

---

### Rec-4: Reduce Tier Array Size When Unused

**Location:** `extsort/types.go` and throughout aggregation

**Current behavior:**
```go
const MaxTiers = 12  // Always allocate for 12 tiers

type PrefixStats struct {
    TierCounts [MaxTiers]uint64  // 96 bytes even if unused
    TierBytes  [MaxTiers]uint64  // 96 bytes even if unused
}
```

**Issue:** 192 bytes per prefix for tier data, even when tier tracking is disabled.

**Proposed change:**
```go
type PrefixStats struct {
    Depth      uint16
    Count      uint64
    TotalBytes uint64
    TierCounts *[MaxTiers]uint64  // nil if not tracking
    TierBytes  *[MaxTiers]uint64  // nil if not tracking
}

func (s *PrefixStats) Add(size uint64, tierID tiers.ID) {
    s.Count++
    s.TotalBytes += size
    if s.TierCounts != nil {
        s.TierCounts[tierID]++
        s.TierBytes[tierID] += size
    }
}
```

**Expected benefit:**
- Without tier tracking: ~210 bytes → ~26 bytes per prefix (8x smaller)
- Significant memory reduction for most use cases

**Risk level:** Medium (struct changes, nil checks needed)

**How to test:** Benchmark with tier tracking on/off; verify correctness

---

### Rec-5: Async Run File Write

**Location:** `extsort/pipeline.go:594-675`

**Current behavior:**
```go
// flushAggregator blocks until write completes
rows := agg.Drain()
SortPrefixRows(rows)
writer.WriteSorted(rows)
writer.Close()
runtime.GC()  // Force GC after flush
```

**Issue:** Parsing/aggregation pauses during run file write.

**Proposed change:**
```go
// Use a goroutine for writing while aggregation continues
func (p *Pipeline) flushAggregator(ctx context.Context, agg *Aggregator) error {
    rows := agg.Drain()
    SortPrefixRows(rows)

    // Clone rows for async write (rows slice will be reused)
    writeRows := make([]*PrefixRow, len(rows))
    copy(writeRows, rows)

    go func() {
        writer.WriteSorted(writeRows)
        writer.Close()
    }()

    return nil
}
```

**Expected benefit:** Overlap writing with parsing; ~10-20% faster overall

**Risk level:** High (synchronization complexity, memory for cloned rows)

**Recommendation:** Skip unless flush is a clear bottleneck

---

### Rec-6: Inline PrefixStats.Add

**Location:** `extsort/types.go:145-150`

**Current behavior:**
```go
func (s *PrefixStats) Add(size uint64, tierID tiers.ID) {
    s.Count++
    s.TotalBytes += size
    s.TierCounts[tierID]++
    s.TierBytes[tierID] += size
}
```

**Issue:** Function call overhead in hot path.

**Proposed change:** The Go compiler should inline this. Verify with:
```bash
go build -gcflags='-m' 2>&1 | grep 'Add.*inlining'
```

If not inlining, consider:
```go
//go:inline
func (s *PrefixStats) Add(size uint64, tierID tiers.ID) {
    ...
}
```

**Expected benefit:** Eliminate function call (~1 ns per call)

**Risk level:** Low

**How to test:** Check compiler output; microbenchmark

---

## Summary

Stage 4 is the core memory-intensive phase. The main concerns are:

1. **Map memory usage:** ~288 bytes per unique prefix
2. **Single-threaded aggregation:** Cannot parallelize without sharding
3. **Sort overhead:** Large drains require significant sort time

**Priority of recommendations:**

| ID | Title | Priority | Impact |
|----|-------|----------|--------|
| Rec-4 | Reduce tier array when unused | High | 8x memory reduction |
| Rec-1 | Parallel sorting | Medium | 2-4x faster flush |
| Rec-2 | Pre-size map | Low | 5-10% faster aggregation |
| Rec-3 | Batch slash operations | Low | 1-2% faster |
| Rec-6 | Inline Add | Low | Marginal |
| Rec-5 | Async write | Skip | Too complex |

**Overall assessment:** Stage 4 is efficient for its purpose. The main opportunity is Rec-4 (conditional tier arrays) which can significantly reduce memory usage when tier tracking is disabled.
