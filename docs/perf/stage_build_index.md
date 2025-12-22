# Stage 6: Index Building (`stage_build_index`)

## Stage Summary

**What this stage does:** Builds the final index from a sorted stream of `PrefixRow` records. Computes preorder positions, subtree ranges, and depth metadata on-the-fly. Builds a minimal perfect hash function (MPHF) for O(1) prefix lookup.

**Inputs:**
- Sorted stream of `PrefixRow` (from merge iterator or single run file)
- Output directory for index files

**Outputs:**
| File | Type | Content |
|------|------|---------|
| `object_count.u64` | uint64[] | Object count per prefix |
| `total_bytes.u64` | uint64[] | Total bytes per prefix |
| `depth.u32` | uint32[] | Prefix depth |
| `subtree_end.u64` | uint64[] | Last position in subtree |
| `max_depth_in_subtree.u32` | uint32[] | Max depth in subtree |
| `mph.bin` | binary | MPHF data (bbhash) |
| `mph_fp.u64` | uint64[] | Fingerprints for verification |
| `mph_pos.u64` | uint64[] | Preorder positions indexed by hash |
| `prefix_blob.bin` | blob | Prefix strings |
| `prefix_offsets.u64` | uint64[] | Blob offsets |
| `depth_index.*` | various | Depth-based range index |
| `tier_stats/*` | various | Per-tier stats (if enabled) |
| `manifest.json` | JSON | Index metadata |

---

## Control Flow and Data Flow

### Main Flow

```
Sorted PrefixRow stream
    │
    ▼
IndexBuilder.Add(row) [for each row]
    │
    ├── findCommonAncestorDepth() → determine which stack nodes to close
    │
    ├── closeNodesAbove() → finalize subtreeEnd for closed nodes
    │
    ├── Push new entry to stack
    │
    ├── Write streaming arrays:
    │       ├── objectCountW.WriteU64(row.Count)
    │       ├── totalBytesW.WriteU64(row.TotalBytes)
    │       └── depthW.WriteU32(row.Depth)
    │
    ├── mphfBuilder.Add(prefix, pos)
    │
    └── writeTierStats(row)
    │
    ▼
IndexBuilder.Finalize()
    │
    ├── closeNodesAbove(0) → finalize all remaining nodes
    │
    ├── Write subtree arrays:
    │       ├── subtree_end.u64
    │       └── max_depth_in_subtree.u32
    │
    ├── depthIndexBuilder.Build()
    │
    ├── mphfBuilder.Build() [CPU-intensive]
    │       ├── bbhash.New(hashes) [MPHF construction]
    │       ├── computeFingerprintsParallel()
    │       ├── writeArraysParallel()
    │       └── writePrefixBlobOrdered()
    │
    └── Write manifest.json
```

### Key Files and Line References

| File | Function | Lines | Purpose |
|------|----------|-------|---------|
| `indexbuild.go` | `Add()` | 154-199 | Process one prefix row |
| `indexbuild.go` | `closeTopNode()` | 229-248 | Finalize subtree ranges |
| `indexbuild.go` | `AddAllWithContext()` | 323-374 | Batch process from iterator |
| `indexbuild.go` | `FinalizeWithContext()` | 384-504 | Write remaining files |
| `mphf_streaming.go` | `Add()` | 65-88 | Add prefix to MPHF builder |
| `mphf_streaming.go` | `Build()` | 109-211 | Construct MPHF |
| `mphf_streaming.go` | `computeFingerprintsParallel()` | 224-361 | Parallel fingerprint computation |
| `writer.go` | `WriteU64()` | 63-74 | Write array element |
| `writer.go` | `WriteU64Batch()` | 79-96 | Batch write |

---

## Performance Analysis

### CPU Hotspots

1. **MPHF Construction (`bbhash.New`):**
   ```go
   mph, err := bbhash.New(b.hashes, bbhash.Gamma(2.0))
   ```
   - O(n) expected time complexity
   - CPU-intensive: ~0.5-1 second per 1M prefixes
   - Single-threaded (bbhash library limitation)

2. **Fingerprint Computation (parallel):**
   ```go
   fingerprints[hashPos] = computeFingerprintBytes(item.prefixBytes)
   ```
   - O(n * k) where k = average prefix length
   - Parallelized across all CPUs
   - FNV-64 hash per prefix

3. **Prefix String Comparison:**
   ```go
   if len(entry.prefix) <= len(prefix) && prefix[:len(entry.prefix)] == entry.prefix
   ```
   - O(d * k) per row where d = depth, k = prefix length
   - For finding common ancestor depth

4. **Stack Management:**
   ```go
   b.stack = append(b.stack, stackEntry{...})  // Push
   b.stack = b.stack[:len(b.stack)-1]          // Pop
   ```
   - O(1) amortized
   - No allocations after initial growth

### Complexity Summary

- **Add():** O(d) where d = depth (stack operations)
- **bbhash.New():** O(n) with high constant factor
- **Fingerprint computation:** O(n * k / P) with P workers
- **Total:** O(n * k + n * log n) for bbhash + fingerprinting

### Typical Performance

| Operation | Time (1M prefixes) |
|-----------|-------------------|
| Add all rows | ~1-2 seconds |
| bbhash.New | ~0.5-1 second |
| Fingerprints | ~0.3-0.5 seconds |
| Write arrays | ~0.5-1 second |
| **Total** | ~3-5 seconds |

---

## Memory Analysis

### StreamingMPHFBuilder Memory

```go
type StreamingMPHFBuilder struct {
    hashes      []uint64   // 8 bytes per prefix
    preorderPos []uint64   // 8 bytes per prefix
    // Prefixes written to disk - not in memory!
}
```

**Per-prefix memory:** 16 bytes (only hashes + positions)

This is a major improvement over the non-streaming builder which stored all prefix strings (~50+ bytes average per prefix).

### IndexBuilder Memory

```go
type IndexBuilder struct {
    stack              []stackEntry   // ~50 bytes * depth (typically < 32)
    subtreeEnds        []uint64       // 8 bytes per prefix
    maxDepthInSubtrees []uint32       // 4 bytes per prefix
    // Streaming writers don't hold data in memory
}
```

**Per-prefix memory:** 12 bytes (subtreeEnds + maxDepthInSubtrees)

### Total Memory During Build

For 10M prefixes:
- Hashes: 80 MB
- Positions: 80 MB
- SubtreeEnds: 80 MB
- MaxDepthInSubtrees: 40 MB
- **Total:** ~280 MB

Plus temporary allocations during MPHF build:
- Fingerprints: 80 MB (freed after write)
- Positions copy: 80 MB (freed after write)

**Peak memory:** ~440 MB for 10M prefixes

---

## Concurrency & Parallelism

### Current Parallelism

1. **Fingerprint Computation:**
   ```go
   numWorkers := runtime.NumCPU()
   for range numWorkers {
       go func() {
           for work := range workChan {
               // Compute fingerprints
           }
       }()
   }
   ```
   - Full CPU utilization
   - Work distributed in 50K-prefix chunks

2. **Array Writes:**
   ```go
   func writeArraysParallel(outDir string, fingerprints, positions []uint64) error {
       var wg sync.WaitGroup
       wg.Add(2)
       go func() { /* write fingerprints */ }()
       go func() { /* write positions */ }()
       wg.Wait()
   }
   ```
   - Two parallel file writes
   - Limited by disk bandwidth

### Single-Threaded Portions

1. **bbhash.New():** Library is single-threaded
2. **Add() loop:** Sequential processing required (maintains state)
3. **Prefix blob write:** Sequential (maintains order)

---

## I/O Patterns

### Write Patterns

1. **Streaming writes (during Add):**
   - `object_count.u64`, `total_bytes.u64`, `depth.u32`
   - Buffered (default 4KB from bufio.Writer)
   - Sequential append

2. **Deferred writes (during Finalize):**
   - `subtree_end.u64`, `max_depth_in_subtree.u32`
   - Held in memory during construction
   - Written in one pass

3. **MPHF-related writes:**
   - `mph.bin`: Single write (MPHF serialized)
   - `mph_fp.u64`, `mph_pos.u64`: Parallel batch writes
   - `prefix_blob.bin`, `prefix_offsets.u64`: Sequential

### Disk Usage

For 10M prefixes (~50 bytes avg prefix):
| File | Size |
|------|------|
| object_count.u64 | 80 MB |
| total_bytes.u64 | 80 MB |
| depth.u32 | 40 MB |
| subtree_end.u64 | 80 MB |
| max_depth_in_subtree.u32 | 40 MB |
| mph.bin | ~4 MB |
| mph_fp.u64 | 80 MB |
| mph_pos.u64 | 80 MB |
| prefix_blob.bin | ~500 MB |
| prefix_offsets.u64 | 88 MB (N+1 entries) |
| **Total** | ~1.1 GB |

---

## Concrete Recommendations

### Rec-1: Parallel bbhash Construction

**Location:** `mphf_streaming.go:128`

**Current behavior:**
```go
mph, err := bbhash.New(b.hashes, bbhash.Gamma(2.0))
```

**Issue:** bbhash library is single-threaded, taking ~0.5-1s per 1M prefixes.

**Proposed change:**
- Use or create a parallel MPHF library
- Or implement sharded construction:
  ```go
  // Build N smaller MPHFs in parallel
  // Combine with tier-based lookup
  ```

**Expected benefit:** 2-4x faster MPHF construction

**Risk level:** High (requires library change or custom implementation)

**How to test:** Benchmark with various sizes; verify correctness with VerifyMPHF.

---

### Rec-2: Reduce Subtree Array Memory

**Location:** `indexbuild.go:47-48`

**Current behavior:**
```go
subtreeEnds        []uint64 // 8 bytes per prefix
maxDepthInSubtrees []uint32 // 4 bytes per prefix
```

**Issue:** These arrays are held in memory during entire construction.

**Proposed change:**
```go
// Use memory-mapped file for construction
// Or use compressed representation for sparse data
type subtreeData struct {
    file   *os.File
    mmap   []byte
}
```

**Expected benefit:** Reduce memory by ~12 bytes per prefix

**Risk level:** Medium (complexity of mmap management)

**How to test:** Memory profile before/after; verify index correctness.

---

### Rec-3: Batch Array Writes in Add()

**Location:** `indexbuild.go:179-187`

**Current behavior:**
```go
b.objectCountW.WriteU64(row.Count)
b.totalBytesW.WriteU64(row.TotalBytes)
b.depthW.WriteU32(uint32(row.Depth))
```

**Issue:** Three separate buffered writes per row.

**Proposed change:**
```go
// Batch multiple rows into a single write buffer
const batchSize = 1000

type batchedWriter struct {
    counts     []uint64
    bytes      []uint64
    depths     []uint32
    batchIndex int
}

func (b *batchedWriter) Add(row *PrefixRow) {
    b.counts[b.batchIndex] = row.Count
    b.bytes[b.batchIndex] = row.TotalBytes
    b.depths[b.batchIndex] = uint32(row.Depth)
    b.batchIndex++
    if b.batchIndex >= batchSize {
        b.flush()
    }
}
```

**Expected benefit:** Reduce write system call overhead

**Risk level:** Low

**How to test:** Benchmark Add() throughput.

---

### Rec-4: Streaming MPHF with Smaller Hash Memory

**Location:** `mphf_streaming.go:28-42`

**Current behavior:**
```go
hashes      []uint64   // 8 bytes per prefix, all in memory
preorderPos []uint64   // 8 bytes per prefix, all in memory
```

**Issue:** For very large datasets, 16 bytes/prefix is significant.

**Proposed change:**
```go
// Use disk-backed arrays with memory-mapped access
type StreamingMPHFBuilder struct {
    hashesFile   *os.File
    hashesMap    []uint64  // mmap'd
    positionsMap []uint64  // mmap'd
    // ...
}
```

**Expected benefit:** Support larger datasets without OOM

**Risk level:** Medium

**How to test:** Test with 100M+ prefix datasets.

---

### Rec-5: Skip Unused Tier Writers

**Location:** `indexbuild.go:192-196` and `indexbuild.go:251-277`

**Current behavior:**
```go
for tierID := tiers.ID(0); tierID < tiers.NumTiers; tierID++ {
    if row.TierCounts[tierID] > 0 || row.TierBytes[tierID] > 0 {
        b.presentTiers[tierID] = true
    }
}
```

**Issue:** Iterates through all 12 tiers for every row.

**Proposed change:**
```go
// Track which tiers are present during aggregation
// Only create writers for present tiers at start
func (b *IndexBuilder) SetPresentTiers(tiers []tiers.ID) {
    for _, tierID := range tiers {
        b.createTierWriter(tierID)
    }
}
```

**Expected benefit:** Reduce per-row overhead when few tiers are used.

**Risk level:** Low

**How to test:** Profile with tier tracking on/off.

---

### Rec-6: Parallel Prefix Blob Write

**Location:** `mphf_streaming.go:365-416`

**Current behavior:**
```go
// Sequential loop to re-read prefixes and write blob
for i := range n {
    // Read prefix from temp file
    // Write to blob
}
```

**Issue:** Single-threaded, I/O bound.

**Proposed change:**
- Use memory-mapped temp file for parallel reads
- Parallelize blob writing with buffered chunks

**Expected benefit:** Faster blob write for large datasets.

**Risk level:** Medium

**How to test:** Profile with 10M+ prefixes.

---

## Summary

Stage 6 is well-designed for streaming construction. The main concerns are:

1. **bbhash single-threaded:** Major bottleneck for large datasets
2. **Subtree arrays in memory:** 12 bytes per prefix during construction
3. **Sequential blob write:** I/O bound for large prefix sets

**Priority of recommendations:**

| ID | Title | Priority | Impact |
|----|-------|----------|--------|
| Rec-1 | Parallel MPHF | High | 2-4x faster MPHF |
| Rec-3 | Batch array writes | Medium | Reduce write overhead |
| Rec-5 | Skip unused tier writers | Low | Minor per-row savings |
| Rec-2 | mmap subtree arrays | Low | Memory reduction |
| Rec-4 | mmap hash arrays | Low | Support larger datasets |
| Rec-6 | Parallel blob write | Low | Minor I/O improvement |

**Overall assessment:** The streaming MPHF builder is a major memory improvement. The main bottleneck is now bbhash construction, which is single-threaded. For very large datasets (100M+ prefixes), consider a parallel MPHF library or sharded approach.
