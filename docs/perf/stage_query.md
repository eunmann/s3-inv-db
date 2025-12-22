# Stage 7: Query (`stage_query`)

## Stage Summary

**What this stage does:** Provides O(1) prefix lookup and O(log n) range queries against a built index. Uses memory-mapped files for near-zero startup time and minimal memory footprint.

**Inputs:**
- Index directory path
- Prefix strings for lookup
- Position queries for stats, depth, subtree operations

**Outputs:**
- Prefix position (from MPHF lookup)
- Statistics (object count, total bytes)
- Tier breakdown (per-storage-class stats)
- Descendant positions (at specific depths)

---

## Control Flow and Data Flow

### Index Loading

```
indexread.Open(dir)
    │
    ├── format.OpenArray("subtree_end.u64") → mmap
    ├── format.OpenArray("depth.u32") → mmap
    ├── format.OpenArray("object_count.u64") → mmap
    ├── format.OpenArray("total_bytes.u64") → mmap
    ├── format.OpenArray("max_depth_in_subtree.u32") → mmap
    ├── format.OpenDepthIndex(dir) → mmap ranges
    ├── format.OpenMPHF(dir)
    │       ├── Load mph.bin
    │       ├── mmap mph_fp.u64
    │       ├── mmap mph_pos.u64
    │       └── mmap prefix_blob + offsets
    └── format.OpenTierStats(dir) → mmap per-tier arrays
```

### Query Flow

```
idx.Lookup(prefix)
    │
    ├── hashString(prefix) → uint64 key
    ├── mphf.Find(key) → hashPos
    ├── Verify fingerprint: fingerprints[hashPos] == computeFingerprint(prefix)
    └── Return: preorderPos[hashPos]

idx.Stats(pos)
    │
    ├── objectCount.UnsafeGetU64(pos) → count
    └── totalBytes.UnsafeGetU64(pos) → bytes

idx.DescendantsAtDepth(prefixPos, relDepth)
    │
    ├── targetDepth = depth[prefixPos] + relDepth
    ├── subtreeEnd = subtreeEnd[prefixPos]
    └── depthIndex.GetPositionsInSubtree(targetDepth, prefixPos, subtreeEnd)
```

### Key Files and Line References

| File | Function | Lines | Purpose |
|------|----------|-------|---------|
| `indexread/index.go` | `Open()` | 31-86 | Load and mmap all index files |
| `indexread/index.go` | `Lookup()` | 145-147 | MPHF prefix lookup |
| `indexread/index.go` | `Stats()` | 152-160 | Get count/bytes |
| `indexread/index.go` | `DescendantsAtDepth()` | 223-246 | Find children at depth |
| `format/mphf.go` | `Lookup()` | 275-302 | MPHF lookup with verification |
| `format/reader.go` | `UnsafeGetU64()` | (various) | Direct mmap access |

---

## Performance Analysis

### Query Latencies

| Operation | Complexity | Typical Latency |
|-----------|------------|-----------------|
| `Lookup(prefix)` | O(1) amortized | ~100-500 ns |
| `Stats(pos)` | O(1) | ~10-50 ns |
| `Depth(pos)` | O(1) | ~10-50 ns |
| `SubtreeEnd(pos)` | O(1) | ~10-50 ns |
| `DescendantsAtDepth()` | O(log n + k) | ~1-100 μs |
| `PrefixString(pos)` | O(k) | ~100-500 ns |

Where n = total prefixes, k = result size.

### CPU Hotspots

1. **MPHF Lookup:**
   ```go
   func (m *MPHF) Lookup(prefix string) (pos uint64, ok bool) {
       keyHash := hashString(prefix)        // FNV-64 hash
       hashVal := m.mph.Find(keyHash)       // bbhash lookup
       storedFP := m.fingerprints[hashPos]  // mmap read
       computedFP := computeFingerprint(prefix)  // FNV-64
       // Compare fingerprints
   }
   ```
   - Two FNV-64 hashes per lookup (key + fingerprint)
   - One bbhash lookup (O(1) amortized)
   - One mmap access for fingerprint verification

2. **Array Access:**
   ```go
   func (r *ArrayReader) UnsafeGetU64(idx uint64) uint64 {
       return binary.LittleEndian.Uint64(r.data[offset:])
   }
   ```
   - Direct memory access (mmap'd)
   - No syscall overhead
   - May trigger page fault if not cached

3. **Depth Index Range Query:**
   ```go
   func (d *DepthIndex) GetPositionsInSubtree(depth, start, end uint64) ([]uint64, error) {
       // Binary search for range bounds
       // Filter positions within subtree
   }
   ```
   - O(log n) binary search
   - O(k) result collection

### Memory Access Patterns

1. **Random Access:** MPHF lookups are random; poor cache locality
2. **Sequential Scan:** Depth index queries may scan ranges
3. **Page Faults:** First access to mmap'd region triggers page fault (~1 μs)

---

## Memory Analysis

### Index Memory Model

All data is memory-mapped, meaning:
- Physical memory usage depends on access patterns
- OS manages paging to/from disk
- Working set = frequently accessed pages

### Resident Memory (Typical)

| Component | When Accessed | Size |
|-----------|---------------|------|
| MPHF data | Every lookup | ~2-4 bytes/prefix |
| Fingerprints | Every lookup | 8 bytes/prefix |
| Position map | Every lookup | 8 bytes/prefix |
| Object count | Stats queries | 8 bytes/prefix |
| Total bytes | Stats queries | 8 bytes/prefix |
| Prefix blob | PrefixString | ~50 bytes/prefix |

For 10M prefixes, if all accessed:
- Core lookup data: ~280 MB
- With stats: ~440 MB
- With prefix strings: ~1 GB

In practice, working set is much smaller (only accessed pages).

### mmap Benefits

1. **Fast Startup:** No data loading, just mmap syscall
2. **Lazy Loading:** Pages loaded on demand
3. **Shared Memory:** Multiple processes share physical pages
4. **Automatic Paging:** OS handles memory pressure

---

## Concurrency & Parallelism

### Thread Safety

From `indexread/index.go:12-16`:
```go
// Thread Safety: Index is safe for concurrent read access from multiple
// goroutines. All read methods (Lookup, Stats, TierBreakdown, etc.) can be
// called concurrently.
```

All read operations are thread-safe:
- mmap'd arrays are read-only
- MPHF lookup is stateless
- No shared mutable state

### Scaling

- **Horizontal:** Multiple goroutines can query concurrently
- **Limit:** I/O bandwidth for cold queries (page faults)
- **Optimal:** Keep working set in memory (warm cache)

---

## I/O Patterns

### Index Loading

```
mmap(subtree_end.u64)   // 80 MB for 10M prefixes
mmap(depth.u32)         // 40 MB
mmap(object_count.u64)  // 80 MB
mmap(total_bytes.u64)   // 80 MB
...
```

**Total mmap'd:** ~1 GB for 10M prefixes with all files

### Query I/O

1. **Warm Cache:** Zero I/O, direct memory access
2. **Cold Query:** Page fault → disk read (~1-10 μs per 4KB page)
3. **Sequential Access:** Kernel prefetches nearby pages

### Page Fault Analysis

For a lookup of "data/2024/01/":
1. MPHF page: ~4 KB (may contain multiple entries)
2. Fingerprint page: ~4 KB
3. Position page: ~4 KB

If all cold: ~3 page faults = ~10-30 μs

---

## Concrete Recommendations

### Rec-1: Warm Index on Startup

**Location:** `indexread/index.go` or application code

**Current behavior:** First query pays cold-start penalty.

**Proposed change:**
```go
// Add to Index struct
func (idx *Index) Warm(ctx context.Context) error {
    // Touch each mmap'd array to prefetch pages
    for i := uint64(0); i < idx.count; i += 512 { // Every 512th entry (~4KB page)
        _ = idx.objectCount.UnsafeGetU64(i)
        _ = idx.subtreeEnd.UnsafeGetU64(i)
        // ... other arrays
    }
    return nil
}
```

**Expected benefit:** Eliminate cold-start latency for first queries.

**Risk level:** Low

**How to test:** Measure p99 latency before/after warming.

---

### Rec-2: Cache Frequent Lookups

**Location:** Application layer or new `CachedIndex` wrapper

**Current behavior:** Every lookup goes through MPHF.

**Proposed change:**
```go
type CachedIndex struct {
    idx   *Index
    cache sync.Map // prefix -> (pos, Stats)
}

func (c *CachedIndex) LookupCached(prefix string) (uint64, Stats, bool) {
    if v, ok := c.cache.Load(prefix); ok {
        entry := v.(*cacheEntry)
        return entry.pos, entry.stats, true
    }
    pos, ok := c.idx.Lookup(prefix)
    if !ok {
        return 0, Stats{}, false
    }
    stats := c.idx.Stats(pos)
    c.cache.Store(prefix, &cacheEntry{pos: pos, stats: stats})
    return pos, stats, true
}
```

**Expected benefit:** Reduce repeated lookup latency.

**Risk level:** Low (cache invalidation not needed for read-only index)

**How to test:** Benchmark repeated lookups with/without cache.

---

### Rec-3: Batch Lookup API

**Location:** `indexread/index.go`

**Current behavior:** Single lookup per call.

**Proposed change:**
```go
func (idx *Index) LookupBatch(prefixes []string) (positions []uint64, found []bool) {
    positions = make([]uint64, len(prefixes))
    found = make([]bool, len(prefixes))

    // Parallelize for large batches
    if len(prefixes) > 100 {
        var wg sync.WaitGroup
        batchSize := len(prefixes) / runtime.NumCPU()
        for i := 0; i < len(prefixes); i += batchSize {
            end := min(i+batchSize, len(prefixes))
            wg.Add(1)
            go func(start, end int) {
                defer wg.Done()
                for j := start; j < end; j++ {
                    positions[j], found[j] = idx.Lookup(prefixes[j])
                }
            }(i, end)
        }
        wg.Wait()
    } else {
        for i, p := range prefixes {
            positions[i], found[i] = idx.Lookup(p)
        }
    }

    return positions, found
}
```

**Expected benefit:** Better throughput for bulk queries.

**Risk level:** Low

**How to test:** Benchmark batch lookup vs individual calls.

---

### Rec-4: Prefix Bloom Filter

**Location:** `format/mphf.go` or new filter file

**Current behavior:** MPHF + fingerprint verification for every lookup.

**Issue:** MPHF lookup for non-existent prefixes still computes hash.

**Proposed change:**
```go
// Add bloom filter for fast negative lookups
type MPHF struct {
    // ... existing fields
    bloom *bloom.BloomFilter
}

func (m *MPHF) Lookup(prefix string) (pos uint64, ok bool) {
    // Fast negative check
    if m.bloom != nil && !m.bloom.Test([]byte(prefix)) {
        return 0, false
    }
    // Full lookup if bloom says maybe
    return m.lookupFull(prefix)
}
```

**Expected benefit:** Faster rejection of non-existent prefixes.

**Risk level:** Medium (need to add bloom filter to build phase)

**How to test:** Benchmark with 50% non-existent prefix queries.

---

### Rec-5: Reduce Fingerprint Memory

**Location:** `format/mphf.go`

**Current behavior:** 64-bit fingerprints (8 bytes per prefix).

**Issue:** 64-bit fingerprint has very low collision probability (~1/2^64).

**Proposed change:**
```go
// Use 32-bit fingerprints instead
// Collision probability: ~1/2^32 = 2e-10 (still very low)
fingerprints *format.ArrayReader // uint32 instead of uint64
```

**Expected benefit:** 50% reduction in fingerprint memory.

**Risk level:** Medium (format change, backwards compatibility)

**How to test:** Verify collision rate with test dataset.

---

### Rec-6: Direct MPHF Position Indexing

**Location:** `format/mphf.go`

**Current behavior:**
```go
// Two-step lookup: hash -> hashPos -> preorderPos
hashPos := hashVal - 1
preorderPos := m.preorderPos.UnsafeGetU64(hashPos)
```

**Issue:** Extra indirection through position array.

**Proposed change (for index-only access):**
```go
// If index arrays are ordered by hash position (not preorder),
// we can skip the position lookup for arrays that don't need preorder.
// Stats(hashPos) directly instead of Stats(preorderPos)
```

**Expected benefit:** Eliminate one array access per lookup.

**Risk level:** High (requires reordering all arrays during build)

**Recommendation:** Skip - complexity not worth minor gain.

---

## Summary

Stage 7 is highly optimized for read access. The mmap-based design provides excellent performance with minimal memory footprint.

**Priority of recommendations:**

| ID | Title | Priority | Impact |
|----|-------|----------|--------|
| Rec-1 | Warm index on startup | Low | Eliminate cold-start |
| Rec-3 | Batch lookup API | Low | Better bulk throughput |
| Rec-2 | Cache frequent lookups | Low | App-level optimization |
| Rec-4 | Bloom filter | Low | Faster negative lookups |
| Rec-5 | Smaller fingerprints | Skip | Breaking change |
| Rec-6 | Direct hash indexing | Skip | Too complex |

**Overall assessment:** Stage 7 is well-designed and efficient. The main optimization opportunities are at the application level (caching, warming) rather than in the index implementation itself.
