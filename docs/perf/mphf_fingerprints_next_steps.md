# MPHF Fingerprint Pipeline - Optimization Backlog

This document catalogs concrete optimization opportunities in **our code only**
(not in the BBHash library). Each item includes estimated impact based on profiling.

## Summary Table

| ID | Category | Impact | Effort | Location |
|----|----------|--------|--------|----------|
| FP-1 | Hashing | 5% | Low | `mphf.go` |
| FP-2 | Allocations | 2% | Low | `mphf_streaming.go` |
| FP-3 | I/O | 1% | Low | `mphf_streaming.go` |
| FP-4 | Architecture | 0% | N/A | (not actionable) |

**Note**: The primary bottleneck (88% CPU) is in `bbhash.bitVector.rank()`,
which is outside our code. The optimizations below address the remaining 12%.

---

## FP-1: Zero-Copy FNV Hashing

**Category**: Hashing
**Location**: `pkg/format/mphf.go:340-369`
**Current behavior**: Uses `fnv.New64a()` / `fnv.New64()` which have interface dispatch overhead
**Expected impact**: ~5% improvement on fingerprint compute phase

### Current Implementation
```go
func hashBytes(b []byte) uint64 {
    h := fnv.New64a()  // Creates hasher via interface
    h.Write(b)         // Interface dispatch on each call
    return h.Sum64()
}
```

### Proposed Change
```go
// fnv1aZeroCopy computes FNV-1a hash without allocations or interface dispatch.
func fnv1aZeroCopy(b []byte) uint64 {
    const (
        offset64 = 14695981039346656037
        prime64  = 1099511628211
    )
    hash := uint64(offset64)
    for _, c := range b {
        hash ^= uint64(c)
        hash *= prime64
    }
    return hash
}
```

### Benchmark Evidence
```
BenchmarkHashingStrategies/FNV1a_Bytes_NoAlloc-32   13.22 ns/op
BenchmarkHashingStrategies/FNV1a_ZeroCopy-32         7.57 ns/op  (+43% faster)
```

### Risk Level
Low - Simple refactor, same algorithm, just inlined.

### Validation Benchmark
- `BenchmarkHashingStrategies`
- `BenchmarkFingerprintModes/ZeroCopy`

---

## FP-2: Pre-allocate Hash/Position Slices

**Category**: Allocations
**Location**: `pkg/format/mphf_streaming.go:53-55`
**Current behavior**: Initial capacity of 1024, grows via append
**Expected impact**: ~2% improvement (reduces GC pressure during Add phase)

### Current Implementation
```go
return &StreamingMPHFBuilder{
    hashes:      make([]uint64, 0, 1024),
    preorderPos: make([]uint64, 0, 1024),
    // ...
}
```

### Proposed Change
Add optional size hint parameter:
```go
func NewStreamingMPHFBuilderWithHint(tempDir string, sizeHint int) (*StreamingMPHFBuilder, error) {
    if sizeHint < 1024 {
        sizeHint = 1024
    }
    return &StreamingMPHFBuilder{
        hashes:      make([]uint64, 0, sizeHint),
        preorderPos: make([]uint64, 0, sizeHint),
        // ...
    }, nil
}
```

### Benchmark Evidence
Memory profile shows 123 MB for Add phase with 1M prefixes, indicating
multiple reallocations. Pre-allocating would reduce this to ~16 MB.

### Risk Level
Low - Additive API, doesn't change default behavior.

### Validation Benchmark
- `BenchmarkAddPhase_1M`
- Compare allocs/op before and after

---

## FP-3: Reduce Chunk Buffer Reallocations

**Category**: I/O / Allocations
**Location**: `pkg/format/mphf_streaming.go:295-314`
**Current behavior**: May reallocate chunk buffer if prefix is longer than expected
**Expected impact**: <1% (already well-optimized)

### Current Implementation
```go
const estimatedAvgPrefixLen = 24
chunkBuffer := make([]byte, 0, thisChunk*estimatedAvgPrefixLen)

// Buffer grows if prefix exceeds estimate
if cap(chunkBuffer)-start < int(prefixLen) {
    newCap := cap(chunkBuffer)*2 + int(prefixLen)
    newBuf := make([]byte, start, newCap)
    copy(newBuf, chunkBuffer)
    chunkBuffer = newBuf
}
```

### Proposed Change
Track max prefix length during Add phase, use it for better estimates:
```go
type StreamingMPHFBuilder struct {
    maxPrefixLen int  // Track during Add
    // ...
}

// In computeFingerprintsParallel:
estimatedAvgPrefixLen := b.totalBytes / b.count
if estimatedAvgPrefixLen < b.maxPrefixLen/2 {
    estimatedAvgPrefixLen = b.maxPrefixLen / 2
}
```

### Risk Level
Low - Internal optimization, no API change.

### Validation Benchmark
- `BenchmarkFingerprintComputeOnly_1M`

---

## FP-4: Alternative MPHF Architecture (NOT ACTIONABLE)

**Category**: Architecture
**Location**: N/A
**Current behavior**: Uses bbhash with per-key Find() during build
**Expected impact**: Would be ~88% if implemented

This is documented for completeness but is **out of scope** per the user's request
to treat BBHash as a black box.

### The Problem
During `computeFingerprintsParallel`, we call `mph.Find()` for each prefix to
get its hash position. This is necessary because:
1. BBHash assigns positions 1..N in its internal order
2. We need to store fingerprints at those positions
3. The only way to get the position is to call `Find()`

### Why It's Slow
The relab/bbhash `Find()` calls `rank()` which scans through the bitvector.
With gamma=2.0, the bitvector is ~2N bits, and rank is O(k) where k is the
number of hash function levels.

### Alternatives (require BBHash fork or different library)
1. **Rank9/Select support**: Adds rank index for O(1) lookups
2. **Batch Find API**: Process multiple keys more efficiently
3. **Return position during construction**: If `New()` returned positions,
   we wouldn't need to call `Find()`
4. **Different MPHF library**: CHD, PTHash, etc.

---

## Implementation Priority

Given the constraint of not forking BBHash:

1. **FP-1 (Zero-Copy Hashing)**: Implement first - highest impact, lowest risk
2. **FP-2 (Pre-allocate Slices)**: Implement second - reduces memory churn
3. **FP-3 (Chunk Buffer)**: Optional - marginal benefit

**Total expected improvement: ~7%** of build time, addressing 12% of CPU that
isn't in BBHash.

The remaining 88% in BBHash can only be addressed by:
- Forking and improving BBHash (future work)
- Switching to a different MPHF library (breaking change)
- Accepting current performance as baseline
