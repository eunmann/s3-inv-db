# MPHF Fingerprint Build - Profile Analysis

This document analyzes CPU and memory profiles from the fingerprint build pipeline,
focusing on identifying bottlenecks **in our code** vs **in BBHash**.

## Executive Summary

**Critical Finding**: 88% of CPU time during fingerprint computation is spent inside
`github.com/relab/bbhash.bitVector.rank`, which is called via `mph.Find()` for each prefix.

This is NOT primarily a fingerprint computation problem - it's an MPHF lookup problem
that happens during the build phase.

## Benchmark Configuration

- **Benchmark**: `BenchmarkFingerprintPipeline_1M`
- **Prefixes**: 1,000,000 realistic S3-style paths
- **Hardware**: AMD Ryzen 9 5950X 16-Core Processor
- **Go version**: (current)

## Phase Timing Breakdown (1M prefixes)

| Phase | Duration | Percentage |
|-------|----------|------------|
| Add (hash + temp file write) | 150 ms | 19% |
| BBHash.New construction | 29 ms | 4% |
| Fingerprint compute (parallel) | 509 ms | 64% |
| Array writes + blob | ~100 ms | 13% |
| **Total** | ~800 ms | 100% |

The fingerprint computation phase dominates at 64% of total build time.

## CPU Profile Analysis

```
Collected: 24.72s total CPU time (parallel workers)
Duration: 3.31s wall clock

Top CPU consumers:
88.43%  bbhash.bitVector.rank    <- Inside BBHash library
 0.97%  hash/fnv.(*sum64a).Write <- Our hash computation
 1.98%  runtime.mallocgc          <- Allocations
 0.89%  syscalls                  <- I/O
```

### Critical Path: `computeFingerprintsParallel`

The worker function in `computeFingerprintsParallel` does this for each prefix:

```go
keyHash := hashBytes(item.prefixBytes)     // ~13 ns
hashVal := mph.Find(keyHash)               // ~5800 ns  <-- 99.8% of time!
fingerprints[hashPos] = computeFingerprintBytes(item.prefixBytes)  // ~13 ns
preorderPositions[hashPos] = b.preorderPos[item.index]  // trivial
```

**The per-prefix cost is ~5.8 μs, of which 99.8% is `mph.Find()`.**

### Why is `mph.Find()` Slow?

The relab/bbhash library implements `Find()` using a `rank()` operation on its
internal bitvector. Their `rank()` implementation appears to be O(k) where k is
the number of levels in the hash function, but with a large constant factor.

For 1M keys with gamma=2.0:
- Bitvector size is roughly 2M bits
- Each `Find()` calls `rank()` multiple times
- Each `rank()` scans through the bitvector

## Memory Profile Analysis

```
Total allocations: ~300 MB for 1M prefixes

Breakdown:
123 MB (28%)  Add phase - hash/position slices growing
108 MB (24%)  computeFingerprintsParallel - chunk buffers
 45 MB (10%)  Test setup (fmt.Sprintf for prefixes)
 23 MB (5%)   Build phase - output arrays
 15 MB (3%)   ArrayWriter output buffering
```

### Our Code Allocation Patterns

1. **Add phase**: Slice growth for `hashes` and `preorderPos`
   - Initial capacity 1024, grows to 1M
   - Multiple reallocations during growth
   - ~123 MB total (including intermediate copies)

2. **Fingerprint compute**: Chunk buffer allocations
   - One ~1.2 MB buffer per 50K-prefix chunk
   - 20 chunks for 1M prefixes
   - Total ~24 MB per iteration, but GC reclaims between chunks

## Fingerprints: Cost vs Benefit

### What Happens Without Fingerprints?

Tested via `BenchmarkFingerprintModes`:

| Mode | Duration | Delta vs Full |
|------|----------|---------------|
| Full64 (current) | 510.7 ms | baseline |
| NoFingerprint | 501.5 ms | -1.8% |
| ZeroCopy FNV | 487.0 ms | -4.6% |

**Removing fingerprint computation saves only 1.8%** because the actual hash
computation (FNV) is trivial compared to `mph.Find()`.

### Why Do We Need Fingerprints?

Fingerprints are used during **lookup** to detect false positives:

```go
func (m *MPHF) Lookup(prefix string) (pos uint64, ok bool) {
    hashVal := m.mph.Find(hashString(prefix))
    storedFP := m.fingerprints.UnsafeGetU64(hashPos)
    computedFP := computeFingerprint(prefix)
    if storedFP != computedFP {
        return 0, false  // False positive!
    }
    return m.preorderPos.UnsafeGetU64(hashPos), true
}
```

Without fingerprints:
- An MPHF always returns SOME position for ANY input
- Without verification, we'd return wrong data for keys not in the original set
- This is critical for prefix lookups that might not exist

### Correctness Trade-offs

| Fingerprint Bits | False Positive Rate | Memory/entry |
|------------------|---------------------|--------------|
| 64-bit (current) | ~2^-64 | 8 bytes |
| 32-bit | ~2^-32 (1 in 4B) | 4 bytes |
| 16-bit | ~2^-16 (1 in 65K) | 2 bytes |
| None | 100% (unusable) | 0 bytes |

**Recommendation**: Keep 64-bit fingerprints. The memory cost is acceptable
(8 MB per 1M prefixes) and provides extremely strong guarantees.

## What We Can Optimize (Without Forking BBHash)

### In Our Code

1. **Zero-copy FNV hashing**: ~5% improvement
   - Replace `fnv.New64a()` with inline FNV implementation
   - Avoids interface dispatch overhead

2. **Pre-allocate hash/position slices**: Reduces Add phase allocations
   - If we know approximate count, pre-size to avoid growth copies

3. **Skip fingerprint compute if not needed**: ~2% improvement
   - For use cases that tolerate false positives (rare)

### NOT in Our Code (BBHash library)

The real bottleneck (88% of CPU) is in BBHash's `rank()` implementation.
To fix this would require:
- Forking BBHash and implementing Rank9 or similar
- Or using a different MPHF library with better rank performance

## Experimental: Alternative Architectures

### Idea 1: Skip mph.Find() During Build

Instead of calling `mph.Find()` for each prefix, we could:
1. Store fingerprints in original order (not hash order)
2. During lookup, accept that we need the original index

**Problem**: This defeats the purpose of MPHF for position lookup.

### Idea 2: Batch Construction

BBHash's `New()` already batch-constructs the MPHF. But the fingerprint
ordering step requires per-key `Find()` calls, which can't be batched
in the current BBHash API.

### Idea 3: Use a Different MPHF Library

Libraries like CHD or PTHash may have better query performance, but would
require significant code changes.

## Conclusion

The fingerprint build is slow primarily due to `bbhash.bitVector.rank()`,
not due to our fingerprint computation code.

**Actionable optimizations in our code**:
1. Zero-copy FNV: ~5% improvement (easy)
2. Pre-allocate slices: minor improvement (easy)
3. Evaluate if 32-bit fingerprints acceptable: 0% build improvement, 4B/entry memory savings

**Non-actionable without BBHash changes**:
- The 88% spent in `rank()` requires forking BBHash or switching libraries
