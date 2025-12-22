# MPHF Profiling Results

This document presents CPU and memory profiling results for the MPHF (Minimal Perfect Hash Function) implementation.

## Environment

- **CPU**: AMD Ryzen 9 5950X 16-Core Processor
- **Go version**: 1.25.2
- **BBHash version**: v0.0.0-20250331135148-7358f69256fb

## Benchmark Results Summary

### Query Scaling (demonstrates O(n) rank cost)

| MPHF Size | ns/op | Allocs/op | Relative to 10K |
|-----------|-------|-----------|-----------------|
| 10,000 keys | 135 ns | 0 | 1.0x |
| 100,000 keys | 701 ns | 0 | 5.2x |
| 1,000,000 keys | 6,046 ns | 0 | 44.8x |

**Key insight**: Query time scales roughly linearly with MPHF size due to O(n) `bitVector.rank()`.

### Hashing Microbenchmarks

| Function | ns/op | Allocs/op |
|----------|-------|-----------|
| `hashString` | 17.8 ns | 0 |
| `hashBytes` | 17.4 ns | 0 |
| `computeFingerprint` | 16.9 ns | 0 |
| `computeFingerprintBytes` | 17.4 ns | 0 |
| Both hashes | 31.0 ns | 0 |

**Key insight**: Hashing is fast and allocation-free (Go optimizes `[]byte(s)` escape).

---

## CPU Profile: Query Path (1M keys)

```
Duration: 24.25s, Total samples = 60.02s (247.48%)

      flat  flat%   sum%        cum   cum%
    53.54s 89.20% 89.20%     53.58s 89.27%  github.com/relab/bbhash.bitVector.rank (inline)
     0.69s  1.15% 90.35%      0.69s  1.15%  hash/fnv.(*sum64a).Write (inline)
     0.45s  0.75% 91.10%      0.45s  0.75%  runtime.memmove
     0.33s  0.55% 92.37%      0.33s  0.55%  encoding/binary.littleEndian.Uint64 (inline)
     0.24s  0.40% 92.87%     54.03s 90.02%  github.com/relab/bbhash.BBHash2.Find (inline)
     0.19s  0.32% 93.59%     18.06s 30.09%  github.com/eunmann/s3-inv-db/pkg/format.(*MPHF).Lookup
```

### Breakdown

| Area | Flat Time | Percentage | Notes |
|------|-----------|------------|-------|
| `bitVector.rank` | 53.54s | **89.2%** | Dominant hotspot |
| FNV hashing | 0.69s | 1.15% | Fast |
| Memory operations | 0.45s | 0.75% | memmove |
| Binary decoding | 0.33s | 0.55% | Array reads |
| Other | 4.71s | 7.85% | GC, runtime, etc. |

---

## CPU Profile: Build Path (500K keys)

```
Duration: 502.25ms, Total samples = 1.74s (346.44%)

      flat  flat%   sum%        cum   cum%
     1.47s 84.48% 84.48%      1.47s 84.48%  github.com/relab/bbhash.bitVector.rank (inline)
     0.05s  2.87% 87.36%      1.54s 88.51%  computeFingerprintsParallel.func1
     0.03s  1.72% 89.08%      0.03s  1.72%  runtime.memmove
     0.02s  1.15% 90.23%      0.05s  2.87%  runtime.concatstrings
```

### Breakdown

| Area | Flat Time | Percentage | Notes |
|------|-----------|------------|-------|
| `bitVector.rank` | 1.47s | **84.5%** | Called during mph.Find in fingerprint phase |
| computeFingerprintsParallel | 0.05s | 2.87% | Coordinator overhead |
| Memory operations | 0.03s | 1.72% | memmove |
| bbhash.New (compute) | 0.01s | 0.57% | MPHF construction itself is fast |

---

## Memory Profile

Query benchmark (1M keys): **0 B/op, 0 allocs/op**

The query path is completely allocation-free:
- `hashString(s)` uses stack allocation for the FNV hash state
- `[]byte(s)` conversion is optimized away by the compiler
- Array reads use mmap'd data (no copies)

Build benchmark: Allocations are expected for:
- MPHF construction (bitvectors, collision detection)
- Temporary buffers for parallel processing
- Output file buffers

---

## Surprises vs Expectations

### Expected
- `bitVector.rank` would be expensive (confirmed: 84-89% of CPU)
- Hashing would be measurable (actual: only 1.15%)
- Query would be fast for small MPHFs (confirmed: 135 ns for 10K keys)

### Surprises
1. **Rank is even worse than expected**: 89% of query time, not 25% as seen in end-to-end
   - End-to-end profile has other work (I/O, parsing) that dilutes the MPHF cost
   - In isolation, rank completely dominates

2. **Hashing is allocation-free**: Go's escape analysis optimizes `[]byte(s)` in hot paths
   - No performance gain from switching to `hashBytes` in most cases

3. **Build is dominated by rank too**: During fingerprint computation, every prefix calls `mph.Find()` → rank
   - `bbhash.New()` (graph construction) is only 0.57% of build time!

4. **Query time scales linearly with MPHF size**: 44.8x slowdown from 10K to 1M keys
   - This is the O(n) rank behavior in action

---

## Root Causes of MPHF Slowness

### 1. `bitVector.rank` - The Dominant Problem

**What it does:**
```go
func (b bitVector) rank(i uint64) uint64 {
    x := i / 64      // word index
    y := i % 64      // bit offset

    var r int
    for k := uint64(0); k < x; k++ {     // ← O(n) SCAN
        r += bits.OnesCount64(b[k])       // popcount per word
    }
    v := b[x]
    r += bits.OnesCount64(v << (64 - y))  // partial word
    return uint64(r)
}
```

**Why it's slow:**
1. **O(n) complexity per query**: Each rank call scans all words up to position i
2. **Memory access pattern**: Sequential scan, but still touches many cache lines for large bitvectors
3. **Called on every level**: BBHash has multiple levels, each calling rank once per query
4. **Called during build too**: `computeFingerprintsParallel` calls `mph.Find()` for every prefix

**Hypotheses:**
- **Memory latency bound?** Unlikely - scan is sequential, good cache behavior
- **Branch predictor bound?** No - simple loop with predictable branches
- **Unnecessary work per query?** Yes! O(n) when O(1) is achievable with rank9

### 2. BBHash.Find - Multiple Level Traversal

**What it does:**
```go
func (bb BBHash) Find(key uint64) uint64 {
    for lvl, bv := range bb.bits {
        i := fast.Hash(uint64(lvl), key) % bv.size()
        if bv.isSet(i) {
            return bb.ranks[lvl] + bv.rank(i)   // ← calls rank!
        }
    }
    return 0
}
```

**Problem**: Most keys are found at level 0 or 1, but still pay full O(n) rank cost.

### 3. Fingerprint Computation - Minor Contribution

**What it does:**
- `hashString(prefix)` → FNV-1a hash for MPHF key
- `computeFingerprint(prefix)` → FNV-64 hash for verification

**Why it's NOT a big problem:**
- Only 1.15% of CPU in query profile
- Allocation-free due to compiler optimization
- Each call is O(k) where k is prefix length (typically 20-40 bytes)

### 4. Supporting Code - Negligible

**Array readers**: 0.6% of CPU
- `UnsafeGetU64` is just pointer arithmetic + `binary.LittleEndian.Uint64`
- Memory-mapped, no syscalls per read

**I/O and buffers**: Only visible in build profile, not query

---

## Key Takeaways

1. **Single optimization target**: `bitVector.rank` is 84-89% of CPU
2. **Standard solution exists**: Rank9 data structure provides O(1) rank with small space overhead
3. **Requires bbhash library change**: The rank implementation is in the external dependency
4. **Query scaling is broken**: Current O(n) rank makes large MPHFs impractical for high-throughput

---

## Profile Commands Reference

```bash
# CPU profile (query)
go test -run=^$ -bench=BenchmarkMPHFQuery_1M \
    -cpuprofile=mphf_query_cpu.prof \
    -benchtime=30s ./pkg/format

# CPU profile (build)
go test -run=^$ -bench=BenchmarkMPHFBuild_1M \
    -cpuprofile=mphf_build_cpu.prof \
    -benchtime=1x ./pkg/format

# Memory profile
go test -run=^$ -bench=BenchmarkMPHFQuery_1M \
    -memprofile=mphf_query_mem.prof \
    -benchmem ./pkg/format

# Analyze profiles
go tool pprof -http=:8080 mphf_query_cpu.prof
go tool pprof -top mphf_query_cpu.prof
go tool pprof -list='rank' mphf_query_cpu.prof
```
