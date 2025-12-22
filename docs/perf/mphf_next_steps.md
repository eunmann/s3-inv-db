# MPHF Optimization Backlog

This document lists potential optimizations for the MPHF implementation, prioritized by expected impact and risk.

## Summary Table

| ID | Area | Expected Impact | Risk | Status |
|----|------|-----------------|------|--------|
| MPHF-1 | rank | **Critical (10-100x)** | High | Blocked on bbhash |
| MPHF-2 | rank | High (2-4x) | Medium | Potential workaround |
| MPHF-3 | queries | Medium (10-30%) | Low | Feasible |
| MPHF-4 | hashing | Low (2-5%) | Low | Marginal benefit |
| MPHF-5 | layout | Medium (20-40%) | Medium | Cache optimization |
| MPHF-6 | memory | Low (space only) | Low | Space optimization |
| MPHF-7 | compiler | Low (5-15%) | Low | Free optimization |

---

## MPHF-1: Implement Rank9 in BBHash

**Area**: `rank`

**Location(s)**:
- External: `github.com/relab/bbhash/bitvector.go:59` (bitVector.rank)

**Problem**:
Current rank implementation is O(n) - linear scan of all words up to position i:
```go
for k := uint64(0); k < x; k++ {
    r += bits.OnesCount64(b[k])
}
```

**Idea**:
Implement Rank9 data structure that provides O(1) rank with ~3.125% space overhead:
- Divide bitvector into 512-bit superblocks and 64-bit blocks
- Precompute cumulative rank at superblock boundaries
- Store block ranks relative to superblock start
- Query: one lookup per level (superblock + block + popcount)

**Expected Impact**: **10-100x** query speedup
- Current: 6000 ns/op for 1M keys
- Target: 60-600 ns/op (same as 10K keys)

**Risk**: High
- Requires modifying external dependency (fork bbhash)
- Or: convince upstream maintainer to add rank9
- Or: write custom MPHF implementation

**How to Measure**:
- `BenchmarkMPHFQueryScaling` should show constant time across MPHF sizes
- `BenchmarkMPHFQuery_1M` should drop from ~6000 ns to <600 ns

---

## MPHF-2: Cache Rank per Level

**Area**: `rank`

**Location(s)**:
- External: `github.com/relab/bbhash/bbhash.go:34` (BBHash.Find)

**Problem**:
BBHash calls rank once per level during Find. With naive O(n) rank, this compounds the problem.

**Idea**:
Precompute and cache rank values at fixed intervals within each level:
- At MPHF load time, compute rank every 1024 positions
- During query, interpolate: cached_rank + popcount(remaining_words)
- Reduces worst-case from O(n) to O(n/1024) ≈ O(1) for practical sizes

**Expected Impact**: 2-4x speedup
- Reduces average words scanned from n/2 to ~512

**Risk**: Medium
- Requires modifying bbhash or wrapping it
- Adds memory overhead for cached ranks
- Less elegant than proper rank9

**How to Measure**:
- `BenchmarkMPHFQuery_1M` should improve by 2-4x
- Memory benchmark should show modest overhead

---

## MPHF-3: Reduce MPHF Calls During Build

**Area**: `queries`

**Location(s)**:
- `pkg/format/mphf_streaming.go:257-271` (computeFingerprintsParallel worker)

**Problem**:
During build, we call `mph.Find()` for every prefix to get its hash position:
```go
hashVal := mph.Find(keyHash)  // Expensive!
```

**Idea**:
Batch the MPHF lookups and amortize overhead:
1. Use `mph.Find()` in a vectorized manner (if bbhash supports it)
2. Or: sort prefixes by expected level to improve cache locality
3. Or: use level-aware batching to reduce redundant hash computations

**Expected Impact**: 10-30% build speedup

**Risk**: Low
- Changes are local to our code
- No external dependency changes needed

**How to Measure**:
- `BenchmarkMPHFBuild_1M` should show improvement
- Profile should show reduced time in `bitVector.rank`

---

## MPHF-4: Hash State Reuse

**Area**: `hashing`

**Location(s)**:
- `pkg/format/mphf.go:341-345` (hashString)
- `pkg/format/mphf.go:357-361` (computeFingerprint)

**Problem**:
Each hash call creates a new FNV state object:
```go
h := fnv.New64a()   // Allocates on each call
h.Write([]byte(s))
return h.Sum64()
```

**Idea**:
1. Use sync.Pool for hash state objects
2. Or: use a hash function that doesn't require state (xxHash, wyhash)
3. Or: for parallel workers, allocate one hasher per worker

**Expected Impact**: 2-5% overall improvement
- Hashing is only 1.15% of CPU currently
- Diminishing returns

**Risk**: Low
- Local change, no API impact
- Modern hash libraries are well-tested

**How to Measure**:
- `BenchmarkHashString` should show improvement
- `BenchmarkBothHashes` should show ~10-20% improvement
- Overall query benchmark improvement will be minimal

---

## MPHF-5: Improve Data Locality

**Area**: `layout`

**Location(s)**:
- `pkg/format/mphf.go:177-183` (MPHF struct)
- External: bbhash bitvector layout

**Problem**:
Query accesses multiple memory regions:
1. MPHF bitvectors (per level)
2. Fingerprint array
3. Position array

**Idea**:
1. Interleave fingerprint and position data to reduce cache misses
2. Prefetch fingerprint/position while computing hash
3. Align bitvector words to cache line boundaries

**Expected Impact**: 20-40% speedup for cache-bound workloads

**Risk**: Medium
- Requires changing file format (migration needed)
- May hurt some access patterns while helping others

**How to Measure**:
- `BenchmarkMPHFQuerySequential_1M` vs random should narrow
- Cache miss counters (perf stat) should decrease

---

## MPHF-6: Smaller Fingerprints

**Area**: `memory`

**Location(s)**:
- `pkg/format/mphf.go:357` (computeFingerprint)
- `pkg/format/mphf_streaming.go:269` (computeFingerprintBytes)

**Problem**:
Using 64-bit fingerprints when fewer bits may suffice:
- 64-bit: collision probability = 2^-64 per lookup
- 32-bit: collision probability = 2^-32 per lookup (still 1 in 4 billion)

**Idea**:
Use 32-bit fingerprints to halve fingerprint array size:
- For 1M keys: 8MB → 4MB fingerprint array
- Slightly higher false positive rate, but still negligible

**Expected Impact**: 50% reduction in fingerprint array size

**Risk**: Low
- Slight increase in false positive probability
- Requires file format change

**How to Measure**:
- File size reduction (mph_fp.u64 halves)
- No measurable impact on correctness for normal workloads

---

## MPHF-7: Profile-Guided Optimization (PGO)

**Area**: `compiler`

**Location(s)**:
- All MPHF code paths

**Problem**:
Default Go compiler settings may miss optimization opportunities.

**Idea**:
Use PGO to guide compiler optimizations:
```bash
go test -run=^$ -bench=BenchmarkMPHFBuildAndQuery_1M \
    -cpuprofile=default.pgo ./pkg/format
go build -pgo=default.pgo ./cmd/s3inv
```

**Expected Impact**: 5-15% improvement
- Better inlining decisions
- Optimized branch layout
- May help bbhash internals

**Risk**: Low
- Free optimization, no code changes
- Requires maintaining PGO profile

**How to Measure**:
- Compare benchmark results with and without PGO
- See `docs/perf/mphf_pgo.md` for detailed workflow

---

## Recommended Priority Order

### Phase 1: Quick Wins (Now)
1. **MPHF-7** (PGO): Free 5-15% improvement, no code changes
2. **MPHF-4** (Hash reuse): Simple code change, marginal benefit

### Phase 2: Medium Term
3. **MPHF-3** (Batch queries): Moderate effort, moderate benefit
4. **MPHF-2** (Cached rank): Workaround if can't modify bbhash

### Phase 3: High Impact (Requires External Change)
5. **MPHF-1** (Rank9): The real fix, requires bbhash fork or PR
   - This should be the primary focus
   - Everything else is incremental

### Phase 4: Polish
6. **MPHF-5** (Locality): Nice-to-have cache optimization
7. **MPHF-6** (Smaller FP): Space optimization if needed

---

## Decision Point: Fork BBHash or Write Custom?

Given that 89% of CPU is in `bitVector.rank`, the key decision is:

### Option A: Fork BBHash
- Add rank9 support to existing implementation
- Maintain fork long-term or upstream the changes
- Preserves compatibility with existing serialization format

### Option B: Write Custom MPHF
- Full control over implementation
- Can optimize for our specific use case
- More work upfront, but no external dependency

### Recommendation
Start with Option A (fork bbhash), but design the integration layer to allow swapping implementations later.
