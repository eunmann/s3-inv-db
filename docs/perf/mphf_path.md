# MPHF Hot Path Analysis

This document maps the critical paths through the MPHF (Minimal Perfect Hash Function) build and query code.

## Overview

The MPHF implementation uses the BBHash algorithm via `github.com/relab/bbhash`. The key insight from end-to-end profiling is that **`bbhash.bitVector.rank` consumes ~25% of total CPU**, making MPHF the single biggest hotspot.

## Call Graph: Build Path

```
StreamingMPHFBuilder.Build()                    [mphf_streaming.go:109]
├── bbhash.New(hashes, Gamma(2.0))             [external: bbhash]
│   └── BBHash.compute()                        [bbhash.go:54]
│       ├── fast.LevelHash(level)              [fast/hash.go:11]
│       ├── fast.KeyHash(levelHash, k)         [fast/hash.go:16]
│       ├── bcVector.update(h)                 [bcvector.go]
│       ├── bcVector.unsetCollision(h)         [bcvector.go]
│       └── computeLevelRanks()                [bbhash.go:172]
│           └── bitVector.onesCount()          [bitvector.go:50]
│
├── computeFingerprintsParallel()              [mphf_streaming.go:224]
│   ├── [parallel workers]
│   │   ├── hashBytes(prefixBytes)             [mphf.go:349] (FNV-1a)
│   │   ├── mph.Find(keyHash)                  [bbhash_partitioned.go:95]
│   │   │   └── BBHash.Find(key)               [bbhash.go:34] ★ HOT
│   │   │       ├── fast.Hash(level, key)      [fast/hash.go:6]
│   │   │       ├── bitVector.isSet(i)         [bitvector.go:39]
│   │   │       └── bitVector.rank(i)          [bitvector.go:59] ★★ HOTTEST
│   │   └── computeFingerprintBytes(b)         [mphf.go:365] (FNV-64)
│
├── writeArraysParallel()                      [mphf_streaming.go:458]
│   ├── [goroutine] WriteU64Batch(fingerprints)
│   └── [goroutine] WriteU64Batch(positions)
│
└── writePrefixBlobOrdered()                   [mphf_streaming.go:363]
    └── BlobWriter.WriteBytes(prefixBuf)
```

## Call Graph: Query Path

```
MPHF.Lookup(prefix string)                      [mphf.go:275]
├── hashString(prefix)                          [mphf.go:341] (FNV-1a)
│   └── fnv.New64a()                           [stdlib: hash/fnv]
│   └── h.Write([]byte(s))                     ← allocation!
│   └── h.Sum64()
│
├── mph.Find(keyHash)                          [bbhash_partitioned.go:95]
│   └── BBHash.Find(key)                       [bbhash.go:34] ★ HOT
│       └── [for each level]
│           ├── fast.Hash(level, key)          [fast/hash.go:6]
│           │   └── LevelHash(level)           [fast/hash.go:11]
│           │   └── KeyHash(levelHash, key)    [fast/hash.go:16]
│           │       └── mix(h)                 [fast/hash.go:25] x3
│           ├── bitVector.isSet(i)             [bitvector.go:39]
│           │   └── b[i/64] & (1<<(i%64))      ← single memory access
│           └── bitVector.rank(i)              [bitvector.go:59] ★★ HOTTEST
│               └── [for k := 0; k < x; k++]   ← O(n) loop!
│                   └── bits.OnesCount64(b[k]) ← popcount per word
│
├── computeFingerprint(prefix)                 [mphf.go:357] (FNV-64)
│   └── fnv.New64()
│   └── h.Write([]byte(s))                     ← allocation!
│   └── h.Sum64()
│
├── fingerprints.UnsafeGetU64(hashPos)         [reader.go:188]
│   └── binary.LittleEndian.Uint64(data[idx*8:])
│
└── preorderPos.UnsafeGetU64(hashPos)          [reader.go:188]
    └── binary.LittleEndian.Uint64(data[idx*8:])
```

## Where `bitVector.rank` Sits in the Call Stack

The rank operation is called from:
1. **Build time**: `computeFingerprintsParallel` → `mph.Find` → `BBHash.Find` → `bv.rank`
2. **Query time**: `MPHF.Lookup` → `mph.Find` → `BBHash.Find` → `bv.rank`

### The Rank Implementation (bitvector.go:59-70)

```go
func (b bitVector) rank(i uint64) uint64 {
    x := i / 64      // word index
    y := i % 64      // bit offset within word

    var r int
    for k := uint64(0); k < x; k++ {     // ← O(n) SCAN
        r += bits.OnesCount64(b[k])       // popcount
    }
    v := b[x]
    r += bits.OnesCount64(v << (64 - y))  // partial word
    return uint64(r)
}
```

**Key observation**: This is a **naive rank** implementation with O(n) complexity. Each call to rank scans all words from 0 to the target position. For large bitvectors, this is expensive.

## Fingerprint/Hash Computation

Two hash functions are used:

### 1. MPHF Key Hash: FNV-1a 64-bit (`hashString`/`hashBytes`)

```go
// mphf.go:341
func hashString(s string) uint64 {
    h := fnv.New64a()          // allocs hash state
    h.Write([]byte(s))         // allocs []byte from string!
    return h.Sum64()
}

// mphf.go:349 (allocation-free variant)
func hashBytes(b []byte) uint64 {
    h := fnv.New64a()
    h.Write(b)
    return h.Sum64()
}
```

### 2. Verification Fingerprint: FNV-64 (`computeFingerprint`/`computeFingerprintBytes`)

```go
// mphf.go:357
func computeFingerprint(s string) uint64 {
    h := fnv.New64()           // different hash!
    h.Write([]byte(s))         // allocs []byte from string!
    return h.Sum64()
}
```

**Key observation**: `hashString` and `computeFingerprint` both allocate `[]byte(s)` on each call. This is wasteful when called in hot loops.

## BBHash Internal Hashing (fast package)

BBHash uses a custom fast hash for level/key mixing:

```go
// fast/hash.go
func Hash(level, key uint64) uint64 {
    return KeyHash(LevelHash(level), key)
}

func KeyHash(levelHash, key uint64) uint64 {
    var h uint64 = levelHash
    h ^= mix(key)
    h *= m  // m = 0x880355f21e6d1965
    h = mix(h)
    return h
}

func mix(h uint64) uint64 {
    h ^= h >> 23
    h *= 0x2127599bf4325c37
    h ^= h >> 47
    return h
}
```

This is already well-optimized (no allocations, fast mixing).

---

## Benchmarks

### Existing Benchmarks (mphf_test.go)

| Benchmark | Description | Key Count | Expected Runtime |
|-----------|-------------|-----------|------------------|
| `BenchmarkMPHFLookup` | Query performance | 10,000 | ~100µs/op |
| `BenchmarkStreamingMPHFBuild` | Full build cycle | 100,000 | ~1-2s/op |
| `BenchmarkStreamingMPHFBuild500K` | Large build | 500,000 | ~5-10s/op |

### New Targeted Benchmarks Needed

See `pkg/format/mphf_bench_test.go` for dedicated benchmarks:

| Benchmark | Target | Description |
|-----------|--------|-------------|
| `BenchmarkMPHFBuild_1M` | Build | 1M keys, measures bbhash.New + fingerprints |
| `BenchmarkMPHFBuild_5M` | Build | 5M keys, stress test |
| `BenchmarkMPHFQuery_1M` | Query | Random lookups across 1M-key MPHF |
| `BenchmarkMPHFQuery_10M` | Query | Random lookups, larger dataset |
| `BenchmarkHashString` | Hashing | FNV-1a isolated |
| `BenchmarkComputeFingerprint` | Hashing | FNV-64 isolated |

---

## How to Profile

### CPU Profile (Build)

```bash
go test -run=^$ -bench=BenchmarkMPHFBuild_1M \
    -cpuprofile=mphf_build_cpu.prof \
    -benchtime=30s \
    ./pkg/format
```

### CPU Profile (Query)

```bash
go test -run=^$ -bench=BenchmarkMPHFQuery_1M \
    -cpuprofile=mphf_query_cpu.prof \
    -benchtime=30s \
    ./pkg/format
```

### Memory Profile

```bash
go test -run=^$ -bench=BenchmarkMPHFBuild_1M \
    -memprofile=mphf_build_mem.prof \
    -benchmem \
    ./pkg/format
```

### Analyze Profiles

```bash
# Interactive web UI
go tool pprof -http=:8080 mphf_build_cpu.prof

# Top functions
go tool pprof -top mphf_build_cpu.prof

# Annotated source
go tool pprof -list='rank' mphf_build_cpu.prof
```

---

## Summary: Why MPHF is Slow

| Area | Function | Issue |
|------|----------|-------|
| **Rank** | `bitVector.rank` | O(n) linear scan per query |
| **Hashing** | `hashString` | Allocates `[]byte` from string |
| **Hashing** | `computeFingerprint` | Allocates `[]byte` from string |
| **Build** | `bbhash.New` | CPU-intensive graph construction |
| **Lookup** | `BBHash.Find` | Multiple levels, each calling rank |

The dominant cost is `bitVector.rank`. Standard optimization is a **rank9** or **rank-select** data structure with O(1) rank using precomputed superblock/block ranks.
