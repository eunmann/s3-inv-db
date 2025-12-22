# End-to-End Performance: Next Steps

**Based on actual profiling from 2025-12-21**

---

## Summary of Current State

After the previously merged optimizations (S2-R1, S3-R1, S3-R3, S5-R3), profiling reveals:

| Phase | Duration | % Total | Primary Bottleneck |
|-------|----------|---------|-------------------|
| Memory Aggregation | 148ms | 15% | Map operations |
| Sort & Write Run | 286ms | 29% | Sort + I/O |
| Merge & Build Index | 549ms | 56% | **MPHF fingerprints** |

**The merge & build phase now dominates execution time (56%)**, with MPHF fingerprint computation consuming 25% of total CPU time.

---

## Optimization Opportunities (Ranked by Impact)

### HIGH IMPACT

#### 1. Replace Tier Writer Maps with Fixed Arrays

**Location:** `pkg/extsort/indexbuild.go:251-275` (`writeTierStats`)

**Current Problem:**
- `writeTierStats` uses `map[tiers.ID]*ArrayWriter` for tier writers
- Each prefix requires ~24 map accesses (2 per tier × 12 tiers)
- 52% of `writeTierStats` time (5.43s of 10.36s) is spent in map operations

**Current Code:**
```go
func (b *IndexBuilder) writeTierStats(row *PrefixRow) error {
    for tierID := tiers.ID(0); tierID < tiers.NumTiers; tierID++ {
        _, hasCountWriter := b.tierCountWriters[tierID]  // map lookup
        _, hasBytesWriter := b.tierBytesWriters[tierID]  // map lookup
        // ...
        if countW, ok := b.tierCountWriters[tierID]; ok { // map lookup
            // write
        }
        if bytesW, ok := b.tierBytesWriters[tierID]; ok { // map lookup
            // write
        }
    }
}
```

**Proposed Fix:**
```go
type IndexBuilder struct {
    // Replace maps with fixed arrays
    tierCountWriters [tiers.NumTiers]*ArrayWriter
    tierBytesWriters [tiers.NumTiers]*ArrayWriter
    tierWriterInit   [tiers.NumTiers]bool
}

func (b *IndexBuilder) writeTierStats(row *PrefixRow) error {
    for tierID := tiers.ID(0); tierID < tiers.NumTiers; tierID++ {
        if !b.tierWriterInit[tierID] {
            if row.TierCounts[tierID] == 0 && row.TierBytes[tierID] == 0 {
                continue
            }
            if err := b.createTierWriter(tierID, row); err != nil {
                return err
            }
        }
        if countW := b.tierCountWriters[tierID]; countW != nil {
            if err := countW.WriteU64(row.TierCounts[tierID]); err != nil {
                return fmt.Errorf("write tier %d count: %w", tierID, err)
            }
        }
        // Similar for bytesW
    }
    return nil
}
```

**Expected Gain:** 5-10% overall speedup (eliminate 5.43s of map operations)

**Risk:** Low - simple refactoring, same semantics.

**Benchmark:** Run `BenchmarkExtsortEndToEnd` before/after.

---

#### 2. Optimize bbhash Rank Queries

**Location:** `github.com/relab/bbhash` (external dependency)

**Current Problem:**
- `bbhash.bitVector.rank` consumes 24.77% of total CPU (21.43s)
- This is called during `computeFingerprintsParallel` for each of 266K prefixes
- The rank operation has O(1) complexity but high constant factors

**Options:**
1. **Use SIMD-accelerated popcount** - Modern CPUs have POPCNT instruction
2. **Cache rank results** - Pre-compute rank at regular intervals
3. **Alternative MPHF library** - Consider `github.com/dgryski/go-mph` or others
4. **Skip fingerprint phase** - Store prefix→position mapping differently

**Expected Gain:** 10-20% overall speedup if rank queries can be halved

**Risk:** Medium-High - requires modifying or replacing external library.

**Benchmark:** Profile `BenchmarkExtsortPhases/index_build` specifically.

---

#### 3. Batch ArrayWriter Writes

**Location:** `pkg/format/writer.go:62-74` (`WriteU64`)

**Current Problem:**
- `ArrayWriter.WriteU64` is called millions of times (22M allocs attributed)
- Each call writes 8 bytes through bufio.Writer
- Function call overhead adds up

**Current Code:**
```go
func (w *ArrayWriter) WriteU64(val uint64) error {
    var buf [8]byte
    binary.LittleEndian.PutUint64(buf[:], val)
    _, err := w.writer.Write(buf[:])
    w.count++
    return err
}
```

**Proposed Fix:**
```go
// Add batching capability
type ArrayWriter struct {
    // ... existing fields
    batchBuf []byte
    batchPos int
}

func (w *ArrayWriter) WriteU64Batched(val uint64) {
    if w.batchPos+8 > len(w.batchBuf) {
        w.flushBatch()
    }
    binary.LittleEndian.PutUint64(w.batchBuf[w.batchPos:], val)
    w.batchPos += 8
    w.count++
}

func (w *ArrayWriter) flushBatch() {
    w.writer.Write(w.batchBuf[:w.batchPos])
    w.batchPos = 0
}
```

**Expected Gain:** 2-5% overall speedup

**Risk:** Low - maintains same semantics with buffering.

---

### MEDIUM IMPACT

#### 4. Reduce PrefixRow Allocations in Drain()

**Location:** `pkg/extsort/aggregator.go` (`Drain` method)

**Current Problem:**
- `PrefixStats.ToPrefixRow` allocates 662MB (2.9M allocations)
- Each PrefixRow is ~250 bytes
- Created fresh for every prefix during Drain()

**Proposed Fix:**
- Use a pool of PrefixRow objects
- Or reuse the same slice across benchmark iterations (for benchmarks only)

**Expected Gain:** 3-5% overall (reduced GC pressure)

**Risk:** Medium - requires careful lifecycle management.

---

#### 5. Reuse ZStd Encoder

**Location:** `pkg/extsort/compressed_run.go` (or equivalent)

**Current Problem:**
- ZStd encoder allocates 2.4GB across benchmark runs (32.91% of allocs)
- New encoder created per run file

**Proposed Fix:**
```go
var zstdEncoderPool = sync.Pool{
    New: func() any {
        enc, _ := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
        return enc
    },
}

func getEncoder(w io.Writer) *zstd.Encoder {
    enc := zstdEncoderPool.Get().(*zstd.Encoder)
    enc.Reset(w)
    return enc
}

func putEncoder(enc *zstd.Encoder) {
    enc.Reset(nil)
    zstdEncoderPool.Put(enc)
}
```

**Expected Gain:** 5-10% memory reduction, slight CPU improvement from reduced allocs

**Risk:** Low - standard pooling pattern.

---

#### 6. SIMD-Accelerated Prefix Scanning

**Location:** `pkg/extsort/aggregator.go:51` (`AddObject`)

**Current Code:**
```go
for i := range len(key) {
    if key[i] == '/' {
        // extract prefix
    }
}
```

**Proposed Fix:**
```go
for pos := 0; pos < len(key); {
    idx := strings.IndexByte(key[pos:], '/')
    if idx == -1 {
        break
    }
    prefix := key[:pos+idx+1]
    a.accumulate(prefix, depth, size, tierID)
    depth++
    pos += idx + 1
}
```

**Current Impact:** Only 360ms spent in the loop itself (low priority)

**Expected Gain:** 1-2% overall (loop is already fast)

**Risk:** Low - drop-in replacement.

---

### LOW IMPACT / FUTURE

#### 7. Parallel MPHF Construction

**Location:** `github.com/relab/bbhash` (`bbhash.New`)

**Current Problem:**
- MPHF construction is single-threaded
- For 266K prefixes, this takes a non-trivial portion of finalization time

**Expected Gain:** 2-4x faster MPHF build on multi-core

**Risk:** High - no mature Go library exists with parallel construction.

---

#### 8. Memory-Mapped Run Files

**Location:** `pkg/extsort/runfile.go`, `merger.go`

**Current Problem:**
- Run files use buffered file I/O
- Each read involves syscalls

**Expected Gain:** 5-10% faster merge phase

**Risk:** Medium - platform-specific behavior, careful error handling needed.

---

## Recommended Implementation Order

Based on impact and risk assessment:

| Priority | Optimization | Est. Impact | Risk | Effort |
|----------|-------------|-------------|------|--------|
| 1 | Tier writer fixed arrays | 5-10% | Low | 1 day |
| 2 | Batch ArrayWriter writes | 2-5% | Low | 0.5 day |
| 3 | Reuse ZStd encoder pool | 5-10% mem | Low | 0.5 day |
| 4 | SIMD prefix scanning | 1-2% | Low | 0.5 day |
| 5 | PrefixRow pooling | 3-5% | Medium | 1 day |
| 6 | Optimize bbhash rank | 10-20% | High | 3+ days |

**Total estimated improvement: 20-40%** if items 1-5 are implemented.

---

## Metrics to Track

When implementing any optimization:

| Metric | Current Value | Target |
|--------|--------------|--------|
| Total time (100K) | ~970ms | <700ms |
| Allocs/op | 6.9M | <4M |
| Alloc bytes/op | 388MB | <250MB |
| MPHF phase % | 25% | <15% |
| Tier writes % | 12% | <5% |

**Profiling commands:**
```bash
# CPU profile
go test -bench='BenchmarkExtsortEndToEnd/objects=100000' \
  -benchtime=50x -cpuprofile=cpu.prof ./pkg/extsort/...

# Memory profile
go test -bench='BenchmarkExtsortEndToEnd/objects=100000' \
  -benchtime=10x -memprofile=mem.prof ./pkg/extsort/...

# Quick benchmark comparison
go test -bench='BenchmarkExtsortEndToEnd' -benchtime=5x -benchmem ./pkg/extsort/...
```

---

## Previously Completed Optimizations

The following were implemented and merged to `main`:

| ID | Optimization | Impact | Status |
|----|--------------|--------|--------|
| S2-R1 | Memory accounting fix (8MB → 192MB per worker) | Prevents OOM | Merged |
| S3-R1 | Parquet ReaderAt (skip double temp file) | -100MB I/O per chunk | Merged |
| S3-R3 | pgzip parallel decompression | 2-4x faster gzip | Merged |
| S5-R3 | Larger merge fan-in (8 → 16) | Fewer merge rounds | Merged |

---

## Files Referenced

- `pkg/extsort/bench_test.go` - End-to-end benchmark
- `pkg/extsort/aggregator.go` - Memory aggregation
- `pkg/extsort/indexbuild.go` - Index construction (writeTierStats hotspot)
- `pkg/format/mphf_streaming.go` - MPHF builder
- `pkg/format/writer.go` - ArrayWriter
- `docs/perf/e2e_benchmark_profile.md` - Profiling results
