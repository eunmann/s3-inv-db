# End-to-End Benchmark Profiling Results

This document contains actual profiling results from running the end-to-end benchmark.

**Profile Date:** 2025-12-21
**System:** AMD Ryzen 9 5950X 16-Core, Linux, Go 1.24

---

## Benchmark Summary

**Primary Benchmark:** `BenchmarkExtsortEndToEnd/objects=100000`
**File:** `pkg/extsort/bench_test.go:33`

### Per-Iteration Metrics (100K objects)

| Metric | Value |
|--------|-------|
| Total time | ~970ms |
| Prefixes generated | 266,712 |
| Allocations | 6.9M allocs/op |
| Memory allocated | 388MB/op |
| Run file size | 61.4MB |
| Index size | 43.7MB |

### Phase Breakdown

| Phase | Duration | % Total |
|-------|----------|---------|
| Memory Aggregation | 148ms | 15.0% |
| Sort & Write Run | 286ms | 29.1% |
| Merge & Build Index | 549ms | 55.9% |
| **TOTAL** | 983ms | 100% |

---

## CPU Profile Analysis

**Profile Command Used:**
```bash
go test -bench='BenchmarkExtsortEndToEnd/objects=100000' \
  -benchtime=50x -cpuprofile=cpu.prof ./pkg/extsort/...
```

**Total Sample Time:** 86.52s (149.21% CPU utilization)

### Top CPU Hotspots (Flat Time)

| Function | Package | Flat Time | % Total | Notes |
|----------|---------|-----------|---------|-------|
| `bitVector.rank` | bbhash | 21.43s | 24.77% | MPHF lookup bit operations |
| `Syscall6` | syscall | 5.87s | 6.78% | File I/O syscalls |
| `memclrNoHeapPointers` | runtime | 5.86s | 6.77% | Memory clearing |
| `aeshashbody` | runtime | 4.82s | 5.57% | Map hash computation |
| `memmove` | runtime | 3.25s | 3.76% | Memory copy |
| `cmpbody` | runtime | 2.56s | 2.96% | String comparison (sort) |
| `RunFileWriter.Write` | extsort | 1.90s | 2.20% | Binary encoding |
| `RunFileWriter.WriteSorted.func1` | extsort | 1.87s | 2.16% | Sort comparison |
| `PrefixStats.ToPrefixRow` | extsort | 1.50s | 1.73% | Struct conversion |
| `map.matchH2` | runtime | 1.48s | 1.71% | Map probing |

### Top CPU Hotspots (Cumulative Time)

| Function | Cumulative | % Total | Notes |
|----------|------------|---------|-------|
| `computeFingerprintsParallel.func1` | 22.69s | 26.23% | Parallel fingerprint workers |
| `bbhash.BBHash2.Find` | 22.01s | 25.44% | MPHF lookup |
| `IndexBuilder.AddAllWithContext` | 19.17s | 22.16% | Main index building loop |
| `IndexBuilder.Add` | 14.68s | 16.97% | Per-prefix index addition |
| `mallocgc` | 11.59s | 13.40% | Memory allocation |
| `writeTierStats` | 10.36s | 11.97% | Per-tier array writes |
| `RunFileWriter.WriteSorted` | 9.86s | 11.40% | Sort + write run file |
| `mapaccess2` | 7.63s | 8.82% | Map lookups |
| `gcDrain` | 7.35s | 8.50% | GC mark work |
| `Aggregator.AddObject` | 6.92s | 8.00% | Object aggregation |
| `Aggregator.accumulate` | 6.48s | 7.49% | Prefix map updates |
| `slices.SortFunc` | 5.24s | 6.06% | PDQSort |
| `MergeIterator.Next` | 4.45s | 5.14% | K-way merge iteration |

### Key CPU Findings

#### 1. MPHF Fingerprint Computation is the Dominant Hotspot (25%)

The `bbhash.bitVector.rank` function consumes 24.77% of total CPU time. This is called during MPHF fingerprint computation in `computeFingerprintsParallel`, which:
- Iterates through all 266K prefixes
- Calls `mph.Find()` for each to get the hash position
- The Find operation uses bit vector rank queries which are O(1) but have high constant factors

**Code Path:**
```
IndexBuilder.Finalize
  → StreamingMPHFBuilder.Build
    → computeFingerprintsParallel (22.69s cumulative)
      → bbhash.BBHash2.Find (22.01s)
        → bbhash.bitVector.rank (21.43s)
```

#### 2. writeTierStats Has Excessive Map Lookups (12%)

The `writeTierStats` function spends 5.43s (~52% of its time) in map lookups:
- Lines 253-254: `tierCountWriters[tierID]` and `tierBytesWriters[tierID]` checks
- Lines 265-270: Additional map accesses for the actual writes
- This is called 12 times per prefix (once per tier), totaling ~24 map accesses per prefix

**Optimization Opportunity:** Store tier writers in a fixed-size array instead of maps.

#### 3. Sort is Already Efficient (6%)

PDQSort (`slices.SortFunc`) takes only 5.24s (6%) which is expected for O(n log n) complexity with 266K prefixes. No optimization needed here.

#### 4. Aggregation is Lighter Than Expected (8%)

The aggregation phase (`Aggregator.AddObject` + `accumulate`) uses only 8% of CPU, not the predicted 20-30%. The byte-by-byte '/' scanning loop is fast (360ms for the scan itself).

---

## Memory Profile Analysis

**Profile Command Used:**
```bash
go test -bench='BenchmarkExtsortEndToEnd/objects=100000' \
  -benchtime=10x -memprofile=mem.prof ./pkg/extsort/...
```

**Total Allocations:** 7.46GB across 60.2M allocations (for 10 iterations)

### Top Memory Allocators (Bytes)

| Function | Bytes | % Total | Notes |
|----------|-------|---------|-------|
| `zstd.encoderOptions.encoder` | 2,456MB | 32.91% | ZStd compression buffers |
| `RunFileReader.Read` | 787MB | 10.55% | Prefix row deserialization |
| `PrefixStats.ToPrefixRow` | 662MB | 8.87% | Struct creation in Drain() |
| `NewAggregator.func2` (Pool) | 617MB | 8.26% | PrefixStats from sync.Pool |
| `ArrayWriter.WriteU64` | 343MB | 4.59% | Buffered writes |
| `StreamingMPHFBuilder.Add` | 335MB | 4.48% | Hash/position storage |
| `computeFingerprintsParallel` | 310MB | 4.16% | Chunk buffers |
| `bufio.NewWriterSize` | 307MB | 4.12% | Buffered writers |
| `bufio.NewReaderSize` | 207MB | 2.78% | Buffered readers |
| `Aggregator.accumulate` | 206MB | 2.76% | Prefix string allocations |

### Top Allocators (Object Count)

| Function | Allocs | % Total | Notes |
|----------|--------|---------|-------|
| `ArrayWriter.WriteU64` | 22.4M | 37.28% | Per-write overhead |
| `RunFileReader.Read` | 6.5M | 10.88% | Per-row reads |
| `generateKey` | 4.9M | 8.21% | Benchmark data gen |
| `fmt.Sprintf` | 3.7M | 6.13% | String formatting |
| `StreamingMPHFBuilder.Add` | 3.4M | 5.72% | Per-prefix addition |
| `ArrayWriter.WriteU32` | 3.4M | 5.71% | Per-write overhead |
| `PrefixStats.ToPrefixRow` | 2.9M | 4.80% | Struct conversion |
| `sync.Pool.Get` (Aggregator) | 2.9M | 4.79% | Pool allocation |
| `MergeIterator.advanceReader` | 2.8M | 4.68% | Per-row advances |
| `mergeHeap.Pop` | 2.7M | 4.46% | Heap operations |

### Key Memory Findings

#### 1. ZStd Encoder is the Largest Allocator (33%)

The ZStd compression library allocates 2.4GB for encoder state. This happens each time a compressed run file is created. Per iteration (~100K prefixes):
- Each encoder allocates ~245MB
- With 10 benchmark iterations, this adds up

**Recommendation:** Ensure encoder reuse via sync.Pool is working correctly.

#### 2. PrefixRow Creation is Expensive (9%)

`PrefixStats.ToPrefixRow` allocates 662MB (2.9M allocations). Each PrefixRow is ~250 bytes, and we create 266K per iteration.

**Observation:** This is fundamentally required for the pipeline - rows must be created for sorting and writing.

#### 3. ArrayWriter Has Surprising Allocation Count (37%)

`ArrayWriter.WriteU64` shows 22M allocations but the code uses stack-allocated buffers. The allocations are likely:
- Indirect allocations from `bufio.Writer` buffer management
- Counted transitively from called functions

---

## Profiling Commands Reference

### CPU Profiling
```bash
# Generate profile
go test -bench='BenchmarkExtsortEndToEnd/objects=100000' \
  -benchtime=50x -cpuprofile=cpu.prof ./pkg/extsort/...

# Interactive web UI
go tool pprof -http=:8080 cpu.prof

# Top functions by flat time
go tool pprof -top cpu.prof

# Top functions by cumulative time
go tool pprof -top -cum cpu.prof

# Source-level annotation
go tool pprof -list='FunctionName' cpu.prof
```

### Memory Profiling
```bash
# Generate profile
go test -bench='BenchmarkExtsortEndToEnd/objects=100000' \
  -benchtime=10x -memprofile=mem.prof ./pkg/extsort/...

# Allocation bytes
go tool pprof -alloc_space -top mem.prof

# Allocation count
go tool pprof -alloc_objects -top mem.prof

# In-use at end
go tool pprof -inuse_space -top mem.prof
```

### Scaling Tests
```bash
# Run with larger inputs (gated)
S3INV_LONG_BENCH=1 go test -bench='BenchmarkExtsortEndToEnd_Scaling' \
  -benchtime=5x ./pkg/extsort/...
```

---

## Pipeline Coverage

| Stage | Covered | Notes |
|-------|---------|-------|
| S3 Download | No | Uses synthetic data |
| CSV/Parquet Parse | No | Pre-structured data |
| Memory Aggregation | **Yes** | Full coverage |
| Sort & Flush | **Yes** | Full coverage |
| K-Way Merge | Partial | Single run file |
| Index Build | **Yes** | Full coverage |
| MPHF Construction | **Yes** | Full coverage |

---

## Files Referenced

- `pkg/extsort/bench_test.go` - End-to-end benchmark
- `pkg/extsort/aggregator.go` - Memory aggregation
- `pkg/extsort/indexbuild.go` - Index construction
- `pkg/extsort/merger.go` - K-way merge
- `pkg/format/mphf_streaming.go` - MPHF builder
- `pkg/format/writer.go` - ArrayWriter, BlobWriter
- `pkg/benchutil/generator.go` - Synthetic data generation
