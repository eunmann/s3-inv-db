# Performance Analysis

Benchmark results and optimization findings for s3-inv-db.

## Current Performance

**Hardware:** AMD Ryzen 9 5950X 16-Core, Linux

### SQLite Aggregation (with delta accumulation + multi-row UPSERT)

| Objects | Time | Memory | Allocations | Throughput |
|---------|------|--------|-------------|------------|
| 10,000 | 133ms | 68MB | 213k | 75k obj/s |
| 50,000 | 738ms | 320MB | 1.04M | 68k obj/s |
| 100,000 | 1.56s | 637MB | 2.09M | 64k obj/s |

**Prefixes generated:** ~267k for 100k objects (s3_realistic shape)

### Baseline (before optimization)

| Objects | Time | Memory | Allocations |
|---------|------|--------|-------------|
| 10,000 | 237ms | 100MB | 586k |
| 100,000 | 2.5s | 1GB | 5.8M |

**Total Improvement:** ~38% faster, ~36% less memory, ~64% fewer allocations

### Trie Building

| Objects | Time | Memory | Allocations |
|---------|------|--------|-------------|
| 100,000 | 87ms | 86MB | 200k |

### Index Operations

| Operation | Time | Notes |
|-----------|------|-------|
| Index Open (139k prefixes) | 510μs | Memory-mapped, minimal allocation |
| Prefix Lookup + Stats | ~10μs | Random access, 267k prefix index |
| Mixed Workload Query | 250ns - 2.8μs | Depends on tree shape |

## CPU Profile Analysis (with multi-row UPSERT)

### SQLite Aggregation (100k objects)

```
77% flushPendingDeltas -> SQLite exec
47% runtime.cgocall (SQLite CGO)
27% SQLiteStmt.bind (parameter binding)
10% driverArgsConnLocked (SQL arg conversion)
```

**Key Finding:** With multi-row batching (256 rows per statement), SQLite exec calls
are reduced by ~256x compared to single-row upserts. The CGO overhead now dominates
as there's less room for further SQL-level optimization.

### Memory Profile (After Optimization)

```
49% database/sql.driverArgsConnLocked (SQL arg conversion)
30% go-sqlite3.(*SQLiteStmt).bind
11% accumulateDelta (delta map overhead)
```

**Key Finding:** SQL driver overhead remains the largest memory consumer.
Delta map overhead is reasonable at 11% (~150MB for 267k prefixes).

## Parameter Tuning Results

### Delta Flush Threshold

Tested different flush thresholds for 100k objects:

| Threshold | Flushes | Time | Memory | Allocs |
|-----------|---------|------|--------|--------|
| 10k | 11 | 1.73s | 712MB | 3.94M |
| 25k | 5 | 1.70s | 690MB | 3.89M |
| **50k** | 3 | 1.66s | 683MB | 3.85M |
| 100k | 2 | 1.65s | 683MB | 3.85M |
| 200k | 1 | 1.66s | 683MB | 3.85M |

**Optimal:** 50k-100k threshold. Current default (50k) is near-optimal.
Higher thresholds show diminishing returns and increase memory pressure.

### Multi-row Batch Size

Tested different batch sizes for multi-row UPSERT (100k objects):

| Batch Size | Time | Memory | Allocs |
|------------|------|--------|--------|
| 64 | 1.46s | 624MB | 1.91M |
| 128 | 1.41s | 607MB | 1.90M |
| **256** | 1.39s | 590MB | 1.89M |
| 512 | 1.40s | 590MB | 1.89M |
| 1024 | 1.44s | 590MB | 1.89M |

**Optimal:** 256 rows per statement. Smaller batches increase exec call overhead.
Larger batches increase SQL parsing overhead without reducing exec calls significantly.

## Implemented Optimizations

### 1. In-Memory Delta Accumulation

**Location:** `pkg/sqliteagg/aggregator.go`

The aggregator accumulates prefix deltas in a `map[string]*prefixDelta` instead of
calling SQLite UPSERT for every prefix of every object.

- Threshold: 50,000 unique prefixes triggers automatic flush
- Final flush on transaction commit
- Reduces SQLite calls from ~500k to ~50k for 100k objects

**Impact:** 34% faster, 32% less memory

### 2. Map Pre-sizing

Pre-allocate delta map capacity based on flush threshold to reduce map growth overhead.

**Impact:** ~1% memory reduction (683MB → 674MB)

### 3. Multi-row UPSERT Batching

**Location:** `pkg/sqliteagg/aggregator.go`

Instead of executing one UPSERT per prefix, batch 256 prefixes into a single multi-row
INSERT statement. This reduces SQLite exec calls and parameter binding overhead.

- Batch size: 256 rows per statement (tuned via BenchmarkMultiRowBatch)
- Remainder prefixes use single-row upserts
- Both statements prepared once per transaction

**Impact:** ~7% faster, ~5% less memory, ~46% fewer allocations

## Remaining Bottlenecks

### SQLite CGO Overhead (47% of CPU time)

The `runtime.cgocall` overhead for SQLite is fundamental to the go-sqlite3 driver.
With multi-row batching already implemented, CGO overhead is now the dominant cost.
Possible mitigations:
- Direct SQLite C bindings (significant code change)
- Alternative pure-Go database (would lose SQLite's reliability)

### SQL Argument Conversion (10% of CPU time)

The `database/sql` package allocates for every exec call. With multi-row batching,
this overhead is significantly reduced but still present.
Possible mitigations:
- Use go-sqlite3's direct API bypassing database/sql

## Future Optimization Opportunities

### Phase 2: Further Allocation Reduction

1. **String pooling for prefixes**
   - Common prefixes (data/, logs/, etc.) appear millions of times
   - Could intern strings to reduce allocation

2. **Reuse byte buffers**
   - sync.Pool for prefix extraction buffers (marginal gains expected)

### Phase 3: Alternative Approaches

1. **Direct SQLite bindings**
   - Bypass database/sql wrapper
   - ~30% reduction in CGO overhead possible

2. **Staging table + bulk merge**
   - Insert into unindexed staging table
   - Merge with single INSERT...SELECT at commit
   - May reduce index maintenance overhead

## Running Benchmarks

```bash
# Quick benchmarks
go test -bench=. -benchtime=1x -run='^$' ./pkg/sqliteagg/...
go test -bench=. -benchtime=1x -run='^$' ./pkg/triebuild/...
go test -bench=. -benchtime=100x -run='^$' ./pkg/indexread/...

# Detailed throughput metrics
go test -bench='BenchmarkAggregate_Detailed' -benchtime=1x -run='^$' ./pkg/sqliteagg/...

# CPU profiling
go test -bench='BenchmarkAggregate/objects=100000' -benchtime=1x \
    -cpuprofile=cpu.out ./pkg/sqliteagg/...
go tool pprof -top cpu.out

# Memory profiling
go test -bench='BenchmarkAggregate/objects=100000' -benchtime=1x \
    -memprofile=mem.out ./pkg/sqliteagg/...
go tool pprof -top mem.out

# Large-scale and parameter sweep benchmarks (gated)
S3INV_LONG_BENCH=1 go test -bench='Scaling|Threshold|MultiRowBatch' -benchtime=1x ./pkg/...
```

## Benchmark Design Notes

- **sqliteagg/bench_test.go**: Aggregation benchmarks with tier distributions and threshold sweeps
- **triebuild/bench_test.go**: Trie construction from sorted keys
- **indexread/bench_test.go**: Comprehensive query benchmarks (lookup, stats, descendants, concurrent)
- **indexbuild/bench_test.go**: End-to-end index build and phase benchmarks
- **benchutil/**: Shared test data generators with configurable tree shapes
