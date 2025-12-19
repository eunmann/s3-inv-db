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

## Phase: Pitfall Cleanup

### Optimizations Applied

1. **Reuse flush buffers across flushes**
   - `batchArgs` and `singleArgs` slices are now allocated once in `BeginChunk()`
   - Reused across all `flushPendingDeltas()` calls in the transaction
   - Impact: Negligible (saves ~12 allocations per 100k objects)

2. **Reuse PrefixIterator scanDest slice**
   - `scanDest` slice for row scanning is now lazy-initialized once per iterator
   - Previously allocated on every `Next()` call
   - Impact: Reduces allocations during prefix iteration by 1 per row

3. **Inline prefix extraction in hot path**
   - `AddObject()` uses inline loop instead of calling `extractPrefixes()`
   - Avoids slice allocation per object
   - `extractPrefixes()` retained for pipeline.go and tests where allocation is acceptable

### Common Pitfalls Avoided

- **Per-row SQLite statements**: Everything is batched (256 rows per exec)
- **Per-flush buffer allocation**: Reuse `batchArgs`/`singleArgs` across flushes
- **Per-row scan allocation**: Reuse `scanDest` in PrefixIterator
- **Logging in hot loops**: No logging in `AddObject()` or `accumulateDelta()`
- **Interface boxing in hot paths**: Concrete types used throughout aggregation

### Ideas Evaluated but Not Implemented

1. **String interning for common prefixes**
   - Evaluated: Would reduce memory for repeated prefixes like "data/", "logs/"
   - Rejected: The map key already interns strings implicitly; explicit interning
     adds complexity for marginal gain. Profile shows delta map is only 11% of memory.

2. **sync.Pool for prefix slices**
   - Evaluated: Could reduce allocation in `extractPrefixes()`
   - Rejected: Hot path uses inline extraction. Non-hot paths don't benefit enough.

3. **Optimizing tiers.FromS3() string operations**
   - Evaluated: `strings.ToUpper`/`TrimSpace` allocate on every call
   - Rejected: Only called when tier tracking enabled, and most S3 inventory
     data is already uppercase/trimmed so no allocation actually occurs.

### Software Improvements

1. **Config.Validate()**
   - Added validation for `DBPath`, `Synchronous`, `MmapSize`, `CacheSizeKB`
   - Called in `Open()` to fail fast on invalid configuration
   - Comprehensive tests for validation edge cases

## Phase: Bulk Write Investigation

### Investigation Findings

Investigated SQLite configuration for heavy write workloads (14B+ objects).

#### 1. Missing PRAGMAs for Bulk Writes

**Added `BulkWriteMode` flag** that enables:
- `PRAGMA locking_mode=EXCLUSIVE` - Hold lock for entire session
- `PRAGMA wal_autocheckpoint=0` - Disable auto-checkpoint during writes
- Manual checkpoint on close with `PRAGMA wal_checkpoint(TRUNCATE)`

**Benchmark Result:** ~2% improvement with `synchronous=OFF` (marginal because there's no fsync overhead). Impact may be larger with `synchronous=NORMAL` in production.

#### 2. Pipeline Multi-Row Batching Bug (FIXED)

**Critical Bug Found:** `pipeline.go:writeBatch()` was using single-row UPSERT instead of multi-row batching. This bypassed the 256-row batching optimization for the pipelined streaming path.

**Fix:** Rewrote `writeBatch()` to use `multiRowStmt` for batches of 256 rows, with single-row fallback for remainders. Uses aggregator's reusable `batchArgs` and `singleArgs` buffers.

#### 3. Staging Table Approach (Evaluated, Not Adopted)

Tested alternative approach: INSERT into unindexed staging table, then aggregate with single `INSERT...SELECT...GROUP BY` at end.

**Result:** Staging approach was **~15% slower** than UPSERT + delta accumulation because:
- Staging inserts every (prefix, delta) tuple individually (~267k inserts for 100k objects)
- UPSERT with delta accumulation only flushes unique prefixes per batch (much fewer rows)

The current in-memory delta accumulation is already an effective optimization.

#### 4. Schema Analysis

Current schema uses `prefix TEXT PRIMARY KEY`:
- Pros: Simple, natural ordering for iteration
- Cons: Variable-length text keys mean expensive B-tree operations

**Potential future optimization:** Integer prefix ID with separate prefix→ID lookup. Not implemented due to complexity and unclear benefit vs CGO overhead.

## Phase: Profile-Guided Schema Optimization

### Investigation Summary

Challenged previous assumption that "CGO overhead is the bottleneck." Performed systematic comparison of different SQLite write strategies with 100k-500k objects.

### Strategy Comparison (500k objects, 1.2M prefixes)

| Strategy | Time | obj/s | vs Baseline |
|----------|------|-------|-------------|
| Current UPSERT (TEXT PK) | 8.8s | 56,607 | baseline |
| Staging + Aggregate | 10.4s | 47,900 | -15% slower |
| Prefix ID Normalized | 13.9s | 35,992 | -36% slower |
| Full Memory + Deferred Index | 6.6s | 76,070 | **+34% faster** |
| **MemoryAggregator** | **6.0s** | **82,600** | **+46% faster** |

### Key Finding: Deferred Indexing

The biggest win comes from **not maintaining the B-tree index during writes**:
1. Create table without PRIMARY KEY
2. Bulk INSERT all data (no UPSERT, no conflict resolution)
3. Create index AFTER all inserts complete

This eliminates B-tree maintenance overhead during the heavy write phase.

### New: MemoryAggregator

Added `MemoryAggregator` for one-shot build workflows:

```go
cfg := sqliteagg.DefaultMemoryAggregatorConfig(dbPath)
agg := sqliteagg.NewMemoryAggregator(cfg)

for _, obj := range objects {
    agg.AddObject(obj.Key, obj.Size, obj.TierID)
}
agg.Finalize()  // Writes to SQLite with deferred indexing
```

**Advantages:**
- No SQLite I/O during accumulation (all in-memory)
- Single bulk write at end (no UPSERT overhead)
- Deferred index creation (no B-tree maintenance during writes)
- Multi-row INSERT batching (1000 rows/exec)

**Limitations:**
- Not resumable (all data lost on crash)
- Memory usage: ~150 bytes × unique prefix count

### Performance Comparison

| Objects | Standard (UPSERT) | MemoryAggregator | Speedup |
|---------|-------------------|------------------|---------|
| 100k | 1.62s (61.7k/s) | 1.34s (74.6k/s) | 1.21x |
| 500k | 9.45s (52.9k/s) | 6.05s (82.6k/s) | **1.56x** |

The **MemoryAggregator is 56% faster** at scale (500k objects).

### Projected Performance at Scale

For 14 billion objects (assuming prefix ratio holds):
- Standard Aggregator: ~73 hours
- MemoryAggregator: ~47 hours

This is a significant improvement but still far from "a few hours." Further investigation needed to understand what the fast implementation does differently.

### When to Use Each Approach

| Use Case | Recommended |
|----------|-------------|
| One-shot build, ample memory | `MemoryAggregator` |
| Need resumability | Standard `Aggregator` with `BulkWriteMode` |
| Concurrent readers during build | Standard `Aggregator` (no EXCLUSIVE lock) |
| Memory-constrained + resumable | `NormalizedAggregator` |

### NormalizedAggregator (Memory-Constrained)

The `NormalizedAggregator` uses INTEGER PRIMARY KEY instead of TEXT for stats,
with staging tables that spill to disk when memory limits are reached.

**Benchmark Results (100k objects):**
| Aggregator | Throughput | Notes |
|------------|------------|-------|
| MemoryAggregator | 81.4k obj/s | Fastest, in-memory |
| Standard | 63.3k obj/s | UPSERT + delta batching |
| NormalizedAggregator | 50.4k obj/s | Staging overhead |

The NormalizedAggregator is ~21% slower than standard due to staging table overhead,
but provides value when:
- Memory is limited (configurable `MemoryLimitMB`)
- Need resumability across crashes (staging persists to SQLite)
- Processing extremely large datasets that exceed available RAM

```go
cfg := sqliteagg.DefaultNormalizedConfig(dbPath)
cfg.MemoryLimitMB = 512  // Limit memory usage
agg, err := sqliteagg.NewNormalizedAggregator(cfg)
```

### Bulk Write Mode Usage

```go
cfg := sqliteagg.DefaultConfig(dbPath)
cfg.BulkWriteMode = true  // Enable for build workflows
agg, err := sqliteagg.Open(cfg)
```

## Benchmark Design Notes

- **sqliteagg/bench_test.go**: Aggregation benchmarks with tier distributions and threshold sweeps
- **triebuild/bench_test.go**: Trie construction from sorted keys
- **indexread/bench_test.go**: Comprehensive query benchmarks (lookup, stats, descendants, concurrent)
- **indexbuild/bench_test.go**: End-to-end index build and phase benchmarks
- **benchutil/**: Shared test data generators with configurable tree shapes
