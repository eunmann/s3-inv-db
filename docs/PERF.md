# Performance Analysis

Benchmark results and optimization findings for s3-inv-db.

## Current Performance

**Hardware:** AMD Ryzen 9 5950X 16-Core, Linux

### SQLite Aggregation (with delta accumulation)

| Objects | Time | Memory | Allocations |
|---------|------|--------|-------------|
| 10,000 | 165ms | 71MB | 394k |
| 100,000 | 1.65s | 683MB | 3.9M |

**Prefixes generated:** ~267k for 100k objects (s3_realistic shape)

### Baseline (before optimization)

| Objects | Time | Memory | Allocations |
|---------|------|--------|-------------|
| 10,000 | 237ms | 100MB | 586k |
| 100,000 | 2.5s | 1GB | 5.8M |

**Improvement:** ~34% faster, ~32% less memory, ~33% fewer allocations

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

## CPU Profile Analysis

### SQLite Aggregation (100k objects)

```
80% AddObject -> addPrefix -> SQLite exec
44% runtime.cgocall (SQLite CGO)
33% SQLiteStmt.bind (parameter binding)
```

**Key Finding:** Individual UPSERT operations dominate. Each AddObject call executes:
1. Prefix extraction (string operations)
2. For each prefix depth: SQLite UPSERT with parameter binding

### Memory Profile Analysis

### SQLite Aggregation (100k objects)

```
56% database/sql.driverArgsConnLocked (SQL arg conversion)
33% go-sqlite3.(*SQLiteStmt).bind
11% extractPrefixes
```

**Key Finding:** ~90% of allocations are SQL driver overhead for argument conversion.

### Trie Building

```
97% sortKeys (benchmark setup, not production code)
```

Actual trie building is fast; benchmark overhead dominates.

## Identified Bottlenecks

### 1. SQLite Per-Object Overhead (Critical)

**Problem:** Each object triggers multiple SQLite UPSERTs (one per prefix depth).
- 100k objects with avg 5 depths = 500k SQLite operations
- CGO call overhead (~44% of CPU time)
- SQL parameter binding allocation (~90% of memory)

**Potential Optimizations:**
- [ ] Batch inserts: Accumulate deltas in memory, flush periodically
- [ ] Use prepared statement pools more aggressively
- [ ] Consider raw SQLite C bindings vs database/sql wrapper
- [ ] Pre-aggregate deltas in Go maps before SQLite write

### 2. String Allocations in Hot Path

**Problem:** extractPrefixes creates new strings for each prefix.

**Potential Optimizations:**
- [ ] Use string interning for common prefixes
- [ ] Return byte slices instead of strings
- [ ] Reuse prefix extraction buffer

### 3. SQL Argument Conversion

**Problem:** database/sql converts Go types to driver values on every exec.

**Potential Optimizations:**
- [ ] Use go-sqlite3's direct API instead of database/sql
- [ ] Batch multiple values into single statement

## Implemented Optimizations

### In-Memory Delta Accumulation (Completed)

**Location:** `pkg/sqliteagg/aggregator.go`

The aggregator now accumulates prefix deltas in a `map[string]*prefixDelta` instead of
calling SQLite UPSERT for every prefix of every object.

- Threshold: 50,000 unique prefixes triggers automatic flush
- Final flush on transaction commit
- Reduces SQLite calls from ~500k to ~50k for 100k objects with 5 prefix depths

**Results:** 34% faster, 32% less memory, 33% fewer allocations

## Future Optimization Opportunities

### Phase 2: Further Allocation Reduction

1. **String pooling for prefixes**
   - Common prefixes (data/, logs/, etc.) appear millions of times
   - Intern strings to reduce allocation

2. **Reuse byte buffers**
   - sync.Pool for prefix extraction buffers

### Phase 3: Alternative Approaches

1. **Direct SQLite bindings**
   - Bypass database/sql wrapper
   - ~30% reduction in CGO overhead possible

2. **Multi-row UPSERT**
   - Batch multiple prefix updates into single SQL statement
   - Requires schema/SQL changes

## Running Benchmarks

```bash
# Quick benchmarks
go test -bench=. -benchtime=1x -run='^$' ./pkg/sqliteagg/...
go test -bench=. -benchtime=1x -run='^$' ./pkg/triebuild/...
go test -bench=. -benchtime=100x -run='^$' ./pkg/indexread/...

# CPU profiling
go test -bench='BenchmarkAggregate/objects=100000' -benchtime=1x \
    -cpuprofile=cpu.out ./pkg/sqliteagg/...
go tool pprof -top cpu.out

# Memory profiling
go test -bench='BenchmarkAggregate/objects=100000' -benchtime=1x \
    -memprofile=mem.out ./pkg/sqliteagg/...
go tool pprof -top mem.out

# Large-scale benchmarks (gated)
S3INV_LONG_BENCH=1 go test -bench='LargeScale' -benchtime=1x ./pkg/...
```

## Benchmark Design Notes

- **sqliteagg/bench_test.go**: Tests aggregation path with realistic key distributions
- **triebuild/bench_test.go**: Tests trie construction from sorted keys
- **indexread/bench_test.go**: Comprehensive query benchmarks (lookup, stats, descendants, concurrent)
- **benchutil/**: Shared test data generators with configurable tree shapes
