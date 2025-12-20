# Performance Analysis

Comprehensive performance documentation for s3-inv-db.

**Hardware:** AMD Ryzen 9 5950X 16-Core, Linux

## Current Optimal Solution

The optimal build pipeline uses `MemoryAggregator` with sorted bulk INSERT and deferred
index creation. This is the default path used by `s3inv-index build`.

### High-Level Algorithm

```
┌─────────────────────────────────────────────────────────────────────────┐
│                     END-TO-END BUILD PIPELINE                           │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  1. READ INVENTORY    S3 inventory CSV/Parquet files                    │
│         ↓            (streaming, parallel downloads)                    │
│                                                                         │
│  2. MEMORY AGGREGATE  In-memory map[prefix] → stats                     │
│         ↓            (no SQLite I/O during accumulation)                │
│                                                                         │
│  3. SORTED INSERT     Sort prefixes, batch INSERT to SQLite             │
│         ↓            (1170 rows/statement, deferred index)              │
│                                                                         │
│  4. BUILD TRIE        Iterate SQLite ORDER BY prefix                    │
│         ↓            (builds in-memory trie with subtree stats)         │
│                                                                         │
│  5. WRITE INDEX       Parallel file writes (columnar arrays, MPHF)      │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### SQLite Schema (Sparse Tier Storage)

```sql
-- Base stats: 4 columns per prefix (minimal CGO overhead)
CREATE TABLE prefix_stats (
    prefix TEXT NOT NULL PRIMARY KEY,
    depth INTEGER NOT NULL,
    total_count INTEGER NOT NULL DEFAULT 0,
    total_bytes INTEGER NOT NULL DEFAULT 0
) WITHOUT ROWID;

-- Tier stats: only rows for tiers with actual data
CREATE TABLE prefix_tier_stats (
    prefix TEXT NOT NULL,
    tier_code INTEGER NOT NULL,
    tier_count INTEGER NOT NULL DEFAULT 0,
    tier_bytes INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (prefix, tier_code)
) WITHOUT ROWID;
```

**Why this schema:**
- `WITHOUT ROWID` enables clustered index storage for efficient ORDER BY scans
- `TEXT PRIMARY KEY` provides natural lexicographic ordering for trie building
- Sparse tier storage: only store tiers with data (vs 28 columns for all tiers)
- 4 columns per row minimizes CGO overhead per scan

### SQLite PRAGMAs (Bulk Write Mode)

```go
pragmas := []string{
    "PRAGMA page_size=32768",           // Large pages for sequential I/O
    "PRAGMA journal_mode=WAL",          // Write-ahead logging
    "PRAGMA synchronous=OFF",           // Speed over durability (build is re-runnable)
    "PRAGMA temp_store=MEMORY",         // Temp tables in RAM
    "PRAGMA mmap_size=268435456",       // 256MB memory-mapped I/O
    "PRAGMA cache_size=-262144",        // 256MB page cache
    "PRAGMA locking_mode=EXCLUSIVE",    // Hold lock for entire session
    "PRAGMA wal_autocheckpoint=0",      // Manual checkpoints only
}
```

### Batching Strategy

```go
const (
    SQLiteMaxVariables = 32766                      // SQLite's SQLITE_MAX_VARIABLE_NUMBER
    MaxRowsPerBatch = SQLiteMaxVariables / 4        // 8191 rows per INSERT (4 cols per row)
)
```

**Why sorted writes matter:**
- Sequential B-tree index updates reduce page splits
- Cache locality improves as adjacent prefixes hit same pages
- Sorted UPSERT is 27% faster than random order

### Concrete-Type SQLite Scan Optimization

The SQLite read path uses concrete Go types exclusively - no `interface{}` or `any`
in hot loops. This eliminates reflection overhead and enables the compiler to optimize.

```go
// Type-safe scan helpers - concrete types only
func scanPrefixBaseStats(rows *sql.Rows, r *PrefixRow) error {
    return rows.Scan(&r.Prefix, &r.Depth, &r.TotalCount, &r.TotalBytes)
}

func scanTierRow(rows *sql.Rows, r *TierRow) error {
    return rows.Scan(&r.Prefix, &r.TierCode, &r.TierCount, &r.TierBytes)
}
```

**Key optimizations:**
1. **Narrow projections:** Only 4 columns per query (vs 28 in wide schema)
2. **Concrete struct scanning:** Direct field pointers, no reflection
3. **Struct reuse:** Same `PrefixRow`/`TierRow` reused across iterations
4. **RowPtr() API:** Returns pointer to avoid 256-byte struct copy

**Benchmark Results (50k objects → 139k prefixes):**

| Mode | Time | Allocations | Notes |
|------|------|-------------|-------|
| With tier data | 398ms | 59MB / 1.28M allocs | Tier map pre-loading |
| Without tier data | 154ms | 9MB / 551k allocs | 2.6x faster |

**Key insight:** Tier data pre-loading (building the map) dominates scan time.
When tier data isn't needed, iteration is 2.6x faster with 6.5x less memory.

Run SQLite scan benchmarks:
```bash
go test -bench=BenchmarkSQLiteScan -benchmem ./pkg/sqliteagg/...
```

### Slice/Map Capacity Optimization

All hot path allocations use pre-calculated or tuned initial capacities to minimize
re-allocations and reduce GC pressure.

**Key optimizations:**

| Location | Before | After | Impact |
|----------|--------|-------|--------|
| `extractPrefixes()` | `var []string` | `make([]string, 0, slashCount)` | Exact capacity, 0 allocs for flat keys |
| `PresentTiers()` | `var []tiers.ID` | `make([]tiers.ID, 0, tiers.NumTiers)` | Max 8 tiers, no reallocs |
| `Builder.stack` | nil | `make([]stackNode, 0, 16)` | Typical max depth |
| `Builder.nodes` | nil | `make([]Node, 0, 64)` | Initial capacity, grows 2x |
| `DepthIndexBuilder.buckets` | `make(map)` | `make(map, 16)` | Typical depth count |
| `GetDescendants()` | `var []Node` | `make([]Node, 0, subtreeSize)` | Exact capacity |
| `DescendantsUpToDepth()` | `var [][]uint64` | `make([][]uint64, 0, depthLevels)` | Exact capacity |
| `DescendantsAtDepthFiltered()` | `var []uint64` | `make([]uint64, 0, len(input))` | Worst-case capacity |

**Benchmark results for `extractPrefixes` (per call):**

| Key Type | Time | Allocations |
|----------|------|-------------|
| Shallow (no slashes) | 15ns | 0 B/op, 0 allocs |
| Medium (2 levels) | 73ns | 80 B/op, 1 alloc |
| Deep (5+ levels) | 109ns | 240 B/op, 1 alloc |

Run triebuild benchmarks:
```bash
go test -bench=BenchmarkExtractPrefixes -benchmem ./pkg/triebuild/...
```

### End-to-End Performance (500k objects → 1.2M prefixes)

| Phase | Duration | % Total | Notes |
|-------|----------|---------|-------|
| Memory Aggregation | 508ms | 1.8% | In-memory map, no I/O |
| SQLite Write | 4.96s | 17.4% | Sorted INSERT + index creation |
| Trie Build | 6.88s | 24.2% | Sequential SQLite read |
| Index Write | 16.1s | 56.6% | MPHF is largest component |
| **Total** | **28.4s** | **100%** | **17,600 obj/s** |

**I/O Characteristics:**
- Input: 25.3 MB synthetic keys
- SQLite DB: 130.1 MB
- Index files: 44.3 MB

## End-to-End CPU Profile

CPU breakdown for 500k objects (29.3s total):

| Component | CPU % | Notes |
|-----------|-------|-------|
| MPHF build (bbhash) | 27% | Perfect hash construction |
| SQLite CGO overhead | 23% | Fundamental driver cost |
| SQLite row iteration | 31% | Trie build reads |
| SQLite writes | 15% | MemoryAggregator.Finalize |
| Memory aggregation | 1.5% | Very efficient in-memory |
| Other (GC, runtime) | 2.5% | |

**Key Insights:**
1. **MPHF construction dominates index_write phase** - Building the minimal perfect
   hash function for prefix lookups takes ~9s of the 16s index write time.

2. **SQLite CGO is unavoidable** - The 23% overhead from `runtime.cgocall` is
   fundamental to the go-sqlite3 driver. Further optimization would require
   direct C bindings.

3. **Memory aggregation is essentially free** - At 1.5% CPU, the in-memory
   `map[string]*PrefixStats` approach has negligible overhead.

4. **SQLite reads are efficient** - Sequential ORDER BY iteration achieves
   good cache locality.

## Quick Performance Reference

### Aggregation Throughput

| Objects | Prefixes | Time | Throughput | Memory |
|---------|----------|------|------------|--------|
| 10,000 | 30k | 140ms | 71k obj/s | 4MB |
| 100,000 | 267k | 1.1s | 91k obj/s | 38MB |
| 500,000 | 1.2M | 5.0s | 100k obj/s | 175MB |

### End-to-End Build

| Objects | Total Time | Throughput |
|---------|------------|------------|
| 100,000 | 4.9s | 20k obj/s |
| 500,000 | 28.4s | 17.6k obj/s |

### Index Query Performance

| Operation | Time | Notes |
|-----------|------|-------|
| Index Open | 510μs | Memory-mapped |
| Prefix Lookup | ~10μs | Via MPHF |
| Stats Access | <1μs | Direct array read |

## Running Benchmarks

```bash
# Quick end-to-end benchmark
go test -bench='BenchmarkEndToEnd$' -benchtime=1x ./pkg/indexbuild/...

# Full scaling tests (100k, 500k, 1M objects)
S3INV_LONG_BENCH=1 go test -bench='BenchmarkEndToEnd_Scaling' -benchtime=1x ./pkg/indexbuild/...

# CPU profiling
go test -bench='BenchmarkEndToEnd_Scaling/objects=500000' -benchtime=1x \
    -cpuprofile=cpu.out ./pkg/indexbuild/...
go tool pprof -top cpu.out

# Memory profiling
go test -bench='BenchmarkEndToEnd/objects=100000' -benchtime=1x \
    -memprofile=mem.out ./pkg/indexbuild/...
go tool pprof -top mem.out
```

---

## Historical Exploration Notes

The following sections document optimization strategies that were evaluated but
not adopted, or were superseded by the current optimal design.

### Optimization Timeline

1. **Baseline:** Per-object SQLite UPSERT (slow due to CGO overhead per call)
2. **Delta accumulation:** Batch deltas in memory, flush periodically (+34%)
3. **Multi-row batching:** INSERT multiple rows per statement (+7%)
4. **Max variable usage:** 1170 rows per batch using 32,760 parameters (+5%)
5. **Sorted writes:** Sort prefixes before INSERT (+27%)
6. **Deferred indexing:** Create index after all INSERTs (minor gain)

### Strategies Evaluated and Rejected

| Strategy | Result | Why Rejected |
|----------|--------|--------------|
| Temp table + GROUP BY | -33% | GROUP BY overhead exceeds B-tree cost |
| Prefix ID normalization | -60% | Two-table overhead too high |
| Staging heap + final merge | -31% | Extra copy step adds overhead |
| INTEGER PRIMARY KEY | -5% | TEXT PK with sorted writes is faster |

### Key Lessons Learned

1. **Sorted writes are the biggest win** - A single `slices.Sort()` call before
   INSERT provides 27% throughput improvement.

2. **In-memory aggregation is faster than SQLite** - Accumulating in a Go map
   and batch-writing is faster than incremental UPSERT.

3. **CGO overhead is the fundamental limit** - With the go-sqlite3 driver,
   ~23% of CPU time goes to CGO transitions.

4. **MPHF construction is expensive** - The perfect hash function for prefix
   lookup takes significant time but provides O(1) query performance.

### Projected Performance at Scale

Based on 500k object benchmarks (linear extrapolation):

| Objects | Aggregation Time | Full Build Time |
|---------|------------------|-----------------|
| 1M | ~10s | ~1 min |
| 10M | ~100s | ~10 min |
| 100M | ~17 min | ~2 hours |
| 1B | ~3 hours | ~20 hours |
| 14B | ~40 hours | ~12 days |

**Note:** These are rough projections. Actual performance depends on prefix
distribution, disk I/O, and memory availability.
