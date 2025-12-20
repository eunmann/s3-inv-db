# S3 Inventory Index: Full Performance Analysis

**Date:** 2025-12-20
**Codebase:** `github.com/eunmann/s3-inv-db`
**CLI Command:** `s3inv-index build`

This document provides a comprehensive performance analysis of the index build pipeline, covering CPU hotspots, memory allocation patterns, SQLite usage, concurrency, and index file layout.

---

## Table of Contents

1. [Introduction & Scope](#1-introduction--scope)
2. [Pipeline Overview](#2-pipeline-overview)
3. [CPU Profile: End-to-End Build](#3-cpu-profile-end-to-end-build)
4. [Memory Profile & Allocation Hotspots](#4-memory-profile--allocation-hotspots)
5. [SQLite Usage & Schema Analysis](#5-sqlite-usage--schema-analysis)
6. [Concurrency & Pipelining](#6-concurrency--pipelining)
7. [Index File Layout & Query Locality](#7-index-file-layout--query-locality)
8. [Summary of Current Performance Characteristics](#8-summary-of-current-performance-characteristics)
9. [Appendix: How to Reproduce Profiles & Benchmarks](#9-appendix-how-to-reproduce-profiles--benchmarks)

---

## 1. Introduction & Scope

This analysis covers the complete build pipeline for `s3inv-index build`, which transforms S3 inventory CSV files into a compact, queryable index. The pipeline processes millions of object keys, aggregates prefix statistics, and produces memory-mapped columnar files optimized for O(1) prefix lookups.

### Benchmark Environment

- **CPU:** AMD Ryzen 9 5950X 16-Core Processor
- **OS:** Linux 6.14.0 (amd64)
- **Go Version:** 1.24+
- **SQLite:** via `github.com/mattn/go-sqlite3` (CGO)

### Representative Workload

The analysis uses synthetic benchmarks simulating realistic S3 inventory patterns:
- **10K objects:** ~30K unique prefixes, max depth 7
- **100K objects:** ~267K unique prefixes, max depth 7
- **Realistic key patterns:** `{org}/{date}/{category}/{subcategory}/{filename}`

---

## 2. Pipeline Overview

The build pipeline consists of 8 major stages:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           S3 Inventory Build Pipeline                        │
└─────────────────────────────────────────────────────────────────────────────┘

 S3 Manifest          CSV Chunks            In-Memory              SQLite DB
     │                    │                 Aggregation                │
     ▼                    ▼                     │                      ▼
┌─────────┐        ┌─────────────┐        ┌─────────┐          ┌─────────────┐
│ Fetch   │───────▶│ Stream &    │───────▶│ Memory  │─────────▶│ Bulk Write  │
│manifest │        │ Decompress  │        │Aggregate│          │ (batched)   │
│ .json   │        │ (gzip CSV)  │        │prefixes │          │             │
└─────────┘        └─────────────┘        └─────────┘          └─────────────┘
                                                                      │
                                                                      ▼
┌─────────────┐        ┌─────────────┐        ┌─────────────┐  ┌─────────────┐
│ Index Files │◀───────│ Parallel    │◀───────│ Trie Build  │◀─│ Sequential  │
│ (columnar)  │        │ File Write  │        │ (streaming) │  │ Scan        │
└─────────────┘        └─────────────┘        └─────────────┘  └─────────────┘
```

### Stage Breakdown

| Stage | Package | Key Files | Description |
|-------|---------|-----------|-------------|
| 1. Manifest Fetch | `s3fetch` | `client.go`, `manifest.go` | Download and parse `manifest.json` |
| 2. CSV Streaming | `s3fetch`, `inventory` | `client.go`, `reader.go` | Stream gzipped CSV chunks from S3 |
| 3. Memory Aggregation | `sqliteagg` | `memory_aggregator.go` | Accumulate prefix stats in `map[string]*PrefixStats` |
| 4. SQLite Write | `sqliteagg` | `memory_aggregator.go` | Bulk INSERT with 8191-row batches |
| 5. Sequential Scan | `sqliteagg` | `aggregator.go` | ORDER BY prefix scan via B-tree |
| 6. Trie Build | `sqliteagg`, `triebuild` | `indexbuild.go`, `builder.go` | Stack-based streaming construction |
| 7. Parallel File Write | `indexbuild`, `format` | `builder.go`, `writer.go` | 4-worker parallel columnar writes |
| 8. MPHF Build | `format` | `mphf.go` | BBHash minimal perfect hash function |

### Key Types and Data Flow

```
S3 Object Record
├── key: string       (e.g., "data/2024/01/file.csv")
├── size: uint64      (object size in bytes)
└── tierID: tiers.ID  (storage class)
         │
         ▼
   extractPrefixes()
         │
         ▼
PrefixStats (in-memory map)
├── prefix: string
├── depth: int
├── totalCount: uint64
├── totalBytes: uint64
└── tiers: map[tiers.ID]*TierStats  (sparse)
         │
         ▼
   SQLite Tables
├── prefix_stats (4 columns, WITHOUT ROWID)
└── prefix_tier_stats (4 columns, sparse)
         │
         ▼
   triebuild.Node
├── Prefix: string
├── Pos: uint64          (preorder position)
├── Depth: uint32
├── SubtreeEnd: uint64
├── ObjectCount: uint64
├── TotalBytes: uint64
├── MaxDepthInSubtree: uint32
├── TierBytes: [12]uint64
└── TierCounts: [12]uint64
         │
         ▼
   Columnar Index Files
├── subtree_end.u64
├── object_count.u64
├── total_bytes.u64
├── depth.u32
├── max_depth_in_subtree.u32
├── depth_index (offsets + positions)
├── mphf.bin + mphf_keys.bin
└── tier_N_{count,bytes}.u64 (per present tier)
```

---

## 3. CPU Profile: End-to-End Build

### Profile Summary (100K objects, 267K prefixes)

**Total Runtime:** ~3.3 seconds
**CPU Samples:** 5.94s (148% of wall time due to multi-core)

| Category | % of Total | Primary Functions |
|----------|-----------|-------------------|
| **GC / Memory Management** | 30.1% | `gcDrain`, `scanobject`, `mallocgc` |
| **CGO / SQLite Interop** | 20.7% | `cgocall`, SQLite driver functions |
| **SQLite Scanning** | 16.5% | `Rows.Next`, `Rows.Scan` |
| **MPHF Building** | 8.3% | `BBHash.Find`, `bitVector.rank` |
| **SQLite Writing** | 6.9% | `Stmt.Exec`, bind parameters |
| **Trie Building** | 4.7% | `trieBuilder.addPrefix` |
| **Other** | 12.8% | String operations, I/O, misc |

### Top Functions by Flat Time

```
      flat  flat%   cum%    function
     0.90s 15.15% 15.15%    runtime.cgocall
     0.83s 13.97% 29.12%    runtime.scanobject
     0.38s  6.40% 35.52%    github.com/relab/bbhash.bitVector.rank
     0.23s  3.87% 39.39%    runtime.(*gcBits).bitp
     0.19s  3.20% 42.59%    runtime.(*mspan).base
     0.18s  3.03% 45.62%    runtime.memclrNoHeapPointers
     0.18s  3.03% 48.65%    runtime.memmove
     0.12s  2.02% 50.67%    runtime.findObject
     0.11s  1.85% 52.53%    internal/runtime/maps.ctrlGroup.matchH2
     0.10s  1.68% 54.21%    runtime.typePointers.nextFast
```

### Phase-Level CPU Breakdown

Based on benchmark phase timing (100K objects):

| Phase | Duration | % of Total | Primary Cost |
|-------|----------|-----------|--------------|
| Memory Aggregation | ~160ms | 4.8% | Map operations, prefix extraction |
| SQLite Write | ~730ms | 22.0% | CGO overhead, B-tree insertions |
| Trie Build (SQLite scan) | ~950ms | 28.6% | CGO row scanning, tier data load |
| Index File Write | ~500ms | 15.1% | Disk I/O, buffered writes |
| MPHF Build | ~490ms | 14.8% | BBHash algorithm |
| Other | ~480ms | 14.5% | Logging, cleanup, GC |

### Key Observations

1. **CGO Dominates:** SQLite via CGO accounts for ~35% of CPU time (writing + reading)
2. **GC Pressure is Significant:** 30% of CPU in garbage collection
3. **MPHF Building is Expensive:** BBHash's `bitVector.rank` is a hot function
4. **Trie Building is Efficient:** Only 4.7% of CPU despite processing 267K nodes

---

## 4. Memory Profile & Allocation Hotspots

### Allocation Summary (100K objects)

**Total Allocated:** ~2,021 MB
**Total Allocations:** ~13.3M objects

| Allocation Source | Bytes | % of Total | Allocs | Notes |
|------------------|-------|-----------|--------|-------|
| `NewMemoryAggregator` | 695 MB | 34.4% | 1 | Initial map capacity (1M entries) |
| `trieBuilder.storeNode` | 514 MB | 25.4% | ~267K | Node slice growth |
| `IteratePrefixesForTiers` | 186 MB | 9.2% | ~2.6M | Tier data loading |
| `newTrieBuilder` | 130 MB | 6.4% | 1 | Nodes slice pre-allocation |
| `driverArgsConnLocked` | 83 MB | 4.1% | ~164K | SQLite parameter conversion |
| `SQLiteStmt.bind` | 73 MB | 3.6% | ~582K | CGO string/int binding |
| `MemoryAggregator.accumulate` | 64 MB | 3.2% | ~941K | PrefixStats allocations |
| `ArrayWriter.WriteU64` | 38 MB | 1.9% | ~2.5M | Buffered I/O |
| Other | ~238 MB | 11.8% | - | Misc |

### Top Functions by Allocation Count

```
     allocs  alloc%   function
   2,457,635  19.0%   ArrayWriter.WriteU64
   2,261,024  17.5%   SQLiteRows.nextSyncLocked
   1,140,902   8.8%   _Cfunc_GoStringN (SQLite string copy)
   1,059,515   8.2%   fmt.Sprintf
     949,747   7.3%   Generator.generateKey (benchmark)
     941,232   7.3%   MemoryAggregator.accumulate
     688,137   5.3%   Generator.generateSegment (benchmark)
     622,600   4.8%   strconv.formatBits
     581,718   4.5%   SQLiteStmt.bind
```

### Memory Usage by Phase

| Phase | Peak Memory | Retained After Phase |
|-------|-------------|---------------------|
| Memory Aggregation | ~700 MB | ~700 MB (prefix map) |
| SQLite Write | ~800 MB | ~50 MB (after map clear) |
| Trie Build | ~650 MB | ~650 MB (nodes slice) |
| Index File Write | ~700 MB | Minimal |

### Key Observations

1. **Initial Map Allocation Dominates:** 34% of allocations from `make(map, 1M)`
2. **SQLite CGO is Allocation-Heavy:** String copies across CGO boundary cause ~1.1M allocations
3. **Trie Node Storage is Significant:** 514 MB for 267K nodes (~1.9 KB/node)
4. **Benchmark Artifacts:** ~24% of allocations from test data generation (not production code)

---

## 5. SQLite Usage & Schema Analysis

### Schema Definition

```sql
-- Base prefix stats (4 columns, compact)
CREATE TABLE prefix_stats (
    prefix TEXT NOT NULL PRIMARY KEY,
    depth INTEGER NOT NULL,
    total_count INTEGER NOT NULL DEFAULT 0,
    total_bytes INTEGER NOT NULL DEFAULT 0
) WITHOUT ROWID;

-- Sparse tier stats (only populated for present tiers)
CREATE TABLE prefix_tier_stats (
    prefix TEXT NOT NULL,
    tier_code INTEGER NOT NULL,
    tier_count INTEGER NOT NULL DEFAULT 0,
    tier_bytes INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (prefix, tier_code)
) WITHOUT ROWID;
```

### PRAGMA Configuration

**Write Mode (Bulk Loading):**
```sql
PRAGMA page_size=32768;           -- 32KB pages
PRAGMA journal_mode=WAL;          -- Write-ahead logging
PRAGMA synchronous=OFF;           -- Maximum speed (unsafe on crash)
PRAGMA temp_store=MEMORY;         -- Temp tables in RAM
PRAGMA mmap_size=268435456;       -- 256MB memory-mapped I/O
PRAGMA cache_size=-262144;        -- 256MB page cache
PRAGMA locking_mode=EXCLUSIVE;    -- Hold lock for session
PRAGMA wal_autocheckpoint=0;      -- Manual checkpoints only
```

**Read Mode (Index Building):**
```sql
PRAGMA query_only=ON;             -- Prevent accidental writes
PRAGMA mmap_size=2147483648;      -- 2GB aggressive mmap
PRAGMA cache_size=-524288;        -- 512MB page cache
```

### Write Patterns

1. **Pre-sorted Bulk Insert:** Prefixes sorted in memory before INSERT
2. **Batched Statements:** 8,191 rows per INSERT (SQLite variable limit / 4 columns)
3. **Prepared Statements:** Reused for all batches
4. **Single Transaction:** All inserts in one transaction, checkpoint at end

**Write Query:**
```sql
INSERT INTO prefix_stats VALUES
    (?, ?, ?, ?), (?, ?, ?, ?), ..., (?, ?, ?, ?)
-- 8,191 rows × 4 columns = 32,764 parameters (under 32,766 limit)
```

### Read Patterns

1. **Sequential B-tree Scan:** ORDER BY prefix matches clustered PRIMARY KEY
2. **Narrow Projections:** Only fetch columns for present tiers
3. **Pre-loaded Tier Data:** Tier stats loaded into map before prefix scan

**Read Query:**
```sql
-- Base stats (always 4 columns)
SELECT prefix, depth, total_count, total_bytes
FROM prefix_stats
ORDER BY prefix;

-- Tier stats (pre-loaded into map)
SELECT prefix, tier_code, tier_count, tier_bytes
FROM prefix_tier_stats
ORDER BY prefix, tier_code;
```

### Row Counts (100K objects)

| Table | Rows | Avg Row Size | Total Size |
|-------|------|--------------|------------|
| `prefix_stats` | 267K | ~45 bytes | ~12 MB |
| `prefix_tier_stats` | ~400K | ~50 bytes | ~20 MB |
| **Total DB Size** | - | - | ~24 MB |

### Schema Efficiency Analysis

**Strengths:**
- `WITHOUT ROWID` eliminates rowid overhead
- Sparse tier schema reduces columns per row (4 vs 28)
- Pre-sorted inserts minimize B-tree rebalancing
- Batched inserts reduce SQLite exec calls by 8000×

**Considerations:**
- CGO overhead remains significant (~20% CPU)
- String copies across CGO boundary are allocation-heavy
- Two-table scan requires map join (memory overhead)

---

## 6. Concurrency & Pipelining

### Current Parallelism Model

```
┌──────────────────────────────────────────────────────────────────────────┐
│                         Concurrency Structure                             │
└──────────────────────────────────────────────────────────────────────────┘

           Sequential                    Parallel
              │                             │
              ▼                             ▼
┌──────────────────────┐          ┌──────────────────────┐
│  S3 Chunk Download   │          │  Index File Writing  │
│   (single-threaded)  │          │   (4 goroutines)     │
└──────────────────────┘          └──────────────────────┘
              │                             │
              ▼                             │
┌──────────────────────┐          ┌──────────────────────┐
│  CSV Parse + Agg     │          │  Worker Pool Pattern │
│   (single-threaded)  │          │  - Task channel      │
└──────────────────────┘          │  - WaitGroup sync    │
              │                   │  - Error collection  │
              ▼                   └──────────────────────┘
┌──────────────────────┐
│  SQLite Write        │
│   (single-threaded)  │
└──────────────────────┘
              │
              ▼
┌──────────────────────┐
│  SQLite Scan + Trie  │
│   (single-threaded)  │
└──────────────────────┘
```

### Parallel Components

**Index File Writing (`indexbuild/builder.go:125-232`):**
- **Workers:** 4 goroutines (configurable)
- **Pattern:** Channel-based work distribution
- **Synchronization:** `sync.WaitGroup`, `sync.Mutex` for errors
- **Tasks:** 8 independent file writes (parallelizable)

```go
// Task distribution
taskCh := make(chan writeTask, len(tasks))
for _, t := range tasks {
    taskCh <- t
}
close(taskCh)

// Worker goroutines
for i := 0; i < concurrency; i++ {
    go func() {
        for task := range taskCh {
            bytesWritten, err := task.fn()
            // ...
        }
    }()
}
```

### Sequential Components

| Component | Reason for Sequential |
|-----------|----------------------|
| S3 Chunk Download | Configurable but default=1 |
| CSV Parsing | Per-chunk processing |
| Memory Aggregation | Single shared map (not sharded) |
| SQLite Write | Single-writer semantics |
| SQLite Scan | Cursor-based iteration |
| Trie Building | Streaming algorithm requires order |

### CPU Utilization Analysis

During end-to-end build (observed via `htop`):

| Phase | CPU Cores Active | Bottleneck |
|-------|-----------------|------------|
| Memory Aggregation | 1-2 | Single map, GC |
| SQLite Write | 1-2 | Single writer, CGO |
| Trie Build | 1-2 | Sequential scan, CGO |
| Index File Write | 4-5 | Disk I/O, parallel |

### Pipelining Opportunities (Not Currently Implemented)

1. **S3 Download + CSV Parse:** Could overlap chunk download with parsing
2. **Aggregation + SQLite Write:** Could stream to SQLite during aggregation
3. **SQLite Scan + Trie Build:** Already tightly coupled (streaming)
4. **Trie Build + File Write:** Could stream nodes to files during construction

---

## 7. Index File Layout & Query Locality

### File Format: Columnar Arrays

All columnar files share a common header:

```
┌─────────────────────────────────────────┐
│              Header (20 bytes)           │
├─────────────────────────────────────────┤
│ Magic:   4 bytes  (0x53334944 = "S3ID") │
│ Version: 4 bytes  (1)                   │
│ Count:   8 bytes  (number of elements)  │
│ Width:   4 bytes  (element size)        │
├─────────────────────────────────────────┤
│              Data (Count × Width)        │
│  Element 0: [Width bytes]               │
│  Element 1: [Width bytes]               │
│  ...                                    │
│  Element N-1: [Width bytes]             │
└─────────────────────────────────────────┘
```

### Index File Structure

| File | Width | Purpose | Query Pattern |
|------|-------|---------|---------------|
| `subtree_end.u64` | 8 | Last position in subtree | Descendant enumeration |
| `object_count.u64` | 8 | Total objects under prefix | Aggregate queries |
| `total_bytes.u64` | 8 | Total bytes under prefix | Size queries |
| `depth.u32` | 4 | Directory depth | Depth filtering |
| `max_depth_in_subtree.u32` | 4 | Max depth in subtree | Pruning |
| `mphf.bin` | Variable | BBHash minimal perfect hash | O(1) prefix lookup |
| `mphf_keys.bin` | Variable | Prefix strings (packed) | Collision resolution |
| `depth_offsets.u64` | 8 | Start position per depth | Depth-based iteration |
| `depth_positions.u64` | 8 | All positions per depth | Depth-based iteration |
| `tier_N_count.u64` | 8 | Tier N object counts | Tier breakdown |
| `tier_N_bytes.u64` | 8 | Tier N byte counts | Tier breakdown |

### Memory-Mapped Access

All files are accessed via `mmap(2)`:

```go
// Read access (reader.go)
data, err := unix.Mmap(int(f.Fd()), 0, int(size),
    unix.PROT_READ, unix.MAP_SHARED)

// Array access - no bounds checking for hot paths
func (r *ArrayReader) UnsafeGetU64(idx uint64) uint64 {
    return binary.LittleEndian.Uint64(r.data[idx*8:])
}
```

### Query Access Patterns

**Single Prefix Lookup (O(1)):**
```
1. Hash prefix → MPHF position
2. Verify prefix string at position
3. Read stats from columnar arrays
```

**Descendant Enumeration (O(subtree size)):**
```
1. Lookup prefix → position p
2. Read subtree_end[p] → end_pos
3. Iterate positions [p+1, end_pos]
4. Read stats for each position
```

**Depth-Based Iteration (O(nodes at depth)):**
```
1. Read depth_offsets[d] → start
2. Read depth_offsets[d+1] → end
3. Read positions from depth_positions[start:end]
4. Read stats for each position
```

### Cache Locality Analysis

**Strengths:**
- Columnar layout: reading same field for many nodes is contiguous
- Preorder numbering: subtree nodes are contiguous in all arrays
- Memory mapping: OS page cache handles LRU eviction
- Small element sizes: many elements per cache line

**Considerations:**
- Random prefix lookups touch multiple files (MPHF + stats)
- Tier stats in separate files (one per tier)
- String blob access for prefix verification

### Typical Query Memory Access

| Query Type | Files Accessed | Cache Lines |
|------------|----------------|-------------|
| Single prefix stats | 4-5 | ~10 |
| Prefix with tier breakdown | 6-8 | ~15 |
| Descendant walk (100 nodes) | 3-4 | ~200 |
| Depth enumeration (1000 nodes) | 3-4 | ~500 |

---

## 8. Summary of Current Performance Characteristics

### Performance Profile (100K objects → 267K prefixes)

| Metric | Value |
|--------|-------|
| **Total Build Time** | ~3.3 seconds |
| **Objects/Second** | ~30,000 |
| **Prefixes/Second** | ~81,000 |
| **Peak Memory** | ~800 MB |
| **Total Allocations** | ~13.3M objects |
| **SQLite DB Size** | ~24 MB |
| **Index File Size** | ~15 MB |

### Phase Timing Breakdown

| Phase | Time | % |
|-------|------|---|
| Memory Aggregation | 160ms | 5% |
| SQLite Write | 730ms | 22% |
| Trie Build (SQLite scan) | 950ms | 29% |
| Index File Write | 500ms | 15% |
| MPHF Build | 490ms | 15% |
| Other (GC, logging, cleanup) | 480ms | 14% |

### Key Conclusions

**Biggest CPU Hotspots:**
1. CGO overhead for SQLite operations (~35% of CPU)
2. Garbage collection (~30% of CPU)
3. MPHF construction (~8% of CPU)

**Biggest Allocation Hotspots:**
1. Initial map allocation (695 MB, 34%)
2. Trie node storage (514 MB, 25%)
3. SQLite string copies across CGO (186 MB, 9%)

**SQLite Usage Assessment:**
- Schema is optimally sparse (4 columns vs 28)
- Batching is maximal (8191 rows per INSERT)
- Pre-sorting before insert is effective
- CGO overhead remains the dominant cost

**Concurrency Assessment:**
- Index file writing is well-parallelized (4 workers)
- SQLite operations are necessarily sequential
- No pipelining between major phases
- S3 download is single-threaded by default

**Index Format Assessment:**
- Columnar layout is cache-efficient for scans
- MPHF enables O(1) prefix lookup
- Memory-mapped access leverages OS page cache
- Preorder numbering enables efficient subtree queries

### Scaling Characteristics

| Objects | Prefixes | Build Time | Memory | SQLite DB | Index Files |
|---------|----------|------------|--------|-----------|-------------|
| 10K | 30K | ~340ms | ~174 MB | ~3 MB | ~2 MB |
| 100K | 267K | ~3.3s | ~1.3 GB | ~24 MB | ~15 MB |
| 1M* | ~2.5M | ~35s* | ~12 GB* | ~250 MB* | ~150 MB* |

*Extrapolated from observed scaling behavior

---

## 9. Appendix: How to Reproduce Profiles & Benchmarks

### Running End-to-End Benchmarks

```bash
# Basic benchmark
go test -bench='BenchmarkEndToEnd$' -benchmem -count=5 ./pkg/indexbuild/

# With phase breakdown logging
go test -bench='BenchmarkEndToEnd$' -benchmem ./pkg/indexbuild/ 2>&1 | grep -v '^{'

# Scaling benchmark
go test -bench='BenchmarkEndToEnd_Scaling' -benchmem ./pkg/indexbuild/
```

### CPU Profiling

```bash
# Generate CPU profile
go test -bench='BenchmarkEndToEnd/objects=100000' -benchtime=1x \
    -cpuprofile=cpu.out ./pkg/indexbuild/

# View top functions (flat time)
go tool pprof -top cpu.out

# View top functions (cumulative time)
go tool pprof -top -cum cpu.out

# Interactive exploration
go tool pprof cpu.out
# (pprof) top 30
# (pprof) list functionName
# (pprof) web
```

### Memory Profiling

```bash
# Generate memory profile
go test -bench='BenchmarkEndToEnd/objects=100000' -benchtime=1x \
    -memprofile=mem.out ./pkg/indexbuild/

# View by allocated space
go tool pprof -top mem.out

# View by allocation count
go tool pprof -alloc_objects -top mem.out

# Interactive exploration
go tool pprof mem.out
```

### Comparing Benchmarks

```bash
# Run baseline
go test -bench='BenchmarkEndToEnd$' -benchmem -count=5 ./pkg/indexbuild/ \
    > baseline.txt 2>&1

# Make changes, then run again
go test -bench='BenchmarkEndToEnd$' -benchmem -count=5 ./pkg/indexbuild/ \
    > new.txt 2>&1

# Compare with benchstat
benchstat baseline.txt new.txt
```

### SQLite Analysis

```bash
# Open database for inspection
sqlite3 /path/to/prefix-agg.db

# Check table sizes
.schema
SELECT COUNT(*) FROM prefix_stats;
SELECT COUNT(*) FROM prefix_tier_stats;

# Analyze query plan
EXPLAIN QUERY PLAN SELECT * FROM prefix_stats ORDER BY prefix;

# Check pragmas
PRAGMA page_size;
PRAGMA journal_mode;
PRAGMA mmap_size;
```

---

*This analysis was generated on 2025-12-20 and reflects the current state of the codebase at commit `6cd443c`.*
