# Architecture Overview

This document describes the high-level architecture of s3-inv-db, explaining how the system transforms S3 inventory CSV files into a queryable index.

## System Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                        BUILD PIPELINE                           │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐ │
│  │   S3     │───►│  SQLite  │───►│  Trie    │───►│ Columnar │ │
│  │ Streaming│    │Aggregator│    │ Builder  │    │  Writer  │ │
│  └──────────┘    └──────────┘    └──────────┘    └──────────┘ │
│       │               │               │               │        │
│       ▼               ▼               ▼               ▼        │
│  CSV.GZ streams   Prefix stats   Prefix trie    Index files    │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                        QUERY ENGINE                             │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌──────────┐    ┌──────────┐    ┌──────────┐                  │
│  │   MPHF   │───►│ Columnar │───►│  Depth   │                  │
│  │  Lookup  │    │  Arrays  │    │  Index   │                  │
│  └──────────┘    └──────────┘    └──────────┘                  │
│       │               │               │                         │
│       ▼               ▼               ▼                         │
│   O(1) prefix    Stats lookup   Depth queries                   │
│     lookup                                                      │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## Core Components

### 1. S3 Streaming (`pkg/s3fetch`, `pkg/sqliteagg`)

Streams S3 inventory files directly from S3:
- Fetches inventory manifest.json
- Streams each CSV.GZ file without downloading to disk
- Decompresses and parses on-the-fly

**Key interfaces:**
```go
type Client struct {
    // S3 operations
}

func (c *Client) StreamObject(ctx, bucket, key string) (io.ReadCloser, error)
```

### 2. SQLite Aggregator (`pkg/sqliteagg`)

Aggregates prefix statistics in a SQLite database during streaming:
- Groups by prefix, computing count and bytes per prefix
- In-memory aggregation for maximum throughput
- Efficient upsert operations with proper indexing

**Why SQLite streaming instead of external sort?**
- **No disk copies**: Data flows directly from S3 into aggregation
- **Lower memory**: In-memory aggregation with final flush to SQLite
- **Simpler**: No k-way merge, no temporary files to manage
- **Maximum throughput**: No checkpoint/resume overhead

**Schema:**
```sql
CREATE TABLE prefix_stats (
    prefix TEXT PRIMARY KEY,
    depth INTEGER,
    total_count INTEGER,
    total_bytes INTEGER,
    -- Per-tier columns when tier tracking enabled
    tier_0_count INTEGER, tier_0_bytes INTEGER,
    ...
);
```

### 3. Trie Builder (`pkg/triebuild`, `pkg/sqliteagg`)

Constructs a prefix trie from the pre-aggregated prefix stats:

```
Prefixes: ["a/", "a/b/", "a/c/"]

Trie:
  (root)
    └── a/
        ├── a/b/
        └── a/c/
```

**Key properties:**
- **Pre-aggregated**: Stats already computed during streaming
- **Sorted input**: SQLite provides ORDER BY prefix
- **Pre-order positions**: Nodes assigned positions in DFS pre-order
- **Subtree ranges**: Each node knows the position range of all descendants

**Data collected per node:**
- `Prefix`: The full prefix string (e.g., "data/2024/01/")
- `Pos`: Pre-order position in the trie
- `Depth`: Number of "/" characters in prefix
- `SubtreeEnd`: Last descendant's position (enables range queries)
- `ObjectCount`: Total objects under this prefix
- `TotalBytes`: Total bytes under this prefix
- `MaxDepthInSubtree`: Deepest descendant's depth
- `TierBytes[N]`: Per-tier byte counts (when tier tracking enabled)
- `TierCounts[N]`: Per-tier object counts (when tier tracking enabled)

### 4. Index Format (`pkg/format`)

Writes the trie to disk in a columnar, memory-mapped format:

**Columnar arrays** (one value per node, indexed by position):
- `subtree_end.u64`: Subtree end positions
- `depth.u32`: Node depths
- `object_count.u64`: Object counts
- `total_bytes.u64`: Byte totals
- `max_depth_in_subtree.u32`: Max subtree depths

**MPHF dictionary**:
- Minimal Perfect Hash Function for O(1) prefix→position lookup
- Uses BBHash algorithm (~2.5 bits per key)

**Depth index**:
- Posting lists of positions at each depth level
- Enables efficient depth-based queries

### 5. Index Reader (`pkg/indexread`)

Provides the query API over memory-mapped index files:

```go
type Index struct {
    // Memory-mapped arrays
    subtreeEnd        []uint64
    depth             []uint32
    objectCount       []uint64
    totalBytes        []uint64
    maxDepthInSubtree []uint32

    // MPHF for prefix lookup
    mphf *bbhash.BBHash

    // Depth posting lists
    depthIndex *DepthIndex
}
```

## Data Flow

### Build Phase

```
1. Stream from S3
   └── s3fetch.Client streams inventory files
   └── inventory.Reader parses CSV records

2. Aggregate in SQLite
   └── sqliteagg.MemoryAggregator aggregates in memory
   └── Final flush to SQLite for persistence

3. Build trie
   └── sqliteagg.BuildTrieFromSQLite reads sorted prefixes
   └── Computes subtree ranges from pre-aggregated stats

4. Write index
   └── format.ArrayWriter writes columnar arrays
   └── format.MPHFBuilder creates hash function
   └── format.DepthIndexBuilder creates depth lists

5. Finalize
   └── Write manifest with checksums
   └── Atomic rename to final location
```

### Query Phase

```
1. Lookup prefix
   └── MPHF hash → candidate position
   └── Verify prefix matches (handles collisions)

2. Get stats
   └── Direct array access by position

3. Find descendants at depth
   └── Depth index → positions at target depth
   └── Filter by subtree range [pos, subtree_end]

4. Iterate children
   └── DescendantIterator yields matching positions
```

## Design Decisions

### Why columnar format?
- **Cache efficiency**: Queries touch minimal data
- **mmap-friendly**: OS manages memory automatically
- **Simple**: No complex B-tree or LSM structures

### Why MPHF instead of hash table?
- **Space efficient**: ~2.5 bits/key vs ~10+ bytes/key
- **No collisions in position space**: Perfect hash to positions
- **Fast**: Single hash computation for lookup

### Why pre-order positions?
- **Subtree ranges**: All descendants are contiguous
- **Efficient queries**: Range check replaces tree traversal
- **Simple iteration**: Just iterate positions in range

### Why depth index?
- **Common query pattern**: "Show me all folders at depth 2"
- **Avoids full scan**: Jump directly to relevant positions
- **Composable**: Combine with subtree range for scoped queries

### Why SQLite for aggregation?
- **Streaming**: No need to hold entire dataset in memory
- **Persistent**: Index can be built from stored aggregation
- **Efficient**: Proper indexing for upsert operations
- **Simple**: No custom external sort implementation

## Memory Usage

At query time, the index uses memory-mapped files:
- **Resident memory**: Only pages actually accessed
- **Shared across processes**: Multiple readers share pages
- **No deserialization**: Direct access to on-disk format

Build-time memory is bounded by:
- SQLite page cache (configurable)
- Chunk processing buffer
- MPHF construction (~8 bytes per key temporarily)

## Concurrency

- **Read operations**: Fully thread-safe, lock-free
- **Build operations**: Single-threaded streaming
- **S3 streaming**: Sequential per-file (parallel across files not implemented)
