# Architecture Overview

This document describes the high-level architecture of s3-inv-db, explaining how the system transforms S3 inventory CSV files into a queryable index.

## System Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                        BUILD PIPELINE                           │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐ │
│  │   S3     │───►│ In-Memory│───►│ External │───►│ Streaming│ │
│  │ Streaming│    │Aggregator│    │  Merge   │    │  Builder │ │
│  └──────────┘    └──────────┘    └──────────┘    └──────────┘ │
│       │               │               │               │        │
│       ▼               ▼               ▼               ▼        │
│  CSV.GZ streams   Run files      Sorted stream   Index files   │
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

### 1. S3 Streaming (`pkg/s3fetch`)

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

### 2. External Sort Pipeline (`pkg/extsort`)

The build pipeline uses a streaming external sort approach with bounded memory:

**Stage A: Streaming Ingest + Chunked Aggregation**
- Streams CSV chunks from S3 in parallel
- Aggregates prefix statistics in memory using `Aggregator`
- Uses fixed-size `[MaxTiers]uint64` arrays instead of maps for tier data
- Flushes to sorted run files when memory threshold is exceeded

**Stage B: Sort and Run File Spilling**
- When memory threshold is reached, aggregator drains to sorted run file
- Run files are sorted by prefix (lexicographic order) before writing
- Binary format: length-prefixed records with tier arrays

**Stage C: K-Way Merge**
- Heap-based merge of all run files
- Produces globally sorted stream of unique prefixes
- Duplicates from different runs are merged (stats summed)

**Stage D: Streaming Index Build**
- Single-pass construction of columnar arrays
- Computes subtree ranges on-the-fly using depth stack
- Writes MPHF, depth index, and tier stats

**Key properties:**
- **Pure Go**: No CGO dependencies, cross-compiles easily
- **Bounded memory**: Configurable threshold (~256MB default)
- **Streaming**: No need to hold entire dataset in memory
- **Dynamic scaling**: Concurrency scales with CPU count

### 3. Index Format (`pkg/format`)

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

**Tier statistics** (optional):
- Per-tier count and byte arrays in `tier_stats/` directory
- Only created for tiers with data

### 4. Index Reader (`pkg/indexread`)

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

    // Tier statistics
    tierStats *TierStatsReader
}
```

## Data Flow

### Build Phase

```
1. Stream from S3
   └── s3fetch.Client streams inventory files
   └── inventory.Reader parses CSV records

2. Aggregate in memory
   └── extsort.Aggregator accumulates prefix stats
   └── sync.Pool for PrefixStats to reduce allocations
   └── Flush to run files when memory threshold reached

3. K-way merge
   └── extsort.MergeIterator heap-merges run files
   └── Yields globally sorted, deduplicated stream

4. Build index
   └── extsort.IndexBuilder processes sorted stream
   └── Computes subtree ranges via depth stack
   └── Writes columnar arrays in single pass

5. Finalize
   └── Build MPHF from all prefixes
   └── Write depth index posting lists
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

### Why external sort instead of SQLite?
- **Pure Go**: No CGO, simpler deployment and cross-compilation
- **Bounded memory**: Configurable memory threshold
- **Performance**: 3x faster than SQLite for large inventories
- **Streaming**: Process arbitrarily large datasets

## Memory Usage

At query time, the index uses memory-mapped files:
- **Resident memory**: Only pages actually accessed
- **Shared across processes**: Multiple readers share pages
- **No deserialization**: Direct access to on-disk format

Build-time memory is bounded by:
- In-memory aggregator buffer (configurable, default ~256MB)
- Run file I/O buffers (~4MB per file)
- MPHF construction (~8 bytes per key temporarily)

## Concurrency

- **Read operations**: Fully thread-safe, lock-free
- **Build operations**: Concurrent S3 download and CSV parsing
- **Dynamic scaling**: Concurrency scales with `runtime.NumCPU()`
