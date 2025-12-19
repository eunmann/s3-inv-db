# Architecture Overview

This document describes the high-level architecture of s3-inv-db, explaining how the system transforms S3 inventory CSV files into a queryable index.

## System Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                        BUILD PIPELINE                            │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐  │
│  │ Inventory│───►│ External │───►│  Trie    │───►│ Columnar │  │
│  │  Reader  │    │   Sort   │    │ Builder  │    │  Writer  │  │
│  └──────────┘    └──────────┘    └──────────┘    └──────────┘  │
│       │               │               │               │         │
│       ▼               ▼               ▼               ▼         │
│   CSV/GZ files   Sorted runs    Prefix trie    Index files      │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                        QUERY ENGINE                              │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌──────────┐    ┌──────────┐    ┌──────────┐                   │
│  │   MPHF   │───►│ Columnar │───►│  Depth   │                   │
│  │  Lookup  │    │  Arrays  │    │  Index   │                   │
│  └──────────┘    └──────────┘    └──────────┘                   │
│       │               │               │                          │
│       ▼               ▼               ▼                          │
│   O(1) prefix    Stats lookup   Depth queries                    │
│     lookup                                                       │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

## Core Components

### 1. Inventory Reader (`pkg/inventory`)

Parses AWS S3 Inventory CSV files, handling:
- Gzip-compressed files (`.csv.gz`)
- Header detection and column mapping
- Headerless files with explicit schema (for AWS S3 inventory format)

**Key interfaces:**
```go
type Reader interface {
    Read() (Record, error)
    Close() error
}

type Record struct {
    Key    string
    Size   uint64
    TierID tiers.ID  // Storage tier (when tracking enabled)
}
```

### 2. External Sort (`pkg/extsort`)

Sorts inventory records by key using external merge sort:
- Handles datasets larger than memory
- Configurable chunk size (default: 1M records)
- K-way merge with priority queue

**Why external sort?**
S3 inventories can contain billions of objects. External sort allows processing inventories of any size with bounded memory usage.

**Algorithm:**
1. Read records in chunks, sort each chunk in memory
2. Write sorted chunks to temporary files
3. K-way merge all chunks using a min-heap

### 3. Trie Builder (`pkg/triebuild`)

Constructs a prefix trie from sorted keys in a single streaming pass:

```
Keys: ["a/b/file1.txt", "a/b/file2.txt", "a/c/file.txt"]

Trie:
  (root)
    └── a/
        ├── a/b/
        └── a/c/
```

**Key properties:**
- **Streaming**: Processes sorted input in O(n) time, O(max_depth) memory
- **Pre-order positions**: Nodes assigned positions in DFS pre-order
- **Aggregated stats**: Each node stores cumulative object count and bytes for its subtree
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
1. Read inventory files
   └── inventory.Reader parses CSV records

2. External sort
   └── extsort.Sorter produces sorted iterator

3. Build trie
   └── triebuild.Builder processes sorted keys
   └── Computes aggregates and subtree ranges

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

## Memory Usage

At query time, the index uses memory-mapped files:
- **Resident memory**: Only pages actually accessed
- **Shared across processes**: Multiple readers share pages
- **No deserialization**: Direct access to on-disk format

Build-time memory is bounded by:
- External sort chunk size (configurable)
- Trie depth (typically <100 levels)
- MPHF construction (~8 bytes per key temporarily)

## Concurrency

- **Read operations**: Fully thread-safe, lock-free
- **Build operations**: Single-threaded (streaming)
- **S3 downloads**: Concurrent with configurable parallelism
