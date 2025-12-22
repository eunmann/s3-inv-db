# Architecture

s3-inv-db transforms S3 inventory data into a compact, queryable index using streaming aggregation and external sort.

## System Overview

```
BUILD PIPELINE
┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐
│   S3     │───►│ In-Memory│───►│ External │───►│ Streaming│
│ Streaming│    │Aggregator│    │  Merge   │    │  Builder │
└──────────┘    └──────────┘    └──────────┘    └──────────┘
      │               │               │               │
      ▼               ▼               ▼               ▼
 Inventory data   Run files      Sorted stream   Index files

QUERY ENGINE
┌──────────┐    ┌──────────┐    ┌──────────┐
│   MPHF   │───►│ Columnar │───►│  Depth   │
│  Lookup  │    │  Arrays  │    │  Index   │
└──────────┘    └──────────┘    └──────────┘
      │               │               │
      ▼               ▼               ▼
  O(1) prefix    Stats lookup   Depth queries
    lookup
```

## Packages

| Package | Purpose |
|---------|---------|
| `pkg/s3fetch` | Stream S3 inventory files, parse manifest |
| `pkg/inventory` | Parse CSV/Parquet records |
| `pkg/extsort` | External sort pipeline, aggregation, index building |
| `pkg/format` | Columnar array format, MPHF, depth index |
| `pkg/indexread` | Query API over memory-mapped files |
| `pkg/pricing` | Storage cost estimation |
| `pkg/membudget` | Memory budget management |

## Build Pipeline

**Stage 1: Stream & Aggregate** (`pkg/s3fetch`, `pkg/extsort`)
- Stream inventory files from S3
- Aggregate prefix statistics in bounded memory
- Flush to sorted run files when threshold reached

**Stage 2: K-Way Merge** (`pkg/extsort`)
- Heap-based merge of all run files
- Produce globally sorted, deduplicated prefixes

**Stage 3: Index Build** (`pkg/extsort`, `pkg/format`)
- Single-pass construction of columnar arrays
- Compute subtree ranges via depth stack
- Build MPHF, depth index, and tier stats

## Index Format

**Columnar arrays** (one value per node):
- `subtree_end.u64`, `depth.u32`, `object_count.u64`, `total_bytes.u64`, `max_depth_in_subtree.u32`

**MPHF** (prefix → position lookup):
- BBHash (~2.5 bits/key), fingerprints (FNV-1), position mapping

**Depth index** (efficient depth queries):
- `depth_offsets.u64`, `depth_positions.u64`

**Prefix strings**:
- `prefix_blob.bin`, `prefix_offsets.u64`

## Design Rationale

| Choice | Why |
|--------|-----|
| Columnar format | Cache-efficient, mmap-friendly, simple |
| MPHF | ~2.5 bits/key vs ~10+ bytes for hash table |
| Pre-order positions | Subtree ranges are contiguous; range check replaces tree traversal |
| Depth index | Jump directly to positions at target depth |
| External sort | Pure Go, bounded memory, streaming |

## Memory Model

**Build time**: Bounded by `--mem-budget` (default: 50% of system RAM)
- Aggregator buffer (50% of budget)
- Run file I/O buffers (25%)
- Merge and index building (25%)
- MPHF construction (~8 bytes/key, temporary)

**Query time**: Memory-mapped files
- Only accessed pages are resident
- Multiple readers share OS page cache
- No deserialization; direct access to on-disk format

## Concurrency

- **Read operations**: Thread-safe, lock-free
- **Build operations**: Concurrent S3 download, parsing, merge workers
- **Scaling**: Concurrency scales with `runtime.NumCPU()`
