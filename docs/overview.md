# Overview

s3inv-index transforms S3 inventory reports into a compact index optimized for prefix-based queries.

## Problem

S3 inventory reports can contain billions of objects. Answering questions like "how much data is under `logs/2024/`?" requires scanning the entire inventory—slow and expensive.

## Solution

Build an index once, query instantly:

1. **Aggregate** object metadata by prefix during a streaming build
2. **Store** prefix statistics in memory-mapped columnar files
3. **Query** with O(1) prefix lookups via minimal perfect hashing

## Build Pipeline

```
S3 Inventory CSV/Parquet
         │
         ▼
┌─────────────────────┐
│   Parse & Extract   │  Extract key, size, storage class
│     (streaming)     │  from each inventory row
└─────────────────────┘
         │
         ▼
┌─────────────────────┐
│  Prefix Aggregation │  For each object, aggregate stats
│   (bounded memory)  │  at every ancestor prefix
└─────────────────────┘
         │
         ▼
┌─────────────────────┐
│   External Sort     │  Sort prefix aggregates when
│  (disk-backed)      │  memory threshold reached
└─────────────────────┘
         │
         ▼
┌─────────────────────┐
│    K-Way Merge      │  Merge sorted runs, combining
│                     │  duplicate prefixes
└─────────────────────┘
         │
         ▼
┌─────────────────────┐
│   Index Build       │  Build MPHF, depth index,
│   (streaming)       │  columnar arrays
└─────────────────────┘
         │
         ▼
    Index Files
```

### Memory Management

The build uses a configurable memory budget (default: 50% of RAM). When the in-memory aggregator reaches its threshold, it flushes sorted prefix data to a temporary run file. After processing all inventory files, a k-way merge combines all runs into the final sorted stream.

This allows indexing inventories of any size with bounded memory.

### Prefix Extraction

For an object key like `data/2024/01/15/file.csv`, the pipeline extracts prefixes at each depth:

```
data/
data/2024/
data/2024/01/
data/2024/01/15/
```

Statistics (object count, total bytes, per-tier breakdown) accumulate at each prefix level.

## Query Path

```
Query: "data/2024/"
         │
         ▼
┌─────────────────────┐
│   MPHF Lookup       │  Hash prefix → candidate position
│      O(1)           │  Verify with fingerprint
└─────────────────────┘
         │
         ▼
┌─────────────────────┐
│  Columnar Access    │  Read stats at position from
│  (memory-mapped)    │  mmap'd arrays
└─────────────────────┘
         │
         ▼
    Stats Result
```

### Subtree Queries

The index stores prefixes in preorder traversal. A prefix's descendants form a contiguous range `[pos, subtree_end)`. Combined with the depth index, this enables efficient "children at depth N" queries without scanning.

## Storage Tiers

The index tracks statistics for all 12 S3 storage classes:

| Tier | Description |
|------|-------------|
| Standard | S3 Standard |
| StandardIA | S3 Standard-IA |
| OneZoneIA | S3 One Zone-IA |
| GlacierIR | Glacier Instant Retrieval |
| GlacierFR | Glacier Flexible Retrieval |
| DeepArchive | Glacier Deep Archive |
| ReducedRedundancy | Reduced Redundancy (legacy) |
| ITFrequent | Intelligent-Tiering Frequent Access |
| ITInfrequent | Intelligent-Tiering Infrequent Access |
| ITArchiveInstant | Intelligent-Tiering Archive Instant |
| ITArchive | Intelligent-Tiering Archive |
| ITDeepArchive | Intelligent-Tiering Deep Archive |

Per-tier statistics are stored in separate columnar files, allowing queries to break down storage by class.
