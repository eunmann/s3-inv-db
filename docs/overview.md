# Overview

s3-inv-db transforms S3 inventory reports into a compact index optimized for prefix-based queries.

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

The build uses GOMEMLIMIT (env var, cgroup `memory.max`, or 60% of detected RAM — whichever is smallest) installed at process startup. The aggregator spills when its footprint hits `0.15 × GOMEMLIMIT` divided across workers, or when overall heap pressure exceeds 85% of the limit, whichever fires first. After all inventory files are processed, a k-way merge combines the run files into the final sorted stream.

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

The index tracks statistics for 13 S3 storage classes:

| Tier ID | S3 name | Notes |
|---|---|---|
| Standard | `STANDARD` |  |
| StandardIA | `STANDARD_IA` | 128 KiB minimum billable size |
| OneZoneIA | `ONEZONE_IA` | 128 KiB minimum billable size |
| GlacierIR | `GLACIER_IR` | 128 KiB minimum billable size |
| GlacierFR | `GLACIER` | Per-object metadata overhead |
| DeepArchive | `DEEP_ARCHIVE` | Per-object metadata overhead |
| ReducedRedundancy | `REDUCED_REDUNDANCY` | Deprecated by AWS |
| ITFrequent | `INTELLIGENT_TIERING_FREQUENT` | Monitored |
| ITInfrequent | `INTELLIGENT_TIERING_INFREQUENT` | Monitored |
| ITArchiveInstant | `INTELLIGENT_TIERING_ARCHIVE_INSTANT` | Monitored |
| ITArchive | `INTELLIGENT_TIERING_ARCHIVE` | Monitored + Glacier overhead |
| ITDeepArchive | `INTELLIGENT_TIERING_DEEP_ARCHIVE` | Monitored + Glacier overhead |
| ITFrequentSmall | `INTELLIGENT_TIERING_FREQUENT_SMALL` | Synthetic bucket for IT-Frequent objects < 128 KiB; billed at Frequent rate but excluded from the monitoring fee |

`pkg/tiers.Resolve(id, size)` re-routes IT-Frequent objects below
128 KiB into the synthetic `ITFrequentSmall` bucket at ingest time so
cost estimates honour the AWS minimum-monitored-size rule exactly.
Per-tier statistics live in `tier_stats/tier_stats_row.bin` beside
the main index files — one row-major file holding `(count, bytes)`
slots for every tier per prefix; see `docs/index-format.md`.

## Server features

When the `s3-inv-db-server` binary is started with `--auto-load` and
`--max-index-disk`, it runs a background poller that discovers new
inventory runs, plans evictions against a per-config retention count
and a global byte cap, and loads runs through a single-flight gate.
Pinned runs are protected from auto-eviction. The dashboard surfaces
the budget gauge; the inventories page exposes per-configuration
toggles. See [HTTP API](http-api.md) for the routes and [README](../README.md#configuration)
for the JSON config-file schema that drives all flags.
