# Overview

s3-inv-db turns an S3 Inventory report (CSV or Parquet, often billions
of rows) into a compact memory-mapped index that answers per-prefix
size/count questions in microseconds.

## Why an index

An S3 Inventory report lists every object in a bucket. Answering
"how much data is under `logs/2024/`?" by scanning the report costs
minutes-to-hours per query and is wasted work every time. Building an
index once collapses that to a constant-time mmap lookup; the index
can then be reused for every query, every tier-cost estimate, and
every diff between two inventory runs.

## Build pipeline

```
S3 Inventory CSV/Parquet
         │
         ▼
┌─────────────────────┐
│   Parse & Extract   │  key, size, storage class — streamed
└─────────────────────┘
         │
         ▼
┌─────────────────────┐
│  Prefix Aggregation │  for each object, accumulate into every
│   (bounded memory)  │  ancestor prefix; spill when memory fills
└─────────────────────┘
         │
         ▼
┌─────────────────────┐
│   External Sort     │  disk-backed runs of sorted prefix rows
└─────────────────────┘
         │
         ▼
┌─────────────────────┐
│    K-Way Merge      │  combine duplicate prefixes across runs
└─────────────────────┘
         │
         ▼
┌─────────────────────┐
│   Index Build       │  MPHF + depth index + columnar arrays
└─────────────────────┘
         │
         ▼
    Index files
```

For an object key `data/2024/01/15/file.csv`, the aggregator emits a
row at every ancestor depth (`data/`, `data/2024/`, `data/2024/01/`,
`data/2024/01/15/`). The merge then combines duplicate prefixes across
spill runs into one row per unique prefix.

Memory is bounded by `GOMEMLIMIT`; see
[performance.md](performance.md#memory) for the exact spill rules.

## Query path

```
Query: "data/2024/"
         │
         ▼
┌─────────────────────┐
│   MPHF Lookup       │  hash → candidate position; verify
│      O(1)           │  with a second-hash fingerprint
└─────────────────────┘
         │
         ▼
┌─────────────────────┐
│  Columnar Access    │  read stats at that position from
│  (memory-mapped)    │  the mmap'd row-major file
└─────────────────────┘
         │
         ▼
    Stats result
```

Prefixes are stored in preorder traversal, so a prefix's descendants
form a contiguous position range. Combined with a depth index, this
makes "children at depth N" a binary search inside a range rather
than a tree walk.

## Per-tier statistics

The index tracks count and bytes per S3 storage class (13 classes plus
one synthetic bucket for IT-Frequent objects under 128 KiB, which AWS
bills at the Frequent rate but excludes from the monitoring fee).
Tiers with no data in a given index consume no disk. The full
storage-class list and file layout live in
[index-format.md](index-format.md#tier-statistics).

## Server behaviour

`s3-inv-db-server` adds three things on top of the read API:

- **Discovery**: a poller lists configured S3 sources, registers new
  inventory runs in the state DB, and surfaces them in the UI.
- **Auto-load + budget**: when `--auto-load` is on, runs marked
  `auto_load=true` are built into the local cache. The planner respects
  a per-configuration retention count and a global `--max-index-disk`
  byte cap; pinned runs are never auto-evicted.
- **Async builds**: a Load click queues a job and returns 202; SSE
  drives the UI to a live progress row until the build finishes or
  fails. State + job history persist in `$CACHE_DIR/state.db` (SQLite,
  WAL, pure-Go driver); jobs in flight at shutdown are flipped to
  `aborted` on next boot so the UI never shows a forever-spinner.

Routes, JSON shapes, and the partial-HTML conventions are in
[http-api.md](http-api.md).
