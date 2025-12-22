# S3 Inventory Index Pipeline Overview

This document describes the logical stages of the S3 inventory indexing pipeline, from manifest discovery through index building and querying.

## Pipeline Flow Diagram

```
                    CLI Entry Point
                    (cmd/s3inv-index/main.go)
                          │
                          ▼
                    cli.Run(args)
                          │
                          ▼
                    runBuild()
                          │
                          ▼
            ┌─────────────────────────────────┐
            │ Pipeline.Run()                  │
            │ (pkg/extsort/pipeline.go)       │
            └─────────────────────────────────┘
                          │
    ┌─────────────────────┴─────────────────────┐
    │                                           │
    ▼                                           ▼
┌───────────────────┐                   ┌───────────────────┐
│ STAGE 1           │                   │ STAGE 2           │
│ Manifest Discovery│                   │ S3 Download       │
│ & Parsing         │──────────────────▶│ (Concurrent)      │
└───────────────────┘                   └───────────────────┘
                                                │
                                                ▼
                                        ┌───────────────────┐
                                        │ STAGE 3           │
                                        │ Parse CSV/Parquet │
                                        │ (Concurrent)      │
                                        └───────────────────┘
                                                │
                                                ▼
                                        ┌───────────────────┐
                                        │ STAGE 4           │
                                        │ Aggregation &     │
                                        │ Run File Creation │
                                        └───────────────────┘
                                                │
                                                ▼
                                        ┌───────────────────┐
                                        │ STAGE 5           │
                                        │ K-Way Merge       │
                                        │ (Parallel)        │
                                        └───────────────────┘
                                                │
                                                ▼
                                        ┌───────────────────┐
                                        │ STAGE 6           │
                                        │ Index Building    │
                                        │ (MPHF, Arrays)    │
                                        └───────────────────┘
                                                │
                                                ▼
                                        ┌───────────────────┐
                                        │ STAGE 7           │
                                        │ Query (Runtime)   │
                                        │ (Separate Process)│
                                        └───────────────────┘
```

## Stage Summary

| Stage | Name | Responsibility | Main Entry Point |
|-------|------|----------------|------------------|
| 1 | `stage_manifest` | Fetch and parse S3 inventory manifest.json | `s3fetch.FetchManifest()` |
| 2 | `stage_download` | Download S3 inventory chunks with parallelism | `s3fetch.Downloader.DownloadToReader()` |
| 3 | `stage_parse` | Parse CSV/Parquet, extract key/size/tier | `inventory.NewCSVInventoryReaderFromStream()` |
| 4 | `stage_aggregate` | In-memory prefix aggregation, flush to run files | `extsort.Aggregator.AddObject()` |
| 5 | `stage_merge` | K-way merge of sorted run files | `extsort.ParallelMerger.MergeAll()` |
| 6 | `stage_build_index` | Build MPHF, depth index, columnar arrays | `extsort.IndexBuilder.AddAllWithContext()` |
| 7 | `stage_query` | Read index, lookup prefixes, compute stats | `indexread.Open()` |

---

## Stage 1: Manifest Discovery (`stage_manifest`)

**Responsibility:** Fetch the S3 inventory manifest.json and extract chunk file locations.

**Main Files:**
- `pkg/s3fetch/client.go` - S3 client wrapper, `FetchManifest()`
- `pkg/s3fetch/manifest.go` - Manifest parsing, `ParseManifest()`

**Key Functions:**
| Function | File | Description |
|----------|------|-------------|
| `FetchManifest()` | `s3fetch/client.go:52-68` | Downloads manifest.json from S3 |
| `ParseManifest()` | `s3fetch/manifest.go:40-51` | Parses JSON into `Manifest` struct |
| `DetectFormat()` | `s3fetch/manifest.go` | Determines CSV vs Parquet format |

**Input:** S3 URI (e.g., `s3://bucket/path/manifest.json`)
**Output:** `Manifest` struct with file list and schema

---

## Stage 2: S3 Download (`stage_download`)

**Responsibility:** Download inventory chunk files from S3 with parallel range requests.

**Main Files:**
- `pkg/s3fetch/downloader.go` - Multi-part downloader using AWS SDK
- `pkg/s3fetch/client.go` - `DownloadObject()` wrapper

**Key Functions:**
| Function | File | Description |
|----------|------|-------------|
| `DownloadToReader()` | `s3fetch/downloader.go` | Downloads to temp file, returns reader |
| `DownloadToFile()` | `s3fetch/downloader.go` | Downloads directly to file |
| `NewDownloader()` | `s3fetch/downloader.go` | Creates downloader with concurrency |

**Concurrency:** Configurable via `S3DownloadConcurrency` (default: 10 parts)
**Input:** S3 object keys from manifest
**Output:** Local file handles or readers for parsing

---

## Stage 3: Parse CSV/Parquet (`stage_parse`)

**Responsibility:** Parse inventory files, extract object key, size, and storage tier.

**Main Files:**
- `pkg/inventory/unified.go` - Unified reader interface
- `pkg/inventory/reader.go` - CSV reader implementation
- `pkg/inventory/parquet.go` - Parquet reader implementation

**Key Functions:**
| Function | File | Description |
|----------|------|-------------|
| `NewCSVInventoryReaderFromStream()` | `inventory/unified.go` | Creates CSV reader with gzip support |
| `NewParquetInventoryReaderFromStream()` | `inventory/unified.go` | Creates Parquet reader |
| `InventoryReader.Next()` | Interface | Returns next `InventoryRow` |

**Concurrency:** Multiple parse workers process chunks in parallel
**Input:** Downloaded file readers
**Output:** Stream of `InventoryRow{Key, Size, StorageClass, AccessTier}`

---

## Stage 4: Aggregation & Run File Creation (`stage_aggregate`)

**Responsibility:** Aggregate objects by prefix in memory, flush sorted run files at memory threshold.

**Main Files:**
- `pkg/extsort/aggregator.go` - In-memory prefix aggregation
- `pkg/extsort/pipeline.go` - `runIngestPhase()`, `flushAggregator()`
- `pkg/extsort/runfile.go` - Run file writer
- `pkg/extsort/compressed_run.go` - Compressed run file writer

**Key Functions:**
| Function | File | Description |
|----------|------|-------------|
| `AddObject()` | `extsort/aggregator.go:42-61` | Adds object to all ancestor prefixes |
| `ShouldFlush()` | `extsort/aggregator.go:121-130` | Checks if memory threshold exceeded |
| `Drain()` | `extsort/aggregator.go:138-150` | Extracts sorted prefix rows |
| `flushAggregator()` | `extsort/pipeline.go:594-675` | Sorts and writes run file |

**Memory Management:** Monitors heap via `runtime.MemStats`, flushes at 80% threshold
**Input:** Stream of `InventoryRow` from parsers
**Output:** Multiple sorted run files (`.bin` or `.crun`)

---

## Stage 5: K-Way Merge (`stage_merge`)

**Responsibility:** Merge all sorted run files into a single globally-sorted stream.

**Main Files:**
- `pkg/extsort/parallel_merge.go` - Multi-round parallel merge coordinator
- `pkg/extsort/merger.go` - Heap-based K-way merge iterator

**Key Functions:**
| Function | File | Description |
|----------|------|-------------|
| `MergeAll()` | `parallel_merge.go:122-188` | Orchestrates multi-round merge |
| `mergeRound()` | `parallel_merge.go:192-253` | One round of parallel merge |
| `executeMerge()` | `parallel_merge.go:287-409` | Single K-way merge job |
| `MergeIterator.Next()` | `merger.go:104-140` | Returns next merged row |

**Parallelism:** `NumWorkers` merge jobs run concurrently (default: CPU/2)
**Fan-in:** Up to `MaxFanIn` files merged per job (default: 8)
**Input:** List of sorted run file paths
**Output:** Single merged run file (or iterator for streaming)

---

## Stage 6: Index Building (`stage_build_index`)

**Responsibility:** Build the final index from merged prefix stream.

**Main Files:**
- `pkg/extsort/indexbuild.go` - Main index builder
- `pkg/format/mphf.go` - MPHF builder
- `pkg/format/mphf_streaming.go` - Streaming MPHF builder
- `pkg/format/depthindex.go` - Depth-based range index
- `pkg/format/writer.go` - Columnar array writers

**Key Functions:**
| Function | File | Description |
|----------|------|-------------|
| `Add()` | `indexbuild.go:154-199` | Processes one prefix row |
| `AddAllWithContext()` | `indexbuild.go:323-374` | Batch processes from iterator |
| `Finalize()` | `indexbuild.go:384-504` | Writes remaining data, builds MPHF |
| `StreamingMPHFBuilder.Build()` | `mphf_streaming.go` | Builds MPHF from stream |

**Output Files:**
| File | Type | Content |
|------|------|---------|
| `object_count.u64` | uint64[] | Object count per prefix |
| `total_bytes.u64` | uint64[] | Total bytes per prefix |
| `depth.u32` | uint32[] | Prefix depth |
| `subtree_end.u64` | uint64[] | Last position in subtree |
| `max_depth_in_subtree.u32` | uint32[] | Max depth in subtree |
| `mph.bin` | binary | MPHF data |
| `mph_fp.u64` | uint64[] | MPHF fingerprints |
| `mph_pos.u64` | uint64[] | MPHF position mapping |
| `prefix_blob.bin` | blob | Prefix strings |
| `prefix_offsets.u64` | uint64[] | Blob offsets |
| `depth_index.*` | various | Depth-based range index |
| `tier_stats/*` | various | Per-tier statistics (optional) |
| `manifest.json` | JSON | Index metadata |

---

## Stage 7: Query (`stage_query`)

**Responsibility:** Load index and answer prefix lookup/aggregation queries.

**Main Files:**
- `pkg/indexread/index.go` - Main index reader
- `pkg/format/reader.go` - Array reader (mmap)
- `pkg/format/mphf.go` - MPHF reader

**Key Functions:**
| Function | File | Description |
|----------|------|-------------|
| `Open()` | `indexread/index.go:31-86` | Opens and mmaps all index files |
| `Lookup()` | `indexread/index.go:145-147` | O(1) prefix lookup via MPHF |
| `Stats()` | `indexread/index.go` | Returns stats for a position |
| `SubtreeStats()` | `indexread/index.go` | Aggregate subtree query |

**Input:** Index directory path, prefix queries
**Output:** Stats (object count, total bytes, tier breakdown)

---

## Per-Stage Documentation

Detailed performance analysis for each stage is available in:

- [Stage 1: Manifest Discovery](stage_manifest.md)
- [Stage 2: S3 Download](stage_download.md)
- [Stage 3: Parse CSV/Parquet](stage_parse.md)
- [Stage 4: Aggregation](stage_aggregate.md)
- [Stage 5: K-Way Merge](stage_merge.md)
- [Stage 6: Index Building](stage_build_index.md)
- [Stage 7: Query](stage_query.md)

## Cross-Stage Analysis

See [Pipeline Summary](pipeline_summary.md) for:
- Pipeline-wide bottlenecks
- Inter-stage dependencies
- Prioritized optimization backlog
