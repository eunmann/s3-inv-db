# Concurrency Model

This document describes the concurrency architecture of the s3inv-index pipeline.

## Overview

The pipeline uses a worker pool pattern to maximize throughput when processing S3 inventory files. The main bottleneck is network I/O (downloading inventory chunks from S3), so the design focuses on parallelizing downloads and parsing while keeping aggregation simple and correct.

## Architecture

```
                    ┌─────────────────┐
                    │   Job Sender    │
                    │  (goroutine)    │
                    └────────┬────────┘
                             │
                      jobs channel
                      (buffered)
                             │
              ┌──────────────┼──────────────┐
              ▼              ▼              ▼
        ┌──────────┐  ┌──────────┐  ┌──────────┐
        │ Worker 1 │  │ Worker 2 │  │ Worker N │
        │ Download │  │ Download │  │ Download │
        │ + Parse  │  │ + Parse  │  │ + Parse  │
        └────┬─────┘  └────┬─────┘  └────┬─────┘
             │             │             │
             └─────────────┼─────────────┘
                           │
                    results channel
                    (buffered)
                           │
                           ▼
                  ┌─────────────────┐
                  │   Aggregator    │
                  │ (main goroutine)│
                  └────────┬────────┘
                           │
                    flush when memory
                    threshold exceeded
                           │
                           ▼
                  ┌─────────────────┐
                  │   Run Files     │
                  └─────────────────┘
```

## Concurrency Stages

### 1. Chunk Download/Parse Workers

Multiple workers concurrently:
- Download inventory chunks from S3
- Decompress (if gzipped)
- Parse CSV or Parquet format
- Extract key, size, and storage tier

Workers are CPU and network bound. The number of workers defaults to `runtime.NumCPU()` (capped at 16).

### 2. Aggregation

A single goroutine consumes parsed objects and:
- Aggregates statistics per prefix
- Tracks counts and bytes per storage tier
- Flushes to sorted run files when memory threshold is exceeded

Aggregation is kept single-threaded to avoid lock contention. The aggregator's map operations are fast compared to network I/O.

### 3. K-way Merge

After all chunks are processed, a k-way merge combines sorted run files into the final index. This phase is I/O bound and uses buffered readers.

## Configuration

### CLI Flags

```
--workers N      Number of concurrent S3 download/parse workers
                 Default: CPU count (max 16)

--memory-mb N    Memory threshold for aggregator flush (in MB)
                 Default: 25% of system RAM (max 1GB)

--max-depth N    Maximum prefix depth to track (0 = unlimited)
```

### Programmatic Configuration

```go
config := extsort.DefaultConfig()
config.S3DownloadConcurrency = 8    // Worker count
config.MemoryThreshold = 512 * 1024 * 1024  // 512MB
config.MaxDepth = 5  // Only track up to 5 levels deep
```

## Tuning Guidelines

### For Large Inventories (>100M objects)

1. Increase workers if network is not saturated:
   ```
   --workers 16
   ```

2. Increase memory threshold to reduce flush frequency:
   ```
   --memory-mb 1024
   ```

### For Limited Memory Systems

1. Reduce memory threshold to flush earlier:
   ```
   --memory-mb 128
   ```

2. Reduce workers to limit concurrent memory usage:
   ```
   --workers 2
   ```

### Monitoring Progress

Progress logs show:
- Chunk processing rate
- Object count
- ETA based on average chunk time

```json
{"level":"info","chunk":50,"total":200,"progress_pct":25.0,"objects":5000000,"eta_ms":180000,"msg":"ingest progress"}
```

## Backpressure

The pipeline uses bounded channels to prevent unbounded memory growth:

- **Jobs channel**: `workers * 2` capacity - limits pending downloads
- **Results channel**: `workers * 2` capacity - limits buffered batches

If the aggregator can't keep up, workers will block on the results channel, naturally throttling downloads.

## Error Handling

On worker error:
1. First error is captured
2. Context is cancelled, signaling all workers to stop
3. Remaining results are drained
4. Error is returned after cleanup

This ensures clean shutdown without goroutine leaks.
