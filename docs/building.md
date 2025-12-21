# Building Indexes

This document explains how to build s3-inv-db indexes from S3 inventory data.

## Overview

The build process streams S3 inventory files (CSV or Parquet) through a pure-Go external sort pipeline to construct a compact, queryable index:

```
S3 Inventory Files  →  Streaming Aggregation  →  External Sort  →  Index Files
```

The pipeline is fully streaming with bounded memory usage, and requires no CGO dependencies.

## Input Format

### AWS S3 Inventory Format

AWS S3 Inventory produces:
1. A `manifest.json` describing the inventory
2. Multiple data files (CSV, CSV.GZ, or Parquet)

CSV files have **no header row**; column order is defined in the manifest's `fileSchema` field. Parquet files have embedded schema that is auto-detected.

**Supported columns:**
- `Key`: Object key (required)
- `Size`: Object size in bytes (required)
- `StorageClass`: Storage tier (optional, enables tier tracking)
- `IntelligentTieringAccessTier`: Access tier for Intelligent-Tiering (optional)

## CLI Usage

### Building from S3 Inventory

```bash
s3inv-index build \
  --s3-manifest s3://inventory-bucket/my-bucket/2024-01-15T00-00Z/manifest.json \
  --out ./my-index
```

**Options:**
- `--out`: Output directory for index files (required)
- `--s3-manifest`: S3 URI to manifest.json (required)
- `--verbose`: Enable debug level logging
- `--pretty-logs`: Use human-friendly console output

### AWS Credentials

S3 access uses the standard AWS credential chain:
1. Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
2. Shared credentials file (`~/.aws/credentials`)
3. IAM role (when running on EC2/ECS/Lambda)

## Programmatic Usage

### Build from S3 Inventory

```go
import (
    "context"
    "os"
    "os/signal"
    "syscall"

    "github.com/eunmann/s3-inv-db/pkg/extsort"
    "github.com/eunmann/s3-inv-db/pkg/s3fetch"
)

func main() {
    // Create a context that cancels on SIGINT/SIGTERM
    ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
    defer stop()

    // Create S3 client
    client, err := s3fetch.NewClient(ctx)
    if err != nil {
        log.Fatal(err)
    }

    outDir := "./my-index"
    manifestURI := "s3://inventory-bucket/path/manifest.json"

    // Create and run the pipeline
    config := extsort.DefaultConfig()
    pipeline := extsort.NewPipeline(config, client)

    result, err := pipeline.Run(ctx, manifestURI, outDir)
    if err != nil {
        log.Fatal(err)
    }

    log.Printf("Built index with %d prefixes in %v",
        result.PrefixCount, result.Duration)
}
```

## Build Pipeline Details

### 1. S3 Streaming

The `pkg/s3fetch` package streams inventory files:

```go
client, _ := s3fetch.NewClient(ctx)

// Fetch manifest
bucket, key, _ := s3fetch.ParseS3URI(manifestURI)
manifest, _ := client.FetchManifest(ctx, bucket, key)

// Stream each inventory file
for _, file := range manifest.Files {
    reader, _ := client.StreamObject(ctx, destBucket, file.Key)
    // Process reader...
    reader.Close()
}
```

Features:
- No local disk copies required
- Automatic gzip decompression
- Manifest parsing with column detection

### 2. Streaming Aggregation

The `pkg/extsort` package aggregates prefix statistics in bounded memory:

```go
agg := extsort.NewAggregator(expectedObjects, maxDepth)

for record := range records {
    agg.AddObject(record.Key, record.Size, record.TierID)
}

rows := agg.Drain()
```

When memory threshold is reached, sorted runs are flushed to temporary files.

### 3. External Merge Sort

Run files are merged using k-way merge to produce globally sorted output:

```go
merger := extsort.NewMerger(runFiles)
for merger.Next() {
    row := merger.Row()
    // Process sorted, deduplicated row
}
```

### 4. Index Construction

The streaming index builder constructs the final index in a single pass:

```go
builder, _ := extsort.NewIndexBuilder(outDir, tempDir)
for _, row := range sortedRows {
    builder.Add(row)
}
builder.Finalize()
```

**Output files:**
- `subtree_end.u64`: Subtree ranges
- `depth.u32`: Node depths
- `object_count.u64`: Object counts
- `total_bytes.u64`: Byte totals
- `mphf.bin`: Perfect hash function
- `mphf_keys.bin`: Prefix keys for verification
- `depth_index/`: Depth posting lists
- `tier_*_count.u64`, `tier_*_bytes.u64`: Per-tier statistics (if tiers present)
- `manifest.json`: File checksums

### 5. Atomic Finalization

The build uses atomic operations for crash safety:

1. Write all files to `output.tmp/`
2. Sync all files to disk
3. Write manifest with checksums
4. Atomic rename `output.tmp/` → `output/`

If the build crashes, no partial index is left behind.

## One-Shot Builds

The build pipeline is optimized for maximum throughput as a one-shot operation:
- No checkpoint/resume overhead
- Streaming aggregation with bounded memory
- Always rebuilds from scratch for simplicity and speed

If a build fails or is interrupted:
1. Delete the output directory
2. Delete any temporary run files
3. Restart the build

## Performance

### Build Time

Approximate build times (depends on S3 throughput and CPU):

| Objects | Prefixes | Time |
|---------|----------|------|
| 1M | ~100K | ~30s-1min |
| 10M | ~1M | ~5-10min |
| 100M | ~10M | ~1hr |

### Memory Usage

Memory is bounded by:
- In-memory aggregator buffer (configurable, default 256MB)
- Run file buffers (~4MB per run)
- MPHF construction (~8 bytes/key, temporary)

Typical peak: ~400MB for large inventories.

### Disk Usage

Temporary run files: ~100 bytes per unique prefix (cleaned up after merge)
Final index: ~80 bytes per unique prefix

For 10M unique prefixes:
- Temporary files: ~1GB (during build)
- Final index: ~800MB

## Configuration

The pipeline can be configured via `extsort.Config`:

```go
config := extsort.Config{
    TempDir:               "/tmp",
    MemoryThreshold:       256 * 1024 * 1024, // 256MB
    RunFileBufferSize:     4 * 1024 * 1024,   // 4MB
    S3DownloadConcurrency: 4,
    ParseConcurrency:      4,
    IndexWriteConcurrency: 4,
    MaxDepth:              0, // 0 = unlimited
}
```

## Troubleshooting

### S3 Access Denied

Ensure your AWS credentials have:
- `s3:GetObject` on the inventory bucket
- Access to the manifest.json and all data files

### Out of Memory

Reduce the memory threshold:
```go
config := extsort.Config{
    MemoryThreshold: 128 * 1024 * 1024, // 128MB instead of 256MB
}
```

### Disk Space

Ensure you have space for:
- Temporary run files (~100 bytes per prefix during build)
- Final index (~80 bytes per prefix)

### Missing Columns

Error: `column "Key" not found in schema`

Check that your inventory includes the required columns (Key, Size) in the manifest's fileSchema.

### Build Failed

If a build fails:
1. Delete the output directory and any `.tmp` directory
2. Delete temporary run files (usually in system temp dir)
3. Restart the build from scratch
