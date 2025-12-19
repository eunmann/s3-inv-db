# Building Indexes

This document explains how to build s3-inv-db indexes from S3 inventory data.

## Overview

The build process transforms S3 inventory CSV files into a compact, queryable index:

```
Inventory CSV files  →  External Sort  →  Trie Build  →  Index Files
```

## Input Formats

### Local CSV Files

Standard CSV format with headers:

```csv
Bucket,Key,Size,LastModifiedDate
my-bucket,data/file1.txt,1024,2024-01-15T10:30:00Z
my-bucket,data/file2.txt,2048,2024-01-15T11:00:00Z
```

Required columns:
- `Key`: Object key (path)
- `Size`: Object size in bytes

Optional columns are ignored.

### AWS S3 Inventory Format

AWS S3 Inventory produces:
1. A `manifest.json` describing the inventory
2. Multiple data files (CSV or CSV.GZ)

The data files have **no header row**; column order is defined in the manifest's `fileSchema` field.

## CLI Usage

### Building from Local Files

```bash
s3inv-index build \
  --out ./my-index \
  --tmp ./tmp \
  --chunk-size 1000000 \
  inventory-part1.csv.gz inventory-part2.csv.gz
```

**Options:**
- `--out`: Output directory for index files (required)
- `--tmp`: Temporary directory for sort operations (required)
- `--chunk-size`: Records per sort chunk (default: 1,000,000)

### Building from S3 Inventory

```bash
s3inv-index build \
  --s3-manifest s3://inventory-bucket/my-bucket/2024-01-15T00-00Z/manifest.json \
  --out ./my-index \
  --tmp ./tmp \
  --download-concurrency 8 \
  --keep-downloads
```

**S3-specific options:**
- `--s3-manifest`: S3 URI to manifest.json (triggers S3 mode)
- `--download-concurrency`: Parallel downloads (default: 4)
- `--keep-downloads`: Don't delete downloaded files after build

### AWS Credentials

S3 access uses the standard AWS credential chain:
1. Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
2. Shared credentials file (`~/.aws/credentials`)
3. IAM role (when running on EC2/ECS/Lambda)

## Programmatic Usage

### Build from Local Files

```go
import "github.com/eunmann/s3-inv-db/pkg/indexbuild"

cfg := indexbuild.Config{
    OutDir:    "./my-index",
    TmpDir:    "./tmp",
    ChunkSize: 1_000_000,
}

err := indexbuild.Build(ctx, cfg, []string{
    "inventory-part1.csv.gz",
    "inventory-part2.csv.gz",
})
```

### Build from S3 Inventory

```go
import "github.com/eunmann/s3-inv-db/pkg/indexbuild"

cfg := indexbuild.S3Config{
    Config: indexbuild.Config{
        OutDir:    "./my-index",
        TmpDir:    "./tmp",
        ChunkSize: 1_000_000,
    },
    ManifestURI:         "s3://inventory-bucket/path/manifest.json",
    DownloadConcurrency: 8,
    KeepDownloads:       false,
}

err := indexbuild.BuildFromS3(ctx, cfg)
```

### Build with Custom Schema

For CSV files without headers or non-standard column order:

```go
import "github.com/eunmann/s3-inv-db/pkg/indexbuild"

cfg := indexbuild.Config{
    OutDir:    "./my-index",
    TmpDir:    "./tmp",
    ChunkSize: 1_000_000,
}

// keyCol=1 means Key is second column, sizeCol=2 means Size is third
err := indexbuild.BuildWithSchema(ctx, cfg, files, 1, 2)
```

## Build Pipeline Details

### 1. Inventory Reading

The `pkg/inventory` package parses CSV files:

```go
reader, err := inventory.OpenFile("inventory.csv.gz")
for {
    record, err := reader.Read()
    if err == io.EOF {
        break
    }
    // Process record.Key and record.Size
}
```

Features:
- Automatic gzip detection (by `.gz` extension)
- Case-insensitive header matching
- Tolerant of malformed rows (skipped with warning)

### 2. External Sort

The `pkg/extsort` package sorts records larger than memory:

```go
sorter := extsort.NewSorter(extsort.Config{
    MaxRecordsPerChunk: 1_000_000,
    TmpDir:             "./tmp",
})

// Add records from multiple files
for _, file := range files {
    reader, _ := inventory.OpenFile(file)
    sorter.AddRecords(ctx, reader)
}

// Get sorted iterator
iter, _ := sorter.Merge(ctx)
defer iter.Close()

for iter.Next() {
    record := iter.Record()
    // Records are now sorted by Key
}
```

**How it works:**
1. Reads records into memory chunks
2. Sorts each chunk and writes to temp file
3. K-way merge reads from all temp files

**Tuning:**
- Larger chunks = fewer temp files = faster merge
- Smaller chunks = less memory usage
- Default (1M) works well for most cases

### 3. Trie Construction

The `pkg/triebuild` package builds the prefix trie:

```go
builder := triebuild.New()
result, err := builder.Build(sortedIterator)

// result.Nodes contains all prefix nodes
// result.MaxDepth is the deepest level
```

**Streaming algorithm:**
- Maintains a stack of "open" nodes
- Closes nodes when moving to different prefix
- Computes aggregates (count, bytes) on the fly

### 4. Index Writing

The `pkg/format` package writes columnar files:

```go
// Write arrays
writer, _ := format.NewArrayWriter("depth.u32", 4)
for _, node := range result.Nodes {
    writer.WriteU32(node.Depth)
}
writer.Close()

// Build MPHF
mphfBuilder := format.NewMPHFBuilder()
for _, node := range result.Nodes {
    mphfBuilder.Add(node.Prefix, node.Pos)
}
mphfBuilder.Build(outDir)

// Build depth index
depthBuilder := format.NewDepthIndexBuilder()
for _, node := range result.Nodes {
    depthBuilder.Add(node.Pos, node.Depth)
}
depthBuilder.Build(outDir)
```

### 5. Atomic Finalization

The build uses atomic operations for crash safety:

1. Write all files to `output.tmp/`
2. Sync all files to disk
3. Write manifest with checksums
4. Atomic rename `output.tmp/` → `output/`

If the build crashes, no partial index is left behind.

## Performance

### Build Time

Approximate build times (Ryzen 9 5950X):

| Objects | Prefixes | Time |
|---------|----------|------|
| 100K | ~10K | ~1s |
| 1M | ~100K | ~10s |
| 10M | ~1M | ~2min |
| 100M | ~10M | ~20min |

### Memory Usage

Memory is bounded by:
- Sort chunk size × record size (~100 bytes/record)
- MPHF construction (~8 bytes/key, temporary)

With default 1M chunk size: ~100-200 MB peak

### Disk Usage

Temporary space needed:
- Sort runs: ~100 bytes per record
- For 100M records: ~10 GB temp space

Final index size: ~80 bytes per unique prefix.

## Troubleshooting

### Out of Memory

Reduce chunk size:
```bash
s3inv-index build --chunk-size 100000 ...
```

### Slow S3 Downloads

Increase parallelism:
```bash
s3inv-index build --download-concurrency 16 ...
```

### Disk Space

Ensure temp directory has sufficient space:
- Rule of thumb: 2× the compressed inventory size

### Missing Columns

Error: `CSV header missing 'Key' column`

Check that your CSV has the required columns with standard names (`Key`, `Size`).

For non-standard formats, use the programmatic API with `BuildWithSchema()`.
