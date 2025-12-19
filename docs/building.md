# Building Indexes

This document explains how to build s3-inv-db indexes from S3 inventory data.

## Overview

The build process streams S3 inventory CSV files directly into a SQLite-based aggregator, then constructs a compact, queryable index:

```
S3 Inventory Files  →  SQLite Aggregation  →  Trie Build  →  Index Files
```

## Input Format

### AWS S3 Inventory Format

AWS S3 Inventory produces:
1. A `manifest.json` describing the inventory
2. Multiple data files (CSV or CSV.GZ)

The data files have **no header row**; column order is defined in the manifest's `fileSchema` field.

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
- `--db`: Path to SQLite database (default: `<out>.db`)
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

    "github.com/eunmann/s3-inv-db/pkg/indexbuild"
    "github.com/eunmann/s3-inv-db/pkg/s3fetch"
    "github.com/eunmann/s3-inv-db/pkg/sqliteagg"
)

func main() {
    ctx := context.Background()

    // Create S3 client
    client, err := s3fetch.NewClient(ctx)
    if err != nil {
        log.Fatal(err)
    }

    dbPath := "./prefix-agg.db"
    outDir := "./my-index"

    // Configure SQLite
    sqliteCfg := sqliteagg.DefaultConfig(dbPath)

    // Stream from S3 into SQLite
    streamCfg := sqliteagg.StreamConfig{
        ManifestURI:  "s3://inventory-bucket/path/manifest.json",
        DBPath:       dbPath,
        SQLiteConfig: sqliteCfg,
    }

    result, err := sqliteagg.StreamFromS3(ctx, client, streamCfg)
    if err != nil {
        log.Fatal(err)
    }

    log.Printf("Processed %d chunks, %d objects",
        result.ChunksProcessed, result.ObjectsProcessed)

    // Build index from SQLite
    buildCfg := indexbuild.SQLiteConfig{
        OutDir:    outDir,
        DBPath:    dbPath,
        SQLiteCfg: sqliteCfg,
    }

    if err := indexbuild.BuildFromSQLite(buildCfg); err != nil {
        log.Fatal(err)
    }
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

### 2. SQLite Aggregation

The `pkg/sqliteagg` package aggregates prefix statistics:

```go
agg, _ := sqliteagg.Open(sqliteCfg)
defer agg.Close()

// Process a chunk
agg.BeginChunk()
for record := range records {
    agg.AddObject(record.Key, record.Size, record.TierID)
}
agg.MarkChunkDone(chunkID)
agg.Commit()
```

**Resume support:**
- Each chunk is tracked in `chunks_done` table
- On restart, completed chunks are skipped
- Partial chunks are rolled back

### 3. Trie Construction

The `pkg/sqliteagg` package builds the trie from aggregated data:

```go
result, _ := sqliteagg.BuildTrieFromSQLite(agg)

// result.Nodes contains all prefix nodes
// result.MaxDepth is the deepest level
// result.PresentTiers lists tiers with data
```

**Streaming from SQLite:**
- Prefixes ordered by `ORDER BY prefix`
- Stats already aggregated (no re-computation)
- Single pass through sorted data

### 4. Index Writing

The `pkg/indexbuild` package writes the final index:

```go
cfg := indexbuild.SQLiteConfig{
    OutDir:    "./my-index",
    DBPath:    "./prefix-agg.db",
    SQLiteCfg: sqliteCfg,
}

err := indexbuild.BuildFromSQLite(cfg)
```

**Output files:**
- `subtree_end.u64`: Subtree ranges
- `depth.u32`: Node depths
- `object_count.u64`: Object counts
- `total_bytes.u64`: Byte totals
- `mph.bin`: Perfect hash function
- `depth_index/`: Depth posting lists
- `tier_stats/`: Per-tier statistics (if tiers present)
- `manifest.json`: File checksums

### 5. Atomic Finalization

The build uses atomic operations for crash safety:

1. Write all files to `output.tmp/`
2. Sync all files to disk
3. Write manifest with checksums
4. Atomic rename `output.tmp/` → `output/`

If the build crashes, no partial index is left behind.

## Resume Support

The build pipeline supports resuming from interruptions:

### Chunk-level Resume (SQLite Aggregation)

Each inventory file (chunk) is tracked in the `chunks_done` table:

```sql
SELECT chunk_id FROM chunks_done;
```

When restarting:
- Completed chunks are skipped
- The current incomplete chunk is rolled back
- Processing continues from where it left off

### Index-level Resume

If the index already exists and is valid:
- The manifest is read and checksums verified
- If all files match, the build is skipped entirely
- Log message: "resumed from completed build - index already valid"

## Performance

### Build Time

Approximate build times (depends on S3 throughput and CPU):

| Objects | Prefixes | Time |
|---------|----------|------|
| 1M | ~100K | ~1-2min |
| 10M | ~1M | ~10-15min |
| 100M | ~10M | ~1-2hr |

### Memory Usage

Memory is bounded by:
- SQLite page cache (configurable, default 256MB)
- Current chunk buffer (~100MB)
- MPHF construction (~8 bytes/key, temporary)

Typical peak: ~500MB for large inventories.

### Disk Usage

SQLite database: ~100 bytes per unique prefix
Final index: ~80 bytes per unique prefix

For 10M unique prefixes:
- SQLite DB: ~1GB
- Final index: ~800MB

## Troubleshooting

### S3 Access Denied

Ensure your AWS credentials have:
- `s3:GetObject` on the inventory bucket
- Access to the manifest.json and all data files

### Out of Memory

SQLite cache can be reduced:
```go
cfg := sqliteagg.Config{
    DBPath:      dbPath,
    CacheSizeKB: 65536,  // 64MB instead of default 256MB
}
```

### Disk Space

Ensure you have space for:
- SQLite database (~100 bytes per prefix)
- Final index (~80 bytes per prefix)
- WAL file (up to database size during build)

### Missing Columns

Error: `column "Key" not found in schema`

Check that your inventory includes the required columns (Key, Size) in the manifest's fileSchema.

### Resume Not Working

If chunks keep being reprocessed:
- Check that `--db` points to the same database file
- Verify the SQLite database wasn't deleted
- Check logs for "skipping already processed chunk" messages
