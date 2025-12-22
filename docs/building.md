# Building Indexes

Transform S3 inventory files into a queryable index using a streaming external sort pipeline.

## Input

AWS S3 Inventory produces:
1. A `manifest.json` describing the inventory
2. Multiple data files (CSV, CSV.GZ, or Parquet)

**Required columns:** `Key`, `Size`
**Optional columns:** `StorageClass` (enables automatic tier tracking)

## CLI Usage

```bash
s3inv-index build \
  --s3-manifest s3://inventory-bucket/path/manifest.json \
  --out ./my-index
```

| Flag | Description |
|------|-------------|
| `--out` | Output directory for index files (required) |
| `--s3-manifest` | S3 URI to inventory manifest.json (required) |
| `--mem-budget` | Memory budget (e.g., `4GiB`, `8GB`). Default: 50% of RAM |
| `--workers` | Concurrent S3 download/parse workers. Default: CPU count |
| `--max-depth` | Maximum prefix depth to track (0 = unlimited) |
| `--verbose` | Enable debug logging |
| `--pretty-logs` | Human-friendly console output |

### AWS Credentials

Uses standard AWS credential chain:
1. Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
2. Shared credentials file (`~/.aws/credentials`)
3. IAM role (EC2/ECS/Lambda)

## Build Pipeline

```
S3 Inventory → Streaming Aggregation → External Sort → Index Files
```

**Stages:**

1. **Stream from S3**: Fetch and parse inventory files without downloading to disk
2. **Aggregate in memory**: Accumulate prefix statistics, flush to run files when threshold reached
3. **K-way merge**: Heap-based merge producing globally sorted prefixes
4. **Build index**: Single-pass construction of columnar arrays, MPHF, and depth index
5. **Finalize**: Write manifest with checksums, atomic rename to final location

## Output Files

| File | Description |
|------|-------------|
| `manifest.json` | Metadata and SHA-256 checksums |
| `subtree_end.u64` | Subtree end positions |
| `depth.u32` | Node depths |
| `object_count.u64` | Object counts per prefix |
| `total_bytes.u64` | Byte totals per prefix |
| `max_depth_in_subtree.u32` | Maximum depth in each subtree |
| `depth_offsets.u64` | Depth index offsets |
| `depth_positions.u64` | Positions sorted by depth |
| `mph.bin` | BBHash MPHF |
| `mph_fp.u64` | Fingerprints for verification |
| `mph_pos.u64` | Hash position → preorder position mapping |
| `prefix_blob.bin` | Concatenated prefix strings |
| `prefix_offsets.u64` | Offsets into prefix blob |

**Tier statistics (if StorageClass present):**
- `tiers.json`: Tier manifest
- `tier_stats/<tier>_bytes.u64`, `tier_stats/<tier>_count.u64`: Per-tier arrays

## Programmatic Usage

```go
import (
    "context"
    "github.com/eunmann/s3-inv-db/pkg/extsort"
    "github.com/eunmann/s3-inv-db/pkg/s3fetch"
)

ctx := context.Background()
client, _ := s3fetch.NewClient(ctx)

config := extsort.DefaultConfig()
pipeline := extsort.NewPipeline(config, client)

result, err := pipeline.Run(ctx, "s3://bucket/manifest.json", "./my-index")
```

## Resource Usage

**Memory**: Bounded by `--mem-budget`. Default is 50% of system RAM, allocated as:
- 50% for in-memory aggregator
- 25% for run file buffers
- 25% for merge and index building

**Disk**: Temporary run files (~100 bytes/prefix) are cleaned up after merge. Final index is ~120 bytes/prefix.

## Crash Safety

Builds use atomic operations:
1. Write all files to `output.tmp/`
2. Sync to disk
3. Write manifest with checksums
4. Atomic rename to final location

If interrupted, delete output directory and restart.

## Troubleshooting

| Error | Solution |
|-------|----------|
| S3 Access Denied | Ensure `s3:GetObject` permission on inventory bucket |
| Out of Memory | Reduce `--mem-budget` |
| Column not found | Check inventory includes `Key` and `Size` in manifest schema |
