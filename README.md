# s3-inv-db

A high-performance index for querying AWS S3 Inventory data. Build once, query instantly.

## Features

- **O(1) prefix lookups** using minimal perfect hash functions
- **Instant aggregate statistics** (object count, total bytes) for any prefix
- **Automatic storage tier tracking** with per-tier byte and object counts
- **Cost estimation** by storage class (Standard, Glacier, etc.)
- **Sub-millisecond query latency** via memory-mapped columnar format

## Quick Start

```bash
# Install
go install github.com/eunmann/s3-inv-db/cmd/s3inv-index@latest

# Build index from S3 inventory
s3inv-index build \
  --s3-manifest s3://inventory-bucket/path/manifest.json \
  --out ./my-index

# Query
s3inv-index query --index ./my-index --prefix "data/2024/" --show-tiers --estimate-cost
```

## CLI Reference

### Build Command

```bash
s3inv-index build [options]
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

### Query Command

```bash
s3inv-index query [options]
```

| Flag | Description |
|------|-------------|
| `--index` | Index directory to query (required) |
| `--prefix` | Prefix to query (required) |
| `--show-tiers` | Show per-tier breakdown |
| `--estimate-cost` | Estimate monthly storage cost |
| `--price-table` | Path to custom price table JSON |

## Library Usage

```go
idx, _ := indexread.Open("./my-index")
defer idx.Close()

pos, ok := idx.Lookup("data/2024/")
if ok {
    stats := idx.Stats(pos)
    fmt.Printf("Objects: %d, Bytes: %d\n", stats.ObjectCount, stats.TotalBytes)

    // Tier breakdown (automatic if inventory includes StorageClass)
    if idx.HasTierData() {
        for _, tb := range idx.TierBreakdown(pos) {
            fmt.Printf("  %s: %d bytes\n", tb.TierName, tb.Bytes)
        }
    }
}
```

## Documentation

- [Index Format](docs/index-format.md) - On-disk format specification
- [Building](docs/building.md) - Build pipeline details
- [Querying](docs/querying.md) - Query API reference
- [Architecture](docs/architecture.md) - System design overview

## Requirements

- Go 1.23+
- AWS credentials (for S3 inventory access)

## License

MIT
