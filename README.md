# s3-inv-db

A high-performance index for querying AWS S3 Inventory data. Build once, query instantly.

> **Note**: This project was generated with AI assistance.

## Features

- **O(1) prefix lookups** using minimal perfect hash functions
- **Instant aggregate statistics** (object count, total bytes) for any prefix
- **Storage tier tracking** with per-tier byte and object counts
- **Cost estimation** by storage class (Standard, Glacier, etc.)
- **Sub-millisecond query latency** via memory-mapped columnar format

## Quick Start

```bash
# Install
go install github.com/eunmann/s3-inv-db/cmd/s3inv-index@latest

# Build from S3 inventory
s3inv-index build \
  --s3-manifest s3://inventory-bucket/path/manifest.json \
  --out ./my-index \
  --tmp ./tmp \
  --track-tiers

# Query
s3inv-index query --index ./my-index --prefix "data/2024/" --show-tiers --estimate-cost
```

## Library Usage

```go
idx, _ := indexread.Open("./my-index")
defer idx.Close()

pos, ok := idx.Lookup("data/2024/")
if ok {
    stats := idx.Stats(pos)
    fmt.Printf("Objects: %d, Bytes: %d\n", stats.ObjectCount, stats.TotalBytes)

    // Get tier breakdown (if built with --track-tiers)
    if idx.HasTierData() {
        for _, tb := range idx.TierBreakdown(pos) {
            fmt.Printf("  %s: %d bytes\n", tb.TierName, tb.Bytes)
        }
    }
}
```

## Documentation

- [Architecture](docs/architecture.md) - System design and data structures
- [Index Format](docs/index-format.md) - On-disk format specification
- [Building](docs/building.md) - Build pipeline and configuration
- [Querying](docs/querying.md) - Query API and patterns

## Requirements

- Go 1.23+
- AWS credentials (for S3 inventory fetching)

## License

MIT
