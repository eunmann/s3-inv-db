# s3-inv-db

A high-performance index for querying AWS S3 Inventory data. Build once, query instantly.

## Overview

S3 Inventory reports can contain billions of objects, making ad-hoc analysis slow and expensive. `s3-inv-db` builds a compact, memory-mapped index that enables:

- **O(1) prefix lookups** using minimal perfect hash functions
- **Instant aggregate statistics** (object count, total bytes) for any prefix
- **Depth-based queries** to explore folder hierarchies
- **Sub-millisecond query latency** even for million+ object inventories

## Use Cases

- **Storage cost analysis**: Quickly find which prefixes consume the most storage
- **Data governance**: Audit object counts across organizational prefixes
- **Capacity planning**: Track storage growth by analyzing inventory snapshots
- **Migration planning**: Identify large prefixes for prioritized migration

## Installation

```bash
go install github.com/eunmann/s3-inv-db/cmd/s3inv-index@latest
```

## Quick Start

### Build from Local CSV Files

```bash
# Build index from inventory CSV files
s3inv-index build \
  --out ./my-index \
  --tmp ./tmp \
  inventory-part1.csv.gz inventory-part2.csv.gz
```

### Build from S3 Inventory

```bash
# Build directly from S3 inventory manifest
s3inv-index build \
  --s3-manifest s3://inventory-bucket/my-bucket/2024-01-15T00-00Z/manifest.json \
  --out ./my-index \
  --tmp ./tmp \
  --download-concurrency 8
```

### Query the Index (Programmatic)

```go
package main

import (
    "fmt"
    "github.com/eunmann/s3-inv-db/pkg/indexread"
)

func main() {
    idx, _ := indexread.Open("./my-index")
    defer idx.Close()

    // Look up a prefix
    pos, ok := idx.Lookup("data/2024/")
    if ok {
        stats := idx.Stats(pos)
        fmt.Printf("Objects: %d, Bytes: %d\n", stats.ObjectCount, stats.TotalBytes)
    }

    // Get immediate children at depth 1
    children, _ := idx.DescendantsAtDepth(pos, 1)
    for _, childPos := range children {
        prefix, _ := idx.PrefixString(childPos)
        stats := idx.Stats(childPos)
        fmt.Printf("  %s: %d objects\n", prefix, stats.ObjectCount)
    }
}
```

## Performance

Benchmarks on a Ryzen 9 5950X with 1M prefixes:

| Operation | Latency |
|-----------|---------|
| Prefix lookup | 5-6 μs |
| Stats retrieval | 5-6 μs |
| Query 1M children | 2.4 ms |
| Random child lookup | 5.8 μs |

Index build time scales linearly: ~500ms for 100K objects, ~5s for 1M objects.

## Architecture

The index uses a columnar, memory-mapped format optimized for read performance:

```
my-index/
├── manifest.json      # Metadata and checksums
├── prefixes.blob      # Prefix strings (MPHF-indexed)
├── mphf.bin           # Minimal perfect hash function
├── subtree_end.u64    # Subtree range bounds
├── depth.u32          # Node depths
├── object_count.u64   # Aggregate object counts
├── total_bytes.u64    # Aggregate byte sizes
├── max_depth_in_subtree.u32
└── depth_index/       # Posting lists by depth
    ├── depth_1.bin
    ├── depth_2.bin
    └── ...
```

See [docs/architecture.md](docs/architecture.md) for detailed design documentation.

## Documentation

- [Architecture Overview](docs/architecture.md) - System design and data structures
- [Index Format](docs/index-format.md) - On-disk format specification
- [Building Indexes](docs/building.md) - Build pipeline and configuration
- [Querying](docs/querying.md) - Query operations and performance

## Requirements

- Go 1.23+
- AWS credentials (for S3 inventory fetching)

## License

MIT
