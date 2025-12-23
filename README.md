# s3inv-index

A high-performance indexer for S3 inventory reports. Builds a compact, memory-mapped index that enables O(1) prefix lookups and fast subtree aggregation queries.

## Features

- **O(1) prefix lookups** using minimal perfect hashing (BBHash)
- **Memory-mapped queries** with sub-millisecond latency
- **Bounded memory builds** via external sort with configurable budget
- **Per-tier storage statistics** for all 12 S3 storage classes
- **Subtree aggregation** queries by depth with filtering
- **Pure Go** implementation with no CGO dependencies

## Installation

```bash
go install github.com/eunmann/s3-inv-db/cmd/s3inv-index@latest
```

Or build from source:

```bash
git clone https://github.com/eunmann/s3-inv-db.git
cd s3-inv-db
go build -o s3inv-index ./cmd/s3inv-index
```

## Quick Start

### Build an Index

```bash
s3inv-index build \
  --s3-manifest s3://my-bucket/inventory/data/manifest.json \
  --out ./my-index
```

### Query the Index

```bash
# Basic prefix lookup
s3inv-index query --index ./my-index --prefix "data/2024/"

# With tier breakdown and cost estimate
s3inv-index query --index ./my-index --prefix "data/2024/" \
  --show-tiers --estimate-cost
```

## Architecture

The index stores prefix statistics in a columnar format optimized for memory-mapped access:

```
my-index/
├── manifest.json         # File checksums and metadata
├── subtree_end.u64       # Preorder subtree ranges
├── depth.u32             # Prefix depths
├── object_count.u64      # Object counts per prefix
├── total_bytes.u64       # Byte totals per prefix
├── max_depth_in_subtree.u32
├── depth_offsets.u64     # Depth index for range queries
├── depth_positions.u64
├── mph.bin               # BBHash MPHF
├── mph_fp.u64            # Fingerprints for verification
├── mph_pos.u64           # Position mapping
├── prefix_blob.bin       # Concatenated prefix strings
├── prefix_offsets.u64    # Offsets into prefix blob
└── tier_stats/           # Per-tier statistics (optional)
    ├── tier_0_count.u64
    ├── tier_0_bytes.u64
    └── ...
```

## Library Usage

```go
import "github.com/eunmann/s3-inv-db/pkg/indexread"

idx, err := indexread.Open("./my-index")
if err != nil {
    log.Fatal(err)
}
defer idx.Close()

// O(1) prefix lookup
pos, ok := idx.Lookup("data/2024/")
if !ok {
    log.Fatal("prefix not found")
}

// Get statistics
stats := idx.Stats(pos)
fmt.Printf("Objects: %d, Bytes: %d\n", stats.ObjectCount, stats.TotalBytes)

// Get tier breakdown
if idx.HasTierData() {
    for _, tb := range idx.TierBreakdown(pos) {
        fmt.Printf("%s: %d objects\n", tb.TierName, tb.ObjectCount)
    }
}
```

## Documentation

- [Overview](docs/overview.md) - System design and data flow
- [Index Format](docs/index-format.md) - On-disk format specification
- [CLI Reference](docs/cli.md) - Command-line interface
- [Library API](docs/library-api.md) - Go package documentation
- [Performance](docs/performance.md) - Benchmarks and tuning

## Requirements

- Go 1.21+
- AWS credentials configured for S3 access (build only)
- S3 inventory configured in CSV or Parquet format

## License

MIT
