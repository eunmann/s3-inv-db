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

## Development: Seed Data

For local development and testing, generate synthetic inventory indexes:

```bash
# Generate 3 inventories with 10K objects each (default)
make seed

# Custom generation
./bin/s3inv-seeder --out ./seed-data --count 5 --objects 50000 --preset large

# Clean up generated data
make clean-seed
```

### Presets

| Preset | Fanout | Max Depth | Description |
|--------|--------|-----------|-------------|
| small | 5 | 3 | Compact test data |
| medium | 10 | 5 | Moderate complexity |
| large | 20 | 8 | Deep, wide trees |
| realistic | 15 | 7 | S3-like path patterns (default) |

### Using Seed Data with Server

1. Generate seed data: `make seed`
2. Start the server: `./bin/s3inv-server --dev --verbose`
3. Register an inventory:
   ```bash
   curl -X POST http://localhost:8080/api/inventories \
     -H "Content-Type: application/json" \
     -d '{"id":"inv-001","name":"Test Inventory","path":"./seed-data/inv-001"}'
   ```
4. Load the inventory:
   ```bash
   curl -X POST http://localhost:8080/api/inventories/inv-001/load
   ```
5. Query prefix stats:
   ```bash
   curl "http://localhost:8080/api/stats/inv-001?prefix=data/"
   ```

### Output Structure

```
seed-data/
├── summary.json       # Registry of all generated inventories
├── inv-001/           # First inventory index directory
│   ├── subtree_end.u64
│   ├── depth.u32
│   ├── object_count.u64
│   └── ... (all index files)
├── inv-002/
└── inv-003/
```

## Docker

The `infra/` directory carries a compose stack with three profiles. All commands accept the `S3INV_DEV_PORT` / `S3INV_PROD_PORT` env vars to override the host ports.

| Profile | Make target | What it does |
|---|---|---|
| `dev` | `make docker-dev` | Source-mounted server with air hot-reload on `:8080` |
| `prod` | `make docker-prod` | Slim multi-stage image (~21 MB) on `:8081`; mounts `./seed-data` read-only at `/data` |
| `seed` | `make docker-seed` | One-shot inventory generator; writes to `./seed-data` on the host |

```bash
# Generate seed data inside a container, then run the slim image against it
make docker-seed
make docker-prod

# Or develop with hot reload
make docker-dev

# Stop and clean up volumes
make docker-down
```

When running against the dev container, register inventories using the in-container path (`/app/seed-data/inv-001`); against the prod container, use `/data/inv-001`.

## Requirements

- Go 1.25+
- AWS credentials configured for S3 access (build only)
- S3 inventory configured in CSV or Parquet format

## License

MIT
