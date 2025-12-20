# Memory Budget Management

This document describes how s3-inv-db manages memory usage and configures memory budgets.

## Overview

The build pipeline uses a central memory budget to coordinate memory usage across all pipeline stages. By default, the budget is set to 50% of system RAM, ensuring predictable memory consumption.

## Memory Budget Architecture

The `pkg/membudget` package provides the central `Budget` type that manages memory allocation across pipeline stages:

```
Total Budget (50% of RAM by default)
├── Aggregator: 50% - In-memory prefix aggregation
├── Run Buffers: 20% - Run file I/O buffers
├── Merge: 15% - K-way merge phase buffers
├── Index Build: 10% - Index building and MPHF
└── Headroom: 5% - Worker in-flight buffers and misc
```

## Configuration

### CLI Flag

```bash
s3inv-index build --mem-budget 4GiB --s3-manifest s3://bucket/manifest.json --out ./index
```

### Environment Variable

```bash
export S3INV_MEM_BUDGET=8GB
s3inv-index build --s3-manifest s3://bucket/manifest.json --out ./index
```

### Priority Order

1. `--mem-budget` CLI flag (highest priority)
2. `S3INV_MEM_BUDGET` environment variable
3. 50% of detected system RAM (default)

### Supported Size Formats

| Format | Meaning | Example |
|--------|---------|---------|
| Plain number | Bytes | `536870912` |
| B | Bytes | `512B` |
| KB | Kilobytes (1000) | `500KB` |
| KiB, K | Kibibytes (1024) | `512KiB` |
| MB | Megabytes (1000²) | `500MB` |
| MiB, M | Mebibytes (1024²) | `256MiB` |
| GB | Gigabytes (1000³) | `8GB` |
| GiB, G | Gibibytes (1024³) | `4GiB` |
| TB | Terabytes (1000⁴) | `1TB` |
| TiB, T | Tebibytes (1024⁴) | `0.5TiB` |

## Budget Allocation

### Aggregator Budget (50%)

The aggregator accumulates per-prefix statistics in memory. Each prefix uses approximately 288 bytes:
- Map entry overhead: ~48 bytes
- Average prefix string: ~30 bytes
- PrefixStats struct: ~210 bytes

When the aggregator reaches this budget limit, it flushes to a sorted run file.

### Run Buffer Budget (20%)

Used for buffered I/O when writing run files during the ingest phase. The full budget is used for a single writer at a time.

### Merge Budget (15%)

During the merge phase, this budget is divided among all run file readers. The per-reader buffer size is calculated as:

```
per_reader_buffer = merge_budget / num_run_files
```

Clamped to [64KB, 8MB] per reader.

### Index Build Budget (10%)

Covers memory used by the index builder during streaming:
- `subtreeEnds` array: 8 bytes per prefix
- `maxDepthInSubtrees` array: 4 bytes per prefix
- MPHF builder prefixes: ~30 bytes per prefix
- MPHF builder positions: 8 bytes per prefix

Total: ~50 bytes per prefix during streaming, plus temporary arrays during MPHF construction.

### Headroom (5%)

Reserved for:
- Worker in-flight object batches
- Channel buffers
- Miscellaneous allocations

## Concurrency Coordination

The pipeline automatically limits worker count if it would exceed the headroom budget:

```
max_workers = headroom_budget / 8MB
```

Each worker can have up to 8MB in-flight (batch buffer plus channel space). If the requested worker count exceeds this limit, it's reduced with a warning.

## System Memory Detection

The `pkg/sysmem` package provides cross-platform system memory detection:

| Platform | Method | Notes |
|----------|--------|-------|
| Linux | `unix.Sysinfo` | Uses `Totalram * Unit` |
| macOS (Darwin) | `sysctl hw.memsize` | Returns total physical memory |
| Windows | `GlobalMemoryStatusEx` | Uses `TotalPhys` field |
| FreeBSD, OpenBSD, NetBSD, DragonFlyBSD | `sysctl hw.physmem` | Falls back to `hw.realmem` on FreeBSD |
| Other platforms | Fallback | Returns 8 GB default |

When detection fails, the budget uses the fallback default (8 GB budget source: "default").

## Startup Logging

At startup, the pipeline logs the memory budget configuration:

```json
{"level":"info","total_ram":"32.00 GiB","mem_budget":"16.00 GiB","mem_budget_source":"auto-50pct","msg":"memory budget configured"}
{"level":"info","manifest_uri":"s3://...","total_budget_mb":16384,"aggregator_budget_mb":8192,"run_buffer_budget_mb":3276,"merge_budget_mb":2457,"index_budget_mb":1638,"msg":"pipeline starting"}
```

Budget sources:
- `auto-50pct`: Automatically set to 50% of detected RAM
- `default`: Used fallback when RAM detection failed
- `cli`: Set via `--mem-budget` flag
- `env`: Set via `S3INV_MEM_BUDGET` environment variable

## Example Configurations

### Low Memory System (8 GB RAM)

```bash
# Default: 4 GB budget
s3inv-index build --s3-manifest s3://bucket/manifest.json --out ./index

# Explicit conservative setting
s3inv-index build --mem-budget 2GiB --s3-manifest s3://bucket/manifest.json --out ./index
```

### High Memory System (64 GB RAM)

```bash
# Default: 32 GB budget
s3inv-index build --s3-manifest s3://bucket/manifest.json --out ./index

# Process very large inventories
s3inv-index build --mem-budget 48GiB --workers 16 --s3-manifest s3://bucket/manifest.json --out ./index
```

### Container Environments

```bash
# Match container memory limit
docker run -m 8g -e S3INV_MEM_BUDGET=6GiB myimage s3inv-index build ...
```

## API Usage

```go
import "github.com/eunmann/s3-inv-db/pkg/membudget"

// Create from system RAM (50%)
budget := membudget.NewFromSystemRAM()

// Create with explicit size
budget := membudget.New(membudget.Config{
    TotalBytes: 4 * 1024 * 1024 * 1024, // 4 GiB
    Source:     membudget.BudgetSourceCLI,
})

// Get budget allocations
fmt.Printf("Total: %s\n", membudget.FormatBytes(budget.Total()))
fmt.Printf("Aggregator: %s\n", membudget.FormatBytes(budget.AggregatorBudget()))
fmt.Printf("Run Buffers: %s\n", membudget.FormatBytes(budget.RunBufferBudget()))
fmt.Printf("Merge: %s\n", membudget.FormatBytes(budget.MergeBudget()))
fmt.Printf("Index Build: %s\n", membudget.FormatBytes(budget.IndexBuildBudget()))

// Parse human-readable size
bytes, err := membudget.ParseHumanSize("4GiB")
```

## Testing

```bash
go test ./pkg/membudget/... -v
go test ./pkg/sysmem/... -v
go test ./internal/cli/... -v -run TestDetermineMemoryBudget
```
