# Parquet Inventory Support

This document describes the Parquet inventory file support added to s3-inv-db.

## Overview

AWS S3 Inventory can generate inventory reports in three formats: CSV, ORC, and Parquet. s3-inv-db now supports both CSV and Parquet formats, with automatic detection based on the manifest's `fileFormat` field.

## Format Detection

The inventory format is detected automatically in two ways:

1. **Explicit `fileFormat` field** (preferred): The manifest's `fileFormat` field can be "CSV" or "Parquet"
2. **File extension fallback**: If `fileFormat` is not set, the extension of the first inventory file is checked:
   - `.parquet` → Parquet format
   - `.csv` or `.csv.gz` → CSV format
   - Default → CSV format

## Architecture

### Unified InventoryReader Interface

Both CSV and Parquet readers implement the unified `InventoryReader` interface:

```go
type InventoryRow struct {
    Key          string
    Size         uint64
    StorageClass string
    AccessTier   string
}

type InventoryReader interface {
    Next() (InventoryRow, error)
    Close() error
}
```

### Implementation Details

**CSV Reader** (`csvInventoryReader`):
- Streams data directly using Go's `encoding/csv`
- Supports gzip-compressed files (`.csv.gz`)
- Very low memory overhead

**Parquet Reader** (`parquetInventoryReader`):
- Uses `github.com/parquet-go/parquet-go` library
- Buffers to temp file (Parquet requires random access)
- Streams through row groups for memory efficiency
- Batch reads 1024 rows at a time for performance

## Performance Characteristics

| Aspect | CSV | Parquet |
|--------|-----|---------|
| File Size | Larger (text format) | Smaller (compressed columnar) |
| Memory | Streaming (low) | Temp file buffering |
| Parse Speed | Fast | Moderate |
| S3 Bandwidth | Higher | Lower |

For most use cases, both formats perform well. Parquet files are smaller due to compression, which reduces S3 transfer costs and time.

## Schema Detection

For Parquet files, column indices are detected from the Parquet schema:
- `key` → Object key column
- `size` → Object size column
- `storage_class` → Storage class column (optional)
- `intelligent_tiering_access_tier` → IT access tier (optional)

For CSV files, column indices come from the manifest's `fileSchema` field.

## Usage

No special configuration is needed. Point the CLI at a manifest.json and the format is detected automatically:

```bash
# Works with both CSV and Parquet inventories
s3inv-index build s3://bucket/path/to/manifest.json ./output
```

## Testing

The test suite includes:
- Unit tests for both CSV and Parquet readers
- Equivalence tests ensuring both formats produce identical output
- Benchmarks comparing CSV vs Parquet performance

Run tests:
```bash
go test ./pkg/inventory/... -v
```

Run benchmarks:
```bash
go test ./pkg/inventory/... -bench=.
```

## Dependencies

Parquet support adds these dependencies:
- `github.com/parquet-go/parquet-go` - Pure Go Parquet library
- Various compression libraries (brotli, lz4, snappy) for Parquet codecs

These are all pure Go with no CGO requirements, maintaining cross-compilation compatibility.
