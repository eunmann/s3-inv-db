# Stage 3: Parse CSV/Parquet (`stage_parse`)

## Stage Summary

**What this stage does:** Parses S3 inventory chunk files (CSV.gz or Parquet format) and extracts object key, size, and storage tier information. Produces a stream of `InventoryRow` objects.

**Inputs:**
- Downloaded file reader (from Stage 2)
- Column configuration from manifest
- File format: CSV (gzip-compressed) or Parquet

**Outputs:**
- Stream of `InventoryRow{Key, Size, StorageClass, AccessTier}`
- Each chunk typically yields 100K-10M objects

---

## Control Flow and Data Flow

### CSV Parsing Flow

```
io.ReadCloser (from temp file)
    │
    ▼
gzip.NewReader() [if .gz extension]
    │
    ▼
csv.NewReader() [with ReuseRecord=true]
    │
    ▼
csvInventoryReader.Next()
    ├── csv.Read() → []string (reused buffer)
    ├── strconv.ParseUint(size)
    └── Return InventoryRow
```

### Parquet Parsing Flow

```
io.ReadCloser (from temp file)
    │
    ▼
io.Copy() to new temp file [Parquet needs random access]
    │
    ▼
parquet.OpenFile()
    │
    ▼
parquetInventoryReader.Next()
    ├── ReadRows() → []parquet.Row (batch of 1024)
    ├── rowToInventoryRow() for each
    └── Return InventoryRow
```

### Key Files and Line References

| File | Function | Lines | Purpose |
|------|----------|-------|---------|
| `inventory/unified.go` | `NewCSVInventoryReaderFromStream()` | 92-119 | Create CSV reader with gzip |
| `inventory/unified.go` | `csvInventoryReader.Next()` | 122-162 | Parse single CSV row |
| `inventory/parquet.go` | `NewParquetInventoryReaderFromStream()` | 61-104 | Create Parquet reader |
| `inventory/parquet.go` | `parquetInventoryReader.Next()` | 203-232 | Read from row group |
| `inventory/parquet.go` | `rowToInventoryRow()` | 235-261 | Convert Parquet row |

---

## Performance Analysis

### CSV Parser

**CPU Hotspots:**

1. **gzip Decompression:** `gzip.NewReader()` decompresses entire stream on-the-fly.
   - Standard library gzip is single-threaded
   - Typically the bottleneck for CSV parsing

2. **CSV Parsing:** `csv.Read()` parses quoted fields, handles escaping.
   - Go's `csv.Reader` is efficient
   - `ReuseRecord = true` avoids per-row allocations

3. **Size Parsing:** `strconv.ParseUint()` for each row.
   - Very fast, not a bottleneck

**Complexity:**
- O(n) where n = number of rows
- O(m) per row where m = average field length

**Measured Performance (typical):**
- CSV: ~500K-1M rows/second after gzip decompression
- Bottleneck is usually gzip, not CSV parsing

### Parquet Parser

**CPU Hotspots:**

1. **Row Group Decompression:** Parquet uses per-column compression (snappy/zstd).
   - More efficient than gzip for columnar data
   - Decompression happens in batches

2. **Column Value Extraction:** `rowToInventoryRow()` iterates over all columns.
   - Linear scan through `[]parquet.Value`
   - Could be optimized with column indexing

3. **String Allocation:** `val.String()` allocates for each key.
   - Major allocation source

**Complexity:**
- O(n) where n = number of rows
- O(c) per row where c = number of columns

**Measured Performance (typical):**
- Parquet: ~1-3M rows/second (faster than gzip CSV)
- More CPU-efficient due to better compression

---

## Memory Analysis

### CSV Memory Usage

| Allocation | Size | Lifetime |
|------------|------|----------|
| gzip decompressor state | ~128KB | Per-file |
| csv.Reader buffer | ~4KB | Per-file |
| Reused record slice | ~10 fields * 100 bytes | Per-file (reused) |
| `InventoryRow` | ~120 bytes | Per-row (returned) |

**Key Optimization:** `csv.ReuseRecord = true` reuses the field slice across reads.

### Parquet Memory Usage

| Allocation | Size | Lifetime |
|------------|------|----------|
| Row buffer | 1024 rows * ~200 bytes | Per-file |
| Row group metadata | ~10KB per row group | Per-file |
| Decompression buffers | ~1MB per column | Per-row-group |
| `InventoryRow` | ~120 bytes | Per-row (returned) |

**Key Issue:** `rowBuf := make([]parquet.Row, 1024)` pre-allocates 1024 rows.
- Each row contains variable-length values
- Memory grows with column count and data size

---

## Concurrency & Parallelism

### Current Model

**No intra-file parallelism.** Each chunk is parsed by a single goroutine.

**Inter-file parallelism:** Multiple chunk workers parse different files concurrently.

### Potential for Parallelism

1. **CSV:** Limited - gzip decompression is sequential
   - Could use parallel gzip libraries (e.g., `pgzip`), but marginal benefit
   - Go's gzip is well-optimized

2. **Parquet:** High potential
   - Row groups can be read in parallel
   - Columns within a row group can be read in parallel
   - The `parquet-go` library may already do some internal parallelism

---

## I/O Patterns

### CSV I/O

1. **Input:** Sequential read from gzip reader
2. **Buffer Size:** Default 4KB in csv.Reader
3. **Pattern:** Streaming, low memory

### Parquet I/O

1. **Input:** Random access via `ReaderAt` interface
2. **Extra Copy:** For streaming input, must copy to temp file first (line 62-73)
3. **Pattern:** Row-group batched reads

### Observations

1. **Double Temp File for Parquet:** Download creates temp file, then Parquet creates another temp file.
   - Wastes disk space and I/O
   - Could pass the download temp file directly

2. **No Memory Mapping:** Parquet uses file I/O, not mmap.
   - mmap could reduce syscall overhead for large files

---

## Concrete Recommendations

### Rec-1: Avoid Double Temp File for Parquet

**Location:** `inventory/parquet.go:61-104` and `s3fetch/downloader.go`

**Current behavior:**
1. Downloader creates temp file A, downloads content
2. `NewParquetInventoryReaderFromStream()` creates temp file B, copies from A
3. Two temp files exist simultaneously

**Proposed change:**
```go
// Option 1: Pass ReaderAt directly
// Modify tempFileReader to implement io.ReaderAt + Size() interface
// (Already implemented! See downloader.go:224-243)

// In pipeline.go, detect Parquet and use ReaderAt path:
if cfg.format == s3fetch.InventoryFormatParquet {
    // Cast to ReaderAt (tempFileReader already implements this)
    readerAt, ok := body.(interface{
        io.ReaderAt
        Size() (int64, error)
    })
    if ok {
        size, _ := readerAt.Size()
        reader, err = inventory.NewParquetInventoryReader(readerAt, size, parquetCfg)
    }
}
```

**Expected benefit:**
- Eliminate one temp file creation and ~100MB copy per chunk
- 10-20% reduction in chunk processing time for Parquet

**Risk level:** Low (tempFileReader already implements ReaderAt)

**How to test:** Verify Parquet parsing works; benchmark before/after.

---

### Rec-2: Optimize Parquet Column Lookup

**Location:** `inventory/parquet.go:235-261`

**Current behavior:**
```go
func (r *parquetInventoryReader) rowToInventoryRow(row parquet.Row) InventoryRow {
    for _, val := range row {           // O(c) iteration for every row
        colIdx := val.Column()
        switch colIdx {
        case r.keyCol: ...
        case r.sizeCol: ...
        }
    }
}
```

**Issue:** Linear scan through all columns for every row.

**Proposed change:**
```go
func (r *parquetInventoryReader) rowToInventoryRow(row parquet.Row) InventoryRow {
    inv := InventoryRow{}

    // Direct index access (if column layout is consistent)
    if r.keyCol < len(row) && !row[r.keyCol].IsNull() {
        inv.Key = row[r.keyCol].String()
    }
    if r.sizeCol < len(row) && !row[r.sizeCol].IsNull() {
        inv.Size = row[r.sizeCol].Uint64()
    }
    // ... similar for storage/tier

    return inv
}
```

**Expected benefit:** Reduce per-row overhead from O(c) to O(1).

**Risk level:** Medium (assumes consistent column ordering in parquet.Row)

**How to test:** Unit tests with various schemas; verify correct extraction.

---

### Rec-3: Use Parallel Gzip for CSV

**Location:** `inventory/unified.go:97`

**Current behavior:** Standard library gzip (single-threaded).

**Proposed change:**
```go
import "github.com/klauspost/pgzip"

if strings.HasSuffix(strings.ToLower(key), ".gz") {
    gzr, err := pgzip.NewReader(r)
    if err != nil {
        ...
    }
    gzr.SetNumReaders(4) // Parallel decompression
    ...
}
```

**Expected benefit:** 2-4x faster gzip decompression on multi-core.

**Risk level:** Low (drop-in replacement for compress/gzip)

**How to test:** Benchmark CSV parsing with/without pgzip.

**Dependencies:** Add `github.com/klauspost/pgzip`

---

### Rec-4: Increase Parquet Row Buffer Size

**Location:** `inventory/parquet.go:196`

**Current behavior:**
```go
rowBuf: make([]parquet.Row, 1024)
```

**Issue:** 1024 rows may be small for large row groups (often 1M+ rows).

**Proposed change:**
```go
// Adaptive buffer size based on row group size
const defaultBufSize = 8192
const maxBufSize = 65536

bufSize := defaultBufSize
if len(rowGroups) > 0 {
    estimatedRows := rowGroups[0].NumRows()
    if estimatedRows > int64(maxBufSize) {
        bufSize = maxBufSize
    } else if estimatedRows > int64(defaultBufSize) {
        bufSize = int(estimatedRows)
    }
}
rowBuf: make([]parquet.Row, bufSize)
```

**Expected benefit:** Reduce overhead from repeated `ReadRows` calls.

**Risk level:** Low

**How to test:** Benchmark with various row group sizes.

---

### Rec-5: Pool InventoryRow Allocations

**Location:** `inventory/unified.go:148-151` and `inventory/parquet.go:235-261`

**Current behavior:** Each `Next()` returns a new `InventoryRow` struct.

**Issue:** `InventoryRow` contains strings (Key, StorageClass, AccessTier), so pooling the struct alone doesn't help much. The string allocations dominate.

**Proposed change (limited benefit):**
```go
// For CSV: Key is already from the reused record slice
// The string slice lives until next Read() - but we copy to objectRecord anyway

// For Parquet: val.String() always allocates
// Could use byte slices instead of strings if callers support it
type InventoryRowBytes struct {
    Key          []byte
    Size         uint64
    StorageClass []byte
    AccessTier   []byte
}
```

**Expected benefit:** Minimal for current usage (strings copied to objectRecord anyway).

**Risk level:** High (API change, caller modifications needed)

**Recommendation:** Skip this optimization - the aggregator stage copies strings anyway.

---

## Summary

Stage 3 parsing is reasonably efficient. The main opportunities are:

1. **Parquet double temp file:** Easily fixable, wastes I/O
2. **Parquet column lookup:** Minor but easy optimization
3. **Parallel gzip:** Moderate benefit, external dependency

**Priority of recommendations:**

| ID | Title | Priority | Impact |
|----|-------|----------|--------|
| Rec-1 | Avoid double temp file (Parquet) | High | 10-20% chunk time |
| Rec-3 | Parallel gzip (CSV) | Medium | 2-4x gzip speed |
| Rec-2 | Optimize column lookup (Parquet) | Low | Minor CPU |
| Rec-4 | Larger Parquet buffer | Low | Reduce call overhead |
| Rec-5 | Pool allocations | Skip | Minimal benefit |

**Overall assessment:** Stage 3 is not a major bottleneck. CSV is gzip-bound, Parquet is efficient. The main quick win is Rec-1 (eliminate double temp file for Parquet).
