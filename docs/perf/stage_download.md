# Stage 2: S3 Download (`stage_download`)

## Stage Summary

**What this stage does:** Downloads S3 inventory chunk files (CSV.gz or Parquet) from S3 using parallel range requests for high throughput. Each chunk is downloaded to a local temp file before parsing.

**Inputs:**
- S3 bucket and key from manifest
- Chunk files: typically 10-200 MB compressed each
- Inventory may have 1-1000+ chunk files

**Outputs:**
- `io.ReadCloser` for each chunk (backed by temp file)
- `DownloadResult` with timing statistics
- Temp file automatically cleaned up on close

---

## Control Flow and Data Flow

### Main Functions

```
Pipeline.runIngestPhase()
    └── spawn N chunkWorker goroutines
            └── chunkWorker()
                    └── processChunkToBatch()
                            └── s3Client.DownloadObject()
                                    └── Downloader.DownloadToReader()
                                            ├── os.CreateTemp() [create temp file]
                                            ├── manager.Download() [AWS SDK parallel download]
                                            └── tempFile.Seek(0) [rewind for reading]
```

### Concurrency Model

```
                    ┌──────────────────────────────────────────────────────────────┐
                    │                       runIngestPhase                          │
                    └──────────────────────────────────────────────────────────────┘
                                               │
                         ┌─────────────────────┼─────────────────────┐
                         │                     │                     │
                         ▼                     ▼                     ▼
                 ┌───────────────┐     ┌───────────────┐     ┌───────────────┐
                 │ chunkWorker 1 │     │ chunkWorker 2 │     │ chunkWorker N │
                 │ (goroutine)   │     │ (goroutine)   │     │ (goroutine)   │
                 └───────────────┘     └───────────────┘     └───────────────┘
                         │                     │                     │
                         ▼                     ▼                     ▼
                 ┌───────────────┐     ┌───────────────┐     ┌───────────────┐
                 │ AWS Download  │     │ AWS Download  │     │ AWS Download  │
                 │ Manager       │     │ Manager       │     │ Manager       │
                 │ (parallel     │     │ (parallel     │     │ (parallel     │
                 │  range GET)   │     │  range GET)   │     │  range GET)   │
                 └───────────────┘     └───────────────┘     └───────────────┘
```

**Two levels of parallelism:**
1. **Chunk-level:** N worker goroutines process different chunks concurrently
2. **Part-level:** Each worker uses AWS SDK Download Manager with M concurrent range requests

### Key Files and Line References

| File | Function | Lines | Purpose |
|------|----------|-------|---------|
| `s3fetch/downloader.go` | `DownloadToReader()` | 117-163 | Main download entry point |
| `s3fetch/downloader.go` | `NewDownloader()` | 63-94 | Configure AWS Download Manager |
| `s3fetch/downloader.go` | `tempFileReader` | 198-243 | Temp file wrapper with cleanup |
| `extsort/pipeline.go` | `chunkWorker()` | 467-497 | Worker goroutine loop |
| `extsort/pipeline.go` | `processChunkToBatch()` | 509-592 | Download + parse orchestration |

---

## Performance Analysis

### CPU Hotspots

**Current:** CPU is not the bottleneck in this stage. Most time is I/O-bound waiting for S3 response.

**Key operations:**
- AWS SDK request signing: O(1), ~microseconds
- Temp file creation: O(1), ~1ms
- File seek: O(1), ~microseconds

### I/O Hotspots

1. **S3 Network I/O:** Dominant cost. Downloading 100MB chunk over network.
2. **Temp File Write:** AWS Download Manager writes chunks to disk.
3. **Disk Seek:** After download completes, seek to beginning for parsing.

### Throughput Analysis

**AWS Download Manager Benefits:**
- Parallel range requests: splits large objects into `PartSize` chunks (default 16MB)
- Concurrent downloads: `Concurrency` parts downloaded in parallel (default: min(16, NumCPU))
- Connection reuse: HTTP/2 multiplexing when available

**Typical Performance (empirical):**
- Single 100MB file: ~1-3 seconds depending on S3 proximity
- With 8-16 concurrent parts: approaches network bandwidth limit
- Bottleneck shifts from latency to bandwidth

---

## Memory Analysis

### Allocations

| Allocation | Size | Lifetime |
|------------|------|----------|
| AWS SDK buffers | `PartSize * Concurrency` = 128-256 MB | Per-download |
| Buffer pool | 32KB per pooled buffer | Reused |
| Temp file path string | ~50 bytes | Per-download |
| `DownloadResult` struct | ~48 bytes | Returned to caller |

### Peak Memory During Download

**Per Worker:**
- AWS Download Manager buffer pool: `PartSize * Concurrency` = 16MB * 8 = 128MB
- This is managed by `manager.NewPooledBufferedWriterReadFromProvider`

**Total Pipeline Memory:**
- With N workers: N * 128MB = 256-1024 MB for download buffers alone
- Memory budget calculation in `runIngestPhase()` limits this (lines 282-301)

### Memory Budget Enforcement

```go
// From pipeline.go:282-301
const bytesPerWorkerInFlight = 8 * 1024 * 1024 // 8MB per worker
headroom := p.config.MemoryBudget.Total() - <other budgets>
maxWorkersFromBudget := int(headroom / bytesPerWorkerInFlight)
```

**Issue:** The 8MB estimate is too low. Actual per-worker memory is higher due to AWS SDK buffers.

---

## Concurrency & Parallelism

### Current Model

1. **Worker Pool:** N goroutines pull from `jobs` channel
2. **Per-Worker Download:** Each uses AWS Download Manager with M concurrent range requests
3. **Effective Parallelism:** N * M concurrent S3 range requests

### Configuration

```go
// Defaults from downloader.go:37-53
Concurrency: min(16, NumCPU)  // Parts per download
PartSize:    16 * 1024 * 1024 // 16MB per part

// From pipeline.go:274-277
numWorkers = p.config.S3DownloadConcurrency
if numWorkers <= 0 {
    numWorkers = runtime.NumCPU()
}
```

### Observations

1. **Over-parallelism Risk:** With NumCPU=16, could have 16 workers * 16 parts = 256 concurrent S3 requests.
   - S3 rate limits per prefix
   - Memory pressure from 256 * 16MB = 4GB buffers

2. **Under-utilization Risk:** If chunks are small (< 16MB), parallel parts don't help.

3. **Channel Buffering:**
   ```go
   jobs := make(chan chunkJob, numWorkers)       // Minimal buffer
   results := make(chan objectBatch, 2)          // Very small buffer
   ```
   - Good: Prevents excessive queuing of large batches
   - Risk: If aggregator is slow, workers may block waiting to send results

---

## I/O Patterns

### S3 Access Pattern

1. **Parallel Range GET:** AWS SDK issues multiple `GetObject` with `Range` header
2. **Part Size:** Default 16MB chunks
3. **Concurrency:** Up to 16 concurrent part downloads per worker

### Disk Access Pattern

1. **Temp File Creation:** Sequential write from S3 to temp file
2. **AWS SDK WriterAt:** Uses `WriteAt` for out-of-order part completion
3. **Post-Download Seek:** Single seek to position 0

### Observations

1. **Full Download Before Parse:** Current design downloads entire file to disk before parsing.
   - Pro: Simple, reliable, works with Parquet (needs random access)
   - Con: Cannot overlap download with parsing for CSV

2. **Temp File Cleanup:** `tempFileReader.Close()` removes file, but if process crashes, orphan files remain.

---

## Concrete Recommendations

### Rec-1: Accurate Per-Worker Memory Accounting

**Location:** `extsort/pipeline.go:282-301`

**Current behavior:**
```go
const bytesPerWorkerInFlight = 8 * 1024 * 1024 // 8MB per worker
```

**Issue:** Underestimates actual AWS SDK buffer usage (16MB * 8 parts = 128MB).

**Proposed change:**
```go
// Account for AWS Download Manager buffers per worker
// PartSize * Concurrency, plus some overhead
dlConfig := p.s3Client.DownloaderConfig()
bytesPerWorkerInFlight := int64(dlConfig.PartSize * dlConfig.Concurrency)
if bytesPerWorkerInFlight < 16*1024*1024 {
    bytesPerWorkerInFlight = 16 * 1024 * 1024 // Minimum 16MB
}
```

**Expected benefit:** More accurate memory budget prevents OOM under high parallelism.

**Risk level:** Low

**How to test:** Memory profiling under various worker counts; verify heap stays within budget.

---

### Rec-2: Dynamic Concurrency Based on File Size

**Location:** `s3fetch/downloader.go` or `extsort/pipeline.go`

**Current behavior:** Fixed part concurrency regardless of file size.

**Issue:** Small files (< 16MB) don't benefit from parallel parts. Large files (> 500MB) could use more.

**Proposed change:**
```go
func (d *Downloader) optimalConcurrencyForSize(size int64) int {
    minUsefulParts := size / d.config.PartSize
    if minUsefulParts < 2 {
        return 1 // Single-threaded for small files
    }
    if minUsefulParts > int64(d.config.Concurrency) {
        return d.config.Concurrency
    }
    return int(minUsefulParts)
}
```

**Expected benefit:** Reduce overhead for small files; maintain throughput for large.

**Risk level:** Low

**How to test:** Benchmark with various file sizes; measure latency and throughput.

---

### Rec-3: Streaming CSV Parse (Pipeline Download with Parse)

**Location:** `extsort/pipeline.go:509-592` and `s3fetch/downloader.go`

**Current behavior:** Download entire file, then parse. Two sequential phases.

**Issue:** For CSV files (which are streaming-parseable), we could overlap download with parsing.

**Proposed change (complex):**
```go
// Use io.Pipe to stream decompressed content while downloading
// AWS SDK writes to PipeWriter, gzip decompressor reads from PipeReader
// CSV parser reads from decompressor

// This requires changing AWS Download Manager usage:
// 1. Download to pipe instead of temp file
// 2. Handle backpressure (parser slower than download)
// 3. Fall back to temp file for Parquet (needs random access)
```

**Expected benefit:**
- Reduce per-chunk latency by overlapping download + parse
- Estimated 20-40% reduction in chunk processing time for CSV

**Risk level:** High
- AWS SDK Download Manager uses `io.WriterAt`, incompatible with pipes
- Requires custom streaming downloader or single-threaded `GetObject`
- Backpressure handling needed

**How to test:** Implement behind feature flag; compare end-to-end latency with/without streaming.

---

### Rec-4: Temp File Cleanup on Crash

**Location:** `s3fetch/downloader.go:126`

**Current behavior:** Temp files cleaned up on `Close()`, but not on process crash.

**Issue:** Orphan temp files can accumulate if pipeline crashes.

**Proposed change:**
```go
// Option 1: Use temp dir that OS cleans periodically
tempDir := filepath.Join(os.TempDir(), "s3inv-tmp")
os.MkdirAll(tempDir, 0755)

// Option 2: Cleanup old files on startup
func CleanupStaleTempFiles(dir string, maxAge time.Duration) {
    files, _ := filepath.Glob(filepath.Join(dir, "s3download-*.tmp"))
    for _, f := range files {
        info, _ := os.Stat(f)
        if time.Since(info.ModTime()) > maxAge {
            os.Remove(f)
        }
    }
}
```

**Expected benefit:** Prevent disk space leaks from crashed processes.

**Risk level:** Low

**How to test:** Integration test that simulates crash; verify cleanup on next run.

---

### Rec-5: Connection Pool Warming

**Location:** `s3fetch/client.go` or `s3fetch/downloader.go`

**Current behavior:** First requests establish new connections, incurring TLS handshake overhead.

**Issue:** Cold start penalty for first few chunks.

**Proposed change:**
```go
// Issue a small prefetch to warm connection pool before main downloads
func (d *Downloader) WarmConnectionPool(ctx context.Context, bucket string, sampleKeys []string) {
    for _, key := range sampleKeys[:min(3, len(sampleKeys))] {
        // Issue HEAD request to establish connections
        d.s3Client.HeadObject(ctx, &s3.HeadObjectInput{
            Bucket: aws.String(bucket),
            Key:    aws.String(key),
        })
    }
}
```

**Expected benefit:** Reduce latency on first few chunk downloads.

**Risk level:** Low

**How to test:** Measure first-chunk latency with/without warming.

---

## Summary

Stage 2 uses AWS SDK Download Manager effectively for high-throughput downloads. The main concerns are:

1. **Memory accounting mismatch** between budget calculation and actual AWS SDK usage
2. **No streaming** for CSV files (download-then-parse, not pipelined)
3. **Fixed concurrency** regardless of file size

**Priority of recommendations:**

| ID | Title | Priority | Impact |
|----|-------|----------|--------|
| Rec-1 | Accurate per-worker memory | High | Prevent OOM |
| Rec-3 | Streaming CSV parse | Medium | 20-40% latency reduction |
| Rec-2 | Dynamic concurrency | Low | Minor efficiency gain |
| Rec-4 | Temp file cleanup | Low | Disk space hygiene |
| Rec-5 | Connection warming | Low | First-chunk latency |

**Overall assessment:** Stage 2 is well-implemented. The AWS SDK handles most complexity. The main opportunity is overlapping download with parsing for CSV (Rec-3), but this is architecturally complex.
