# Logging Guidelines

This document describes the logging conventions for s3-inv-db.

## Log Levels

**Info** - Progress events that users should see:
- Pipeline start/complete
- Phase transitions (ingest, merge)
- Progress updates with ETA (every ~10% of work)
- Run file creation
- Final results

**Debug** - Technical details for troubleshooting:
- Individual chunk processing times
- Memory thresholds and decisions
- Internal timing breakdowns

**Error** - Failures that prevent completion:
- S3 access errors
- File I/O errors
- Invalid manifest data

## Log Structure

All log messages use zerolog with structured JSON output. Key fields:

### Phase Logging

```
{"level":"info","manifest_uri":"s3://...","memory_threshold_mb":256,"msg":"pipeline starting"}
{"level":"info","format":"Parquet","chunks":50,"msg":"inventory manifest loaded"}
{"level":"info","chunk":10,"total":50,"progress_pct":20,"objects":500000,"eta_ms":30000,"msg":"ingest progress"}
{"level":"info","run_files":3,"objects":1500000,"flushes":3,"duration_ms":45000,"msg":"ingest phase complete"}
{"level":"info","run_files":3,"msg":"merge phase starting"}
{"level":"info","prefixes":250000,"max_depth":12,"duration_ms":8000,"msg":"merge phase complete"}
{"level":"info","total_duration_ms":53000,"objects":1500000,"prefixes":250000,"msg":"pipeline complete"}
```

### Run File Events

```
{"level":"info","run_index":0,"prefixes":85000,"duration_ms":2500,"msg":"run file written"}
```

### Debug Events

```
{"level":"debug","chunk":5,"total":50,"chunk_ms":1200,"msg":"chunk processed"}
```

## Field Naming Conventions

Use snake_case for all field names:

| Field | Type | Description |
|-------|------|-------------|
| `chunk` | int | Current chunk number (1-indexed) |
| `total` | int | Total count (chunks, runs, etc.) |
| `objects` | int64 | Number of S3 objects processed |
| `prefixes` | uint64 | Number of unique prefixes |
| `progress_pct` | float64 | Percentage complete (0-100) |
| `eta_ms` | duration | Estimated time remaining |
| `duration_ms` | duration | Time taken for operation |
| `run_files` | int | Number of run files |
| `run_index` | int | Run file index (0-indexed) |
| `format` | string | Inventory format ("CSV" or "Parquet") |
| `max_depth` | uint32 | Maximum trie depth |

## What NOT to Log

- Per-row processing (too noisy)
- Full file paths (security, noise)
- Redundant start/end for short operations
- Internal algorithm state
- Anything inside hot loops

## Progress Reporting

Progress logs appear every ~10% of work during ingest:

```go
progressInterval := max(totalChunks/10, 1)
if chunkNum%progressInterval == 0 || chunkNum == totalChunks {
    log.Info().
        Int("chunk", chunkNum).
        Int("total", totalChunks).
        Float64("progress_pct", pct).
        Int64("objects", objectCount).
        Dur("eta_ms", remaining).
        Msg("ingest progress")
}
```

ETA calculation uses simple average:
```go
elapsed := time.Since(startTime)
avgPerChunk := elapsed / time.Duration(chunkNum)
remaining := time.Duration(totalChunks-chunkNum) * avgPerChunk
```

## Human-Readable Mode

With `--pretty-logs`, the console writer formats output for readability. Additional `_h` suffix fields are added for human-readable values when pretty mode is enabled:

- `duration_h`: "2m 30s"
- `bytes_h`: "1.5 GB"
- `throughput_h`: "150 MB/s"

## Testing Logs

Use `logging.SetLogger()` to inject test loggers:

```go
var buf bytes.Buffer
log := zerolog.New(&buf)
logging.SetLogger(log)
// ... run code ...
output := buf.String()
```
