# Context and Signal Handling

This document describes how the s3inv-index tool handles context cancellation and OS signals.

## Overview

The pipeline is designed to respond gracefully to cancellation requests, whether triggered by:
- OS signals (Ctrl+C / SIGINT, SIGTERM)
- Context cancellation from parent code
- Context timeout/deadline

When cancellation occurs, the pipeline:
1. Stops starting new work
2. Allows in-flight work to complete or abort cleanly
3. Cleans up temporary files
4. Returns an appropriate error

## Signal Handling

### CLI Behavior

The `s3inv-index build` command sets up signal handling at startup:

```go
ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
defer stop()
```

When a signal is received:
- `ctx.Done()` is closed
- All pipeline stages check for cancellation and stop
- A warning message is logged indicating cancellation
- The command exits with a non-zero status

### Handled Signals

| Signal | Trigger | Behavior |
|--------|---------|----------|
| SIGINT | Ctrl+C | Graceful shutdown |
| SIGTERM | kill command | Graceful shutdown |

## Context Propagation

The context is passed through all major pipeline stages:

```
CLI (signal.NotifyContext)
    │
    └─► Pipeline.Run(ctx, ...)
            │
            ├─► runIngestPhase(ctx, ...)
            │       │
            │       ├─► s3Client.FetchManifest(ctx, ...)
            │       ├─► Worker goroutines (check ctx.Done())
            │       └─► processChunkToBatch(ctx, ...)
            │               └─► s3Client.StreamObject(ctx, ...)
            │
            └─► runMergeBuildPhase(ctx, ...)
                    └─► builder.AddAllWithContext(ctx, ...)
```

## Cancellation Points

### Ingest Phase

Context is checked at multiple points during chunk processing:

1. **Job sender goroutine**: Stops sending jobs when context is cancelled
2. **Worker goroutines**: Check ctx.Done() before each job and when sending results
3. **Chunk parsing loop**: Checks ctx.Done() periodically during row iteration
4. **Aggregation loop**: Checks ctx.Done() on each batch received

### Merge/Build Phase

1. **Phase start**: Checks ctx.Done() before beginning
2. **Row iteration**: AddAllWithContext checks ctx.Done() every 1000 rows

## Resource Cleanup

### Temporary Files

On cancellation or error:
- All temporary run files are deleted
- The temporary directory is removed (if created by the pipeline)

The cleanup is handled by the `Pipeline.cleanup()` method called in the deferred function.

### Open Connections

- S3 streams are automatically closed when their readers are closed
- InventoryReader.Close() is called via defer in all processing paths

### Partial Output

If cancellation occurs during the merge phase:
- The partial index directory is cleaned up via `builder.cleanup()`
- No incomplete index is left on disk

## Error Handling

### Context Errors

When cancellation occurs, functions return the context error:
- `context.Canceled` for explicit cancellation
- `context.DeadlineExceeded` for timeout

These errors are wrapped with phase information:
```
ingest phase: process chunk: context canceled
merge/build phase: build index: context canceled
```

### Logging

Cancellation is logged at the warning level:
```
{"level":"warn","msg":"pipeline cancelled during ingest phase"}
{"level":"warn","msg":"build cancelled by user"}
```

## Integrating with Other Applications

When using the pipeline as a library:

```go
import (
    "context"
    "time"

    "github.com/eunmann/s3-inv-db/pkg/extsort"
    "github.com/eunmann/s3-inv-db/pkg/s3fetch"
)

func buildIndex(parentCtx context.Context) error {
    // Create a child context with timeout
    ctx, cancel := context.WithTimeout(parentCtx, 30*time.Minute)
    defer cancel()

    client, err := s3fetch.NewClient(ctx)
    if err != nil {
        return err
    }

    pipeline := extsort.NewPipeline(extsort.DefaultConfig(), client)
    _, err = pipeline.Run(ctx, manifestURI, outDir)
    return err
}
```

### Using Timeouts

```go
// Cancel if build takes longer than 1 hour
ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
defer cancel()

_, err := pipeline.Run(ctx, manifestURI, outDir)
if errors.Is(err, context.DeadlineExceeded) {
    log.Println("Build timed out")
}
```

### Using Deadlines

```go
// Cancel at a specific time
deadline := time.Date(2024, 1, 1, 6, 0, 0, 0, time.UTC)
ctx, cancel := context.WithDeadline(context.Background(), deadline)
defer cancel()

_, err := pipeline.Run(ctx, manifestURI, outDir)
```

## Manual Testing

To verify signal handling:

1. Start a build:
   ```
   s3inv-index build --s3-manifest s3://bucket/path/manifest.json --out ./index
   ```

2. While running, press Ctrl+C

3. Expected behavior:
   - Log message: `"msg":"pipeline cancelled during ingest phase"` (or merge phase)
   - Log message: `"msg":"build cancelled by user"`
   - Clean exit (no stack traces)
   - Temporary files cleaned up

## Implementation Details

### Check Intervals

To avoid excessive overhead, context is not checked on every operation:
- Aggregation loop: Every batch (typically 100k+ objects per batch)
- Index building: Every 1000 rows

This provides responsive cancellation while minimizing overhead.

### Goroutine Cleanup

All worker goroutines have clear exit paths:
- `ctx.Done()` channel select
- Input channel closure
- WaitGroup coordination ensures all workers exit before function returns
