# External Sort Backend

The external sort (`extsort`) backend provides a pure-Go alternative to the SQLite-based indexing pipeline. It is designed for high-scale S3 inventory processing with bounded memory usage.

## Usage

Enable the extsort backend with the `--backend=extsort` flag:

```bash
s3inv-index build \
  --s3-manifest s3://bucket/inventory/manifest.json \
  --out /path/to/index \
  --backend extsort
```

## Architecture

The extsort pipeline processes S3 inventory data in four stages:

### Stage A: Streaming Ingest + Chunked Aggregation

- Streams CSV chunks from S3 in parallel
- Aggregates prefix statistics in memory using `Aggregator`
- Flushes to sorted run files when memory threshold is exceeded
- Uses fixed-size `[MaxTiers]uint64` arrays instead of maps for tier data

### Stage B: Sort and Run File Spilling

- When memory threshold (default: 256MB) is reached, aggregator drains to run file
- Run files are sorted by prefix (lexicographic order) before writing
- Binary format with 4MB buffered I/O for sequential throughput
- Run file format:
  - Header (16 bytes): Magic + Version + Count
  - Records (variable): PrefixLen + Prefix + Stats

### Stage C: K-Way Merge with Global Aggregation

- Opens all run files with a min-heap keyed by prefix
- Merges duplicate prefixes (same prefix from different runs)
- Outputs globally sorted, aggregated prefix stream
- Memory usage: O(k) where k = number of run files

### Stage D: Streaming Index Build

- Builds index files in a single streaming pass
- Uses depth stack algorithm to compute subtree ranges
- No in-memory trie - writes columnar arrays directly
- MPHF built after collecting all prefixes

## Performance Comparison

Benchmark results for 100,000 objects:

| Metric | Extsort | SQLite | Improvement |
|--------|---------|--------|-------------|
| Time | 1.14s | 3.4s | **3x faster** |
| Memory | 337MB | 1.3GB | **4x less** |
| Allocations | 4.2M | 13.3M | **3x fewer** |

### Phase Breakdown (Extsort)

| Phase | Duration | % Total |
|-------|----------|---------|
| Memory Aggregation | 147ms | 12.9% |
| Sort and Write Run | 270ms | 23.7% |
| Merge and Build Index | 723ms | 63.4% |

## Configuration

The pipeline can be configured via `extsort.Config`:

```go
cfg := extsort.Config{
    TempDir:               "/tmp",              // Directory for run files
    MemoryThreshold:       256 * 1024 * 1024,   // 256MB flush threshold
    RunFileBufferSize:     4 * 1024 * 1024,     // 4MB I/O buffers
    MaxDepth:              0,                    // 0 = unlimited
}
```

## Key Design Decisions

### Fixed-Size Tier Arrays

Instead of `map[TierID]uint64` for tier statistics, the pipeline uses `[MaxTiers]uint64` arrays. This eliminates map allocations in the hot path and enables more efficient memory layout.

### Binary Run File Format

Run files use a compact binary format optimized for sequential I/O:
- Little-endian encoding for cross-platform compatibility
- Variable-length prefix strings (no null terminators)
- Fixed-size tier arrays for predictable record sizes

### Streaming Index Construction

Unlike the SQLite backend which builds an in-memory trie, the extsort backend writes index files directly during the merge phase. This eliminates the memory overhead of the trie structure.

### sync.Pool for Object Reuse

`PrefixStats` objects are pooled and reused during aggregation to reduce allocation pressure. The pool is drained when flushing to run files.

## When to Use

Choose `--backend=extsort` when:
- Pure-Go deployment is required (no CGO)
- Processing very large inventories (100M+ objects)
- Memory is constrained
- Single-use builds (no incremental updates)

Choose `--backend=sqlite` (default) when:
- Incremental updates are needed
- Random access to aggregated data is required
- Debugging/inspection of intermediate state is useful

## Running Benchmarks

```bash
# Quick benchmark (10k, 100k objects)
go test -bench='BenchmarkExtsortEndToEnd' -benchtime=1x ./pkg/extsort/...

# Scaling benchmark (100k-1M objects, requires S3INV_LONG_BENCH=1)
S3INV_LONG_BENCH=1 go test -bench='BenchmarkExtsortEndToEnd_Scaling' -benchtime=1x ./pkg/extsort/...

# Compare with SQLite backend
go test -bench='BenchmarkEndToEnd/objects=100000' -benchtime=1x ./pkg/indexbuild/...
```
