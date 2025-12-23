# Performance

## Query Performance

### Lookup Latency

| Operation | Complexity | Typical Latency |
|-----------|------------|-----------------|
| `Lookup` | O(1) | ~200ns |
| `Stats` | O(1) | ~50ns |
| `TierBreakdown` | O(tiers) | ~500ns |
| `DescendantsAtDepth` | O(log n + k) | ~1-10μs |

Lookup performance is dominated by:
1. Two hash computations (FNV-1a + FNV-1)
2. BBHash query
3. Fingerprint comparison
4. Memory-mapped array access

### Scaling

Query latency is largely independent of index size due to O(1) MPHF lookups. The memory-mapped design means working set depends on access patterns, not total index size.

Tested up to 10M prefixes with consistent sub-microsecond lookups.

### Concurrent Access

The `Index` type is lock-free for reads. Concurrent query throughput scales linearly with CPU cores up to memory bandwidth limits.

## Build Performance

Build performance depends on:
- S3 download bandwidth
- CSV/Parquet parsing throughput
- Memory budget (affects flush frequency)
- Number of unique prefixes

### Memory Budget

The `--mem-budget` flag controls the trade-off between memory usage and build speed:

| Budget | Effect |
|--------|--------|
| Higher | Fewer flush cycles, faster builds |
| Lower | More flush cycles, lower memory usage |

Default: 50% of system RAM

Minimum recommended: 512MB

### Tuning Flags

```bash
# More workers for higher S3 throughput
s3inv-index build --workers 16 ...

# Limit depth to reduce prefix count
s3inv-index build --max-depth 5 ...

# Constrain memory usage
s3inv-index build --mem-budget 2GiB ...
```

### Build Phases

1. **Download & Parse**: Bottlenecked by S3 bandwidth and CPU for CSV parsing
2. **Aggregation**: Bounded by memory budget, flushes when threshold reached
3. **Merge**: I/O bound, reads/writes temporary files
4. **Index Build**: CPU bound for MPHF construction

## Running Benchmarks

### Query Benchmarks

```bash
# Run all indexread benchmarks
go test -bench=. -benchmem ./pkg/indexread/

# Run specific benchmark
go test -bench=BenchmarkLookup -benchmem ./pkg/indexread/

# Run with larger dataset (requires S3INV_LONG_BENCH=1)
S3INV_LONG_BENCH=1 go test -bench=. -benchmem ./pkg/indexread/
```

### MPHF Benchmarks

```bash
# Build performance at different scales
go test -bench=BenchmarkMPHFBuild -benchmem ./pkg/format/

# Query performance
go test -bench=BenchmarkMPHFQuery -benchmem ./pkg/format/

# Full pipeline benchmark
go test -bench=BenchmarkFingerprintPipeline -benchmem ./pkg/format/
```

### Memory Profiling

```bash
# Profile memory allocations
go test -bench=BenchmarkLookup -benchmem -memprofile=mem.out ./pkg/indexread/
go tool pprof mem.out
```

## Index Size

Index size scales approximately linearly with prefix count:

| Prefixes | Index Size |
|----------|------------|
| 100K | ~20MB |
| 1M | ~200MB |
| 10M | ~2GB |

Size breakdown per prefix:
- Columnar arrays: ~50 bytes
- MPHF + fingerprints: ~20 bytes
- Prefix strings: variable (avg ~30 bytes)
- Tier stats: ~192 bytes (if enabled)

## Memory Usage

### Build Phase

Memory usage during build:

```
Total Budget
├── Aggregator (50%)     Prefix map + statistics
├── Run Buffers (25%)    Temp file I/O buffers
├── Merge (15%)          K-way merge heap
└── Index Build (10%)    Final array construction
```

### Query Phase

Query-time memory is minimal:
- File descriptors for mmap'd files
- OS page cache for accessed regions
- Stack allocations for iterators

The OS manages page cache automatically. Frequently accessed index regions stay resident; cold regions are paged out.

## Bottleneck Analysis

### Build Bottlenecks

| Symptom | Likely Cause | Fix |
|---------|--------------|-----|
| Low CPU usage | S3 bandwidth limited | Increase `--workers` |
| High memory, slow | Too many unique prefixes | Use `--max-depth` |
| Lots of temp files | Memory budget too low | Increase `--mem-budget` |
| Slow final phase | Large MPHF build | Expected for >5M prefixes |

### Query Bottlenecks

| Symptom | Likely Cause | Fix |
|---------|--------------|-----|
| First query slow | Cold page cache | Expected, subsequent queries fast |
| Descendant queries slow | Large subtrees | Use iterator API |
| High memory usage | OS caching full index | Expected behavior, safe |

## Best Practices

1. **Size memory budget appropriately**: 50% of RAM works well for dedicated build servers
2. **Use `--max-depth` for large buckets**: Limits prefix explosion from deep hierarchies
3. **Pre-warm for latency-sensitive queries**: Read index files sequentially to populate page cache
4. **Use iterators for large result sets**: Avoids allocating million-element slices
5. **Monitor temp disk usage**: External sort needs 2-3x index size in temp space
