# MPHF Profile-Guided Optimization (PGO) Workflow

This document describes how to use Go's Profile-Guided Optimization (PGO) to improve MPHF performance through compiler optimizations.

## What is PGO?

Go 1.20+ supports PGO, which uses CPU profiles from real workloads to guide compiler optimizations:
- **Inlining decisions**: Inline hot functions even if they exceed normal size thresholds
- **Devirtualization**: Convert interface calls to direct calls when the concrete type is predictable
- **Branch prediction**: Optimize branch layout based on actual execution patterns
- **Register allocation**: Prioritize hot paths for register usage

## Expected PGO Benefits for MPHF

Based on the profiling results, PGO may help with:

| Area | Potential Benefit | Likelihood |
|------|------------------|------------|
| `bitVector.rank` inlining | May inline the popcount loop | Medium |
| `BBHash.Find` level loop | Better branch prediction | Low |
| `fast.Hash` chain | May inline all mixing functions | High |
| FNV hash calls | Already inlined, minimal benefit | Low |

**Reality check**: PGO won't fix the fundamental O(n) rank problem. It may provide 5-15% improvement, not the 10-100x needed.

---

## Step 1: Choose the Right Benchmark for PGO Profile

The best benchmark for PGO profile generation should:
1. Exercise both build and query paths
2. Run long enough for a representative profile (10+ seconds)
3. Use realistic data (key sizes, distributions)

### Recommended Benchmark

Use `BenchmarkMPHFBuildAndQuery_1M`:

```go
// BenchmarkMPHFBuildAndQuery_1M exercises both build and query paths.
// It builds a 1M-key MPHF, then performs 10M queries.
func BenchmarkMPHFBuildAndQuery_1M(b *testing.B)
```

This benchmark:
- Builds an MPHF with 1M realistic prefixes
- Performs 10M queries per build
- Matches production ratio (query-heavy workloads)

---

## Step 2: Generate PGO Profile

### Command

```bash
# Generate CPU profile from the combined benchmark
go test -run=^$ -bench=BenchmarkMPHFBuildAndQuery_1M \
    -cpuprofile=default.pgo \
    -benchtime=1x \
    ./pkg/format
```

**Note**: The profile must be named `default.pgo` and placed in the package's root directory for automatic PGO.

### Alternative: Query-Only Profile

If queries dominate your workload:

```bash
go test -run=^$ -bench=BenchmarkMPHFQuery_1M \
    -cpuprofile=default.pgo \
    -benchtime=30s \
    ./pkg/format
```

### Alternative: Build-Only Profile

If build time is critical:

```bash
go test -run=^$ -bench=BenchmarkMPHFBuild_1M \
    -cpuprofile=default.pgo \
    -benchtime=1x \
    ./pkg/format
```

---

## Step 3: Apply PGO to Build

### Option A: Automatic PGO (Recommended)

Place `default.pgo` in the main package directory:

```bash
# Move profile to main package
cp default.pgo cmd/s3inv/default.pgo

# Build with automatic PGO (Go 1.21+)
go build ./cmd/s3inv
```

Go will automatically detect and use `default.pgo`.

### Option B: Explicit PGO

```bash
# Build with explicit profile
go build -pgo=default.pgo ./cmd/s3inv

# Or with path to profile
go build -pgo=/path/to/mphf_combined.pgo ./cmd/s3inv
```

### Option C: PGO for Tests

```bash
# Run tests with PGO
go test -pgo=default.pgo ./...

# Run benchmarks with PGO to measure improvement
go test -run=^$ -bench=BenchmarkMPHFQuery_1M \
    -pgo=default.pgo \
    -benchtime=10s \
    ./pkg/format
```

---

## Step 4: Verify PGO is Active

```bash
# Check if PGO was used during build
go build -pgo=default.pgo -v ./cmd/s3inv 2>&1 | grep -i pgo

# The build should show: "using profile file: default.pgo"
```

---

## Step 5: Measure PGO Impact

### Benchmark Without PGO (Baseline)

```bash
go test -run=^$ -bench=BenchmarkMPHFQuery_1M \
    -benchtime=10s \
    ./pkg/format > baseline.txt
```

### Benchmark With PGO

```bash
go test -run=^$ -bench=BenchmarkMPHFQuery_1M \
    -pgo=default.pgo \
    -benchtime=10s \
    ./pkg/format > pgo.txt
```

### Compare Results

```bash
# Using benchstat
go install golang.org/x/perf/cmd/benchstat@latest
benchstat baseline.txt pgo.txt
```

---

## PGO Profile Maintenance

### When to Regenerate

1. **After significant code changes**: New hot paths may appear
2. **After changing key distributions**: Different access patterns
3. **Periodically**: Every few releases to capture evolving usage

### Profile Location Convention

```
cmd/s3inv/
├── main.go
└── default.pgo    # PGO profile for main binary

pkg/format/
└── default.pgo    # PGO profile for package tests (optional)
```

---

## Limitations and Caveats

### What PGO Won't Fix

1. **O(n) rank complexity**: No amount of optimization fixes algorithmic complexity
2. **Memory access patterns**: PGO doesn't change data layout
3. **External dependencies**: bbhash library code may not benefit as much

### Current Reality

Given that `bitVector.rank` is 89% of query time:
- Even a 20% PGO speedup on rank would only yield ~18% total improvement
- The real fix is replacing O(n) rank with O(1) rank9

### Recommendation

Use PGO as a "free" optimization, but prioritize:
1. Rank9 implementation in bbhash (would give 10-100x improvement)
2. Reducing number of rank calls (if possible)
3. PGO for incremental gains

---

## Quick Reference

```bash
# Full PGO workflow for MPHF optimization

# 1. Generate profile
go test -run=^$ -bench=BenchmarkMPHFBuildAndQuery_1M \
    -cpuprofile=default.pgo \
    -benchtime=1x \
    ./pkg/format

# 2. Build with PGO
go build -pgo=default.pgo ./cmd/s3inv

# 3. Verify improvement
go test -run=^$ -bench=BenchmarkMPHFQuery_1M \
    -pgo=default.pgo \
    ./pkg/format
```
