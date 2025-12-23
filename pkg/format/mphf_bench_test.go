package format

import (
	"fmt"
	"math/rand/v2"
	"testing"
)

// generateRealisticPrefixes generates prefixes similar to real S3 paths.
// These are hierarchical paths like "bucket/dir1/dir2/file".
func generateRealisticPrefixes(n int) []string {
	prefixes := make([]string, n)

	// Create a mix of shallow and deep paths
	for i := range n {
		depth := 1 + (i % 5) // depths 1-5
		prefix := ""
		for d := range depth {
			// Generate path segment like "segment_123/"
			segmentNum := (i*7 + d*13) % 1000000
			prefix += fmt.Sprintf("seg%d/", segmentNum)
		}
		prefixes[i] = prefix
	}

	return prefixes
}

// ----------------------------------------------------------------------------
// BUILD BENCHMARKS
// ----------------------------------------------------------------------------

// BenchmarkMPHFBuild_1M benchmarks MPHF construction with 1 million keys.
// This is the primary benchmark for build performance analysis.
// Expected runtime: ~10-30s per iteration on typical hardware.
func BenchmarkMPHFBuild_1M(b *testing.B) {
	const numPrefixes = 1_000_000
	prefixes := generateRealisticPrefixes(numPrefixes)

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		dir := b.TempDir()
		builder, err := NewStreamingMPHFBuilder(dir)
		if err != nil {
			b.Fatalf("NewStreamingMPHFBuilder failed: %v", err)
		}

		for i, p := range prefixes {
			if err := builder.Add(p, uint64(i)); err != nil {
				builder.Close()
				b.Fatalf("Add failed: %v", err)
			}
		}

		if err := builder.Build(dir); err != nil {
			builder.Close()
			b.Fatalf("Build failed: %v", err)
		}
		builder.Close()
	}
}

// BenchmarkMPHFBuild_5M benchmarks MPHF construction with 5 million keys.
// Use this for stress testing and getting longer profiles.
// Expected runtime: ~60-120s per iteration.
func BenchmarkMPHFBuild_5M(b *testing.B) {
	const numPrefixes = 5_000_000
	prefixes := generateRealisticPrefixes(numPrefixes)

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		dir := b.TempDir()
		builder, err := NewStreamingMPHFBuilder(dir)
		if err != nil {
			b.Fatalf("NewStreamingMPHFBuilder failed: %v", err)
		}

		for i, p := range prefixes {
			if err := builder.Add(p, uint64(i)); err != nil {
				builder.Close()
				b.Fatalf("Add failed: %v", err)
			}
		}

		if err := builder.Build(dir); err != nil {
			builder.Close()
			b.Fatalf("Build failed: %v", err)
		}
		builder.Close()
	}
}

// BenchmarkBBHashNew_1M isolates the bbhash.New call (MPHF construction only).
// This excludes fingerprint computation and I/O.
func BenchmarkBBHashNew_1M(b *testing.B) {
	const numPrefixes = 1_000_000
	prefixes := generateRealisticPrefixes(numPrefixes)

	// Pre-compute hashes (this is what bbhash.New receives)
	hashes := make([]uint64, numPrefixes)
	for i, p := range prefixes {
		hashes[i] = hashString(p)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		dir := b.TempDir()
		builder, err := NewStreamingMPHFBuilder(dir)
		if err != nil {
			b.Fatalf("NewStreamingMPHFBuilder failed: %v", err)
		}

		// Only Add phase - to get hashes into the builder
		for i, p := range prefixes {
			if err := builder.Add(p, uint64(i)); err != nil {
				builder.Close()
				b.Fatalf("Add failed: %v", err)
			}
		}

		// Build includes bbhash.New
		if err := builder.Build(dir); err != nil {
			builder.Close()
			b.Fatalf("Build failed: %v", err)
		}
		builder.Close()
	}
}

// ----------------------------------------------------------------------------
// QUERY BENCHMARKS
// ----------------------------------------------------------------------------

// benchmarkMPHFQuery is a helper for MPHF query benchmarks with random access pattern.
func benchmarkMPHFQuery(b *testing.B, numPrefixes int) {
	b.Helper()
	prefixes := generateRealisticPrefixes(numPrefixes)

	// Build the MPHF once
	dir := b.TempDir()
	builder, err := NewStreamingMPHFBuilder(dir)
	if err != nil {
		b.Fatalf("NewStreamingMPHFBuilder failed: %v", err)
	}

	for i, p := range prefixes {
		if err := builder.Add(p, uint64(i)); err != nil {
			builder.Close()
			b.Fatalf("Add failed: %v", err)
		}
	}

	if err := builder.Build(dir); err != nil {
		builder.Close()
		b.Fatalf("Build failed: %v", err)
	}
	builder.Close()

	// Open for queries
	m, err := OpenMPHF(dir)
	if err != nil {
		b.Fatalf("OpenMPHF failed: %v", err)
	}
	defer m.Close()

	// Prepare random query order to avoid cache effects
	queryOrder := make([]int, numPrefixes)
	for i := range queryOrder {
		queryOrder[i] = i
	}
	rand.Shuffle(len(queryOrder), func(i, j int) {
		queryOrder[i], queryOrder[j] = queryOrder[j], queryOrder[i]
	})

	b.ResetTimer()
	b.ReportAllocs()

	for i := range b.N {
		idx := queryOrder[i%numPrefixes]
		_, _ = m.Lookup(prefixes[idx])
	}
}

// BenchmarkMPHFQuery_1M benchmarks lookup performance with 1M keys.
// This is the primary benchmark for query performance analysis.
func BenchmarkMPHFQuery_1M(b *testing.B) {
	benchmarkMPHFQuery(b, 1_000_000)
}

// BenchmarkMPHFQuery_5M benchmarks lookup with 5M keys for larger bitvectors.
func BenchmarkMPHFQuery_5M(b *testing.B) {
	benchmarkMPHFQuery(b, 5_000_000)
}

// BenchmarkMPHFQuerySequential_1M benchmarks sequential lookups (better cache).
func BenchmarkMPHFQuerySequential_1M(b *testing.B) {
	const numPrefixes = 1_000_000
	prefixes := generateRealisticPrefixes(numPrefixes)

	dir := b.TempDir()
	builder, err := NewStreamingMPHFBuilder(dir)
	if err != nil {
		b.Fatalf("NewStreamingMPHFBuilder failed: %v", err)
	}

	for i, p := range prefixes {
		if err := builder.Add(p, uint64(i)); err != nil {
			builder.Close()
			b.Fatalf("Add failed: %v", err)
		}
	}

	if err := builder.Build(dir); err != nil {
		builder.Close()
		b.Fatalf("Build failed: %v", err)
	}
	builder.Close()

	m, err := OpenMPHF(dir)
	if err != nil {
		b.Fatalf("OpenMPHF failed: %v", err)
	}
	defer m.Close()

	b.ResetTimer()
	b.ReportAllocs()

	for i := range b.N {
		_, _ = m.Lookup(prefixes[i%numPrefixes])
	}
}

// ----------------------------------------------------------------------------
// HASHING MICROBENCHMARKS
// ----------------------------------------------------------------------------

// BenchmarkHashString benchmarks the FNV-1a hashing function.
func BenchmarkHashString(b *testing.B) {
	prefixes := generateRealisticPrefixes(1000)

	b.ResetTimer()
	b.ReportAllocs()

	for i := range b.N {
		_ = hashString(prefixes[i%len(prefixes)])
	}
}

// BenchmarkHashBytes benchmarks the allocation-free hash variant.
func BenchmarkHashBytes(b *testing.B) {
	prefixes := generateRealisticPrefixes(1000)
	// Pre-convert to bytes
	prefixBytes := make([][]byte, len(prefixes))
	for i, p := range prefixes {
		prefixBytes[i] = []byte(p)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := range b.N {
		_ = hashBytes(prefixBytes[i%len(prefixBytes)])
	}
}

// BenchmarkComputeFingerprint benchmarks fingerprint computation.
func BenchmarkComputeFingerprint(b *testing.B) {
	prefixes := generateRealisticPrefixes(1000)

	b.ResetTimer()
	b.ReportAllocs()

	for i := range b.N {
		_ = computeFingerprint(prefixes[i%len(prefixes)])
	}
}

// BenchmarkComputeFingerprintBytes benchmarks allocation-free fingerprint.
func BenchmarkComputeFingerprintBytes(b *testing.B) {
	prefixes := generateRealisticPrefixes(1000)
	prefixBytes := make([][]byte, len(prefixes))
	for i, p := range prefixes {
		prefixBytes[i] = []byte(p)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := range b.N {
		_ = computeFingerprintBytes(prefixBytes[i%len(prefixBytes)])
	}
}

// BenchmarkBothHashes benchmarks computing both hashes (as Lookup does).
func BenchmarkBothHashes(b *testing.B) {
	prefixes := generateRealisticPrefixes(1000)

	b.ResetTimer()
	b.ReportAllocs()

	for i := range b.N {
		p := prefixes[i%len(prefixes)]
		_ = hashString(p)
		_ = computeFingerprint(p)
	}
}

// ----------------------------------------------------------------------------
// COMBINED BUILD+QUERY BENCHMARK (for PGO profiling)
// ----------------------------------------------------------------------------

// BenchmarkMPHFBuildAndQuery_1M is designed for PGO profile generation.
// It exercises both build and query paths in realistic proportions.
func BenchmarkMPHFBuildAndQuery_1M(b *testing.B) {
	const numPrefixes = 1_000_000
	const queriesPerBuild = 10_000_000 // 10M queries per build
	prefixes := generateRealisticPrefixes(numPrefixes)

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		// Build phase
		dir := b.TempDir()
		builder, err := NewStreamingMPHFBuilder(dir)
		if err != nil {
			b.Fatalf("NewStreamingMPHFBuilder failed: %v", err)
		}

		for i, p := range prefixes {
			if err := builder.Add(p, uint64(i)); err != nil {
				builder.Close()
				b.Fatalf("Add failed: %v", err)
			}
		}

		if err := builder.Build(dir); err != nil {
			builder.Close()
			b.Fatalf("Build failed: %v", err)
		}
		builder.Close()

		// Query phase
		m, err := OpenMPHF(dir)
		if err != nil {
			b.Fatalf("OpenMPHF failed: %v", err)
		}

		for i := range queriesPerBuild {
			_, _ = m.Lookup(prefixes[i%numPrefixes])
		}
		m.Close()
	}
}

// ----------------------------------------------------------------------------
// SCALING BENCHMARKS
// ----------------------------------------------------------------------------

// BenchmarkMPHFQueryScaling runs queries at different MPHF sizes.
// This helps identify if rank() cost scales with bitvector size.
func BenchmarkMPHFQueryScaling(b *testing.B) {
	sizes := []int{10_000, 100_000, 1_000_000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("N=%d", size), func(b *testing.B) {
			prefixes := generateRealisticPrefixes(size)

			dir := b.TempDir()
			builder, err := NewStreamingMPHFBuilder(dir)
			if err != nil {
				b.Fatalf("NewStreamingMPHFBuilder failed: %v", err)
			}

			for i, p := range prefixes {
				if err := builder.Add(p, uint64(i)); err != nil {
					builder.Close()
					b.Fatalf("Add failed: %v", err)
				}
			}

			if err := builder.Build(dir); err != nil {
				builder.Close()
				b.Fatalf("Build failed: %v", err)
			}
			builder.Close()

			m, err := OpenMPHF(dir)
			if err != nil {
				b.Fatalf("OpenMPHF failed: %v", err)
			}
			defer m.Close()

			b.ResetTimer()
			b.ReportAllocs()

			for i := range b.N {
				_, _ = m.Lookup(prefixes[i%size])
			}
		})
	}
}
