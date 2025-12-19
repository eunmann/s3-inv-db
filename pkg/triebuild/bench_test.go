package triebuild

import (
	"fmt"
	"os"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/benchutil"
)

/*
Benchmark Categories for Trie Building:

1. BenchmarkTrieBuild - Tests trie construction from sorted keys
   - Measures: prefixes/sec, memory allocations
   - Tree shapes: deep_narrow, wide_shallow, balanced, s3_realistic, wide_single_level
   - Sizes: 1k, 10k, 100k objects

2. BenchmarkTrieBuild_LargeScale - 1M scale test (gated)
   - Run separately with: go test -bench=LargeScale -benchtime=1x

3. BenchmarkExtractPrefixes - Tests prefix extraction from keys
   - Measures: ns/op for path parsing
*/

// Standard sizes for quick benchmarks
var treeSizes = []int{1000, 10000, 100000}

// Tree shapes for benchmarking
var treeShapes = []string{"deep_narrow", "wide_shallow", "balanced", "s3_realistic", "wide_single_level"}

// sortKeys sorts keys lexicographically (required for trie builder)
func sortKeys(keys []string) []string {
	sorted := make([]string, len(keys))
	copy(sorted, keys)
	// Simple insertion sort for benchmark setup
	for i := 1; i < len(sorted); i++ {
		key := sorted[i]
		j := i - 1
		for j >= 0 && sorted[j] > key {
			sorted[j+1] = sorted[j]
			j--
		}
		sorted[j+1] = key
	}
	return sorted
}

func keysToSizes(keys []string) []uint64 {
	sizes := make([]uint64, len(keys))
	for i := range keys {
		sizes[i] = uint64((i%1000 + 1) * 100)
	}
	return sizes
}

// BenchmarkTrieBuild benchmarks trie construction from sorted keys.
func BenchmarkTrieBuild(b *testing.B) {
	for _, shape := range treeShapes {
		for _, size := range treeSizes {
			name := fmt.Sprintf("%s/size=%d", shape, size)

			// Generate and sort keys once
			keys := sortKeys(benchutil.GenerateKeys(size, shape))
			sizes := keysToSizes(keys)

			b.Run(name, func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					_, err := BuildFromKeys(keys, sizes)
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

// BenchmarkTrieBuild_LargeScale tests at 1M scale.
// Run separately with: go test -bench=LargeScale -benchtime=1x ./pkg/triebuild/...
func BenchmarkTrieBuild_LargeScale(b *testing.B) {
	if os.Getenv("S3INV_LONG_BENCH") == "" {
		b.Skip("set S3INV_LONG_BENCH=1 to run large scale benchmark")
	}

	size := 1000000
	keys := sortKeys(benchutil.GenerateKeys(size, "s3_realistic"))
	sizes := keysToSizes(keys)

	b.Run("s3_realistic/size=1000000", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			result, err := BuildFromKeys(keys, sizes)
			if err != nil {
				b.Fatal(err)
			}
			if i == b.N-1 {
				b.Logf("nodes=%d", len(result.Nodes))
			}
		}
	})
}

// BenchmarkExtractPrefixes benchmarks prefix extraction from keys.
func BenchmarkExtractPrefixes(b *testing.B) {
	keys := []struct {
		name string
		key  string
	}{
		{"shallow", "a/b/file.txt"},
		{"medium", "data/2024/01/15/user123/file.json"},
		{"deep", "a/b/c/d/e/f/g/h/i/j/k/l/m/n/o/file.txt"},
	}

	for _, k := range keys {
		b.Run(k.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_ = extractPrefixes(k.key)
			}
		})
	}
}

// BenchmarkTrieBuild_Scaling runs scaling tests for different sizes (gated).
func BenchmarkTrieBuild_Scaling(b *testing.B) {
	if os.Getenv("S3INV_LONG_BENCH") == "" {
		b.Skip("set S3INV_LONG_BENCH=1 to run scaling benchmark")
	}

	sizes := []int{10000, 50000, 100000, 250000, 500000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("s3_realistic/size=%d", size), func(b *testing.B) {
			keys := sortKeys(benchutil.GenerateKeys(size, "s3_realistic"))
			sizes := keysToSizes(keys)

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				result, err := BuildFromKeys(keys, sizes)
				if err != nil {
					b.Fatal(err)
				}
				if i == b.N-1 {
					b.Logf("objects=%d nodes=%d", size, len(result.Nodes))
				}
			}
		})
	}
}
