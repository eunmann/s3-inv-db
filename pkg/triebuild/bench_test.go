package triebuild

import (
	"fmt"
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

// BenchmarkTrieBuild benchmarks trie construction from sorted keys.
func BenchmarkTrieBuild(b *testing.B) {
	for _, shape := range benchutil.TreeShapes {
		for _, size := range benchutil.BenchmarkSizes {
			name := fmt.Sprintf("%s/size=%d", shape, size)

			// Generate and sort keys once
			keys := benchutil.SortKeys(benchutil.GenerateKeys(size, shape))
			sizes := benchutil.KeysToSizes(keys)

			b.Run(name, func(b *testing.B) {
				b.ReportAllocs()
				for range b.N {
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
	benchutil.SkipIfNoLongBench(b)

	size := 1000000
	keys := benchutil.SortKeys(benchutil.GenerateKeys(size, "s3_realistic"))
	sizes := benchutil.KeysToSizes(keys)

	b.Run("s3_realistic/size=1000000", func(b *testing.B) {
		b.ReportAllocs()
		for i := range b.N {
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
			for range b.N {
				_ = extractPrefixes(k.key)
			}
		})
	}
}

// BenchmarkTrieBuild_Scaling runs scaling tests for different sizes (gated).
func BenchmarkTrieBuild_Scaling(b *testing.B) {
	benchutil.SkipIfNoLongBench(b)

	for _, size := range benchutil.ScalingSizes {
		b.Run(fmt.Sprintf("s3_realistic/size=%d", size), func(b *testing.B) {
			keys := benchutil.SortKeys(benchutil.GenerateKeys(size, "s3_realistic"))
			sizes := benchutil.KeysToSizes(keys)

			b.ResetTimer()
			b.ReportAllocs()

			for i := range b.N {
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
