package sqliteagg

import (
	"fmt"
	"os"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/benchutil"
)

// TestChunkAggregator runs all shared tests on all implementations.
func TestChunkAggregator(t *testing.T) {
	tests := []struct {
		name string
		fn   func(t *testing.T, factory ChunkAggregatorFactory)
	}{
		{"Basic", testChunkAggregatorBasic},
		{"Rollback", testChunkAggregatorRollback},
		{"Iterate", testChunkAggregatorIterate},
		{"Aggregation", testChunkAggregatorAggregation},
		{"PresentTiers", testChunkAggregatorPresentTiers},
		{"MultiChunk", testChunkAggregatorMultiChunk},
		{"DeepHierarchy", testChunkAggregatorDeepHierarchy},
		{"Empty", testChunkAggregatorEmpty},
		{"AllTiers", testChunkAggregatorAllTiers},
	}

	for _, impl := range aggregatorImplementations() {
		impl := impl // capture range variable
		t.Run(impl.name, func(t *testing.T) {
			for _, tt := range tests {
				tt := tt // capture range variable
				t.Run(tt.name, func(t *testing.T) {
					tt.fn(t, impl.factory)
				})
			}
		})
	}
}

// BenchmarkChunkAggregator runs comparative benchmarks on all implementations.
func BenchmarkChunkAggregator(b *testing.B) {
	sizes := []int{10000, 100000}

	for _, size := range sizes {
		gen := benchutil.NewGenerator(benchutil.S3RealisticConfig(size))
		objects := gen.Generate()

		for _, impl := range benchmarkAggregatorFactories() {
			impl := impl // capture range variable
			b.Run(fmt.Sprintf("objects=%d/%s", size, impl.name), func(b *testing.B) {
				benchmarkAggregatorIngest(b, impl.factory, objects)
			})
		}
	}
}

// BenchmarkChunkAggregator_Iterate benchmarks iteration performance.
func BenchmarkChunkAggregator_Iterate(b *testing.B) {
	sizes := []int{10000, 100000}

	for _, size := range sizes {
		for _, impl := range benchmarkAggregatorFactories() {
			impl := impl // capture range variable
			b.Run(fmt.Sprintf("objects=%d/%s", size, impl.name), func(b *testing.B) {
				benchmarkAggregatorIterate(b, impl.factory, size)
			})
		}
	}
}

// BenchmarkChunkAggregator_Scaling runs larger scale tests (gated).
func BenchmarkChunkAggregator_Scaling(b *testing.B) {
	if os.Getenv("S3INV_LONG_BENCH") == "" {
		b.Skip("set S3INV_LONG_BENCH=1 to run scaling benchmark")
	}

	sizes := []int{10000, 50000, 100000, 250000, 500000}

	for _, size := range sizes {
		gen := benchutil.NewGenerator(benchutil.S3RealisticConfig(size))
		objects := gen.Generate()

		for _, impl := range benchmarkAggregatorFactories() {
			impl := impl // capture range variable
			b.Run(fmt.Sprintf("objects=%d/%s", size, impl.name), func(b *testing.B) {
				benchmarkAggregatorIngest(b, impl.factory, objects)
			})
		}
	}
}
