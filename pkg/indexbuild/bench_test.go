package indexbuild

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/benchutil"
	"github.com/eunmann/s3-inv-db/pkg/sqliteagg"
)

/*
Benchmark Categories for Index Build:

1. BenchmarkBuildFromSQLite - End-to-end index build from SQLite
   - Measures: total build time, prefixes/sec
   - Sizes: 10k, 100k objects

2. BenchmarkBuildFromSQLite_Scaling - Scaling tests (gated)
   - Sizes: 10k to 500k objects
   - Run with: S3INV_LONG_BENCH=1 go test -bench=Scaling
*/

// BenchmarkBuildFromSQLite benchmarks end-to-end index building.
func BenchmarkBuildFromSQLite(b *testing.B) {
	sizes := []int{10000, 100000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("objects=%d", size), func(b *testing.B) {
			benchmarkBuildFromSQLite(b, size)
		})
	}
}

func benchmarkBuildFromSQLite(b *testing.B, numObjects int) {
	b.Helper()

	// Setup: populate SQLite database once
	setup := setupSQLiteAggregator(b, numObjects)
	setup.Close() // Close aggregator, keep DB file for index building

	cfg := sqliteagg.DefaultConfig(setup.DBPath)
	cfg.Synchronous = "OFF"
	tmpDir := filepath.Dir(setup.DBPath)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		outDir := filepath.Join(tmpDir, fmt.Sprintf("index-%d", i))
		b.StartTimer()

		buildCfg := SQLiteConfig{
			OutDir:    outDir,
			DBPath:    setup.DBPath,
			SQLiteCfg: cfg,
		}

		if err := BuildFromSQLite(buildCfg); err != nil {
			b.Fatalf("BuildFromSQLite failed: %v", err)
		}

		if i == b.N-1 {
			// Log metrics on last iteration
			b.StopTimer()
			var indexSize int64
			filepath.Walk(outDir, func(path string, info os.FileInfo, err error) error {
				if err == nil && !info.IsDir() {
					indexSize += info.Size()
				}
				return nil
			})
			b.Logf("objects=%d index_size_mb=%.2f", numObjects, float64(indexSize)/(1024*1024))
		}
	}
}

// BenchmarkBuildFromSQLite_Scaling runs larger scale tests (gated).
func BenchmarkBuildFromSQLite_Scaling(b *testing.B) {
	benchutil.SkipIfNoLongBench(b)

	for _, size := range benchutil.ScalingSizes {
		b.Run(fmt.Sprintf("objects=%d", size), func(b *testing.B) {
			benchmarkBuildFromSQLite(b, size)
		})
	}
}

// BenchmarkBuildPhases benchmarks individual build phases.
func BenchmarkBuildPhases(b *testing.B) {
	numObjects := 50000

	// Setup: populate SQLite database
	setup := setupSQLiteAggregator(b, numObjects)
	defer setup.Close()

	b.Logf("setup: objects=%d prefixes=%d", numObjects, setup.PrefixCount)

	// Benchmark trie building phase only
	// (Index writing is tested via BenchmarkBuildFromSQLite since writeIndex is unexported)
	b.Run("trie_build", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			result, err := sqliteagg.BuildTrieFromSQLite(setup.Agg)
			if err != nil {
				b.Fatalf("BuildTrieFromSQLite failed: %v", err)
			}
			if i == b.N-1 {
				b.Logf("nodes=%d max_depth=%d", len(result.Nodes), result.MaxDepth)
			}
		}
	})
}

// BenchmarkTreeShapes benchmarks build performance for different tree structures.
func BenchmarkTreeShapes(b *testing.B) {
	numObjects := 50000

	for _, shape := range benchutil.TreeShapes {
		b.Run(shape, func(b *testing.B) {
			// Generate keys for this shape and setup SQLite
			keys := benchutil.GenerateKeys(numObjects, shape)
			setup := setupSQLiteFromKeys(b, keys)
			setup.Close() // Close aggregator, keep DB file

			cfg := sqliteagg.DefaultConfig(setup.DBPath)
			cfg.Synchronous = "OFF"
			tmpDir := filepath.Dir(setup.DBPath)

			b.Logf("shape=%s objects=%d prefixes=%d", shape, numObjects, setup.PrefixCount)

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				b.StopTimer()
				outDir := filepath.Join(tmpDir, fmt.Sprintf("index-%d", i))
				b.StartTimer()

				buildCfg := SQLiteConfig{
					OutDir:    outDir,
					DBPath:    setup.DBPath,
					SQLiteCfg: cfg,
				}

				if err := BuildFromSQLite(buildCfg); err != nil {
					b.Fatalf("BuildFromSQLite failed: %v", err)
				}
			}
		})
	}
}
