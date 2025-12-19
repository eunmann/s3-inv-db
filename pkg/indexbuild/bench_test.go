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
	tmpDir := b.TempDir()
	dbPath := filepath.Join(tmpDir, "prefix-agg.db")

	gen := benchutil.NewGenerator(benchutil.S3RealisticConfig(numObjects))
	objects := gen.Generate()

	cfg := sqliteagg.DefaultConfig(dbPath)
	cfg.Synchronous = "OFF"
	agg, err := sqliteagg.Open(cfg)
	if err != nil {
		b.Fatalf("Open failed: %v", err)
	}

	if err := agg.BeginChunk(); err != nil {
		b.Fatalf("BeginChunk failed: %v", err)
	}
	for _, obj := range objects {
		if err := agg.AddObject(obj.Key, obj.Size, obj.TierID); err != nil {
			b.Fatalf("AddObject failed: %v", err)
		}
	}
	if err := agg.MarkChunkDone("setup-chunk"); err != nil {
		b.Fatalf("MarkChunkDone failed: %v", err)
	}
	if err := agg.Commit(); err != nil {
		b.Fatalf("Commit failed: %v", err)
	}
	agg.Close()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		outDir := filepath.Join(tmpDir, fmt.Sprintf("index-%d", i))
		b.StartTimer()

		buildCfg := SQLiteConfig{
			OutDir:    outDir,
			DBPath:    dbPath,
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
	if os.Getenv("S3INV_LONG_BENCH") == "" {
		b.Skip("set S3INV_LONG_BENCH=1 to run scaling benchmark")
	}

	sizes := []int{10000, 50000, 100000, 250000, 500000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("objects=%d", size), func(b *testing.B) {
			benchmarkBuildFromSQLite(b, size)
		})
	}
}

// BenchmarkBuildPhases benchmarks individual build phases.
func BenchmarkBuildPhases(b *testing.B) {
	numObjects := 50000

	// Setup: populate SQLite database
	tmpDir := b.TempDir()
	dbPath := filepath.Join(tmpDir, "prefix-agg.db")

	gen := benchutil.NewGenerator(benchutil.S3RealisticConfig(numObjects))
	objects := gen.Generate()

	cfg := sqliteagg.DefaultConfig(dbPath)
	cfg.Synchronous = "OFF"
	agg, err := sqliteagg.Open(cfg)
	if err != nil {
		b.Fatalf("Open failed: %v", err)
	}

	if err := agg.BeginChunk(); err != nil {
		b.Fatalf("BeginChunk failed: %v", err)
	}
	for _, obj := range objects {
		if err := agg.AddObject(obj.Key, obj.Size, obj.TierID); err != nil {
			b.Fatalf("AddObject failed: %v", err)
		}
	}
	if err := agg.MarkChunkDone("setup-chunk"); err != nil {
		b.Fatalf("MarkChunkDone failed: %v", err)
	}
	if err := agg.Commit(); err != nil {
		b.Fatalf("Commit failed: %v", err)
	}

	prefixCount, _ := agg.PrefixCount()
	b.Logf("setup: objects=%d prefixes=%d", numObjects, prefixCount)

	// Benchmark trie building phase only
	// (Index writing is tested via BenchmarkBuildFromSQLite since writeIndex is unexported)
	b.Run("trie_build", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			result, err := sqliteagg.BuildTrieFromSQLite(agg)
			if err != nil {
				b.Fatalf("BuildTrieFromSQLite failed: %v", err)
			}
			if i == b.N-1 {
				b.Logf("nodes=%d max_depth=%d", len(result.Nodes), result.MaxDepth)
			}
		}
	})

	agg.Close()
}

// BenchmarkTreeShapes benchmarks build performance for different tree structures.
func BenchmarkTreeShapes(b *testing.B) {
	shapes := []string{"deep_narrow", "wide_shallow", "balanced", "s3_realistic", "wide_single_level"}
	numObjects := 50000

	for _, shape := range shapes {
		b.Run(shape, func(b *testing.B) {
			// Generate keys for this shape
			keys := benchutil.GenerateKeys(numObjects, shape)

			// Setup: populate SQLite database
			tmpDir := b.TempDir()
			dbPath := filepath.Join(tmpDir, "prefix-agg.db")

			cfg := sqliteagg.DefaultConfig(dbPath)
			cfg.Synchronous = "OFF"
			agg, err := sqliteagg.Open(cfg)
			if err != nil {
				b.Fatalf("Open failed: %v", err)
			}

			if err := agg.BeginChunk(); err != nil {
				b.Fatalf("BeginChunk failed: %v", err)
			}
			for i, key := range keys {
				size := uint64((i%1000 + 1) * 100)
				if err := agg.AddObject(key, size, 0); err != nil {
					b.Fatalf("AddObject failed: %v", err)
				}
			}
			if err := agg.MarkChunkDone("setup-chunk"); err != nil {
				b.Fatalf("MarkChunkDone failed: %v", err)
			}
			if err := agg.Commit(); err != nil {
				b.Fatalf("Commit failed: %v", err)
			}

			prefixCount, _ := agg.PrefixCount()
			agg.Close()

			b.Logf("shape=%s objects=%d prefixes=%d", shape, numObjects, prefixCount)

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				b.StopTimer()
				outDir := filepath.Join(tmpDir, fmt.Sprintf("index-%d", i))
				b.StartTimer()

				buildCfg := SQLiteConfig{
					OutDir:    outDir,
					DBPath:    dbPath,
					SQLiteCfg: cfg,
				}

				if err := BuildFromSQLite(buildCfg); err != nil {
					b.Fatalf("BuildFromSQLite failed: %v", err)
				}
			}
		})
	}
}
