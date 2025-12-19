package sqliteagg

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/benchutil"
	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

/*
Benchmark Categories for SQLite Aggregation:

1. BenchmarkAggregate - Tests SQLite aggregator performance
   - Measures: objects/sec, prefixes generated, DB size
   - Sizes: 10k, 100k objects (quick dev benchmarks)

2. BenchmarkAggregate_Scaling - Scaling tests (gated)
   - Sizes: 10k to 500k objects
   - Run with: S3INV_LONG_BENCH=1 go test -bench=Scaling

3. BenchmarkBuildTrieFromSQLite - Tests trie building from SQLite
   - Measures: prefixes/sec
   - Uses pre-populated SQLite databases
*/

// BenchmarkAggregate benchmarks the SQLite aggregator.
func BenchmarkAggregate(b *testing.B) {
	sizes := []int{10000, 100000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("objects=%d", size), func(b *testing.B) {
			benchmarkAggregate(b, size)
		})
	}
}

func benchmarkAggregate(b *testing.B, numObjects int) {
	b.Helper()

	// Generate test data once
	gen := benchutil.NewGenerator(benchutil.S3RealisticConfig(numObjects))
	objects := gen.Generate()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		b.StopTimer()

		// Setup: create temp directory and DB
		tmpDir := b.TempDir()
		dbPath := filepath.Join(tmpDir, "prefix-agg.db")

		b.StartTimer()

		// Create aggregator
		cfg := DefaultConfig(dbPath)
		cfg.Synchronous = "OFF" // Faster for benchmarks
		agg, err := Open(cfg)
		if err != nil {
			b.Fatalf("Open failed: %v", err)
		}

		// Begin chunk
		if err := agg.BeginChunk(); err != nil {
			b.Fatalf("BeginChunk failed: %v", err)
		}

		// Add all objects
		for _, obj := range objects {
			if err := agg.AddObject(obj.Key, obj.Size, obj.TierID); err != nil {
				b.Fatalf("AddObject failed: %v", err)
			}
		}

		// Commit
		if err := agg.MarkChunkDone("bench-chunk"); err != nil {
			b.Fatalf("MarkChunkDone failed: %v", err)
		}
		if err := agg.Commit(); err != nil {
			b.Fatalf("Commit failed: %v", err)
		}

		b.StopTimer()

		// Log metrics on last iteration
		if i == b.N-1 {
			prefixCount, _ := agg.PrefixCount()
			agg.Close()

			// Get DB file size
			fi, _ := os.Stat(dbPath)
			dbSize := int64(0)
			if fi != nil {
				dbSize = fi.Size()
			}

			b.Logf("objects=%d prefixes=%d db_size_mb=%.2f",
				numObjects, prefixCount, float64(dbSize)/(1024*1024))
		} else {
			agg.Close()
		}
	}
}

// BenchmarkAggregate_Scaling runs larger scale tests (gated).
func BenchmarkAggregate_Scaling(b *testing.B) {
	if os.Getenv("S3INV_LONG_BENCH") == "" {
		b.Skip("set S3INV_LONG_BENCH=1 to run scaling benchmark")
	}

	sizes := []int{10000, 50000, 100000, 250000, 500000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("objects=%d", size), func(b *testing.B) {
			benchmarkAggregate(b, size)
		})
	}
}

// BenchmarkBuildTrieFromSQLite benchmarks trie building from SQLite.
func BenchmarkBuildTrieFromSQLite(b *testing.B) {
	sizes := []int{10000, 100000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("objects=%d", size), func(b *testing.B) {
			benchmarkBuildTrieFromSQLite(b, size)
		})
	}
}

func benchmarkBuildTrieFromSQLite(b *testing.B, numObjects int) {
	b.Helper()

	// Setup: populate SQLite database once
	tmpDir := b.TempDir()
	dbPath := filepath.Join(tmpDir, "prefix-agg.db")

	gen := benchutil.NewGenerator(benchutil.S3RealisticConfig(numObjects))
	objects := gen.Generate()

	cfg := DefaultConfig(dbPath)
	cfg.Synchronous = "OFF"
	agg, err := Open(cfg)
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

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		result, err := BuildTrieFromSQLite(agg)
		if err != nil {
			b.Fatalf("BuildTrieFromSQLite failed: %v", err)
		}

		if i == b.N-1 {
			b.Logf("result: nodes=%d max_depth=%d tiers=%v",
				len(result.Nodes), result.MaxDepth, result.PresentTiers)
		}
	}

	b.StopTimer()
	agg.Close()
}

// BenchmarkBuildTrieFromSQLite_Scaling runs larger scale tests (gated).
func BenchmarkBuildTrieFromSQLite_Scaling(b *testing.B) {
	if os.Getenv("S3INV_LONG_BENCH") == "" {
		b.Skip("set S3INV_LONG_BENCH=1 to run scaling benchmark")
	}

	sizes := []int{10000, 50000, 100000, 250000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("objects=%d", size), func(b *testing.B) {
			benchmarkBuildTrieFromSQLite(b, size)
		})
	}
}

// BenchmarkAddObject benchmarks the per-object insertion cost.
func BenchmarkAddObject(b *testing.B) {
	tmpDir := b.TempDir()
	dbPath := filepath.Join(tmpDir, "prefix-agg.db")

	cfg := DefaultConfig(dbPath)
	cfg.Synchronous = "OFF"
	agg, err := Open(cfg)
	if err != nil {
		b.Fatalf("Open failed: %v", err)
	}
	defer agg.Close()

	if err := agg.BeginChunk(); err != nil {
		b.Fatalf("BeginChunk failed: %v", err)
	}

	// Pre-generate keys to avoid measuring key generation
	gen := benchutil.NewGenerator(benchutil.S3RealisticConfig(b.N))
	objects := gen.Generate()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		obj := objects[i%len(objects)]
		if err := agg.AddObject(obj.Key, obj.Size, obj.TierID); err != nil {
			b.Fatalf("AddObject failed: %v", err)
		}
	}

	b.StopTimer()

	if err := agg.MarkChunkDone("bench-chunk"); err != nil {
		b.Fatalf("MarkChunkDone failed: %v", err)
	}
	if err := agg.Commit(); err != nil {
		b.Fatalf("Commit failed: %v", err)
	}
}

// BenchmarkIteratePrefixes benchmarks prefix iteration from SQLite.
func BenchmarkIteratePrefixes(b *testing.B) {
	sizes := []int{10000, 100000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("prefixes=%d", size), func(b *testing.B) {
			benchmarkIteratePrefixes(b, size)
		})
	}
}

func benchmarkIteratePrefixes(b *testing.B, numObjects int) {
	b.Helper()

	// Setup: populate SQLite database
	tmpDir := b.TempDir()
	dbPath := filepath.Join(tmpDir, "prefix-agg.db")

	gen := benchutil.NewGenerator(benchutil.S3RealisticConfig(numObjects))
	objects := gen.Generate()

	cfg := DefaultConfig(dbPath)
	cfg.Synchronous = "OFF"
	agg, err := Open(cfg)
	if err != nil {
		b.Fatalf("Open failed: %v", err)
	}
	defer agg.Close()

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

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		it, err := agg.IteratePrefixes()
		if err != nil {
			b.Fatalf("IteratePrefixes failed: %v", err)
		}

		count := 0
		for it.Next() {
			_ = it.Row()
			count++
		}
		if err := it.Err(); err != nil {
			b.Fatalf("Iterator error: %v", err)
		}
		it.Close()

		if i == b.N-1 {
			b.Logf("iterated %d prefixes", count)
		}
	}
}

// BenchmarkTierDistribution benchmarks aggregation with different tier distributions.
func BenchmarkTierDistribution(b *testing.B) {
	distributions := []struct {
		name  string
		tiers map[tiers.ID]float64
	}{
		{
			name:  "single_tier",
			tiers: map[tiers.ID]float64{tiers.Standard: 1.0},
		},
		{
			name: "mixed_tiers",
			tiers: map[tiers.ID]float64{
				tiers.Standard:   0.50,
				tiers.StandardIA: 0.20,
				tiers.GlacierIR:  0.15,
				tiers.ITFrequent: 0.10,
				tiers.ITArchive:  0.05,
			},
		},
		{
			name: "all_tiers",
			tiers: map[tiers.ID]float64{
				tiers.Standard:         0.20,
				tiers.StandardIA:       0.10,
				tiers.OneZoneIA:        0.05,
				tiers.GlacierIR:        0.10,
				tiers.GlacierFR:        0.05,
				tiers.DeepArchive:      0.05,
				tiers.ITFrequent:       0.15,
				tiers.ITInfrequent:     0.10,
				tiers.ITArchiveInstant: 0.10,
				tiers.ITArchive:        0.05,
				tiers.ITDeepArchive:    0.05,
			},
		},
	}

	numObjects := 50000

	for _, dist := range distributions {
		b.Run(dist.name, func(b *testing.B) {
			cfg := benchutil.S3RealisticConfig(numObjects)
			cfg.TierDistribution = dist.tiers

			gen := benchutil.NewGenerator(cfg)
			objects := gen.Generate()

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				b.StopTimer()

				tmpDir := b.TempDir()
				dbPath := filepath.Join(tmpDir, "prefix-agg.db")
				aggCfg := DefaultConfig(dbPath)
				aggCfg.Synchronous = "OFF"
				agg, err := Open(aggCfg)
				if err != nil {
					b.Fatalf("Open failed: %v", err)
				}

				b.StartTimer()

				if err := agg.BeginChunk(); err != nil {
					b.Fatalf("BeginChunk failed: %v", err)
				}
				for _, obj := range objects {
					if err := agg.AddObject(obj.Key, obj.Size, obj.TierID); err != nil {
						b.Fatalf("AddObject failed: %v", err)
					}
				}
				if err := agg.MarkChunkDone("bench-chunk"); err != nil {
					b.Fatalf("MarkChunkDone failed: %v", err)
				}
				if err := agg.Commit(); err != nil {
					b.Fatalf("Commit failed: %v", err)
				}

				b.StopTimer()
				agg.Close()
			}
		})
	}
}
