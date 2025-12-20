package sqliteagg

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/benchutil"
	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

/*
SQLite Scan Performance Benchmarks

These benchmarks measure the hot path for reading prefix stats from SQLite,
which is critical for index building performance.

Key optimizations validated by these benchmarks:
1. Concrete-type scanning (no interface{}/any in hot loops)
2. Type-safe scan helpers (scanPrefixBaseStats, scanTierRow)
3. RowPtr() vs Row() to avoid struct copying
4. Narrow column projections (4 columns vs 28)

Run with:
  go test -bench=BenchmarkSQLiteScan -benchmem ./pkg/sqliteagg/...
*/

// setupSQLiteForScanBench creates a populated SQLite database for benchmarking.
func setupSQLiteForScanBench(b *testing.B, numObjects int) (*Aggregator, string) {
	b.Helper()

	tmpDir := b.TempDir()
	dbPath := filepath.Join(tmpDir, "prefix-agg.db")

	// Use MemoryAggregator to create the database
	memAgg := NewMemoryAggregator(DefaultMemoryAggregatorConfig(dbPath))

	gen := benchutil.NewGenerator(benchutil.S3RealisticConfig(numObjects))
	objects := gen.Generate()

	for _, obj := range objects {
		memAgg.AddObject(obj.Key, obj.Size, obj.TierID)
	}

	if err := memAgg.Finalize(); err != nil {
		b.Fatalf("Finalize failed: %v", err)
	}

	// Open for reading with read-optimized config
	cfg := ReadOptimizedConfig(dbPath)
	agg, err := Open(cfg)
	if err != nil {
		b.Fatalf("Open failed: %v", err)
	}

	return agg, dbPath
}

// BenchmarkSQLiteScan_PrefixIterator benchmarks full prefix iteration.
func BenchmarkSQLiteScan_PrefixIterator(b *testing.B) {
	sizes := []int{10000, 50000, 100000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("objects=%d", size), func(b *testing.B) {
			agg, _ := setupSQLiteForScanBench(b, size)
			defer agg.Close()

			prefixCount, _ := agg.PrefixCount()

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				iter, err := agg.IteratePrefixes()
				if err != nil {
					b.Fatalf("IteratePrefixes failed: %v", err)
				}

				count := uint64(0)
				for iter.Next() {
					_ = iter.RowPtr() // Use RowPtr to avoid copy
					count++
				}
				if iter.Err() != nil {
					b.Fatalf("iterator error: %v", iter.Err())
				}
				iter.Close()

				if count != prefixCount {
					b.Fatalf("count mismatch: got %d, want %d", count, prefixCount)
				}
			}

			b.ReportMetric(float64(prefixCount), "prefixes")
			b.ReportMetric(float64(prefixCount)/float64(b.Elapsed().Seconds())*float64(b.N), "prefixes/sec")
		})
	}
}

// BenchmarkSQLiteScan_RowVsRowPtr compares Row() vs RowPtr() overhead.
func BenchmarkSQLiteScan_RowVsRowPtr(b *testing.B) {
	agg, _ := setupSQLiteForScanBench(b, 50000)
	defer agg.Close()

	b.Run("Row_copy", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			iter, _ := agg.IteratePrefixes()
			for iter.Next() {
				row := iter.Row() // Copy 256 bytes
				_ = row.TotalBytes
			}
			iter.Close()
		}
	})

	b.Run("RowPtr_nocopy", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			iter, _ := agg.IteratePrefixes()
			for iter.Next() {
				row := iter.RowPtr() // No copy, pointer to internal struct
				_ = row.TotalBytes
			}
			iter.Close()
		}
	})
}

// BenchmarkSQLiteScan_NoTiers benchmarks iteration without tier data.
func BenchmarkSQLiteScan_NoTiers(b *testing.B) {
	agg, _ := setupSQLiteForScanBench(b, 50000)
	defer agg.Close()

	b.Run("with_tiers", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			iter, _ := agg.IteratePrefixes() // Loads tier data
			for iter.Next() {
				_ = iter.RowPtr()
			}
			iter.Close()
		}
	})

	b.Run("without_tiers", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			iter, _ := agg.IteratePrefixesForTiers([]tiers.ID{}) // Empty = no tier loading
			for iter.Next() {
				_ = iter.RowPtr()
			}
			iter.Close()
		}
	})
}

// BenchmarkSQLiteScan_TierDataLoad benchmarks tier data pre-loading.
func BenchmarkSQLiteScan_TierDataLoad(b *testing.B) {
	sizes := []int{10000, 50000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("objects=%d", size), func(b *testing.B) {
			agg, _ := setupSQLiteForScanBench(b, size)
			defer agg.Close()

			presentTiers, _ := agg.PresentTiers()

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				// This includes tier data loading + prefix iteration
				iter, err := agg.IteratePrefixesForTiers(presentTiers)
				if err != nil {
					b.Fatalf("IteratePrefixesForTiers failed: %v", err)
				}

				for iter.Next() {
					_ = iter.RowPtr()
				}
				iter.Close()
			}

			b.ReportMetric(float64(len(presentTiers)), "tiers")
		})
	}
}

// BenchmarkSQLiteScan_Scaling runs larger scale tests (gated).
func BenchmarkSQLiteScan_Scaling(b *testing.B) {
	benchutil.SkipIfNoLongBench(b)

	for _, size := range benchutil.ScalingSizes {
		b.Run(fmt.Sprintf("objects=%d", size), func(b *testing.B) {
			agg, _ := setupSQLiteForScanBench(b, size)
			defer agg.Close()

			prefixCount, _ := agg.PrefixCount()

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				iter, _ := agg.IteratePrefixes()
				for iter.Next() {
					_ = iter.RowPtr()
				}
				iter.Close()
			}

			b.ReportMetric(float64(prefixCount), "prefixes")
		})
	}
}
