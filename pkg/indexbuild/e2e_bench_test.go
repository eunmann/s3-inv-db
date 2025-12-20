package indexbuild

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/eunmann/s3-inv-db/pkg/benchutil"
	"github.com/eunmann/s3-inv-db/pkg/sqliteagg"
)

/*
End-to-end profiling benchmarks for the complete build pipeline.

These benchmarks measure the full path:
  1. Generate synthetic S3 inventory data
  2. Aggregate prefixes in memory (MemoryAggregator)
  3. Write aggregated data to SQLite (sorted bulk INSERT + deferred index)
  4. Build trie from SQLite
  5. Write index files to disk

Run with CPU profiling:
  go test -bench='BenchmarkEndToEnd/objects=500000' -benchtime=1x \
      -cpuprofile=cpu.out ./pkg/indexbuild/...
  go tool pprof -top cpu.out

Run scaling tests:
  S3INV_LONG_BENCH=1 go test -bench='BenchmarkEndToEnd_Scaling' -benchtime=1x ./pkg/indexbuild/...
*/

// PhaseMetrics captures per-phase timing and I/O metrics.
type PhaseMetrics struct {
	Phase    string
	Duration time.Duration
	BytesIn  int64
	BytesOut int64
}

// BenchmarkEndToEnd benchmarks the complete build pipeline with phase timing.
func BenchmarkEndToEnd(b *testing.B) {
	sizes := []int{10000, 100000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("objects=%d", size), func(b *testing.B) {
			benchmarkEndToEnd(b, size)
		})
	}
}

// BenchmarkEndToEnd_Scaling runs larger scale end-to-end tests (gated).
func BenchmarkEndToEnd_Scaling(b *testing.B) {
	benchutil.SkipIfNoLongBench(b)

	sizes := []int{100000, 500000, 1000000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("objects=%d", size), func(b *testing.B) {
			benchmarkEndToEnd(b, size)
		})
	}
}

func benchmarkEndToEnd(b *testing.B, numObjects int) {
	b.Helper()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		b.StopTimer()

		tmpDir := b.TempDir()
		dbPath := filepath.Join(tmpDir, "prefix-agg.db")
		outDir := filepath.Join(tmpDir, "index")

		// Generate synthetic data
		gen := benchutil.NewGenerator(benchutil.S3RealisticConfig(numObjects))
		objects := gen.Generate()

		// Calculate input data size (for bandwidth reporting)
		var inputBytes int64
		for _, obj := range objects {
			inputBytes += int64(len(obj.Key)) + 8 // key + size
		}

		phases := make([]PhaseMetrics, 0, 5)

		b.StartTimer()
		totalStart := time.Now()

		// Phase 1: Memory Aggregation
		phase1Start := time.Now()
		cfg := sqliteagg.DefaultMemoryAggregatorConfig(dbPath)
		agg := sqliteagg.NewMemoryAggregator(cfg)

		for _, obj := range objects {
			agg.AddObject(obj.Key, obj.Size, obj.TierID)
		}
		phase1Duration := time.Since(phase1Start)
		phases = append(phases, PhaseMetrics{
			Phase:   "memory_aggregation",
			Duration: phase1Duration,
			BytesIn: inputBytes,
		})

		// Phase 2: SQLite Write (sorted bulk INSERT + deferred index)
		phase2Start := time.Now()
		if err := agg.Finalize(); err != nil {
			b.Fatalf("Finalize failed: %v", err)
		}
		phase2Duration := time.Since(phase2Start)

		// Get DB size
		dbInfo, _ := os.Stat(dbPath)
		dbSize := int64(0)
		if dbInfo != nil {
			dbSize = dbInfo.Size()
		}

		phases = append(phases, PhaseMetrics{
			Phase:    "sqlite_write",
			Duration: phase2Duration,
			BytesOut: dbSize,
		})

		prefixCount := agg.PrefixCount()

		// Phase 3: Build Trie from SQLite
		phase3Start := time.Now()

		readCfg := sqliteagg.DefaultConfig(dbPath)
		readCfg.Synchronous = "OFF"
		readAgg, err := sqliteagg.Open(readCfg)
		if err != nil {
			b.Fatalf("Open for read failed: %v", err)
		}

		result, err := sqliteagg.BuildTrieFromSQLite(readAgg)
		if err != nil {
			b.Fatalf("BuildTrieFromSQLite failed: %v", err)
		}
		phase3Duration := time.Since(phase3Start)
		phases = append(phases, PhaseMetrics{
			Phase:   "trie_build",
			Duration: phase3Duration,
		})
		readAgg.Close()

		// Phase 4: Write Index Files
		phase4Start := time.Now()

		buildCfg := SQLiteConfig{
			OutDir:    outDir,
			DBPath:    dbPath,
			SQLiteCfg: readCfg,
		}

		if err := BuildFromSQLite(buildCfg); err != nil {
			b.Fatalf("BuildFromSQLite failed: %v", err)
		}
		phase4Duration := time.Since(phase4Start)

		// Get index size
		var indexSize int64
		filepath.Walk(outDir, func(path string, info os.FileInfo, err error) error {
			if err == nil && !info.IsDir() {
				indexSize += info.Size()
			}
			return nil
		})

		phases = append(phases, PhaseMetrics{
			Phase:    "index_write",
			Duration: phase4Duration,
			BytesOut: indexSize,
		})

		totalDuration := time.Since(totalStart)
		b.StopTimer()

		// Report metrics on last iteration
		if i == b.N-1 {
			b.Logf("\n=== End-to-End Build Metrics (objects=%d) ===", numObjects)
			b.Logf("Prefixes: %d | Nodes: %d | Max Depth: %d",
				prefixCount, len(result.Nodes), result.MaxDepth)
			b.Logf("")
			b.Logf("Phase Breakdown:")
			b.Logf("%-20s %12s %8s %12s %12s",
				"Phase", "Duration", "% Total", "Bytes In", "Bytes Out")
			b.Logf("%-20s %12s %8s %12s %12s",
				"----", "--------", "-------", "--------", "---------")

			for _, p := range phases {
				pct := float64(p.Duration) / float64(totalDuration) * 100
				bytesIn := "-"
				bytesOut := "-"
				if p.BytesIn > 0 {
					bytesIn = formatBytes(p.BytesIn)
				}
				if p.BytesOut > 0 {
					bytesOut = formatBytes(p.BytesOut)
				}
				b.Logf("%-20s %12s %7.1f%% %12s %12s",
					p.Phase, p.Duration.Round(time.Millisecond), pct, bytesIn, bytesOut)
			}

			b.Logf("%-20s %12s %8s", "----", "--------", "-------")
			b.Logf("%-20s %12s %7.1f%%", "TOTAL", totalDuration.Round(time.Millisecond), 100.0)
			b.Logf("")
			b.Logf("Performance:")
			b.Logf("  Throughput: %.0f obj/s", float64(numObjects)/totalDuration.Seconds())
			b.Logf("  DB Size: %s | Index Size: %s", formatBytes(dbSize), formatBytes(indexSize))
			b.Logf("  Input Data: %s | Compression Ratio: %.1fx",
				formatBytes(inputBytes), float64(inputBytes)/float64(dbSize+indexSize))
		}
	}
}

// BenchmarkEndToEnd_PhaseBreakdown provides detailed per-phase benchmarks.
func BenchmarkEndToEnd_PhaseBreakdown(b *testing.B) {
	numObjects := 100000

	// Setup: Generate data once
	gen := benchutil.NewGenerator(benchutil.S3RealisticConfig(numObjects))
	objects := gen.Generate()

	b.Run("phase1_memory_aggregation", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			tmpDir := b.TempDir()
			dbPath := filepath.Join(tmpDir, "prefix-agg.db")
			cfg := sqliteagg.DefaultMemoryAggregatorConfig(dbPath)
			agg := sqliteagg.NewMemoryAggregator(cfg)
			b.StartTimer()

			for _, obj := range objects {
				agg.AddObject(obj.Key, obj.Size, obj.TierID)
			}
		}
	})

	b.Run("phase2_sqlite_write", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			tmpDir := b.TempDir()
			dbPath := filepath.Join(tmpDir, "prefix-agg.db")
			cfg := sqliteagg.DefaultMemoryAggregatorConfig(dbPath)
			agg := sqliteagg.NewMemoryAggregator(cfg)

			// Pre-aggregate in memory
			for _, obj := range objects {
				agg.AddObject(obj.Key, obj.Size, obj.TierID)
			}
			b.StartTimer()

			if err := agg.Finalize(); err != nil {
				b.Fatalf("Finalize failed: %v", err)
			}
		}
	})

	b.Run("phase3_trie_build", func(b *testing.B) {
		// Setup: Create populated SQLite once
		tmpDir := b.TempDir()
		dbPath := filepath.Join(tmpDir, "prefix-agg.db")
		cfg := sqliteagg.DefaultMemoryAggregatorConfig(dbPath)
		agg := sqliteagg.NewMemoryAggregator(cfg)
		for _, obj := range objects {
			agg.AddObject(obj.Key, obj.Size, obj.TierID)
		}
		if err := agg.Finalize(); err != nil {
			b.Fatalf("Finalize failed: %v", err)
		}

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			readCfg := sqliteagg.DefaultConfig(dbPath)
			readCfg.Synchronous = "OFF"
			readAgg, err := sqliteagg.Open(readCfg)
			if err != nil {
				b.Fatalf("Open failed: %v", err)
			}

			_, err = sqliteagg.BuildTrieFromSQLite(readAgg)
			if err != nil {
				b.Fatalf("BuildTrieFromSQLite failed: %v", err)
			}
			readAgg.Close()
		}
	})

	b.Run("phase4_index_write", func(b *testing.B) {
		// Setup: Create populated SQLite and build trie once
		tmpDir := b.TempDir()
		dbPath := filepath.Join(tmpDir, "prefix-agg.db")
		cfg := sqliteagg.DefaultMemoryAggregatorConfig(dbPath)
		agg := sqliteagg.NewMemoryAggregator(cfg)
		for _, obj := range objects {
			agg.AddObject(obj.Key, obj.Size, obj.TierID)
		}
		if err := agg.Finalize(); err != nil {
			b.Fatalf("Finalize failed: %v", err)
		}

		readCfg := sqliteagg.DefaultConfig(dbPath)
		readCfg.Synchronous = "OFF"

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			b.StopTimer()
			outDir := filepath.Join(tmpDir, fmt.Sprintf("index-%d", i))
			b.StartTimer()

			buildCfg := SQLiteConfig{
				OutDir:    outDir,
				DBPath:    dbPath,
				SQLiteCfg: readCfg,
			}

			if err := BuildFromSQLite(buildCfg); err != nil {
				b.Fatalf("BuildFromSQLite failed: %v", err)
			}
		}
	})
}

func formatBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}
