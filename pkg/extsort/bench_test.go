package extsort

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/eunmann/s3-inv-db/pkg/benchutil"
	"github.com/eunmann/s3-inv-db/pkg/indexread"
)

/*
External Sort Backend Benchmarks

These benchmarks compare the external sort backend with the SQLite backend
for the complete build pipeline:
  1. Generate synthetic S3 inventory data
  2. Aggregate prefixes (in-memory for extsort, memory+SQLite for SQLite backend)
  3. Sort and merge (external sort for extsort, trie build from SQLite for SQLite)
  4. Write index files to disk

Run quick comparison:
  go test -bench='BenchmarkExtsortEndToEnd/objects=100000' -benchtime=1x ./pkg/extsort/...

Run scaling tests:
  S3INV_LONG_BENCH=1 go test -bench='BenchmarkExtsortEndToEnd_Scaling' -benchtime=1x ./pkg/extsort/...
*/

// BenchmarkExtsortEndToEnd benchmarks the complete extsort pipeline.
func BenchmarkExtsortEndToEnd(b *testing.B) {
	sizes := []int{10000, 100000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("objects=%d", size), func(b *testing.B) {
			benchmarkExtsortEndToEnd(b, size)
		})
	}
}

// BenchmarkExtsortEndToEnd_Scaling runs larger scale tests (gated).
func BenchmarkExtsortEndToEnd_Scaling(b *testing.B) {
	benchutil.SkipIfNoLongBench(b)

	sizes := []int{100000, 500000, 1000000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("objects=%d", size), func(b *testing.B) {
			benchmarkExtsortEndToEnd(b, size)
		})
	}
}

func benchmarkExtsortEndToEnd(b *testing.B, numObjects int) {
	b.Helper()
	b.ReportAllocs()

	for i := range b.N {
		b.StopTimer()

		tmpDir := b.TempDir()
		outDir := filepath.Join(tmpDir, "index")
		runDir := filepath.Join(tmpDir, "runs")
		os.MkdirAll(runDir, 0o755)

		// Generate synthetic data
		gen := benchutil.NewGenerator(benchutil.S3RealisticConfig(numObjects))
		objects := gen.Generate()

		// Calculate input data size
		var inputBytes int64
		for _, obj := range objects {
			inputBytes += int64(len(obj.Key)) + 8
		}

		b.StartTimer()
		totalStart := time.Now()

		// Phase 1: Memory Aggregation
		phase1Start := time.Now()
		agg := NewAggregator(100000, 0)
		for _, obj := range objects {
			agg.AddObject(obj.Key, obj.Size, obj.TierID)
		}
		prefixCount := agg.PrefixCount()
		phase1Duration := time.Since(phase1Start)

		// Phase 2: Sort and write run file
		phase2Start := time.Now()
		rows := agg.Drain()
		runPath := filepath.Join(runDir, "run_0000.bin")
		writer, err := NewRunFileWriter(runPath, 4*1024*1024)
		if err != nil {
			b.Fatalf("create run file: %v", err)
		}
		if err := writer.WriteSorted(rows); err != nil {
			b.Fatalf("write sorted: %v", err)
		}
		if err := writer.Close(); err != nil {
			b.Fatalf("close run file: %v", err)
		}
		phase2Duration := time.Since(phase2Start)

		// Get run file size
		runInfo, _ := os.Stat(runPath)
		runSize := int64(0)
		if runInfo != nil {
			runSize = runInfo.Size()
		}

		// Phase 3: Merge and build index
		phase3Start := time.Now()
		merger, err := NewMergeIterator([]string{runPath}, 4*1024*1024)
		if err != nil {
			b.Fatalf("create merger: %v", err)
		}

		builder, err := NewIndexBuilder(outDir, "")
		if err != nil {
			merger.Close()
			b.Fatalf("create builder: %v", err)
		}

		if err := builder.AddAll(merger); err != nil {
			b.Fatalf("add all: %v", err)
		}
		merger.Close()

		if err := builder.Finalize(); err != nil {
			b.Fatalf("finalize: %v", err)
		}
		phase3Duration := time.Since(phase3Start)

		// Get index size
		var indexSize int64
		filepath.Walk(outDir, func(_ string, info os.FileInfo, err error) error {
			if err == nil && !info.IsDir() {
				indexSize += info.Size()
			}
			return nil
		})

		totalDuration := time.Since(totalStart)
		b.StopTimer()

		// Verify the index is readable
		idx, err := indexread.Open(outDir)
		if err != nil {
			b.Fatalf("open index: %v", err)
		}
		idxCount := idx.Count()
		idx.Close()

		// Report metrics on last iteration
		if i == b.N-1 {
			b.Logf("\n=== Extsort End-to-End Build Metrics (objects=%d) ===", numObjects)
			b.Logf("Prefixes: %d | Index Prefixes: %d", prefixCount, idxCount)
			b.Logf("")
			b.Logf("Phase Breakdown:")
			b.Logf("%-25s %12s %8s %12s",
				"Phase", "Duration", "% Total", "Size")
			b.Logf("%-25s %12s %8s %12s",
				"----", "--------", "-------", "----")

			b.Logf("%-25s %12s %7.1f%% %12s",
				"memory_aggregation", phase1Duration.Round(time.Millisecond),
				float64(phase1Duration)/float64(totalDuration)*100, "-")
			b.Logf("%-25s %12s %7.1f%% %12s",
				"sort_and_write_run", phase2Duration.Round(time.Millisecond),
				float64(phase2Duration)/float64(totalDuration)*100, formatBytes(runSize))
			b.Logf("%-25s %12s %7.1f%% %12s",
				"merge_and_build_index", phase3Duration.Round(time.Millisecond),
				float64(phase3Duration)/float64(totalDuration)*100, formatBytes(indexSize))

			b.Logf("%-25s %12s %8s", "----", "--------", "-------")
			b.Logf("%-25s %12s %7.1f%%", "TOTAL", totalDuration.Round(time.Millisecond), 100.0)
			b.Logf("")
			b.Logf("Performance:")
			b.Logf("  Throughput: %.0f obj/s", float64(numObjects)/totalDuration.Seconds())
			b.Logf("  Run Size: %s | Index Size: %s", formatBytes(runSize), formatBytes(indexSize))
			b.Logf("  Input Data: %s | Output Ratio: %.1fx",
				formatBytes(inputBytes), float64(inputBytes)/float64(runSize+indexSize))
		}
	}
}

// BenchmarkExtsortPhases provides detailed per-phase benchmarks.
func BenchmarkExtsortPhases(b *testing.B) {
	numObjects := 100000

	// Generate data once
	gen := benchutil.NewGenerator(benchutil.S3RealisticConfig(numObjects))
	objects := gen.Generate()

	b.Run("aggregation", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			agg := NewAggregator(100000, 0)
			for _, obj := range objects {
				agg.AddObject(obj.Key, obj.Size, obj.TierID)
			}
			agg.Drain()
		}
	})

	b.Run("sort_and_write_run", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			b.StopTimer()
			// Pre-aggregate
			agg := NewAggregator(100000, 0)
			for _, obj := range objects {
				agg.AddObject(obj.Key, obj.Size, obj.TierID)
			}
			rows := agg.Drain()

			tmpDir := b.TempDir()
			runPath := filepath.Join(tmpDir, "run.bin")
			b.StartTimer()

			writer, _ := NewRunFileWriter(runPath, 4*1024*1024)
			writer.WriteSorted(rows)
			writer.Close()
		}
	})

	b.Run("merge", func(b *testing.B) {
		b.ReportAllocs()
		// Create run file once
		tmpDir := b.TempDir()

		agg := NewAggregator(100000, 0)
		for _, obj := range objects {
			agg.AddObject(obj.Key, obj.Size, obj.TierID)
		}
		rows := agg.Drain()
		runPath := filepath.Join(tmpDir, "run.bin")
		writer, _ := NewRunFileWriter(runPath, 4*1024*1024)
		writer.WriteSorted(rows)
		writer.Close()

		b.ResetTimer()
		for range b.N {
			merger, _ := NewMergeIterator([]string{runPath}, 4*1024*1024)
			for {
				_, err := merger.Next()
				if err != nil {
					break
				}
			}
			merger.Close()
		}
	})

	b.Run("index_build", func(b *testing.B) {
		b.ReportAllocs()
		// Create run file once
		tmpDir := b.TempDir()

		agg := NewAggregator(100000, 0)
		for _, obj := range objects {
			agg.AddObject(obj.Key, obj.Size, obj.TierID)
		}
		rows := agg.Drain()
		runPath := filepath.Join(tmpDir, "run.bin")
		writer, _ := NewRunFileWriter(runPath, 4*1024*1024)
		writer.WriteSorted(rows)
		writer.Close()

		b.ResetTimer()
		for i := range b.N {
			b.StopTimer()
			outDir := filepath.Join(tmpDir, fmt.Sprintf("index-%d", i))
			b.StartTimer()

			merger, _ := NewMergeIterator([]string{runPath}, 4*1024*1024)
			builder, _ := NewIndexBuilder(outDir, "")
			builder.AddAll(merger)
			merger.Close()
			builder.Finalize()
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

// BenchmarkCompressedVsUncompressed compares run file write performance.
func BenchmarkCompressedVsUncompressed(b *testing.B) {
	numObjects := 50000

	// Generate data once
	gen := benchutil.NewGenerator(benchutil.S3RealisticConfig(numObjects))
	objects := gen.Generate()

	// Pre-aggregate rows
	agg := NewAggregator(100000, 0)
	for _, obj := range objects {
		agg.AddObject(obj.Key, obj.Size, obj.TierID)
	}
	rows := agg.Drain()
	SortPrefixRows(rows)

	b.Run("uncompressed_write", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			tmpDir := b.TempDir()
			path := filepath.Join(tmpDir, "run.bin")
			writer, _ := NewRunFileWriter(path, 4*1024*1024)
			writer.WriteAll(rows)
			writer.Close()
		}
	})

	b.Run("compressed_fastest", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			tmpDir := b.TempDir()
			path := filepath.Join(tmpDir, "run.crun")
			writer, _ := NewCompressedRunWriter(path, CompressedRunWriterOptions{
				BufferSize:       4 * 1024 * 1024,
				CompressionLevel: CompressionFastest,
			})
			writer.WriteAll(rows)
			writer.Close()
		}
	})

	b.Run("compressed_default", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			tmpDir := b.TempDir()
			path := filepath.Join(tmpDir, "run.crun")
			writer, _ := NewCompressedRunWriter(path, CompressedRunWriterOptions{
				BufferSize:       4 * 1024 * 1024,
				CompressionLevel: CompressionDefault,
			})
			writer.WriteAll(rows)
			writer.Close()
		}
	})

	// Report sizes after benchmarks
	b.Run("size_comparison", func(b *testing.B) {
		tmpDir := b.TempDir()

		uncompPath := filepath.Join(tmpDir, "run.bin")
		compPath := filepath.Join(tmpDir, "run.crun")

		uWriter, _ := NewRunFileWriter(uncompPath, 0)
		uWriter.WriteAll(rows)
		uWriter.Close()

		cWriter, _ := NewCompressedRunWriter(compPath, CompressedRunWriterOptions{})
		cWriter.WriteAll(rows)
		cWriter.Close()

		uInfo, _ := os.Stat(uncompPath)
		cInfo, _ := os.Stat(compPath)

		b.Logf("Prefixes: %d", len(rows))
		b.Logf("Uncompressed: %s", formatBytes(uInfo.Size()))
		b.Logf("Compressed:   %s", formatBytes(cInfo.Size()))
		b.Logf("Ratio:        %.1fx", float64(uInfo.Size())/float64(cInfo.Size()))
	})
}

// BenchmarkParallelMerge compares merge strategies and worker counts.
func BenchmarkParallelMerge(b *testing.B) {
	benchutil.SkipIfNoLongBench(b)

	numFiles := 8
	prefixesPerFile := 5000

	// Create test run files once
	setupFiles := func(dir string) []string {
		var paths []string
		for i := range numFiles {
			var rows []*PrefixRow
			for j := range prefixesPerFile {
				rows = append(rows, &PrefixRow{
					Prefix:     fmt.Sprintf("bucket/data/year=2024/month=%02d/day=%02d/file_%08d.parquet", i%12+1, j%28+1, i*prefixesPerFile+j),
					Depth:      6,
					Count:      uint64(j + 1),
					TotalBytes: uint64((j + 1) * 1024),
				})
			}
			SortPrefixRows(rows)

			path := filepath.Join(dir, fmt.Sprintf("run_%02d.crun", i))
			writer, _ := NewCompressedRunWriter(path, CompressedRunWriterOptions{
				CompressionLevel: CompressionFastest,
			})
			writer.WriteAll(rows)
			writer.Close()
			paths = append(paths, path)
		}
		return paths
	}

	for _, workers := range []int{1, 2, 4, 8} {
		b.Run(fmt.Sprintf("workers=%d", workers), func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				b.StopTimer()
				tmpDir := b.TempDir()
				paths := setupFiles(tmpDir)
				b.StartTimer()

				merger := NewParallelMerger(ParallelMergeConfig{
					NumWorkers:     workers,
					MaxFanIn:       4,
					TempDir:        tmpDir,
					UseCompression: true,
				})
				outPath, _ := merger.MergeAll(context.Background(), paths)
				os.Remove(outPath)
				merger.CleanupIntermediateFiles()
			}
		})
	}

	// Compare fan-in settings
	for _, fanIn := range []int{2, 4, 8} {
		b.Run(fmt.Sprintf("fanIn=%d", fanIn), func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				b.StopTimer()
				tmpDir := b.TempDir()
				paths := setupFiles(tmpDir)
				b.StartTimer()

				merger := NewParallelMerger(ParallelMergeConfig{
					NumWorkers:     4,
					MaxFanIn:       fanIn,
					TempDir:        tmpDir,
					UseCompression: true,
				})
				outPath, _ := merger.MergeAll(context.Background(), paths)
				os.Remove(outPath)
				merger.CleanupIntermediateFiles()
			}
		})
	}
}

// BenchmarkReadPerformance compares read speed between compressed and uncompressed.
func BenchmarkReadPerformance(b *testing.B) {
	numObjects := 50000

	// Generate data once
	gen := benchutil.NewGenerator(benchutil.S3RealisticConfig(numObjects))
	objects := gen.Generate()

	agg := NewAggregator(100000, 0)
	for _, obj := range objects {
		agg.AddObject(obj.Key, obj.Size, obj.TierID)
	}
	rows := agg.Drain()
	SortPrefixRows(rows)

	tmpDir := b.TempDir()
	uncompPath := filepath.Join(tmpDir, "run.bin")
	compPath := filepath.Join(tmpDir, "run.crun")

	// Write both formats
	uWriter, _ := NewRunFileWriter(uncompPath, 0)
	uWriter.WriteAll(rows)
	uWriter.Close()

	cWriter, _ := NewCompressedRunWriter(compPath, CompressedRunWriterOptions{})
	cWriter.WriteAll(rows)
	cWriter.Close()

	b.Run("uncompressed_read", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			reader, _ := OpenRunFile(uncompPath, 4*1024*1024)
			for {
				_, err := reader.Read()
				if err != nil {
					break
				}
			}
			reader.Close()
		}
	})

	b.Run("compressed_read", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			reader, _ := OpenCompressedRunFile(compPath, 4*1024*1024)
			for {
				_, err := reader.Read()
				if err != nil {
					break
				}
			}
			reader.Close()
		}
	})
}
