package extsort_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/eunmann/s3-inv-db/internal/benchutil"
	"github.com/eunmann/s3-inv-db/pkg/extsort"
	"github.com/eunmann/s3-inv-db/pkg/indexread"
)

// External Sort Backend Benchmarks
//
// BenchmarkExtsortEndToEnd  — full extsort pipeline at default sizes.
// BenchmarkParallelMerge    — gated worker/fan-in sweep of the
//                             multi-file merge stage.
//
// The dedicated per-component micro-benches (Aggregator, RunFile
// read/write, MergeBuild seam, Build harness) live in their own
// `*_bench_internal_test.go` files in this package.
//
// Run:
//
//	go test -bench='BenchmarkExtsortEndToEnd' -benchtime=1x ./pkg/extsort/...

// BenchmarkExtsortEndToEnd benchmarks the complete extsort pipeline.
func BenchmarkExtsortEndToEnd(b *testing.B) {
	sizes := []int{10_000, 100_000}
	if benchutil.LongBenchEnabled() {
		sizes = append(sizes, 500_000, 1_000_000)
	}

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
		if err := os.MkdirAll(runDir, 0o750); err != nil {
			b.Fatalf("mkdir runs: %v", err)
		}

		gen := benchutil.NewGenerator(benchutil.S3RealisticConfig(numObjects))
		objects := gen.Generate()

		var inputBytes int64
		for _, obj := range objects {
			inputBytes += int64(len(obj.Key)) + 8
		}

		b.StartTimer()
		totalStart := time.Now()

		phase1Start := time.Now()
		agg := extsort.NewAggregator(100_000, 0)
		for _, obj := range objects {
			agg.AddObject(obj.Key, obj.Size, obj.TierID)
		}
		prefixCount := agg.PrefixCount()
		phase1Duration := time.Since(phase1Start)

		phase2Start := time.Now()
		rows := agg.Drain()
		runPath := filepath.Join(runDir, "run_0000.bin")
		writer, err := extsort.NewRunFileWriter(runPath, 4*1024*1024)
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

		runInfo, err := os.Stat(runPath)
		if err != nil {
			b.Fatalf("stat run: %v", err)
		}
		runSize := runInfo.Size()

		phase3Start := time.Now()
		merger, err := extsort.NewMergeIterator([]string{runPath}, 4*1024*1024)
		if err != nil {
			b.Fatalf("create merger: %v", err)
		}

		builder, err := extsort.NewIndexBuilder(outDir, "")
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

		indexSize, err := dirBytes(outDir)
		if err != nil {
			b.Fatalf("walk index: %v", err)
		}

		totalDuration := time.Since(totalStart)
		b.StopTimer()

		idx, err := indexread.Open(outDir)
		if err != nil {
			b.Fatalf("open index: %v", err)
		}
		idxCount := idx.Count()
		idx.Close()

		if i == b.N-1 {
			b.Logf("\n=== Extsort End-to-End Build Metrics (objects=%d) ===", numObjects)
			b.Logf("Prefixes: %d | Index Prefixes: %d", prefixCount, idxCount)
			b.Logf("memory_aggregation:    %12s (%5.1f%%)",
				phase1Duration.Round(time.Millisecond),
				float64(phase1Duration)/float64(totalDuration)*100)
			b.Logf("sort_and_write_run:    %12s (%5.1f%%) size=%s",
				phase2Duration.Round(time.Millisecond),
				float64(phase2Duration)/float64(totalDuration)*100,
				formatBytes(runSize))
			b.Logf("merge_and_build_index: %12s (%5.1f%%) size=%s",
				phase3Duration.Round(time.Millisecond),
				float64(phase3Duration)/float64(totalDuration)*100,
				formatBytes(indexSize))
			b.Logf("TOTAL: %s, throughput=%.0f obj/s, input=%s, ratio=%.1fx",
				totalDuration.Round(time.Millisecond),
				float64(numObjects)/totalDuration.Seconds(),
				formatBytes(inputBytes),
				float64(inputBytes)/float64(runSize+indexSize))
		}
	}
}

// BenchmarkParallelMerge compares merge worker counts and fan-in.
func BenchmarkParallelMerge(b *testing.B) {
	benchutil.SkipIfNoLongBench(b)

	const (
		numFiles        = 8
		prefixesPerFile = 5000
	)
	setupFiles := func(b *testing.B, dir string) []string {
		b.Helper()
		paths := make([]string, 0, numFiles)
		for i := range numFiles {
			rows := make([]*extsort.PrefixRow, 0, prefixesPerFile)
			for j := range prefixesPerFile {
				rows = append(rows, &extsort.PrefixRow{
					Prefix:     fmt.Sprintf("bucket/data/year=2024/month=%02d/day=%02d/file_%08d.parquet", i%12+1, j%28+1, i*prefixesPerFile+j),
					Depth:      6,
					Count:      uint64(j + 1),
					TotalBytes: uint64((j + 1) * 1024),
				})
			}
			extsort.SortPrefixRows(rows)

			path := filepath.Join(dir, fmt.Sprintf("run_%02d.crun", i))
			writer, err := extsort.NewCompressedRunWriter(path, extsort.CompressedRunWriterOptions{
				CompressionLevel: extsort.CompressionFastest,
			})
			if err != nil {
				b.Fatalf("new compressed writer: %v", err)
			}
			if err := writer.WriteAll(rows); err != nil {
				b.Fatalf("write all: %v", err)
			}
			if err := writer.Close(); err != nil {
				b.Fatalf("close writer: %v", err)
			}
			paths = append(paths, path)
		}
		return paths
	}

	runMerger := func(b *testing.B, tmpDir string, paths []string, workers, fanIn int) {
		b.Helper()
		merger := extsort.NewParallelMerger(extsort.ParallelMergeConfig{
			NumWorkers:     workers,
			MaxFanIn:       fanIn,
			TempDir:        tmpDir,
			UseCompression: true,
		})
		outPath, err := merger.MergeAll(b.Context(), paths)
		if err != nil {
			b.Fatalf("MergeAll: %v", err)
		}
		_ = os.Remove(outPath)
		merger.CleanupIntermediateFiles()
	}

	for _, workers := range []int{1, 2, 4, 8} {
		b.Run(fmt.Sprintf("workers=%d", workers), func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				b.StopTimer()
				tmpDir := b.TempDir()
				paths := setupFiles(b, tmpDir)
				b.StartTimer()
				runMerger(b, tmpDir, paths, workers, 4)
			}
		})
	}

	for _, fanIn := range []int{2, 4, 8} {
		b.Run(fmt.Sprintf("fanIn=%d", fanIn), func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				b.StopTimer()
				tmpDir := b.TempDir()
				paths := setupFiles(b, tmpDir)
				b.StartTimer()
				runMerger(b, tmpDir, paths, 4, fanIn)
			}
		})
	}
}

func dirBytes(dir string) (int64, error) {
	var total int64
	err := filepath.Walk(dir, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			total += info.Size()
		}
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("walk %s: %w", dir, err)
	}
	return total, nil
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
