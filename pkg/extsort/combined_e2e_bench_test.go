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

// BenchmarkCombinedRealistic runs a pipeline that exercises the paths the
// individual variant benches hit. Unlike BenchmarkExtsortEndToEnd it
// forces multiple run files (8 flushes), uses compressed runs, and runs
// the merge with non-trivial heap size.
func BenchmarkCombinedRealistic(b *testing.B) {
	sizes := []int{100_000, 500_000}
	const flushes = 8

	for _, total := range sizes {
		b.Run(fmt.Sprintf("objects=%d", total), func(b *testing.B) {
			gen := benchutil.NewGenerator(benchutil.S3RealisticConfig(total))
			objects := gen.Generate()
			perFlush := len(objects) / flushes

			b.ReportAllocs()
			b.ResetTimer()
			for n := range b.N {
				b.StopTimer()
				tmp := b.TempDir()
				outDir := filepath.Join(tmp, "idx")
				runDir := filepath.Join(tmp, "runs")
				os.MkdirAll(runDir, 0o755)
				b.StartTimer()

				start := time.Now()

				// Phase 1+2: aggregate, flush, repeat (multiple compressed runs).
				agg := NewAggregator(perFlush, 0)
				var runPaths []string
				for f := range flushes {
					base := f * perFlush
					for i := range perFlush {
						o := objects[base+i]
						agg.AddObject(o.Key, o.Size, o.TierID)
					}
					rows := agg.Drain()
					path := filepath.Join(runDir, fmt.Sprintf("run_%02d.crun", f))
					w, err := NewCompressedRunWriter(path, CompressedRunWriterOptions{
						BufferSize:       4 * 1024 * 1024,
						CompressionLevel: CompressionFastest,
					})
					if err != nil {
						b.Fatal(err)
					}
					if err := w.WriteSorted(rows); err != nil {
						b.Fatal(err)
					}
					if err := w.Close(); err != nil {
						b.Fatal(err)
					}
					runPaths = append(runPaths, path)
				}
				ingestDuration := time.Since(start)
				_ = ingestDuration

				// Phase 3: open all runs, merge through MergeIterator, build index.
				mergeStart := time.Now()
				readers := make([]*RunFileReader, 0, len(runPaths))
				// Convert .crun → RunFileReader via OpenRunFileAuto when possible.
				// For the merge iterator we need RunFileReader; with compressed
				// files use the CompressedRunReader directly through a custom
				// iterator. Simpler: serialize via merge iterator using
				// OpenRunFileAuto then a singleRunIterator chain.
				// To keep the bench focused, use parallel merger.
				merger := NewParallelMerger(ParallelMergeConfig{
					NumWorkers:     2,
					MaxFanIn:       4,
					BufferSize:     4 * 1024 * 1024,
					TempDir:        runDir,
					UseCompression: true,
				})
				finalPath, err := merger.MergeAll(context.Background(), runPaths)
				if err != nil {
					b.Fatal(err)
				}
				readerAuto, err := OpenRunFileAuto(finalPath, 4*1024*1024)
				if err != nil {
					b.Fatal(err)
				}
				it := &singleRunIterator{reader: readerAuto}
				builder, err := NewIndexBuilder(outDir, runDir, false)
				if err != nil {
					b.Fatal(err)
				}
				if err := builder.AddAll(it); err != nil {
					b.Fatal(err)
				}
				if err := builder.Finalize(); err != nil {
					b.Fatal(err)
				}
				readerAuto.Close()
				mergeDuration := time.Since(mergeStart)

				totalDuration := time.Since(start)
				b.ReportMetric(float64(ingestDuration.Milliseconds()), "ingest_ms")
				b.ReportMetric(float64(mergeDuration.Milliseconds()), "merge_ms")
				_ = totalDuration

				// Verify
				idx, err := indexread.Open(outDir)
				if err != nil {
					b.Fatal(err)
				}
				_ = idx.Count()
				idx.Close()

				_ = readers
				_ = n
			}
		})
	}
}
