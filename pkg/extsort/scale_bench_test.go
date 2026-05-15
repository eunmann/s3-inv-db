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

func skipUnlessScale(b *testing.B) {
	b.Helper()
	if os.Getenv("S3INV_SCALE_BENCH") == "" {
		b.Skip("set S3INV_SCALE_BENCH=1 to run scale benches")
	}
}

var realisticCache = make(map[int][]benchutil.FakeObject)

func getRealistic(n int) []benchutil.FakeObject {
	if v, ok := realisticCache[n]; ok {
		return v
	}
	cfg := benchutil.S3RealisticConfig(n)
	v := benchutil.NewGenerator(cfg).Generate()
	realisticCache[n] = v
	return v
}

// BenchmarkPipelineScale_E2E exercises ingest → 8 flushes → merge → index
// build at 1M and 10M realistic multi-tier objects. Reports per-phase ms.
func BenchmarkPipelineScale_E2E(b *testing.B) {
	skipUnlessScale(b)
	for _, n := range []int{1_000_000, 10_000_000} {
		const flushes = 8
		b.Run(fmt.Sprintf("objects=%d", n), func(b *testing.B) {
			objs := getRealistic(n)
			perFlush := n / flushes

			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				tmp := b.TempDir()
				runDir := filepath.Join(tmp, "runs")
				outDir := filepath.Join(tmp, "idx")
				os.MkdirAll(runDir, 0o755)
				start := time.Now()

				// Ingest phase: 8 flushes of compressed run files.
				var runPaths []string
				ingestStart := time.Now()
				for f := range flushes {
					agg := NewAggregator(perFlush, 0)
					base := f * perFlush
					for i := range perFlush {
						o := objs[base+i]
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
				ingestMs := time.Since(ingestStart).Milliseconds()

				// Merge + build.
				mergeStart := time.Now()
				merger := NewParallelMerger(ParallelMergeConfig{
					NumWorkers:     4,
					MaxFanIn:       8,
					BufferSize:     4 * 1024 * 1024,
					TempDir:        runDir,
					UseCompression: true,
				})
				finalPath, err := merger.MergeAll(context.Background(), runPaths)
				if err != nil {
					b.Fatal(err)
				}
				reader, err := OpenRunFileAuto(finalPath, 4*1024*1024)
				if err != nil {
					b.Fatal(err)
				}
				it := &singleRunIterator{reader: reader}
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
				reader.Close()
				mergeMs := time.Since(mergeStart).Milliseconds()

				total := time.Since(start)
				b.ReportMetric(float64(ingestMs), "ingest_ms")
				b.ReportMetric(float64(mergeMs), "merge_ms")
				b.ReportMetric(float64(total.Milliseconds()), "total_ms")
				b.ReportMetric(float64(n)/total.Seconds(), "objs/sec")

				// Verify.
				idx, err := indexread.Open(outDir)
				if err != nil {
					b.Fatal(err)
				}
				idx.Close()
			}
		})
	}
}
