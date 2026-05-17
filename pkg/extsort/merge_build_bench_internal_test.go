package extsort

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/eunmann/s3-inv-db/internal/benchutil"
)

// BenchmarkMergeBuildSeam exercises the K-way merge → IndexBuilder
// hand-off in isolation. Two variants run side by side:
//
//   - via_disk:   MergeAll → final file → OpenRunFileAuto → builder
//     (the pre-I3 path; writes the full merged stream
//     to disk and re-reads it)
//   - streamed:   MergeAllToIterator → builder consumes directly
//     (the post-I3 path; no final file written)
//
// Setup creates `runFileCount` sorted run files locally (no S3) by
// partitioning a synthetic input across them. Reports ns/op + disk-
// bytes-written.
func BenchmarkMergeBuildSeam(b *testing.B) {
	silenceZerologExtsort(b)
	for _, n := range []int{500_000, 1_000_000} {
		const runFiles = 8 // representative of post-N-1-round count
		b.Run(fmt.Sprintf("n=%d/runs=%d/via_disk", n, runFiles), func(b *testing.B) {
			runMergeBuildSeamBench(b, n, runFiles, false)
		})
		b.Run(fmt.Sprintf("n=%d/runs=%d/streamed", n, runFiles), func(b *testing.B) {
			runMergeBuildSeamBench(b, n, runFiles, true)
		})
	}
}

func runMergeBuildSeamBench(b *testing.B, n, runFileCount int, streamed bool) {
	b.Helper()
	gen := benchutil.NewGenerator(benchutil.S3RealisticConfig(n))
	objects := gen.Generate()

	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		b.StopTimer()
		dir := b.TempDir()
		runDir := filepath.Join(dir, "runs")
		outDir := filepath.Join(dir, "idx")

		runPaths := writePartitionedRunFiles(b, runDir, objects, runFileCount)
		b.StartTimer()

		merger := NewParallelMerger(ParallelMergeConfig{
			NumWorkers:       4,
			MaxFanIn:         runFileCount,
			BufferSize:       1 * 1024 * 1024,
			TempDir:          runDir,
			UseCompression:   true,
			CompressionLevel: CompressionFastest,
		})

		var iter RowIterator
		var capacity uint64
		var cleanup func()
		if streamed {
			it, c, err := merger.MergeAllToIterator(context.Background(), runPaths)
			if err != nil {
				b.Fatalf("MergeAllToIterator: %v", err)
			}
			iter = it
			capacity = it.Remaining()
			cleanup = func() { _ = c() }
		} else {
			finalPath, err := merger.MergeAll(context.Background(), runPaths)
			if err != nil {
				b.Fatalf("MergeAll: %v", err)
			}
			reader, err := OpenRunFileAuto(finalPath, 1*1024*1024)
			if err != nil {
				b.Fatalf("OpenRunFileAuto: %v", err)
			}
			iter = &singleRunIterator{reader: reader}
			capacity = reader.Count()
			cleanup = func() { reader.Close() }
		}

		builder, err := NewIndexBuilderWithCapacity(outDir, "", capacity)
		if err != nil {
			b.Fatalf("NewIndexBuilder: %v", err)
		}
		if err := builder.AddAllWithContext(context.Background(), iter); err != nil {
			b.Fatalf("AddAll: %v", err)
		}
		if err := builder.FinalizeWithContext(context.Background()); err != nil {
			b.Fatalf("Finalize: %v", err)
		}
		cleanup()
	}
}

// writePartitionedRunFiles distributes `objects` round-robin across
// `runFileCount` aggregators, sorts each, and writes a compressed
// run file per partition. Returns the list of paths in order.
func writePartitionedRunFiles(b *testing.B, runDir string, objects []benchutil.FakeObject, runFileCount int) []string {
	b.Helper()
	if err := makeDir(runDir); err != nil {
		b.Fatalf("mkdir: %v", err)
	}
	parts := make([]*Aggregator, runFileCount)
	for i := range parts {
		parts[i] = NewAggregator(len(objects)/runFileCount+1, 0)
	}
	for i, o := range objects {
		parts[i%runFileCount].AddObject(o.Key, o.Size, o.TierID)
	}
	paths := make([]string, runFileCount)
	for i, agg := range parts {
		rows := agg.Drain()
		SortPrefixRows(rows)
		path := filepath.Join(runDir, fmt.Sprintf("run_%02d.crun", i))
		w, err := NewCompressedRunWriter(path, CompressedRunWriterOptions{
			BufferSize:       DefaultRunBufferSize,
			CompressionLevel: CompressionFastest,
		})
		if err != nil {
			b.Fatalf("NewCompressedRunWriter: %v", err)
		}
		if err := w.WriteSorted(rows); err != nil {
			b.Fatalf("WriteSorted: %v", err)
		}
		if err := w.Close(); err != nil {
			b.Fatalf("Close: %v", err)
		}
		paths[i] = path
	}

	return paths
}

func makeDir(path string) error {
	const dirPerm = 0o750
	if err := os.MkdirAll(path, dirPerm); err != nil {
		return fmt.Errorf("mkdir %s: %w", path, err)
	}

	return nil
}
