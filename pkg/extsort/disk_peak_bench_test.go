package extsort_test

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/eunmann/s3-inv-db/internal/benchutil"
	"github.com/eunmann/s3-inv-db/pkg/extsort"
)

// BenchmarkLoadDiskPeak measures the final on-disk index size against
// the synthetic object count so operators can pick a sensible
// --max-index-disk and --index-ratio for their corpus. Run with:
//
//	go test -run=^$ -bench=BenchmarkLoadDiskPeak ./pkg/extsort -benchtime=1x
//
// The b.ReportMetric calls emit per-iteration metrics so a `-benchtime=1x`
// run prints one line per object-count point.
//
// Caveats:
//   - This pipeline does not include the S3 download phase, so its
//     "input bytes" is the synthetic object payload — not the
//     compressed CSV size you'd see in production. Treat the resulting
//     index_bytes / object_count number as the load-bearing one.
//   - Scratch peak is approximated by sampling the run directory after
//     all run files are written but before the index merge starts —
//     this is the natural peak for the on-disk scratch.
func BenchmarkLoadDiskPeak(b *testing.B) {
	sizes := []int{10_000, 100_000, 500_000}
	for _, n := range sizes {
		b.Run(fmt.Sprintf("objects=%d", n), func(b *testing.B) {
			for range b.N {
				measureOne(b, n)
			}
		})
	}
}

func measureOne(b *testing.B, numObjects int) {
	b.Helper()
	tmp := b.TempDir()
	outDir := filepath.Join(tmp, "index")
	runDir := filepath.Join(tmp, "runs")
	if err := os.MkdirAll(runDir, 0o750); err != nil {
		b.Fatalf("mkdir runs: %v", err)
	}

	gen := benchutil.NewGenerator(benchutil.S3RealisticConfig(numObjects))
	objects := gen.Generate()

	var inputBytes int64
	for i := range objects {
		inputBytes += int64(len(objects[i].Key)) + 16 // key + size + tier
	}

	agg := extsort.NewAggregator(numObjects, 0)
	for i := range objects {
		agg.AddObject(objects[i].Key, objects[i].Size, objects[i].TierID)
	}
	rows := agg.Drain()
	runPath := filepath.Join(runDir, "run_0000.bin")
	writer, err := extsort.NewRunFileWriter(runPath, 4*1024*1024)
	if err != nil {
		b.Fatalf("run writer: %v", err)
	}
	if err := writer.WriteSorted(rows); err != nil {
		b.Fatalf("write sorted: %v", err)
	}
	if err := writer.Close(); err != nil {
		b.Fatalf("close run: %v", err)
	}

	scratchPeak, _ := dirSize(runDir)

	merger, err := extsort.NewMergeIterator([]string{runPath}, 4*1024*1024)
	if err != nil {
		b.Fatalf("merger: %v", err)
	}
	builder, err := extsort.NewIndexBuilder(outDir, "")
	if err != nil {
		merger.Close()
		b.Fatalf("builder: %v", err)
	}
	if err := builder.AddAll(merger); err != nil {
		merger.Close()
		b.Fatalf("add all: %v", err)
	}
	if err := merger.Close(); err != nil {
		b.Fatalf("merger close: %v", err)
	}
	if err := builder.Finalize(); err != nil {
		b.Fatalf("finalize: %v", err)
	}

	indexBytes, _ := dirSize(outDir)
	b.ReportMetric(float64(indexBytes), "index_bytes")
	b.ReportMetric(float64(scratchPeak), "scratch_peak_bytes")
	b.ReportMetric(float64(inputBytes), "synth_input_bytes")
	b.ReportMetric(float64(indexBytes)/float64(inputBytes), "index/input_ratio")
}

func dirSize(root string) (int64, error) {
	var total int64
	err := filepath.WalkDir(root, func(_ string, d fs.DirEntry, err error) error {
		if err != nil {
			return fmt.Errorf("walk entry: %w", err)
		}
		if d.IsDir() || !d.Type().IsRegular() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return fmt.Errorf("stat entry: %w", err)
		}
		total += info.Size()

		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("walk %s: %w", root, err)
	}

	return total, nil
}
