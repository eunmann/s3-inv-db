package extsort_test

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/eunmann/s3-inv-db/internal/benchutil"
	"github.com/eunmann/s3-inv-db/pkg/extsort"
)

// BenchmarkScratchPeak measures the on-disk footprint of every
// scratch file the IndexBuilder writes during a build, bucketed by
// filename prefix, and the wall time of the Add() loop vs Finalize.
//
// The sample point is between the last Add() and the start of
// Finalize() — that's the natural peak for mphf_prefixes_*.tmp and
// every u64disk file the streaming MPHF + depth-index builders open.
//
// Run with:
//
//	go test -run=^$ -bench=BenchmarkScratchPeak ./pkg/extsort -benchtime=1x
//
// Per-iteration metrics reported (bytes / wall time):
//
//	prefix_bytes        size of mphf_prefixes_*.tmp
//	hashes_bytes        size of mphf_hashes_*.u64disk
//	preorderpos_bytes   size of mphf_preorderpos_*.u64disk
//	fingerprints_bytes  size of mphf_fingerprints_*.u64disk
//	depth_bytes         sum of depth_NN_*.u64disk files
//	scratch_total_bytes sum across the whole scratch dir
//	add_us              Add() loop wall time
//	finalize_us         Finalize() wall time
//
// Caveats:
//   - The MPHF prefix temp file is fronted by a 1 MiB bufio.Writer;
//     the on-disk size lags the in-buffer size by up to that much.
//     The smallest scale here (100k prefixes ≈ 4-6 MiB of prefix
//     bytes) is past that threshold, so the bias is small relative
//     to the reported size.
//   - Synthetic input uses S3RealisticConfig (benchutil seed 42).
//     Compressibility numbers will track that distribution, not real
//     production keys — treat absolute size as indicative, the
//     before/after ratio as load-bearing.
func BenchmarkScratchPeak(b *testing.B) {
	silenceZerologForBench(b)
	for _, n := range []int{100_000, 500_000, 2_000_000} {
		b.Run(fmt.Sprintf("objects=%d", n), func(b *testing.B) {
			for range b.N {
				runScratchPeakOne(b, n)
			}
		})
	}
}

func runScratchPeakOne(b *testing.B, n int) {
	b.Helper()

	root := b.TempDir()
	outDir := filepath.Join(root, "out")
	tempDir := filepath.Join(root, "scratch")
	if err := os.MkdirAll(tempDir, 0o750); err != nil {
		b.Fatalf("mkdir scratch: %v", err)
	}

	gen := benchutil.NewGenerator(benchutil.S3RealisticConfig(n))
	objects := gen.Generate()

	agg := extsort.NewAggregator(n, 0)
	for i := range objects {
		agg.AddObject(objects[i].Key, objects[i].Size, objects[i].TierID)
	}
	rows := agg.Drain()
	extsort.SortPrefixRows(rows)

	builder, err := extsort.NewIndexBuilderWithCapacity(outDir, tempDir, uint64(len(rows)))
	if err != nil {
		b.Fatalf("builder: %v", err)
	}

	addStart := time.Now()
	for _, row := range rows {
		if err := builder.Add(row); err != nil {
			b.Fatalf("add: %v", err)
		}
	}
	addDur := time.Since(addStart)

	sizes := bucketScratchSizes(tempDir)

	finalizeStart := time.Now()
	if err := builder.Finalize(); err != nil {
		b.Fatalf("finalize: %v", err)
	}
	finalizeDur := time.Since(finalizeStart)

	b.ReportMetric(float64(sizes["prefix"]), "prefix_bytes")
	b.ReportMetric(float64(sizes["hashes"]), "hashes_bytes")
	b.ReportMetric(float64(sizes["preorderpos"]), "preorderpos_bytes")
	b.ReportMetric(float64(sizes["fingerprints"]), "fingerprints_bytes")
	b.ReportMetric(float64(sizes["depth"]), "depth_bytes")
	b.ReportMetric(float64(sizes["total"]), "scratch_total_bytes")
	b.ReportMetric(float64(addDur.Microseconds()), "add_us")
	b.ReportMetric(float64(finalizeDur.Microseconds()), "finalize_us")
}

func bucketScratchSizes(root string) map[string]int64 {
	sizes := map[string]int64{}
	_ = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !d.Type().IsRegular() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return nil
		}
		size := info.Size()
		sizes["total"] += size
		name := filepath.Base(path)
		switch {
		case strings.HasPrefix(name, "mphf_prefixes_"):
			sizes["prefix"] += size
		case strings.HasPrefix(name, "mphf_hashes_"):
			sizes["hashes"] += size
		case strings.HasPrefix(name, "mphf_preorderpos_"):
			sizes["preorderpos"] += size
		case strings.HasPrefix(name, "mphf_fingerprints_"):
			sizes["fingerprints"] += size
		case strings.HasPrefix(name, "depth_"):
			sizes["depth"] += size
		}

		return nil
	})

	return sizes
}
