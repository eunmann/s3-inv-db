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
// the per-prefix u64 scratch files (mphf_*.u64disk for MPHF,
// depth_*.u64stream for the depth index).
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
//	depth_bytes         sum of depth_NN_*.u64stream files
//	scratch_total_bytes sum across the whole scratch dir
//	add_us              Add() loop wall time
//	finalize_us         Finalize() wall time
//
// Caveats:
//   - **Sample-point undercount.** The metrics are sampled between
//     the last Add() and the start of Finalize(). The intermediate
//     writers (bufio + zstd encoder) have not been closed at that
//     point, so anything still in the 1 MiB bufio buffer or the
//     zstd encoder block buffer (~128 KiB at SpeedFastest) is not
//     yet on disk and not counted. This bias is **symmetric**: the
//     pre-compression baseline also uses a 1 MiB bufio in
//     u64DiskArray, so before/after ratios at the same N are
//     comparable. Absolute numbers are a lower bound.
//   - **Small-N depth artifact.** At 100k objects the per-depth
//     scratch comes out to ~32 KiB per bucket (positions × 8 B,
//     spread across ~25 depths), entirely below the 1 MiB bufio
//     flush threshold — so `depth_bytes` reports 0 in both branches
//     and is not a regression introduced by compression. The 500k
//     and 2M points are past the threshold and load-bearing.
//   - **Synthetic input.** S3RealisticConfig with benchutil seed 42.
//     Compressibility numbers will track that distribution, not
//     real production keys — absolute size is indicative, the
//     before/after ratio is what to trust.
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

	sizes, err := bucketScratchSizes(tempDir)
	if err != nil {
		b.Fatalf("scratch sizes: %v", err)
	}

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

func bucketScratchSizes(root string) (map[string]int64, error) {
	sizes := map[string]int64{}
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return fmt.Errorf("walk %s: %w", path, err)
		}
		if d.IsDir() || !d.Type().IsRegular() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return fmt.Errorf("stat %s: %w", path, err)
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
	if err != nil {
		return nil, fmt.Errorf("walk scratch dir: %w", err)
	}

	return sizes, nil
}
