package extsort_test

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/eunmann/s3-inv-db/internal/benchutil"
	"github.com/eunmann/s3-inv-db/pkg/extsort"
)

// BenchmarkScratchPeak measures the on-disk footprint of the scratch
// files the IndexBuilder writes during a build, bucketed by owner, and
// the wall time of the Add() loop vs Finalize.
//
// Run with:
//
//	go test -run=^$ -bench=BenchmarkScratchPeak ./pkg/extsort -benchtime=1x
//
// Per-iteration metrics reported (bytes / wall time):
//
//	spool_bytes         the level spool (level-spool-*/) after the last Add
//	mphf_bytes          the MPHF builder's scratch (mphf*) after the last Add
//	scratch_total_bytes the whole scratch dir after the last Add
//	scratch_peak_bytes  the largest scratch dir size sampled over Add and Finalize
//	add_us              Add() loop wall time
//	finalize_us         Finalize() wall time
//
// Caveats:
//   - **Sample-point undercount.** The after-Add metrics are sampled
//     before Finalize flushes the writers, so anything still in a bufio
//     buffer is not counted. The bias is the same on both sides of a
//     before/after comparison at the same N, so ratios are comparable;
//     absolute numbers are a lower bound.
//   - **Sampled peak.** scratch_peak_bytes polls the scratch dir every
//     scratchPollInterval, so a peak shorter than that can be missed.
//   - **Synthetic input.** S3RealisticConfig with benchutil seed 42.
//     Absolute size is indicative; the before/after ratio is what to
//     trust.
func BenchmarkScratchPeak(b *testing.B) {
	benchutil.SilenceZerolog(b)
	for _, n := range []int{100_000, 500_000, 2_000_000} {
		b.Run(fmt.Sprintf("objects=%d", n), func(b *testing.B) {
			for range b.N {
				runScratchPeakOne(b, n)
			}
		})
	}
}

// scratchPollInterval is how often the peak sampler sizes the scratch
// dir.
const scratchPollInterval = 5 * time.Millisecond

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

	builder, err := extsort.NewIndexBuilder(outDir, tempDir)
	if err != nil {
		b.Fatalf("builder: %v", err)
	}

	peak := startScratchPeak(tempDir)
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

	b.ReportMetric(float64(sizes["spool"]), "spool_bytes")
	b.ReportMetric(float64(sizes["mphf"]), "mphf_bytes")
	b.ReportMetric(float64(sizes["total"]), "scratch_total_bytes")
	b.ReportMetric(float64(peak.stop()), "scratch_peak_bytes")
	b.ReportMetric(float64(addDur.Microseconds()), "add_us")
	b.ReportMetric(float64(finalizeDur.Microseconds()), "finalize_us")
}

// scratchPeak samples a directory's size until stop.
type scratchPeak struct {
	done chan struct{}
	wg   sync.WaitGroup
	max  int64
}

func startScratchPeak(dir string) *scratchPeak {
	p := &scratchPeak{done: make(chan struct{})}
	p.wg.Go(func() {
		t := time.NewTicker(scratchPollInterval)
		defer t.Stop()
		for {
			if sizes, err := bucketScratchSizes(dir); err == nil {
				p.max = max(p.max, sizes["total"])
			}
			select {
			case <-p.done:
				return
			case <-t.C:
			}
		}
	})

	return p
}

func (p *scratchPeak) stop() int64 {
	close(p.done)
	p.wg.Wait()

	return p.max
}

// bucketScratchSizes sizes the files under root: in all, under the level
// spool's directories, and the MPHF builder's (named mphf*, or in a
// directory named so). Files removed during the walk are skipped.
func bucketScratchSizes(root string) (map[string]int64, error) {
	sizes := map[string]int64{}
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("walk %s: %w", path, err)
		}
		if d.IsDir() || !d.Type().IsRegular() {
			return nil
		}
		info, err := d.Info()
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("stat %s: %w", path, err)
		}
		size := info.Size()
		sizes["total"] += size
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return fmt.Errorf("relative path of %s: %w", path, err)
		}
		switch {
		case strings.HasPrefix(rel, "level-spool-"):
			sizes["spool"] += size
		case strings.HasPrefix(rel, "mphf"):
			sizes["mphf"] += size
		}

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("walk scratch dir: %w", err)
	}

	return sizes, nil
}
