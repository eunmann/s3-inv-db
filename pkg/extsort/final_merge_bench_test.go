package extsort_test

import (
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"testing"

	"github.com/eunmann/s3-inv-db/internal/benchutil"
	"github.com/eunmann/s3-inv-db/pkg/extsort"
)

// BenchmarkFinalMerge merges many spilled runs into the streaming
// iterator the index build reads, with the merger's default fan-in
// under a given GOMEMLIMIT: the shape of a large build's merge phase.
// With heavy overlap each prefix sits in ~runs/8 runs (spills of
// unsorted inventories); with light overlap in about one, the runs'
// keys interleaved; with none each run holds its own range of keys
// (spills of key-ordered inventories), so one run wins row after row.
// Reports the merge's peak heap, the disk rounds it took and the bytes
// those rounds wrote.
func BenchmarkFinalMerge(b *testing.B) {
	benchutil.SilenceZerolog(b)
	for _, tc := range []struct {
		runs     int
		limitMiB int64
	}{
		{runs: 48, limitMiB: 256},
		{runs: 48, limitMiB: 1024},
		{runs: 128, limitMiB: 1024},
	} {
		for _, overlap := range []string{"heavy", "light", "none"} {
			b.Run(fmt.Sprintf("runs=%d/limit=%dMiB/overlap=%s", tc.runs, tc.limitMiB, overlap), func(b *testing.B) {
				benchFinalMerge(b, tc.runs, tc.limitMiB<<20, overlap)
			})
		}
	}
}

func benchFinalMerge(b *testing.B, runs int, limit int64, overlap string) {
	b.Helper()
	const rowsPerRun = 20_000
	prev := debug.SetMemoryLimit(limit)
	b.Cleanup(func() { debug.SetMemoryLimit(prev) })
	b.ReportAllocs()
	var peak uint64
	var stats extsort.MergeStatistics
	for range b.N {
		b.StopTimer()
		dir := b.TempDir()
		paths := writeOverlappingRuns(b, dir, runs, rowsPerRun, overlap)
		// Collect the run writers' garbage first, so the peak is the
		// merge's own and not whatever the GC had yet to free.
		runtime.GC()
		b.StartTimer()

		sampler := benchutil.StartHeapPeakSampler()
		merger := extsort.NewParallelMerger(extsort.ParallelMergeConfig{TempDir: dir})
		iter, cleanup, err := merger.MergeAllToIterator(b.Context(), paths)
		if err != nil {
			b.Fatalf("merge: %v", err)
		}
		for {
			if _, err := iter.Next(); err != nil {
				if !errors.Is(err, io.EOF) {
					b.Fatalf("next: %v", err)
				}

				break
			}
		}
		if err := cleanup(); err != nil {
			b.Fatalf("cleanup: %v", err)
		}
		peak = max(peak, sampler.Stop())
		stats = merger.Statistics()
	}
	b.ReportMetric(float64(peak), "peak_heap_B")
	b.ReportMetric(float64(stats.Rounds), "rounds")
	b.ReportMetric(float64(stats.BytesWritten), "round_bytes")
}

// writeOverlappingRuns writes runs sorted spill runs drawn from one key
// space: heavy overlap puts each prefix in ~runs/8 of them, light in
// about one, and none gives run r the directory p<r> to itself.
func writeOverlappingRuns(b *testing.B, dir string, runs, rowsPerRun int, overlap string) []string {
	b.Helper()
	paths := make([]string, 0, runs)
	keySpace := rowsPerRun * runs * 2
	if overlap == "heavy" {
		keySpace = rowsPerRun * 8
	}
	for r := range runs {
		rows := make([]*extsort.PrefixRow, 0, rowsPerRun)
		seen := make(map[int]bool, rowsPerRun)
		for j := range rowsPerRun {
			k := (j*7919 + r*104729) % keySpace
			parent := k % 1000
			if overlap == "none" {
				k, parent = r*rowsPerRun+j, r
			}
			if seen[k] {
				continue
			}
			seen[k] = true
			row := &extsort.PrefixRow{
				Prefix:     fmt.Sprintf("bucket/logs/p%04d/k%07d/", parent, k),
				Depth:      4,
				Count:      uint64(j + 1),
				TotalBytes: uint64(j+1) * 4096,
			}
			row.TierCounts[0], row.TierBytes[0] = row.Count, row.TotalBytes
			rows = append(rows, row)
		}
		extsort.SortPrefixRows(rows)
		path := filepath.Join(dir, fmt.Sprintf("run_%04d.crun", r))
		w, err := extsort.NewCompressedRunWriter(path, extsort.CompressedRunWriterOptions{CompressionLevel: extsort.CompressionFastest})
		if err != nil {
			b.Fatalf("writer: %v", err)
		}
		if err := w.WriteAll(rows); err != nil {
			b.Fatalf("write: %v", err)
		}
		if err := w.Close(); err != nil {
			b.Fatalf("close: %v", err)
		}
		paths = append(paths, path)
	}

	return paths
}
