package extsort

import (
	"errors"
	"fmt"
	"io"
	"testing"
)

// BenchmarkMergeIterator merges k sorted runs held in memory, 2^18 rows
// in all: the merge's own cost, without decoding run files or building
// the memory runs. Disjoint runs each hold one range of keys, so one run
// wins row after row (a key-ordered inventory's spills); interleaved
// runs take turns row by row (an unordered inventory's). Reports ns/row.
func BenchmarkMergeIterator(b *testing.B) {
	const total = 1 << 18
	for _, layout := range []string{"disjoint", "interleaved"} {
		for _, k := range []int{8, 32, 64} {
			b.Run(fmt.Sprintf("layout=%s/k=%d", layout, k), func(b *testing.B) {
				runs := mergeBenchRuns(layout == "interleaved", k, total/k)
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					// Workers build their memory runs, in parallel, before
					// the merge starts.
					b.StopTimer()
					sources := make([]runSource, k)
					for i, run := range runs {
						sources[i] = newSliceRun(run)
					}
					b.StartTimer()
					it, err := newMergeIterator(sources)
					if err != nil {
						b.Fatal(err)
					}
					n := 0
					for {
						if _, err := it.Next(); errors.Is(err, io.EOF) {
							break
						} else if err != nil {
							b.Fatal(err)
						}
						n++
					}
					if n != total {
						b.Fatalf("merged %d rows, want %d", n, total)
					}
				}
				b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*total), "ns/row")
			})
		}
	}
}

// mergeBenchRuns returns k sorted runs of perRun distinct prefixes: run
// r holds keys r*perRun up to (r+1)*perRun, or, interleaved, every k-th
// key from r.
func mergeBenchRuns(interleaved bool, k, perRun int) [][]*PrefixRow {
	runs := make([][]*PrefixRow, k)
	for r := range runs {
		run := make([]*PrefixRow, perRun)
		for j := range run {
			key := r*perRun + j
			if interleaved {
				key = j*k + r
			}
			row := &PrefixRow{Prefix: fmt.Sprintf("bucket/logs/p%04d/k%07d/", key>>8, key), Depth: 4, Count: 1, TotalBytes: 4096}
			row.TierCounts[0], row.TierBytes[0] = 1, 4096
			run[j] = row
		}
		runs[r] = run
	}

	return runs
}
