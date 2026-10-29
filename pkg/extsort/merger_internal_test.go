package extsort

import (
	"errors"
	"fmt"
	"io"
	"strconv"
	"testing"
)

// overlappingRuns writes sorted run files whose prefixes overlap
// heavily, so most merged rows combine duplicates from several runs.
func overlappingRuns(t *testing.T, runs, perRun int) []string {
	t.Helper()
	dir := t.TempDir()
	paths := make([]string, runs)
	for i := range paths {
		prefixes := make([]string, perRun)
		for j := range prefixes {
			prefixes[j] = fmt.Sprintf("p/%07d/", j*(i%4+1)+i%7)
		}
		paths[i] = createTestRunFile(t, dir, fmt.Sprintf("run_%03d.crun", i), prefixes, i*perRun)
	}

	return paths
}

// mergeSources opens paths as merge sources, every third one read into
// memory as a memRun.
func mergeSources(t *testing.T, paths []string) []runSource {
	t.Helper()
	sources := make([]runSource, 0, len(paths))
	for i, path := range paths {
		if i%3 == 0 {
			sources = append(sources, newMemRun(readRunRows(t, path)))

			continue
		}
		r, err := OpenCompressedRunFile(path, 64*1024)
		if err != nil {
			t.Fatal(err)
		}
		sources = append(sources, r)
	}

	return sources
}

// TestMergeIterator_OwnedRowsNeverAliased: rows NextOwned hands out
// stay intact while the merge reads on and merges duplicates, however
// many the caller holds, and none is handed out again before the
// caller recycles it.
func TestMergeIterator_OwnedRowsNeverAliased(t *testing.T) {
	paths := overlappingRuns(t, 12, 3000)
	want := drainSerial(t, paths)
	for _, hold := range []int{1, 7, prefetchBatchRows} {
		t.Run(strconv.Itoa(hold), func(t *testing.T) {
			iter, err := newMergeIterator(mergeSources(t, paths))
			if err != nil {
				t.Fatal(err)
			}
			defer iter.Close()
			held := make([]*PrefixRow, 0, hold)
			holding := make(map[*PrefixRow]bool, hold)
			next := 0
			release := func() {
				for j, row := range held {
					if *row != want[next+j] {
						t.Fatalf("row %d changed while held: %+v, want %+v", next+j, *row, want[next+j])
					}
					delete(holding, row)
				}
				next += len(held)
				iter.Recycle(held)
				held = held[:0]
			}
			for {
				row, err := iter.NextOwned()
				if errors.Is(err, io.EOF) {
					break
				}
				if err != nil {
					t.Fatal(err)
				}
				if holding[row] {
					t.Fatalf("row %d handed out while the caller still holds it", next+len(held))
				}
				holding[row] = true
				held = append(held, row)
				if len(held) == hold {
					release()
				}
			}
			release()
			if next != len(want) {
				t.Fatalf("%d rows, want %d", next, len(want))
			}
		})
	}
}
