package extsort

import (
	"errors"
	"fmt"
	"io"
	"os"
	"testing"
)

// readRunRows returns every row of the run file at path.
func readRunRows(t *testing.T, path string) []*PrefixRow {
	t.Helper()
	r, err := OpenCompressedRunFile(path, 64*1024)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	var rows []*PrefixRow
	for {
		row := &PrefixRow{}
		if err := r.ReadInto(row); errors.Is(err, io.EOF) {
			return rows
		} else if err != nil {
			t.Fatal(err)
		}
		rows = append(rows, row)
	}
}

// TestParallelMerge_MemoryRunsJoinFinalMerge: runs held in memory merge
// with the files into exactly the rows a serial merge of all of them as
// files gives, with or without disk rounds and with no files at all,
// and each memory run lets go of its rows once read.
func TestParallelMerge_MemoryRunsJoinFinalMerge(t *testing.T) {
	for _, c := range []struct {
		name       string
		memEvery   int // every memEvery-th run is held in memory
		finalFanIn int
	}{
		{name: "streamed", memEvery: 3, finalFanIn: 64},
		{name: "with_disk_rounds", memEvery: 3, finalFanIn: 4},
		{name: "memory_only", memEvery: 1, finalFanIn: 64},
	} {
		t.Run(c.name, func(t *testing.T) {
			dir := t.TempDir()
			paths := make([]string, 13)
			for i := range paths {
				prefixes := make([]string, 40)
				for j := range prefixes {
					prefixes[j] = fmt.Sprintf("p/%06d/", j*(i%3+1)+i%5)
				}
				paths[i] = createTestRunFile(t, dir, fmt.Sprintf("in_%04d.crun", i), prefixes, i*40)
			}
			want := drainSerial(t, paths)

			var files []string
			var mem []*memRun
			for i, p := range paths {
				if i%c.memEvery != 0 {
					files = append(files, p)

					continue
				}
				mem = append(mem, newMemRun(readRunRows(t, p)))
				if err := os.Remove(p); err != nil {
					t.Fatal(err)
				}
			}
			merger := NewParallelMerger(ParallelMergeConfig{
				NumWorkers: 2, MaxFanIn: 4, FinalFanIn: c.finalFanIn,
				BufferSize: 64 * 1024, TempDir: t.TempDir(), CompressionLevel: CompressionFastest,
			})
			iter, cleanup, err := merger.mergeAllToIterator(t.Context(), files, mem)
			if err != nil {
				t.Fatal(err)
			}
			var got []PrefixRow
			for {
				row, err := iter.Next()
				if errors.Is(err, io.EOF) {
					break
				}
				if err != nil {
					t.Fatal(err)
				}
				got = append(got, *row)
			}
			for i, run := range mem {
				if run.rows != nil {
					t.Errorf("memory run %d still holds its rows after the merge read them", i)
				}
			}
			if err := cleanup(); err != nil {
				t.Fatal(err)
			}
			if len(got) != len(want) {
				t.Fatalf("%d rows, want %d", len(got), len(want))
			}
			for i := range want {
				if got[i] != want[i] {
					t.Fatalf("row %d: got %+v, want %+v", i, got[i], want[i])
				}
			}
		})
	}
}
