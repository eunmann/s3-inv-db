package extsort

import (
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"slices"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/tiers"
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

// memRunOf encodes rows, in order, as a memory run.
func memRunOf(t *testing.T, rows []*PrefixRow) *memRun {
	t.Helper()

	return encodeMemRun(rows)
}

// encodeMemRun encodes rows, in order, as a memory run, which a sink
// in memory never fails.
func encodeMemRun(rows []*PrefixRow) *memRun {
	w := newMemRunWriter()
	for _, r := range rows {
		if err := w.enc.write(r); err != nil {
			panic(err)
		}
	}

	return w.finish()
}

// A memory run reads back exactly the rows written to it, across
// buffers, and lets go of each buffer once it has read past it.
func TestMemRunReadsBackAcrossBuffers(t *testing.T) {
	rows := randomSortedRows(rand.New(rand.NewPCG(2, 3)), 40_000)
	ptrs := make([]*PrefixRow, len(rows))
	for i := range rows {
		ptrs[i] = &rows[i]
	}
	run := memRunOf(t, ptrs)
	if len(run.chunks) < 3 {
		t.Fatalf("%d buffers; the test needs several", len(run.chunks))
	}
	want := memRunFirstChunk
	for i, c := range run.chunks[:len(run.chunks)-1] {
		if cap(c) != want || len(c) < want-runRecordSlack || len(c) > want {
			t.Fatalf("full buffer %d holds %d of %d bytes, want %d less at most %d", i, len(c), cap(c), want, runRecordSlack)
		}
		want = min(2*want, memRunChunk)
	}
	if last := run.chunks[len(run.chunks)-1]; cap(last) != len(last) {
		t.Errorf("the last buffer holds %d bytes in %d", len(last), cap(last))
	}
	first := len(run.chunks)
	for i := range rows {
		var got PrefixRow
		if _, err := run.readNext(&got); err != nil {
			t.Fatalf("row %d: %v", i, err)
		}
		if got != rows[i] {
			t.Fatalf("row %d: got %+v, want %+v", i, got, rows[i])
		}
		if i == len(rows)/2 && len(run.chunks) >= first {
			t.Errorf("halfway through, the run holds %d of its %d buffers", len(run.chunks), first)
		}
	}
	if _, err := run.readNext(&PrefixRow{}); !errors.Is(err, io.EOF) {
		t.Fatalf("after the last row: %v, want EOF", err)
	}
	if run.chunks != nil || run.ReadCount() != uint64(len(rows)) || run.Count() != uint64(len(rows)) {
		t.Errorf("after the last row: %d buffers kept, read %d of %d", len(run.chunks), run.ReadCount(), run.Count())
	}
}

// A worker's kept last state reads back as Drain's rows, from the main
// tree and from ordered segments merged, and the aggregator lets go of
// its nodes as soon as it is encoded.
func TestKeepResidualEncodesAndReleases(t *testing.T) {
	keys := propertyTestKeys(rand.New(rand.NewPCG(7, 8)), 3000)
	sorted := slices.Clone(keys[1000:])
	slices.Sort(sorted)
	kept, ref := NewAggregator(0, 0), NewAggregator(0, 0)
	for i, k := range append(keys[:1000:1000], sorted...) {
		if i >= 1000 && i%500 == 0 {
			kept.StartSegment()
			ref.StartSegment()
		}
		kept.AddObject(k, uint64(i), tiers.ID(i%MaxTiers))
		ref.AddObject(k, uint64(i), tiers.ID(i%MaxTiers))
	}
	if _, one := kept.soleRun(); one {
		t.Fatal("the test needs a state of several runs")
	}
	want := ref.Drain()

	p := NewPipeline(DefaultConfig(), nil)
	if err := p.keepResidual(kept); err != nil {
		t.Fatalf("keepResidual: %v", err)
	}
	if kept.slabs != nil || kept.EstimatedMemoryUsage() != 0 {
		t.Errorf("the aggregator keeps %d slabs, estimate %d", len(kept.slabs), kept.EstimatedMemoryUsage())
	}
	if len(p.memRuns) != 1 || p.runRows.Load() != uint64(len(want)) {
		t.Fatalf("%d memory runs, %d rows; want 1, %d", len(p.memRuns), p.runRows.Load(), len(want))
	}
	run := p.memRuns[0]
	for i, w := range want {
		var got PrefixRow
		if _, err := run.readNext(&got); err != nil {
			t.Fatalf("row %d: %v", i, err)
		}
		if got != *w {
			t.Fatalf("row %d: got %+v, want %+v", i, got, *w)
		}
	}
	if _, err := run.readNext(&PrefixRow{}); !errors.Is(err, io.EOF) {
		t.Fatalf("after the last row: %v, want EOF", err)
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
				mem = append(mem, memRunOf(t, readRunRows(t, p)))
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
				if run.chunks != nil {
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
