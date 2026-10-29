package extsort

import (
	"errors"
	"fmt"
	"io"
	"strconv"
	"testing"
)

// sliceIterator yields rows 0..n-1, then err (io.EOF when nil). Like
// the merge iterator, it recycles the rows handed back to it; it counts
// the rows it allocates and any handed back that it had not handed out.
type sliceIterator struct {
	err        error
	out        map[*PrefixRow]bool
	free       []*PrefixRow
	n, i       int
	allocated  int
	misreturns int
}

func (s *sliceIterator) NextOwned() (*PrefixRow, error) {
	if s.i >= s.n {
		if s.err != nil {
			return nil, s.err
		}

		return nil, io.EOF
	}
	var row *PrefixRow
	if n := len(s.free); n > 0 {
		row, s.free = s.free[n-1], s.free[:n-1]
	} else {
		row = &PrefixRow{}
		s.allocated++
	}
	if s.out == nil {
		s.out = make(map[*PrefixRow]bool)
	}
	s.out[row] = true
	*row = PrefixRow{Prefix: fmt.Sprintf("p%07d/", s.i), Count: uint64(s.i)}
	s.i++

	return row, nil
}

func (s *sliceIterator) Recycle(rows []*PrefixRow) {
	for _, row := range rows {
		if !s.out[row] {
			s.misreturns++
		}
		delete(s.out, row)
	}
	s.free = append(s.free, rows...)
}

func (s *sliceIterator) Remaining() uint64 { return uint64(s.n - s.i) }

func TestPrefetchIterator_YieldsEveryRowInOrder(t *testing.T) {
	for _, n := range []int{0, 1, prefetchBatchRows - 1, prefetchBatchRows, 5*prefetchBatchRows + 17} {
		t.Run(strconv.Itoa(n), func(t *testing.T) {
			p := newPrefetchIterator(&sliceIterator{n: n})
			defer p.stop()
			if p.Remaining() != uint64(n) {
				t.Fatalf("Remaining = %d, want %d", p.Remaining(), n)
			}
			for i := range n {
				row, err := p.Next()
				if err != nil {
					t.Fatalf("row %d: %v", i, err)
				}
				if row.Count != uint64(i) || row.Prefix != fmt.Sprintf("p%07d/", i) {
					t.Fatalf("row %d = %q/%d", i, row.Prefix, row.Count)
				}
			}
			for range 2 {
				if _, err := p.Next(); !errors.Is(err, io.EOF) {
					t.Fatalf("after %d rows: err = %v, want io.EOF", n, err)
				}
			}
		})
	}
}

var errInner = errors.New("inner failed")

func TestPrefetchIterator_SurfacesErrorAfterRowsBeforeIt(t *testing.T) {
	const n = prefetchBatchRows + 10
	p := newPrefetchIterator(&sliceIterator{n: n, err: errInner})
	defer p.stop()
	for i := range n {
		if _, err := p.Next(); err != nil {
			t.Fatalf("row %d: %v", i, err)
		}
	}
	if _, err := p.Next(); !errors.Is(err, errInner) {
		t.Fatalf("err = %v, want errInner", err)
	}
}

// Stopping mid-stream must end the goroutine even though the consumer
// will never drain the queued batches.
func TestPrefetchIterator_StopMidStream(t *testing.T) {
	inner := &sliceIterator{n: 1 << 30}
	p := newPrefetchIterator(inner)
	if _, err := p.Next(); err != nil {
		t.Fatal(err)
	}
	p.stop()
	p.stop()
	if inner.i > prefetchBuffers*prefetchBatchRows {
		t.Fatalf("producer read %d rows, more than its %d buffers hold", inner.i, prefetchBuffers*prefetchBatchRows)
	}
}

func TestPrefetchIterator_StopBeforeNext(_ *testing.T) {
	p := newPrefetchIterator(&sliceIterator{n: 10})
	p.stop()
}

// TestPrefetchIterator_RecyclesBatches: each batch the consumer hands
// back has its rows recycled, exactly once, before the batch is refilled,
// so a long stream allocates no more rows than the batches hold.
func TestPrefetchIterator_RecyclesBatches(t *testing.T) {
	const n = 20*prefetchBatchRows + 3
	inner := &sliceIterator{n: n}
	p := newPrefetchIterator(inner)
	for i := range n {
		if _, err := p.Next(); err != nil {
			t.Fatalf("row %d: %v", i, err)
		}
	}
	p.stop()
	if inner.misreturns != 0 {
		t.Fatalf("%d rows recycled that were not handed out", inner.misreturns)
	}
	if limit := prefetchBuffers * prefetchBatchRows; inner.allocated > limit {
		t.Fatalf("allocated %d rows, more than the %d the batches hold", inner.allocated, limit)
	}
}

// TestPrefetchIterator_MergedRowsMatchCopiedReference: a long merge with
// duplicates, its rows passed to the consumer by pointer and recycled
// by the prefetch goroutine, yields exactly the rows a serial merge
// copied out row by row does. Under -race it also catches a row in use
// on both goroutines at once.
func TestPrefetchIterator_MergedRowsMatchCopiedReference(t *testing.T) {
	paths := overlappingRuns(t, 16, 8000)
	want := drainSerial(t, paths)
	if len(want) <= prefetchBuffers*prefetchBatchRows {
		t.Fatalf("%d merged rows; the test needs more than the batches hold so rows are recycled", len(want))
	}
	iter, err := newMergeIterator(mergeSources(t, paths))
	if err != nil {
		t.Fatal(err)
	}
	defer iter.Close()
	p := newPrefetchIterator(iter)
	defer p.stop()
	for i := range want {
		row, err := p.Next()
		if err != nil {
			t.Fatalf("row %d: %v", i, err)
		}
		if *row != want[i] {
			t.Fatalf("row %d = %+v, want %+v", i, *row, want[i])
		}
	}
	if _, err := p.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("after %d rows: err = %v, want io.EOF", len(want), err)
	}
}
