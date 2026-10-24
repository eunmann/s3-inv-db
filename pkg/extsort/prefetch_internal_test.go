package extsort

import (
	"errors"
	"fmt"
	"io"
	"strconv"
	"testing"
)

// sliceIterator yields rows 0..n-1, then err (io.EOF when nil). It
// reuses one row, as the merge iterator does.
type sliceIterator struct {
	err  error
	row  PrefixRow
	n, i int
}

func (s *sliceIterator) Next() (*PrefixRow, error) {
	if s.i >= s.n {
		if s.err != nil {
			return nil, s.err
		}

		return nil, io.EOF
	}
	s.row = PrefixRow{Prefix: fmt.Sprintf("p%07d/", s.i), Count: uint64(s.i)}
	s.i++

	return &s.row, nil
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
