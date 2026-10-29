package extsort

import (
	"errors"
	"fmt"
	"io"
	"sync"
)

// aheadBatchRows and aheadBatches size a run's read-ahead in the final
// merge: two batches, one the merge reads while the run's goroutine
// decodes the other. AheadBatchPrefixBytes ends a batch early once its
// prefixes hold that many bytes, so long keys don't grow it.
const (
	aheadBatchRows        = 256
	aheadBatches          = 2
	aheadBatchPrefixBytes = 16 << 10
)

// aheadRunHeapBytes is the most heap one run's read-ahead holds: its
// batches' rows (aheadBatches × aheadBatchRows × ~300 B) and the arena
// chunks their prefixes sit in (aheadBatchPrefixBytes a batch, spread
// over up to two more chunks), ~250 KB.
const aheadRunHeapBytes = 1 << 18

// errMergeClosed is what a merge returns once it is closed.
var errMergeClosed = errors.New("merge closed")

// aheadMerge is the final k-way merge with each run decoded on its own
// goroutine, a batch ahead: the merge's goroutine only compares keys
// (the loser tree of MergeIterator, same order and tie-breaking) and
// copies each merged row out. Decoding was most of that goroutine's
// time, and the building phase waited on it.
//
// One goroutine per run: the final merge reads at most FinalFanIn files
// (DefaultFinalFanIn, or fewer under GOMEMLIMIT) and the workers' last
// runs, and a goroutine with no free batch only waits.
//
// A batch's rows go back to their decoder as soon as the merge has
// copied the last of them out. Their prefixes don't: they are carved
// from the decoder's arena, whose bytes are never written again, so
// rows copied out keep them (the index builder holds its ancestors').
type aheadMerge struct {
	err     error
	sources []runSource
	runs    []aheadCursor
	tree    loserTree
	done    chan struct{}
	out     PrefixRow
	wg      sync.WaitGroup
	total   uint64
	taken   uint64
	closed  bool
}

// aheadBatch is up to a batch of a run's rows, each with how many bytes
// of the previous row's prefix its record codes it as starting with,
// and the error, possibly io.EOF, that ended the run after them.
type aheadBatch struct {
	err    error
	rows   []PrefixRow
	shared []int
	n      int
}

// aheadCursor is the merge's side of a run: the batch it is reading and
// the channels batches come and go on.
type aheadCursor struct {
	cur  *aheadBatch // nil before the first
	full <-chan *aheadBatch
	free chan<- *aheadBatch
	i    int // cur's row the merge is at
}

// newAheadMerge starts a decoder for each source, batches of at most
// batchRows rows, and seeds the merge with each source's first row. On
// error it closes the sources.
func newAheadMerge(sources []runSource, batchRows int) (*aheadMerge, error) {
	k := len(sources)
	m := &aheadMerge{
		sources: sources,
		runs:    make([]aheadCursor, k),
		tree:    newLoserTree(k),
		done:    make(chan struct{}),
	}
	// A run with fewer rows than a batch gets batches its size.
	sizes := make([]int, k)
	slab := 0
	for i, src := range sources {
		count := src.Count()
		m.total += count
		sizes[i] = batchRows
		if count < uint64(batchRows) {
			sizes[i] = max(int(count), 1)
		}
		slab += aheadBatches * sizes[i]
	}
	rows := make([]PrefixRow, slab)
	shared := make([]int, slab)
	batches := make([]aheadBatch, aheadBatches*k)
	for i, src := range sources {
		full := make(chan *aheadBatch, aheadBatches)
		free := make(chan *aheadBatch, aheadBatches)
		for j := range aheadBatches {
			b := &batches[aheadBatches*i+j]
			n := sizes[i]
			b.rows, b.shared = rows[:n:n], shared[:n:n]
			rows, shared = rows[n:], shared[n:]
			free <- b
		}
		m.runs[i] = aheadCursor{full: full, free: free}
		m.wg.Go(func() { decodeAhead(src, free, full, m.done) })
	}
	for i := range m.runs {
		r := &m.runs[i]
		err := r.nextBatch()
		switch {
		case err == nil:
			m.tree.seed(i, r.cur.rows[0].Prefix)
		case errors.Is(err, io.EOF):
		default:
			closeErr := m.Close()

			return nil, errors.Join(fmt.Errorf("seed reader %d: %w", i, err), closeErr)
		}
	}
	m.tree.start()

	return m, nil
}

// decodeAhead decodes src into the batches free hands it and sends each
// on full, until src ends or fails, or done closes. Full holds every
// batch, so a send never waits.
func decodeAhead(src runSource, free <-chan *aheadBatch, full chan<- *aheadBatch, done <-chan struct{}) {
	for {
		var b *aheadBatch
		select {
		case b = <-free:
		case <-done:
			return
		}
		b.n, b.err = 0, nil
		prefixBytes := 0
		for b.n < len(b.rows) && prefixBytes < aheadBatchPrefixBytes {
			shared, err := src.readNext(&b.rows[b.n])
			if err != nil {
				b.err = err

				break
			}
			b.shared[b.n] = shared
			prefixBytes += len(b.rows[b.n].Prefix)
			b.n++
		}
		full <- b
		if b.err != nil {
			return
		}
	}
}

// nextBatch hands the batch read back to the decoder and waits for the
// next: the error that ended the run, io.EOF at its end, once there is
// none.
func (r *aheadCursor) nextBatch() error {
	if r.cur != nil {
		if r.cur.err != nil {
			return r.cur.err
		}
		r.free <- r.cur
	}
	r.cur, r.i = <-r.full, 0
	if r.cur.n == 0 {
		return r.cur.err
	}

	return nil
}

// Next returns the next merged row, valid until the following call.
func (m *aheadMerge) Next() (*PrefixRow, error) {
	if err := m.nextInto(&m.out); err != nil {
		return nil, err
	}

	return &m.out, nil
}

// nextInto writes the next merged row, every source's rows for its
// prefix summed, into dst: io.EOF after the last.
func (m *aheadMerge) nextInto(dst *PrefixRow) error {
	if m.err != nil {
		return m.err
	}
	if m.tree.win.code == ovcExhausted {
		return io.EOF
	}
	src := m.tree.win.src
	r := &m.runs[src]
	*dst = r.cur.rows[r.i]
	if err := m.advance(src, dst.Prefix); err != nil {
		m.err = err

		return err
	}
	// The new winner is coded against dst: equal to it, it differs
	// nowhere before dst ends.
	for dup := ovcCode(len(dst.Prefix), 0); m.tree.win.code == dup; {
		src := m.tree.win.src
		r := &m.runs[src]
		dst.Merge(&r.cur.rows[r.i])
		// The row's prefix equals dst's, so dst's stands for it.
		if err := m.advance(src, dst.Prefix); err != nil {
			m.err = err

			return err
		}
	}

	return nil
}

// advance moves the winner, source src, past its row, whose prefix is
// prev, and puts its next row in the tree.
func (m *aheadMerge) advance(src int, prev string) error {
	m.taken++
	r := &m.runs[src]
	r.i++
	if r.i == r.cur.n {
		err := r.nextBatch()
		switch {
		case errors.Is(err, io.EOF):
			m.tree.replace("", ovcExhausted)

			return nil
		case err != nil:
			return fmt.Errorf("advance reader %d: %w", src, err)
		}
	}
	row := &r.cur.rows[r.i]
	code, sorted := ovcAfter(prev, row.Prefix, r.cur.shared[r.i])
	if !sorted {
		return fmt.Errorf("advance reader %d: %w: %q after %q", src, ErrUnsortedInput, row.Prefix, prev)
	}
	m.tree.replace(row.Prefix, code)

	return nil
}

// Remaining is the sources' rows not yet merged: an upper bound on the
// rows still to come.
func (m *aheadMerge) Remaining() uint64 { return m.total - m.taken }

// Close stops the decoders, waits for them, and closes the sources.
// Safe to call more than once.
func (m *aheadMerge) Close() error {
	if m.closed {
		return nil
	}
	m.closed = true
	close(m.done)
	m.wg.Wait()
	if m.err == nil {
		m.err = errMergeClosed
	}
	errs := make([]error, 0, len(m.sources))
	for _, s := range m.sources {
		if err := s.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	return errors.Join(errs...)
}
