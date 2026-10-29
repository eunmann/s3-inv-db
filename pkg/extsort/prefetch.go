package extsort

import (
	"io"
	"sync"
	"sync/atomic"
)

// prefetchBatchRows and prefetchBuffers size the prefetcher: four
// batches (one being filled, two queued, one being consumed) of 4096
// rows, ~4 MiB in all.
const (
	prefetchBatchRows = 4096
	prefetchBuffers   = 4
)

// prefetchIterator runs inner on its own goroutine, copying rows into
// batches a couple ahead of the consumer, so the final k-way merge's
// decoding overlaps the index build instead of alternating with it.
// A returned row is valid until the next call to Next. Call stop
// before closing whatever inner reads from.
type prefetchIterator struct {
	inner     RowIterator
	full      chan rowBatch
	free      chan []PrefixRow
	done      chan struct{}
	cur       rowBatch
	wg        sync.WaitGroup
	stopOnce  sync.Once
	remaining uint64
	// inputLeft is inner.Remaining() as of the last batch the prefetch
	// goroutine read, for progress reported from the consumer side.
	inputLeft atomic.Uint64
	idx       int
	started   bool
}

// rowBatch is a run of rows and the error, possibly io.EOF, that ended
// the batch early.
type rowBatch struct {
	err  error
	rows []PrefixRow
}

// newPrefetchIterator wraps inner; nothing runs until the first Next.
func newPrefetchIterator(inner RowIterator) *prefetchIterator {
	p := &prefetchIterator{
		inner:     inner,
		full:      make(chan rowBatch, prefetchBuffers-2),
		free:      make(chan []PrefixRow, prefetchBuffers),
		done:      make(chan struct{}),
		remaining: inner.Remaining(),
	}
	p.inputLeft.Store(p.remaining)

	return p
}

// inputRemaining is inner's Remaining as of the last batch read ahead:
// how much of the merge's input is left, at most a few batches stale.
// Safe to call from any goroutine.
func (p *prefetchIterator) inputRemaining() uint64 { return p.inputLeft.Load() }

func (p *prefetchIterator) run() {
	defer close(p.full)
	for {
		var rows []PrefixRow
		select {
		case rows = <-p.free:
		case <-p.done:
			return
		}
		rows = rows[:0]
		var err error
		for len(rows) < cap(rows) {
			row, nextErr := p.inner.Next()
			if nextErr != nil {
				err = nextErr

				break
			}
			rows = rows[:len(rows)+1]
			rows[len(rows)-1].copyFrom(row)
		}
		p.inputLeft.Store(p.inner.Remaining())
		select {
		case p.full <- rowBatch{rows: rows, err: err}:
		case <-p.done:
			return
		}
		if err != nil {
			return
		}
	}
}

// Next returns the next row, or the error that ended inner (io.EOF at
// the end).
func (p *prefetchIterator) Next() (*PrefixRow, error) {
	if !p.started {
		p.started = true
		for range prefetchBuffers {
			p.free <- make([]PrefixRow, 0, prefetchBatchRows)
		}
		p.wg.Go(p.run)
	}
	for p.idx >= len(p.cur.rows) {
		if p.cur.err != nil {
			return nil, p.cur.err
		}
		if p.cur.rows != nil {
			p.free <- p.cur.rows
		}
		batch, ok := <-p.full
		if !ok {
			return nil, io.EOF
		}
		p.cur, p.idx = batch, 0
	}
	row := &p.cur.rows[p.idx]
	p.idx++
	p.remaining -= min(p.remaining, 1)

	return row, nil
}

// Remaining is inner's upper bound, taken before prefetching started,
// less the rows returned since.
func (p *prefetchIterator) Remaining() uint64 { return p.remaining }

// stop ends the prefetch goroutine and waits for it, so inner is no
// longer in use. Safe to call more than once, and before any Next.
func (p *prefetchIterator) stop() {
	p.stopOnce.Do(func() { close(p.done) })
	p.wg.Wait()
}
