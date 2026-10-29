package extsort

import (
	"fmt"
	"io"
)

// memRun is a sorted run held in memory: a worker's last aggregator
// state, encoded as a run file's records are but not compressed, which
// the final merge reads in place instead of writing it to a run file
// and reading it back. A record takes a fraction of the aggregator's
// node, so the worker lets go of its aggregator as soon as it has
// written the run, and the merge lets go of each buffer once it has
// read it.
type memRun struct {
	chunks [][]byte
	off    int // bytes of chunks[0] read
	prev   string
	arena  prefixArena
	next   int
	count  int
}

// A memory run's buffers start at memRunFirstChunk and double up to
// memRunChunk, each allocated whole, so a run grows without copying what
// it holds and a small one takes little.
const (
	memRunFirstChunk = 8 << 10
	memRunChunk      = 1 << 20
)

// memRunWriter encodes a memory run.
type memRunWriter struct {
	enc    runEncoder
	chunks [][]byte
}

func newMemRunWriter() *memRunWriter {
	w := &memRunWriter{}
	w.enc = newRunEncoder(w, memRunFirstChunk)

	return w
}

// take keeps a full buffer and returns the next, twice its size up to
// memRunChunk.
func (w *memRunWriter) take(full []byte) ([]byte, error) {
	w.chunks = append(w.chunks, full)

	return make([]byte, 0, min(2*cap(full), memRunChunk)), nil
}

// finish returns the run written, its last buffer cut to size.
func (w *memRunWriter) finish() *memRun {
	chunks := w.chunks
	if last := w.enc.buf; len(last) > 0 {
		cut := make([]byte, len(last))
		copy(cut, last)
		chunks = append(chunks, cut)
	}
	w.chunks, w.enc.buf = nil, nil

	return &memRun{chunks: chunks, count: int(w.enc.count)}
}

// readNext decodes the next row into into and returns how many bytes of
// the previous row's prefix its record codes it as starting with.
func (m *memRun) readNext(into *PrefixRow) (int, error) {
	if m.next >= m.count {
		m.chunks = nil

		return 0, io.EOF
	}
	for len(m.chunks) > 0 && m.off == len(m.chunks[0]) {
		m.chunks[0] = nil
		m.chunks, m.off = m.chunks[1:], 0
	}
	if len(m.chunks) == 0 {
		return 0, fmt.Errorf("%w: memory run ends after %d of %d records", errRunRecordCorrupt, m.next, m.count)
	}
	rec, err := decodeRunRecord(m.chunks[0][m.off:], m.prev, &m.arena, into)
	if err != nil {
		return 0, fmt.Errorf("memory run record %d: %w", m.next, err)
	}
	m.off += rec.size
	m.prev = into.Prefix
	m.next++

	return rec.shared, nil
}

func (m *memRun) Count() uint64     { return uint64(m.count) }
func (m *memRun) ReadCount() uint64 { return uint64(m.next) }

// Close releases the records.
func (m *memRun) Close() error {
	m.chunks = nil

	return nil
}
