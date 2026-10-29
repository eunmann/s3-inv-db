package extsort

import (
	"errors"
	"fmt"
	"io"
	"math"
)

// runSource is one sorted input of a merge: a run file, a run held in
// memory (memRun) or one of an aggregator's runs (aggCursor).
type runSource interface {
	// readNext reads the next row into row and returns a count of
	// leading bytes its prefix shares with the previous row's: at most
	// all they share, and 0 for the first row. io.EOF after the last.
	readNext(row *PrefixRow) (int, error)
	Count() uint64
	ReadCount() uint64
	Close() error
}

// fileSources returns readers as merge sources.
func fileSources(readers []*CompressedRunReader) []runSource {
	out := make([]runSource, len(readers))
	for i, r := range readers {
		out[i] = r
	}

	return out
}

// MergeIterator provides a k-way merge of sorted runs.
// It reads from multiple runs and yields PrefixRows in globally sorted order,
// automatically merging duplicates (same prefix from different runs).
// A run whose prefixes go backwards is an error (ErrUnsortedInput).
//
// Rows are recycled through a per-iterator free list, and the rows and
// the merge's other state come in a fixed handful of allocations made
// at the start, whatever the merge's fan-in or length.
type MergeIterator struct {
	err     error
	readers []runSource
	heads   []*PrefixRow // each reader's next row; nil once it has none
	tree    loserTree
	free    []*PrefixRow
	last    *PrefixRow
}

// Offset-value codes (Do and Graefe, "Robust and Efficient Sorting with
// Offset-Value Coding", 2023) order keys against a common base key that
// sorts no later than any of them. A key's code is the offset of its
// first byte that differs from the base's and that byte's value plus
// one, or 0 if the key ends there (it equals the base), as one integer
// with the offset inverted: the later a key leaves the base the closer
// it is to it, so the smaller it sorts. Two keys coded against the same
// base sort as their codes do, unless the codes are equal; then the
// keys agree up to the offset and one byte past it, and only the bytes
// after that need comparing. And when a key sorts first, the other's
// code against it is its code against the base: a merge that recodes
// every loser against the key that beat it needs no other recoding.
//
// The value takes ovcValueBits; the highest offset leaves a code's top
// bit clear, so no key codes as ovcExhausted, the code of a source with
// no rows left.
const (
	ovcValueBits = 9
	ovcMaxOffset = math.MaxUint64 >> (ovcValueBits + 1)
	ovcExhausted = math.MaxUint64
)

// ovcCode returns the code of a key that first differs from the base at
// off, with value its byte there plus one, or 0 if it ends there.
func ovcCode(off int, value uint64) uint64 {
	return (ovcMaxOffset-uint64(off))<<ovcValueBits | value
}

// ovcAt returns the code of key against a base it first differs from at
// off.
func ovcAt(key string, off int) uint64 {
	if off < len(key) {
		return ovcCode(off, uint64(key[off])+1)
	}

	return ovcCode(off, 0)
}

// ovcOffset returns the offset a code holds.
func ovcOffset(code uint64) int {
	return int(ovcMaxOffset - code>>ovcValueBits)
}

// ovcAfter returns the code of key against prev, the key before it in its
// run, given that the two share at least their first shared bytes; false
// if key sorts before prev. A run file's records store that count (as
// they are front-coded), so mostly this compares one byte.
func ovcAfter(prev, key string, shared int) (uint64, bool) {
	off := shared
	if off < len(prev) && off < len(key) && prev[off] == key[off] {
		off += sharedPrefixLen(prev[off:], key[off:])
	}
	switch {
	case off == len(key):
		return ovcCode(off, 0), off == len(prev)
	case off < len(prev) && key[off] < prev[off]:
		return 0, false
	}

	return ovcCode(off, uint64(key[off])+1), true
}

// loserTree is a tree of losers over the merge sources' next keys,
// ordered by offset-value codes. Every match is between two keys coded
// against the same base, so it is an integer comparison unless the codes
// tie: the winner's code is against the previous winner, and so is that
// of every loser on its path to the root, since each lost to it last.
// After the winner's source moves on, its next key, coded against the
// winner (its predecessor in the run), replays the matches on that path.
//
// A plain tree of losers replays all log k matches for every row, where
// a heap whose top stays smallest does two comparisons; when one source
// keeps winning, as with runs that each hold a range of keys, the tree
// does one: a key whose code is below every loser's on the path wins
// every match there and changes nothing, so the replay is skipped.
type loserTree struct {
	keys []string // each source's next key
	// nodes[p], for 0 < p < len(keys), is the loser of the match at p.
	// Source i is leaf len(keys)+i; node p's parent is p/2.
	nodes []loserEntry
	// first[p] is, before start, source i's first key at p = len(keys)+i,
	// and, as start plays the first matches, the winner of node p's.
	first []loserEntry
	win   loserEntry
	// runnerUp, when runnerUpKnown, is the smallest loser's code on the
	// winner's path: the code the winner's next key must beat to skip
	// the replay.
	runnerUp      uint64
	runnerUpKnown bool
}

// loserEntry is a source and the code of its next key.
type loserEntry struct {
	code uint64
	src  int
}

// newLoserTree returns a tree over k sources, each with no rows until
// seeded.
func newLoserTree(k int) loserTree {
	entries := make([]loserEntry, 3*k)
	t := loserTree{
		keys:  make([]string, k),
		nodes: entries[:k:k],
		first: entries[k:],
		win:   loserEntry{code: ovcExhausted},
	}
	for i := range k {
		t.first[k+i] = loserEntry{code: ovcExhausted, src: i}
	}

	return t
}

// seed makes key source src's first, coded against "".
func (t *loserTree) seed(src int, key string) {
	t.keys[src] = key
	t.first[len(t.keys)+src].code = ovcAt(key, 0)
}

// start plays the first matches between the seeded keys.
func (t *loserTree) start() {
	k := len(t.keys)
	if k == 0 {
		return
	}
	for p := k - 1; p > 0; p-- {
		x, y := t.first[2*p], t.first[2*p+1]
		if y.code < x.code || (y.code == x.code && !t.breakTie(&x, &y)) {
			x, y = y, x
		}
		t.first[p], t.nodes[p] = x, y
	}
	t.win = t.first[1]
}

// replace makes key, coded against the winner's key, the winner's
// source's next key, and finds the new winner.
func (t *loserTree) replace(key string, code uint64) {
	t.keys[t.win.src] = key
	t.win.code = code
	if t.runnerUpKnown && code < t.runnerUp {
		return
	}
	w := t.win
	least, won := uint64(ovcExhausted), true
	for p := (len(t.keys) + w.src) / 2; p > 0; p /= 2 {
		n := &t.nodes[p]
		if n.code < w.code || (n.code == w.code && !t.breakTie(&w, n)) {
			*n, w = w, *n
			won = false
		}
		least = min(least, n.code)
	}
	// When the replaying key won every match this is still the winner's
	// path; otherwise the new winner's path leaves it below the node
	// where it took over, through nodes not read here.
	t.win, t.runnerUp, t.runnerUpKnown = w, least, won
}

// breakTie decides a match between x and y, whose codes against the
// same base are equal, and recodes the loser against the winner: true
// if x sorts no later than y.
func (t *loserTree) breakTie(x, y *loserEntry) bool {
	if x.code == ovcExhausted || x.code&(1<<ovcValueBits-1) == 0 {
		// Both are exhausted, or both equal the base.
		return true
	}
	a, b := t.keys[x.src], t.keys[y.src]
	d := ovcOffset(x.code) + 1
	d += sharedPrefixLen(a[d:], b[d:])
	if d == len(a) || (d < len(b) && a[d] < b[d]) {
		y.code = ovcAt(b, d)

		return true
	}
	x.code = ovcAt(a, d)

	return false
}

// NewMergeIterator creates a merge iterator from multiple run file paths.
// The caller is responsible for calling Close() to release resources.
func NewMergeIterator(paths []string, bufferSize int) (*MergeIterator, error) {
	readers := make([]*CompressedRunReader, 0, len(paths))
	for _, path := range paths {
		r, err := OpenCompressedRunFile(path, bufferSize)
		if err != nil {
			for _, opened := range readers {
				opened.Close()
			}

			return nil, err
		}
		readers = append(readers, r)
	}

	return newMergeIterator(fileSources(readers))
}

// newMergeIterator merges already-open sources. On error it closes them.
func newMergeIterator(readers []runSource) (*MergeIterator, error) {
	k := len(readers)
	// The most rows live at once: each reader's head, the row being
	// returned, and a duplicate of it taken out to be merged into it.
	rows := make([]PrefixRow, k+2)
	ptrs := make([]*PrefixRow, k+len(rows))
	m := &MergeIterator{
		readers: readers,
		heads:   ptrs[:k:k],
		free:    ptrs[k:],
		tree:    newLoserTree(k),
	}
	for i := range rows {
		m.free[i] = &rows[i]
	}
	for i, r := range readers {
		row := m.freeRow()
		_, err := r.readNext(row)
		switch {
		case err == nil:
			m.heads[i] = row
			m.tree.seed(i, row.Prefix)
		case errors.Is(err, io.EOF):
			m.free = append(m.free, row)
		default:
			m.Close()

			return nil, fmt.Errorf("seed reader %d: %w", i, err)
		}
	}
	m.tree.start()

	return m, nil
}

// Next returns the next merged PrefixRow in sorted order.
// Returns io.EOF when all rows have been consumed.
// Duplicate prefixes from different run files are automatically merged.
// The returned row is valid until the following call to Next; callers
// that keep it longer must copy it.
func (m *MergeIterator) Next() (*PrefixRow, error) {
	if m.last != nil {
		m.free = append(m.free, m.last)
		m.last = nil
	}
	if m.err != nil {
		return nil, m.err
	}

	if m.tree.win.code == ovcExhausted {
		return nil, io.EOF
	}

	result, err := m.takeTop()
	if err != nil {
		m.err = err

		return nil, err
	}
	// The new winner is coded against result: equal to it, it differs
	// nowhere before result ends.
	for dup := ovcCode(len(result.Prefix), 0); m.tree.win.code == dup; {
		row, err := m.takeTop()
		if err != nil {
			m.err = err

			return nil, err
		}
		result.Merge(row)
		m.free = append(m.free, row)
	}

	m.last = result

	return result, nil
}

// takeTop returns the winner's row and puts its reader's next row in its
// place.
func (m *MergeIterator) takeTop() (*PrefixRow, error) {
	src := m.tree.win.src
	top := m.heads[src]
	row := m.freeRow()
	shared, err := m.readers[src].readNext(row)
	switch {
	case err == nil:
		code, sorted := ovcAfter(top.Prefix, row.Prefix, shared)
		if !sorted {
			m.free = append(m.free, row)

			return nil, fmt.Errorf("advance reader %d: %w: %q after %q", src, ErrUnsortedInput, row.Prefix, top.Prefix)
		}
		m.heads[src] = row
		m.tree.replace(row.Prefix, code)
	case errors.Is(err, io.EOF):
		m.free = append(m.free, row)
		m.heads[src] = nil
		m.tree.replace("", ovcExhausted)
	default:
		m.free = append(m.free, row)

		return nil, fmt.Errorf("advance reader %d: %w", src, err)
	}

	return top, nil
}

// freeRow returns a row to read into: a recycled one when there is one.
func (m *MergeIterator) freeRow() *PrefixRow {
	if n := len(m.free); n > 0 {
		row := m.free[n-1]
		m.free = m.free[:n-1]

		return row
	}

	return &PrefixRow{}
}

func (m *MergeIterator) Remaining() uint64 {
	var total uint64
	for _, r := range m.readers {
		total += r.Count() - r.ReadCount()
	}

	return total
}

// Close closes all underlying run file readers.
func (m *MergeIterator) Close() error {
	errs := make([]error, 0, len(m.readers))
	for _, r := range m.readers {
		if err := r.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	return errors.Join(errs...)
}
