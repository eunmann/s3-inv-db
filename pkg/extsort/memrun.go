package extsort

import (
	"io"
	"math"
)

// memRun is a sorted run held in memory: a worker's last aggregator
// state, which the final merge reads in place instead of writing it to
// a run file and reading it back. Its rows (and the aggregator slabs
// behind them) are released as soon as the merge has read the last one.
type memRun struct {
	rows []*PrefixRow
	// shared[i] is how many bytes rows[i]'s prefix shares with the one
	// before it, up to math.MaxUint16: the front-coding a run file
	// stores, so the merge need not compare the prefixes to code them.
	shared []uint16
	next   int
	count  int
}

// newMemRun finds how much each row's prefix shares with the one before
// it. A worker builds its run as it finishes, in parallel with the
// others, so this costs the serial final merge nothing.
func newMemRun(rows []*PrefixRow) *memRun {
	shared := make([]uint16, len(rows))
	for i := 1; i < len(rows); i++ {
		shared[i] = uint16(min(sharedPrefixLen(rows[i-1].Prefix, rows[i].Prefix), math.MaxUint16))
	}

	return &memRun{rows: rows, shared: shared, count: len(rows)}
}

// readNext copies the next row into into.
func (m *memRun) readNext(into *PrefixRow) (int, error) {
	if m.next >= m.count {
		m.rows, m.shared = nil, nil

		return 0, io.EOF
	}
	*into = *m.rows[m.next]
	shared := int(m.shared[m.next])
	m.next++

	return shared, nil
}

func (m *memRun) Count() uint64     { return uint64(m.count) }
func (m *memRun) ReadCount() uint64 { return uint64(m.next) }

// Close releases the rows.
func (m *memRun) Close() error {
	m.rows, m.shared = nil, nil

	return nil
}
