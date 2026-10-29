package extsort

import "io"

// memRun is a sorted run held in memory: a worker's last aggregator
// state, which the final merge reads in place instead of writing it to
// a run file and reading it back. Its rows (and the aggregator slabs
// behind them) are released as soon as the merge has read the last one.
type memRun struct {
	rows  []*PrefixRow
	next  int
	count int
}

func newMemRun(rows []*PrefixRow) *memRun {
	return &memRun{rows: rows, count: len(rows)}
}

// readNext copies the next row into into. It reports no bytes shared
// with the previous row: rows held in memory don't record it.
func (m *memRun) readNext(into *PrefixRow) (int, error) {
	if m.next >= m.count {
		m.rows = nil

		return 0, io.EOF
	}
	*into = *m.rows[m.next]
	m.next++

	return 0, nil
}

func (m *memRun) Count() uint64     { return uint64(m.count) }
func (m *memRun) ReadCount() uint64 { return uint64(m.next) }

// Close releases the rows.
func (m *memRun) Close() error {
	m.rows = nil

	return nil
}
