package extsort

import (
	"errors"
	"fmt"
	"io"
)

// MergeIterator provides a k-way merge of sorted run files.
// It reads from multiple run files and yields PrefixRows in globally sorted order,
// automatically merging duplicates (same prefix from different runs).
//
// Rows are recycled through a per-iterator free list: at most one row
// per reader plus the last result are live, so a merge of any length
// allocates only those.
type MergeIterator struct {
	err     error
	readers []*CompressedRunReader
	heap    typedMergeHeap
	free    []*PrefixRow
	last    *PrefixRow
}

// mergeItem represents an item in the merge heap.
type mergeItem struct {
	row       *PrefixRow
	readerIdx int // index into readers slice
}

// typedMergeHeap is a typed binary min-heap of mergeItem values, ordered
// by mergeItem.row.Prefix. It avoids the per-op allocations that
// container/heap incurs through its interface{}-typed Push/Pop API.
type typedMergeHeap struct {
	items []mergeItem
}

func (h *typedMergeHeap) Len() int { return len(h.items) }

func (h *typedMergeHeap) push(it mergeItem) {
	h.items = append(h.items, it)
	h.siftUp(len(h.items) - 1)
}

func (h *typedMergeHeap) pop() mergeItem {
	top := h.items[0]
	n := len(h.items) - 1
	h.items[0] = h.items[n]
	h.items = h.items[:n]
	if n > 0 {
		h.siftDown(0)
	}

	return top
}

func (h *typedMergeHeap) siftUp(i int) {
	for i > 0 {
		parent := (i - 1) / 2
		if h.items[parent].row.Prefix <= h.items[i].row.Prefix {
			return
		}
		h.items[parent], h.items[i] = h.items[i], h.items[parent]
		i = parent
	}
}

func (h *typedMergeHeap) siftDown(i int) {
	n := len(h.items)
	for {
		left := 2*i + 1
		if left >= n {
			return
		}
		smallest := left
		right := left + 1
		if right < n && h.items[right].row.Prefix < h.items[left].row.Prefix {
			smallest = right
		}
		if h.items[i].row.Prefix <= h.items[smallest].row.Prefix {
			return
		}
		h.items[i], h.items[smallest] = h.items[smallest], h.items[i]
		i = smallest
	}
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

	return newMergeIterator(readers)
}

// newMergeIterator merges already-open readers. On error it closes them.
func newMergeIterator(readers []*CompressedRunReader) (*MergeIterator, error) {
	m := &MergeIterator{
		readers: readers,
		heap:    typedMergeHeap{items: make([]mergeItem, 0, len(readers))},
		free:    make([]*PrefixRow, 0, len(readers)+1),
	}

	for i := range readers {
		if err := m.advanceReader(i); err != nil && !errors.Is(err, io.EOF) {
			m.Close()

			return nil, fmt.Errorf("seed reader %d: %w", i, err)
		}
	}

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

	if m.heap.Len() == 0 {
		return nil, io.EOF
	}

	item := m.heap.pop()
	result := item.row

	if err := m.advanceReader(item.readerIdx); err != nil && !errors.Is(err, io.EOF) {
		m.err = err

		return nil, err
	}

	for m.heap.Len() > 0 && m.heap.items[0].row.Prefix == result.Prefix {
		dup := m.heap.pop()
		result.Merge(dup.row)
		m.free = append(m.free, dup.row)

		if err := m.advanceReader(dup.readerIdx); err != nil && !errors.Is(err, io.EOF) {
			m.err = err

			return nil, err
		}
	}

	m.last = result

	return result, nil
}

// advanceReader reads the next row from the given reader and pushes to heap.
func (m *MergeIterator) advanceReader(idx int) error {
	var row *PrefixRow
	if n := len(m.free); n > 0 {
		row = m.free[n-1]
		m.free = m.free[:n-1]
	} else {
		row = &PrefixRow{}
	}
	if err := m.readers[idx].ReadInto(row); err != nil {
		m.free = append(m.free, row)
		if errors.Is(err, io.EOF) {
			return io.EOF
		}

		return fmt.Errorf("advance reader %d: %w", idx, err)
	}
	m.heap.push(mergeItem{row: row, readerIdx: idx})

	return nil
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
