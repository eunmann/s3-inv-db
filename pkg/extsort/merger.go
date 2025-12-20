package extsort

import (
	"container/heap"
	"io"
)

// MergeIterator provides a k-way merge of sorted run files.
// It reads from multiple run files and yields PrefixRows in globally sorted order,
// automatically merging duplicates (same prefix from different runs).
type MergeIterator struct {
	readers []*RunFileReader
	heap    *mergeHeap
	err     error
}

// mergeItem represents an item in the merge heap.
type mergeItem struct {
	row       *PrefixRow
	readerIdx int // index into readers slice
}

// mergeHeap implements heap.Interface for k-way merge.
type mergeHeap struct {
	items []mergeItem
}

func (h *mergeHeap) Len() int { return len(h.items) }

func (h *mergeHeap) Less(i, j int) bool {
	return h.items[i].row.Prefix < h.items[j].row.Prefix
}

func (h *mergeHeap) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
}

func (h *mergeHeap) Push(x interface{}) {
	h.items = append(h.items, x.(mergeItem))
}

func (h *mergeHeap) Pop() interface{} {
	old := h.items
	n := len(old)
	item := old[n-1]
	h.items = old[0 : n-1]
	return item
}

// NewMergeIterator creates a merge iterator from multiple run file paths.
// The caller is responsible for calling Close() to release resources.
func NewMergeIterator(paths []string, bufferSize int) (*MergeIterator, error) {
	if len(paths) == 0 {
		return &MergeIterator{}, nil
	}

	readers := make([]*RunFileReader, 0, len(paths))
	for _, path := range paths {
		r, err := OpenRunFile(path, bufferSize)
		if err != nil {
			// Close any already opened readers
			for _, opened := range readers {
				opened.Close()
			}
			return nil, err
		}
		readers = append(readers, r)
	}

	return NewMergeIteratorFromReaders(readers)
}

// NewMergeIteratorFromReaders creates a merge iterator from existing readers.
// Takes ownership of the readers; they will be closed when the iterator is closed.
func NewMergeIteratorFromReaders(readers []*RunFileReader) (*MergeIterator, error) {
	m := &MergeIterator{
		readers: readers,
		heap:    &mergeHeap{items: make([]mergeItem, 0, len(readers))},
	}

	// Initialize heap with first row from each reader
	for i, r := range readers {
		row, err := r.Read()
		if err == io.EOF {
			continue // empty reader
		}
		if err != nil {
			m.Close()
			return nil, err
		}
		heap.Push(m.heap, mergeItem{row: row, readerIdx: i})
	}

	heap.Init(m.heap)
	return m, nil
}

// Next returns the next merged PrefixRow in sorted order.
// Returns io.EOF when all rows have been consumed.
// Duplicate prefixes from different run files are automatically merged.
func (m *MergeIterator) Next() (*PrefixRow, error) {
	if m.err != nil {
		return nil, m.err
	}

	if m.heap.Len() == 0 {
		return nil, io.EOF
	}

	// Pop the smallest item
	item := heap.Pop(m.heap).(mergeItem)
	result := item.row

	// Read next row from the same reader and push to heap
	if err := m.advanceReader(item.readerIdx); err != nil && err != io.EOF {
		m.err = err
		return nil, err
	}

	// Merge any duplicates (same prefix from other readers)
	for m.heap.Len() > 0 && m.heap.items[0].row.Prefix == result.Prefix {
		dup := heap.Pop(m.heap).(mergeItem)
		result.Merge(dup.row)

		// Advance that reader too
		if err := m.advanceReader(dup.readerIdx); err != nil && err != io.EOF {
			m.err = err
			return nil, err
		}
	}

	return result, nil
}

// advanceReader reads the next row from the given reader and pushes to heap.
func (m *MergeIterator) advanceReader(idx int) error {
	row, err := m.readers[idx].Read()
	if err != nil {
		return err
	}
	heap.Push(m.heap, mergeItem{row: row, readerIdx: idx})
	return nil
}

// Remaining returns an estimate of remaining rows to process.
func (m *MergeIterator) Remaining() uint64 {
	var total uint64
	for _, r := range m.readers {
		total += r.Count() - r.ReadCount()
	}
	return total
}

// Close closes all underlying run file readers.
func (m *MergeIterator) Close() error {
	var firstErr error
	for _, r := range m.readers {
		if err := r.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// RemoveAll closes all readers and removes their files.
func (m *MergeIterator) RemoveAll() error {
	var firstErr error
	for _, r := range m.readers {
		if err := r.Remove(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
