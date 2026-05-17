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
// Readers use the RunReader interface so both compressed and raw run
// files work uniformly.
type MergeIterator struct {
	readers []RunReader
	heap    typedMergeHeap
	err     error
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

// mergeHeap is an alias used by parallel_merge.go's heap helpers.
type mergeHeap = typedMergeHeap

func (h *typedMergeHeap) Less(i, j int) bool {
	return h.items[i].row.Prefix < h.items[j].row.Prefix
}

func (h *typedMergeHeap) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
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

// NewMergeIterator creates a merge iterator from multiple run file
// paths, auto-detecting compressed vs raw via OpenRunFileAuto.
// The caller is responsible for calling Close() to release resources.
func NewMergeIterator(paths []string, bufferSize int) (*MergeIterator, error) {
	if len(paths) == 0 {
		return &MergeIterator{}, nil
	}

	readers := make([]RunReader, 0, len(paths))
	for _, path := range paths {
		r, err := OpenRunFileAuto(path, bufferSize)
		if err != nil {
			for _, opened := range readers {
				opened.Close()
			}

			return nil, err
		}
		readers = append(readers, r)
	}

	m := &MergeIterator{
		readers: readers,
		heap:    typedMergeHeap{items: make([]mergeItem, 0, len(readers))},
	}

	for i, r := range readers {
		row, err := r.Read()
		if errors.Is(err, io.EOF) {
			continue // empty reader
		}
		if err != nil {
			m.Close()

			return nil, fmt.Errorf("seed reader %d: %w", i, err)
		}
		m.heap.push(mergeItem{row: row, readerIdx: i})
	}

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

	item := m.heap.pop()
	result := item.row

	if err := m.advanceReader(item.readerIdx); err != nil && !errors.Is(err, io.EOF) {
		m.err = err

		return nil, err
	}

	for m.heap.Len() > 0 && m.heap.items[0].row.Prefix == result.Prefix {
		dup := m.heap.pop()
		result.Merge(dup.row)

		if err := m.advanceReader(dup.readerIdx); err != nil && !errors.Is(err, io.EOF) {
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
