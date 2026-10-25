package indexread

import (
	"fmt"
	"slices"
)

// TopMetric selects which statistic ranks results in Top queries.
type TopMetric int

const (
	TopByBytesMetric TopMetric = iota
	TopByCountMetric
)

// TopResult holds a ranked descendant entry. Pos is the preorder
// position; Stats is the per-prefix object count and total bytes.
type TopResult struct {
	Pos   uint64
	Stats Stats
}

// TopByBytes returns up to limit descendants at the given relative depth
// under prefixPos, ranked by TotalBytes descending. Ties are broken by
// preorder position to keep results stable across calls.
//
// Returns (nil, nil) for invalid inputs (matches DescendantsAtDepth).
func (idx *Index) TopByBytes(prefixPos uint64, relDepth, limit int) ([]TopResult, error) {
	return idx.top(prefixPos, relDepth, limit, TopByBytesMetric, Filter{})
}

// TopByCount returns up to limit descendants at the given relative depth
// under prefixPos, ranked by ObjectCount descending. Ties are broken by
// preorder position.
func (idx *Index) TopByCount(prefixPos uint64, relDepth, limit int) ([]TopResult, error) {
	return idx.top(prefixPos, relDepth, limit, TopByCountMetric, Filter{})
}

// TopFiltered ranks descendants by the chosen metric and applies a Filter
// before ranking. Useful for "biggest prefixes with at least N objects.".
func (idx *Index) TopFiltered(prefixPos uint64, relDepth, limit int, metric TopMetric, filter Filter) ([]TopResult, error) {
	return idx.top(prefixPos, relDepth, limit, metric, filter)
}

func (idx *Index) top(prefixPos uint64, relDepth, limit int, metric TopMetric, filter Filter) ([]TopResult, error) {
	if limit <= 0 {
		return nil, nil
	}

	positions, err := idx.DescendantsAtDepth(prefixPos, relDepth)
	if err != nil {
		return nil, fmt.Errorf("descendants for top: %w", err)
	}
	if len(positions) == 0 {
		return nil, nil
	}

	h := &topHeap{metric: metric, items: make([]TopResult, 0, min(limit, len(positions)))}
	for _, pos := range positions {
		s := idx.Stats(pos)
		if s.ObjectCount < filter.MinCount || s.TotalBytes < filter.MinBytes {
			continue
		}
		r := TopResult{Pos: pos, Stats: s}
		if len(h.items) < limit {
			h.push(r)

			continue
		}
		if h.less(r, h.items[0]) {
			continue
		}
		h.items[0] = r
		h.down(0)
	}

	// pop yields the heap-min (smallest metric, with larger Pos
	// preferred on ties — see less). Filling out from the tail forward
	// puts the largest metric at index 0 and ties at the same metric
	// land in ascending-position order, which is the contract.
	out := make([]TopResult, len(h.items))
	for i := range slices.Backward(out) {
		out[i] = h.pop()
	}

	return out, nil
}

// topHeap is a min-heap over the chosen metric; the smallest element is
// at index 0 so we can cheaply test "is this candidate big enough to
// displace the current minimum?" while keeping the top-K. It is typed
// rather than a container/heap.Interface so pushing a result doesn't
// box it.
type topHeap struct {
	items  []TopResult
	metric TopMetric
}

// less reports whether a ranks below b on the chosen metric. Position
// tie-breaks favor lower preorder positions (stable order).
func (h *topHeap) less(a, b TopResult) bool {
	switch h.metric {
	case TopByCountMetric:
		if a.Stats.ObjectCount != b.Stats.ObjectCount {
			return a.Stats.ObjectCount < b.Stats.ObjectCount
		}
	default:
		if a.Stats.TotalBytes != b.Stats.TotalBytes {
			return a.Stats.TotalBytes < b.Stats.TotalBytes
		}
	}

	return a.Pos > b.Pos
}

func (h *topHeap) push(r TopResult) {
	h.items = append(h.items, r)
	h.up(len(h.items) - 1)
}

func (h *topHeap) pop() TopResult {
	n := len(h.items) - 1
	top := h.items[0]
	h.items[0] = h.items[n]
	h.items = h.items[:n]
	if n > 0 {
		h.down(0)
	}

	return top
}

func (h *topHeap) up(i int) {
	for i > 0 {
		parent := (i - 1) / 2
		if !h.less(h.items[i], h.items[parent]) {
			return
		}
		h.items[i], h.items[parent] = h.items[parent], h.items[i]
		i = parent
	}
}

func (h *topHeap) down(i int) {
	n := len(h.items)
	for {
		smallest, left := i, 2*i+1
		if left < n && h.less(h.items[left], h.items[smallest]) {
			smallest = left
		}
		if right := left + 1; right < n && h.less(h.items[right], h.items[smallest]) {
			smallest = right
		}
		if smallest == i {
			return
		}
		h.items[i], h.items[smallest] = h.items[smallest], h.items[i]
		i = smallest
	}
}
