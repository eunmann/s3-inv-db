package indexread

import (
	"container/heap"
	"errors"
	"fmt"
	"sort"
)

// errTopHeapBadElement is returned if the heap yields a non-TopResult;
// only reachable via a programming bug in this file, but err113 still
// wants a sentinel rather than a one-shot dynamic error.
var errTopHeapBadElement = errors.New("top heap: unexpected element type")

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

	h := &topHeap{metric: metric, cap: limit}
	for _, pos := range positions {
		s := idx.Stats(pos)
		if s.ObjectCount < filter.MinCount || s.TotalBytes < filter.MinBytes {
			continue
		}
		r := TopResult{Pos: pos, Stats: s}
		if h.Len() < limit {
			heap.Push(h, r)

			continue
		}
		if h.less(r, h.items[0]) {
			continue
		}
		h.items[0] = r
		heap.Fix(h, 0)
	}

	out := make([]TopResult, h.Len())
	for i := len(out) - 1; i >= 0; i-- {
		r, ok := heap.Pop(h).(TopResult)
		if !ok {
			return nil, errTopHeapBadElement
		}
		out[i] = r
	}

	sort.SliceStable(out, func(i, j int) bool {
		return h.less(out[j], out[i])
	})

	return out, nil
}

// topHeap is a min-heap over the chosen metric; the smallest element is
// at index 0 so we can cheaply test "is this candidate big enough to
// displace the current minimum?" while keeping the top-K.
type topHeap struct {
	items  []TopResult
	metric TopMetric
	cap    int
}

func (h *topHeap) Len() int { return len(h.items) }
func (h *topHeap) Less(i, j int) bool {
	return h.less(h.items[i], h.items[j])
}
func (h *topHeap) Swap(i, j int) { h.items[i], h.items[j] = h.items[j], h.items[i] }

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

func (h *topHeap) Push(x any) {
	r, ok := x.(TopResult)
	if !ok {
		return
	}
	h.items = append(h.items, r)
}

func (h *topHeap) Pop() any {
	n := len(h.items)
	r := h.items[n-1]
	h.items = h.items[:n-1]

	return r
}
