package indexread

import (
	"fmt"
	"slices"

	"github.com/eunmann/s3-inv-db/pkg/format"
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
	return idx.RankDescendants(prefixPos, relDepth, limit, metric, false, filter)
}

func (idx *Index) top(prefixPos uint64, relDepth, limit int, metric TopMetric, filter Filter) ([]TopResult, error) {
	return idx.RankDescendants(prefixPos, relDepth, limit, metric, false, filter)
}

// rankBlockMin is the fewest depth blocks a span needs before ranking
// consults their bounds; below it a plain scan reads about as much.
const rankBlockMin = 8

// RankDescendants returns up to limit of prefixPos's descendants at
// relDepth that pass filter, ordered by metric, largest first or
// smallest first when asc, ties by position (name order).
//
// It reads only the depth blocks (format.DepthBlockSize entries each)
// whose bounds let them place: a first pass over the bounds of the
// blocks inside the span finds a value limit blocks are sure to reach,
// and the scan skips any block whose bound falls short of it or of the
// limit-th result so far. That is at most about limit blocks of stats,
// plus one bound per block of the span, unless many blocks' maximums
// lie within the bounds' rounding (none below 2048, else up to 0.1%)
// of the limit-th result: the bounds can't rule those out, so they are
// scanned too, up to the whole span.
func (idx *Index) RankDescendants(prefixPos uint64, relDepth, limit int, metric TopMetric, asc bool, filter Filter) ([]TopResult, error) {
	if limit <= 0 {
		return nil, nil
	}
	span, depth, err := idx.descendantSpanAt(prefixPos, relDepth)
	if err != nil {
		return nil, fmt.Errorf("descendants to rank: %w", err)
	}
	if span.Len() == 0 {
		return nil, nil
	}

	o := rankOrder{metric: metric, asc: asc}
	h := &topHeap{items: make([]rankItem, 0, min(uint64(limit), span.Len()))}
	scan := func(e format.Span) {
		for j := e.Lo; j < e.Hi; j++ {
			pos := idx.depthIndex.PositionAt(j)
			s := idx.Stats(pos)
			if s.ObjectCount < filter.MinCount || s.TotalBytes < filter.MinBytes {
				continue
			}
			h.offer(rankItem{key: o.key(s), pos: pos}, limit)
		}
	}

	blocks := idx.depthIndex.SpanBlocks(depth, span)
	if blocks.Len() < rankBlockMin {
		scan(span)
	} else {
		sure := o.sureKey(blocks, limit, filter, h)
		for i := range blocks.Len() {
			b := blocks.Bounds(i)
			if b.MaxCount < filter.MinCount || b.MaxBytes < filter.MinBytes {
				continue
			}
			// Blocks go in position order, so a key equal to the
			// limit-th result's comes later and loses the tie.
			best := o.keyBound(b)
			if best < sure || (len(h.items) == limit && best <= h.items[0].key) {
				continue
			}
			scan(blocks.Entries(i))
		}
	}

	// pop yields the heap-min (smallest key, with larger Pos preferred
	// on ties — see less). Filling out from the tail forward puts the
	// best result at index 0 and ties at the same key land in
	// ascending-position order, which is the contract.
	out := make([]TopResult, len(h.items))
	for i := range slices.Backward(out) {
		pos := h.pop().pos
		out[i] = TopResult{Pos: pos, Stats: idx.Stats(pos)}
	}

	return out, nil
}

// rankOrder maps a ranking to keys where larger always ranks first.
type rankOrder struct {
	metric TopMetric
	asc    bool
}

func (o rankOrder) key(s Stats) uint64 {
	v := s.TotalBytes
	if o.metric == TopByCountMetric {
		v = s.ObjectCount
	}
	if o.asc {
		return ^v
	}

	return v
}

// keyBound is the largest key any entry of a block with bounds b can
// have.
func (o rankOrder) keyBound(b format.BlockBounds) uint64 {
	switch {
	case o.metric == TopByCountMetric && o.asc:
		return ^b.MinCount
	case o.metric == TopByCountMetric:
		return b.MaxCount
	case o.asc:
		return ^b.MinBytes
	default:
		return b.MaxBytes
	}
}

// keyFloor is a key some entry of a whole block with bounds b is sure
// to reach: its best entry's.
func (o rankOrder) keyFloor(b format.BlockBounds) uint64 {
	switch {
	case o.metric == TopByCountMetric && o.asc:
		return ^format.MinCeil(b.MinCount)
	case o.metric == TopByCountMetric:
		return format.MaxFloor(b.MaxCount)
	case o.asc:
		return ^format.MinCeil(b.MinBytes)
	default:
		return format.MaxFloor(b.MaxBytes)
	}
}

// sureKey returns a key at least limit entries of the span reach: the
// limit-th largest keyFloor among the blocks wholly inside it, each
// holding a different such entry. Any block whose keyBound is below it
// holds nothing that can place. It is 0 (skips nothing) with a filter,
// which those entries might fail, or with fewer than limit such blocks.
// It borrows scratch's storage, the empty result heap, and leaves it
// empty.
func (o rankOrder) sureKey(blocks format.SpanBlocks, limit int, filter Filter, scratch *topHeap) uint64 {
	if filter != (Filter{}) || blocks.Len() < uint64(limit) {
		return 0
	}
	floors := scratch
	defer func() { floors.items = floors.items[:0] }()
	for i := range blocks.Len() {
		if blocks.Entries(i).Len() < format.DepthBlockSize {
			continue
		}
		floors.offer(rankItem{key: o.keyFloor(blocks.Bounds(i))}, limit)
	}
	if len(floors.items) < limit {
		return 0
	}

	return floors.items[0].key
}

// rankItem is one ranked entry: larger keys rank first, ties go to the
// lower position.
type rankItem struct {
	key uint64
	pos uint64
}

// topHeap is a min-heap of the best entries so far; the worst is at
// index 0 so we can cheaply test "is this candidate good enough to
// displace the current minimum?" while keeping the top-K. It is typed
// rather than a container/heap.Interface so pushing an entry doesn't
// box it.
type topHeap struct {
	items []rankItem
}

// less reports whether a ranks below b. Position tie-breaks favor lower
// preorder positions (stable order).
func (h *topHeap) less(a, b rankItem) bool {
	if a.key != b.key {
		return a.key < b.key
	}

	return a.pos > b.pos
}

// offer keeps r if it ranks among the best limit entries.
func (h *topHeap) offer(r rankItem, limit int) {
	if len(h.items) < limit {
		h.push(r)

		return
	}
	if h.less(r, h.items[0]) {
		return
	}
	h.items[0] = r
	h.down(0)
}

func (h *topHeap) push(r rankItem) {
	h.items = append(h.items, r)
	h.up(len(h.items) - 1)
}

func (h *topHeap) pop() rankItem {
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
