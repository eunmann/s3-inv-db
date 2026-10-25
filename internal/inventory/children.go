package inventory

import (
	"cmp"
	"fmt"
	"math"
	"slices"

	"github.com/eunmann/s3-inv-db/pkg/format"
	"github.com/eunmann/s3-inv-db/pkg/indexread"
)

// ChildQuery selects one sorted page of a prefix's immediate children.
type ChildQuery struct {
	SortBy   string
	Dir      string
	Page     int
	PageSize int
}

// ChildPage is one page of children as index positions, in display
// order, plus the paging summary over all of them.
type ChildPage struct {
	Positions  []uint64
	Pagination BrowsePagination
	Total      int
}

// CostFunc returns a child's monthly cost in microdollars.
type CostFunc func(pos uint64) uint64

type childKey struct {
	pos    uint64
	metric uint64
}

// PageChildren sorts the immediate children of parentPos and returns the
// requested page; callers materialise rows for the returned page only.
//
// Name order is S3 key order: each name with its trailing slash, as S3
// lists them ("2024-01/" before "2024/" because '-' < '/'). That is the
// index's own order (the build rejects any other, #81), so a name-sorted
// page is read straight from the depth index: O(log n + page) however
// many children there are. The other columns read one number per child
// and order only the rows up to the page's last; ties sort by name,
// ascending. Cost is
// called once per child, and only when sorting by cost.
func PageChildren(idx *indexread.Index, parentPos uint64, q ChildQuery, cost CostFunc) (ChildPage, error) {
	return (*LevelCache)(nil).PageChildren(idx, parentPos, q, cost)
}

// PageChildren is the package PageChildren, keeping the full order of a
// large level once it is viewed a second time (see LevelCache), so
// later pages of it are O(log n + page).
func (c *LevelCache) PageChildren(idx *indexread.Index, parentPos uint64, q ChildQuery, cost CostFunc) (ChildPage, error) {
	if q.SortBy == SortColSegment || q.SortBy == "" {
		return pageChildrenByName(idx, parentPos, q)
	}
	span, err := idx.DescendantSpan(parentPos, 1)
	if err != nil {
		return ChildPage{}, fmt.Errorf("children: %w", err)
	}
	desc := q.Dir == SortDirDesc
	key := levelKey{a: idx.Identity(), posA: parentPos, okA: true, sortBy: q.SortBy, dir: q.Dir}
	cached, ok := c.browseOrder(key, span.Len(), func() []uint32 {
		keys := childKeys(idx, span, q.SortBy, cost, true)
		radixSortKeys(keys, desc)
		order := make([]uint32, len(keys))
		for i, k := range keys {
			order[i] = uint32(k.pos)
		}

		return order
	})
	if ok {
		p := Paginate(len(cached), q.Page, q.PageSize)
		page := ChildPage{Pagination: p, Total: len(cached)}
		if p.FirstRow > 0 {
			page.Positions = make([]uint64, 0, p.LastRow-p.FirstRow+1)
			for _, off := range cached[p.FirstRow-1 : p.LastRow] {
				page.Positions = append(page.Positions, idx.SpanPosition(span, uint64(off)))
			}
		}

		return page, nil
	}

	if page, ok, err := rankedPage(idx, parentPos, span, q, desc); ok || err != nil {
		return page, err
	}

	keys := childKeys(idx, span, q.SortBy, cost, false)
	// Ties break by position, which is name order: the order is total,
	// so ordering only part of it is exact.
	order := func(a, b childKey) int {
		c := cmp.Compare(a.metric, b.metric)
		if desc {
			c = -c
		}
		if c != 0 {
			return c
		}

		return cmp.Compare(a.pos, b.pos)
	}

	p := Paginate(len(keys), q.Page, q.PageSize)
	page := ChildPage{Pagination: p, Total: len(keys)}
	if p.FirstRow > 0 {
		// Only the rows up to the page's last are ordered.
		sorted := firstSorted(keys, p.LastRow, order)
		page.Positions = make([]uint64, 0, p.LastRow-p.FirstRow+1)
		for _, k := range sorted[p.FirstRow-1 : p.LastRow] {
			page.Positions = append(page.Positions, k.pos)
		}
	}

	return page, nil
}

// rankedPage serves an early page by size or objects from the index's
// depth-block bounds (indexread.RankDescendants), usually reading about
// a page's worth of blocks instead of every child. It reports false for the
// other columns and for pages deep enough that ordering every child
// costs about the same.
func rankedPage(idx *indexread.Index, parentPos uint64, span format.Span, q ChildQuery, desc bool) (ChildPage, bool, error) {
	var metric indexread.TopMetric
	switch q.SortBy {
	case SortColSize:
		metric = indexread.TopByBytesMetric
	case SortColObjects:
		metric = indexread.TopByCountMetric
	default:
		return ChildPage{}, false, nil
	}
	total := int(span.Len())
	p := Paginate(total, q.Page, q.PageSize)
	if p.LastRow*8 > total {
		return ChildPage{}, false, nil
	}
	page := ChildPage{Pagination: p, Total: total}
	if p.FirstRow == 0 {
		return page, true, nil
	}
	ranked, err := idx.RankDescendants(parentPos, 1, p.LastRow, metric, !desc, indexread.Filter{})
	if err != nil {
		return ChildPage{}, false, fmt.Errorf("children: %w", err)
	}
	page.Positions = make([]uint64, 0, p.LastRow-p.FirstRow+1)
	for _, r := range ranked[p.FirstRow-1:] {
		page.Positions = append(page.Positions, r.Pos)
	}

	return page, true, nil
}

// childKeys reads the sort metric of every child in span. Keys carry
// the child's position, or its offset in the span when offsets is set;
// either way they are in name order.
func childKeys(idx *indexread.Index, span format.Span, sortBy string, cost CostFunc, offsets bool) []childKey {
	keys := make([]childKey, span.Len())
	for i := range keys {
		pos := idx.SpanPosition(span, uint64(i))
		keys[i] = childKey{pos: pos, metric: childMetric(idx, pos, sortBy, cost)}
		if offsets {
			keys[i].pos = uint64(i)
		}
	}

	return keys
}

// radixSortKeys orders keys by metric (descending when desc), keeping
// equal metrics in their current order: a stable LSD radix sort with one
// pass per byte that differs between keys. Keys arrive in name order, so
// ties stay in name order, as the comparison sort's position tiebreak
// orders them.
func radixSortKeys(keys []childKey, desc bool) {
	if len(keys) < 2 {
		return
	}
	var flip uint64
	if desc {
		flip = math.MaxUint64
	}
	anyBits, allBits := uint64(0), uint64(math.MaxUint64)
	for _, k := range keys {
		m := k.metric ^ flip
		anyBits |= m
		allBits &= m
	}
	varying := anyBits ^ allBits
	src, dst := keys, make([]childKey, len(keys))
	for shift := uint(0); shift < 64; shift += 8 {
		if (varying>>shift)&0xff == 0 {
			continue
		}
		var start [256]int
		for _, k := range src {
			start[byte((k.metric^flip)>>shift)]++
		}
		sum := 0
		for i := range start {
			start[i], sum = sum, sum+start[i]
		}
		for _, k := range src {
			b := byte((k.metric ^ flip) >> shift)
			dst[start[b]] = k
			start[b]++
		}
		src, dst = dst, src
	}
	if &src[0] != &keys[0] {
		copy(keys, src)
	}
}

// firstSorted returns the first k elements of s in order, reordering s
// in place. When k is small next to len(s) it keeps the k best in a
// bounded max-heap at the front of s, O(n log k), so an early page
// doesn't pay for sorting every row; otherwise it sorts all of s. Order
// must be total (no two elements equal) for the result to be exact.
func firstSorted[T any](s []T, k int, order func(a, b T) int) []T {
	k = min(max(k, 0), len(s))
	if k*8 > len(s) {
		slices.SortFunc(s, order)

		return s[:k]
	}
	if k == 0 {
		return s[:0]
	}
	heap := s[:k]
	for i := k/2 - 1; i >= 0; i-- {
		siftDownMax(heap, i, order)
	}
	for i := k; i < len(s); i++ {
		if order(s[i], heap[0]) < 0 {
			heap[0], s[i] = s[i], heap[0]
			siftDownMax(heap, 0, order)
		}
	}
	slices.SortFunc(heap, order)

	return heap
}

// siftDownMax restores the max-heap property (the element that sorts
// last at the root) below i.
func siftDownMax[T any](h []T, i int, order func(a, b T) int) {
	for {
		largest, left := i, 2*i+1
		if left < len(h) && order(h[left], h[largest]) > 0 {
			largest = left
		}
		if right := left + 1; right < len(h) && order(h[right], h[largest]) > 0 {
			largest = right
		}
		if largest == i {
			return
		}
		h[i], h[largest] = h[largest], h[i]
		i = largest
	}
}

// pageChildrenByName returns a name-sorted page straight from the
// index's child order: the window itself ascending, or the mirrored
// window reversed descending.
func pageChildrenByName(idx *indexread.Index, parentPos uint64, q ChildQuery) (ChildPage, error) {
	span, err := idx.DescendantSpan(parentPos, 1)
	if err != nil {
		return ChildPage{}, fmt.Errorf("children: %w", err)
	}
	total := int(span.Len())
	p := Paginate(total, q.Page, q.PageSize)
	page := ChildPage{Pagination: p, Total: total}
	if p.FirstRow == 0 {
		return page, nil
	}
	offset := p.FirstRow - 1
	if q.Dir == SortDirDesc {
		offset = total - p.LastRow
	}
	page.Positions = idx.PositionsInSpan(span, offset, p.LastRow-p.FirstRow+1)
	if q.Dir == SortDirDesc {
		slices.Reverse(page.Positions)
	}

	return page, nil
}

func childMetric(idx *indexread.Index, pos uint64, sortBy string, cost CostFunc) uint64 {
	switch sortBy {
	case SortColObjects:
		return idx.Stats(pos).ObjectCount
	case SortColSize:
		return idx.Stats(pos).TotalBytes
	case SortColCost:
		if cost != nil {
			return cost(pos)
		}
	}

	return 0
}
