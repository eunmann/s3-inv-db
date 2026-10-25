package inventory

import (
	"cmp"
	"fmt"
	"slices"

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
	if q.SortBy == SortColSegment || q.SortBy == "" {
		return pageChildrenByName(idx, parentPos, q)
	}
	positions, err := idx.DescendantsAtDepth(parentPos, 1)
	if err != nil {
		return ChildPage{}, fmt.Errorf("children: %w", err)
	}
	keys := make([]childKey, len(positions))
	for i, pos := range positions {
		keys[i] = childKey{pos: pos, metric: childMetric(idx, pos, q.SortBy, cost)}
	}
	desc := q.Dir == SortDirDesc
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
	all, err := idx.DescendantsPage(parentPos, 1, indexread.Filter{}, 0, 0)
	if err != nil {
		return ChildPage{}, fmt.Errorf("children: %w", err)
	}
	p := Paginate(all.Total, q.Page, q.PageSize)
	page := ChildPage{Pagination: p, Total: all.Total}
	if p.FirstRow == 0 {
		return page, nil
	}
	offset := p.FirstRow - 1
	if q.Dir == SortDirDesc {
		offset = all.Total - p.LastRow
	}
	window, err := idx.DescendantsPage(parentPos, 1, indexread.Filter{}, offset, p.LastRow-p.FirstRow+1)
	if err != nil {
		return ChildPage{}, fmt.Errorf("children: %w", err)
	}
	if q.Dir == SortDirDesc {
		slices.Reverse(window.Positions)
	}
	page.Positions = window.Positions

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
