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
// requested page. Sorting reads only numbers — and, for the name sort,
// each child's cached segment — so a page view never builds a string per
// child; callers materialise rows for the returned page only. Cost is
// called once per child, and only when sorting by cost. Ties sort by
// name, ascending.
func PageChildren(idx *indexread.Index, parentPos uint64, q ChildQuery, cost CostFunc) (ChildPage, error) {
	positions, err := idx.DescendantsAtDepth(parentPos, 1)
	if err != nil {
		return ChildPage{}, fmt.Errorf("children: %w", err)
	}
	keys := make([]childKey, len(positions))
	for i, pos := range positions {
		keys[i] = childKey{pos: pos, metric: childMetric(idx, pos, q.SortBy, cost)}
	}
	desc := q.Dir == SortDirDesc
	slices.SortStableFunc(keys, func(a, b childKey) int {
		c := cmp.Compare(a.metric, b.metric)
		if q.SortBy == SortColSegment || q.SortBy == "" || c == 0 {
			c = 0
		} else if desc {
			c = -c
		}
		if c != 0 {
			return c
		}
		byName := cmp.Compare(idx.ChildSegment(a.pos), idx.ChildSegment(b.pos))
		if (q.SortBy == SortColSegment || q.SortBy == "") && desc {
			return -byName
		}

		return byName
	})

	p := Paginate(len(keys), q.Page, q.PageSize)
	page := ChildPage{Pagination: p, Total: len(keys)}
	if p.FirstRow > 0 {
		page.Positions = make([]uint64, 0, p.LastRow-p.FirstRow+1)
		for _, k := range keys[p.FirstRow-1 : p.LastRow] {
			page.Positions = append(page.Positions, k.pos)
		}
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
