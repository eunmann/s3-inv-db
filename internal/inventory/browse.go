package inventory

import (
	"sort"
	"strconv"
	"strings"
)

// Browse-view types and helpers. These describe a single level of the
// prefix trie as rendered by the explorer. They are pure domain values
// (no HTTP, no JSON tags) so the HTTP layer can compose them into its
// response shape and the domain stays test-independent of the renderer.

// Sort column / direction identifiers used in URLs and templates.
const (
	SortColSegment = "segment"
	SortColObjects = "objects"
	SortColSize    = "size"
	SortColCost    = "cost"
	SortDirAsc     = "asc"
	SortDirDesc    = "desc"

	DefaultPageSize = 100
	MaxPageSize     = 500
)

// BrowseCrumb is one segment of the breadcrumb trail.
type BrowseCrumb struct {
	Label  string
	Prefix string
}

// BrowseChild is one immediate-child prefix shown in the explorer.
type BrowseChild struct {
	Segment                 string
	Prefix                  string
	ObjectCount             uint64
	ObjectCountH            string
	TotalBytes              uint64
	TotalBytesH             string
	MonthlyCostMicrodollars uint64
	MonthlyCostFormatted    string
	HasChildren             bool
}

// BrowseSortLink carries the {sort, dir, indicator} bundle a column
// header click should send and display.
type BrowseSortLink struct {
	Sort      string
	Dir       string
	Indicator string
}

// BrowsePagination describes the slice of children rendered for one page.
type BrowsePagination struct {
	Page     int
	PageSize int
	Pages    int
	FirstRow int
	LastRow  int
	PrevPage int
	NextPage int
}

// NormalizePage clamps page (≥1) and size (1..MaxPageSize) from user input.
func NormalizePage(pageStr, sizeStr string) (page, size int) {
	page = 1
	if v, err := strconv.Atoi(pageStr); err == nil && v >= 1 {
		page = v
	}
	size = DefaultPageSize
	if v, err := strconv.Atoi(sizeStr); err == nil && v >= 1 {
		size = v
	}
	if size > MaxPageSize {
		size = MaxPageSize
	}

	return page, size
}

// NormalizeSort clamps sort/dir from user input to known values.
// Unknown column → segment. Unknown direction → segment uses asc, others desc.
func NormalizeSort(sortBy, dir string) (col, direction string) {
	switch sortBy {
	case SortColObjects, SortColSize, SortColCost, SortColSegment:
		col = sortBy
	default:
		col = SortColSegment
	}
	switch dir {
	case SortDirAsc, SortDirDesc:
		direction = dir
	default:
		if col == SortColSegment {
			direction = SortDirAsc
		} else {
			direction = SortDirDesc
		}
	}

	return col, direction
}

// Paginate returns the 1-indexed window for page of total items at pageSize.
func Paginate(total, page, pageSize int) BrowsePagination {
	if total == 0 || pageSize <= 0 {
		return BrowsePagination{Page: 1, PageSize: pageSize}
	}
	pages := (total + pageSize - 1) / pageSize
	if page > pages {
		page = pages
	}
	if page < 1 {
		page = 1
	}
	first := (page-1)*pageSize + 1
	last := min(first+pageSize-1, total)
	p := BrowsePagination{
		Page:     page,
		PageSize: pageSize,
		Pages:    pages,
		FirstRow: first,
		LastRow:  last,
	}
	if page > 1 {
		p.PrevPage = page - 1
	}
	if page < pages {
		p.NextPage = page + 1
	}

	return p
}

// SortChildren orders children in place. Ties resolve by segment-asc.
func SortChildren(children []BrowseChild, sortBy, dir string) {
	less := func(i, j int) bool {
		a, b := &children[i], &children[j]
		var primary bool
		var equal bool
		switch sortBy {
		case SortColObjects:
			primary = a.ObjectCount < b.ObjectCount
			equal = a.ObjectCount == b.ObjectCount
		case SortColSize:
			primary = a.TotalBytes < b.TotalBytes
			equal = a.TotalBytes == b.TotalBytes
		case SortColCost:
			primary = a.MonthlyCostMicrodollars < b.MonthlyCostMicrodollars
			equal = a.MonthlyCostMicrodollars == b.MonthlyCostMicrodollars
		default:
			primary = a.Segment < b.Segment
			equal = a.Segment == b.Segment
		}
		if equal {
			return a.Segment < b.Segment
		}
		if dir == SortDirDesc {
			return !primary
		}

		return primary
	}
	sort.SliceStable(children, less)
}

// SortLinks builds the per-column {sort, dir, indicator} bundle for the
// column headers. The active column toggles direction on click; other
// columns get their default (asc for segment, desc for numerics).
func SortLinks(currentSort, currentDir string) map[string]BrowseSortLink {
	cols := []struct {
		key        string
		defaultDir string
	}{
		{SortColSegment, SortDirAsc},
		{SortColObjects, SortDirDesc},
		{SortColSize, SortDirDesc},
		{SortColCost, SortDirDesc},
	}
	links := make(map[string]BrowseSortLink, len(cols))
	for _, c := range cols {
		link := BrowseSortLink{Sort: c.key, Dir: c.defaultDir}
		if c.key == currentSort {
			if currentDir == SortDirAsc {
				link.Dir = SortDirDesc
				link.Indicator = "↑"
			} else {
				link.Dir = SortDirAsc
				link.Indicator = "↓"
			}
		}
		links[c.key] = link
	}

	return links
}

// Breadcrumbs returns Root + one Crumb per slash-separated segment.
func Breadcrumbs(prefix string) []BrowseCrumb {
	crumbs := []BrowseCrumb{{Label: "Root", Prefix: ""}}
	if prefix == "" {
		return crumbs
	}
	trimmed := strings.TrimSuffix(prefix, "/")
	parts := strings.Split(trimmed, "/")
	var cum strings.Builder
	for _, p := range parts {
		cum.WriteString(p + "/")
		crumbs = append(crumbs, BrowseCrumb{Label: p, Prefix: cum.String()})
	}

	return crumbs
}

// SegmentOf returns the child segment, with the parent prefix and
// trailing slash trimmed.
func SegmentOf(parent, child string) string {
	return strings.TrimSuffix(strings.TrimPrefix(child, parent), "/")
}
