package handlers

import (
	"maps"
	"net/url"
	"strconv"

	"github.com/eunmann/s3-inv-db/internal/inventory"
)

// LevelNav is the navigation around one Browse or Compare level, which
// partials/level_nav.html renders: the breadcrumb trail, the sortable
// column headers and the pager. Every link's URL is both its href, so
// it works without htmx and opens in a new tab, and its hx-get.
type LevelNav struct {
	Sort    map[string]NavSort
	Target  string
	PrevURL string
	NextURL string
	Crumbs  []NavCrumb
	Page    int
	Pages   int
}

// NavCrumb is one breadcrumb; the last is the level shown.
type NavCrumb struct {
	Label   string
	URL     string
	Current bool
}

// NavSort is one sortable column header. Target is the element the
// level swaps into, repeated here because a template "define" takes
// one value.
type NavSort struct {
	Label     string
	URL       string
	Indicator string
	AriaSort  string
	Target    string
}

// levelQuery is the query string of one level view: the params every
// link of the level keeps (the run or runs, the filter, the page size)
// and the prefix, order and page each link sets.
type levelQuery struct {
	keep     url.Values
	path     string
	target   string
	prefix   string
	sortBy   string
	dir      string
	pageSize int
}

func (q levelQuery) url(prefix, sortBy, dir string, page int) string {
	v := maps.Clone(q.keep)
	set := func(k, val string) {
		if val != "" {
			v.Set(k, val)
		}
	}
	set("prefix", prefix)
	set("sort", sortBy)
	set("dir", dir)
	if page > 1 {
		v.Set("page", strconv.Itoa(page))
	}
	if q.pageSize != inventory.DefaultPageSize {
		v.Set("page_size", strconv.Itoa(q.pageSize))
	}

	return q.path + "?" + v.Encode()
}

// buildLevelNav builds a level's links. Breadcrumbs open their level in
// its default order; sort headers keep the prefix and start at page 1;
// the pager keeps the order. The root crumb is labelled rootLabel, each
// column by label.
func buildLevelNav(q levelQuery, rootLabel string, sortLinks map[string]inventory.BrowseSortLink, label func(col string) string, p inventory.BrowsePagination) LevelNav {
	nav := LevelNav{Target: q.target, Page: p.Page, Pages: p.Pages}
	crumbs := inventory.Breadcrumbs(q.prefix)
	nav.Crumbs = make([]NavCrumb, len(crumbs))
	for i, c := range crumbs {
		nav.Crumbs[i] = NavCrumb{Label: c.Label, URL: q.url(c.Prefix, "", "", 1), Current: i == len(crumbs)-1}
	}
	nav.Crumbs[0].Label = rootLabel
	nav.Sort = make(map[string]NavSort, len(sortLinks))
	for col, link := range sortLinks {
		s := NavSort{Label: label(col), URL: q.url(q.prefix, link.Sort, link.Dir, 1), Indicator: link.Indicator, Target: q.target}
		if col == q.sortBy {
			s.AriaSort = "descending"
			if q.dir == inventory.SortDirAsc {
				s.AriaSort = "ascending"
			}
		}
		nav.Sort[col] = s
	}
	if p.PrevPage > 0 {
		nav.PrevURL = q.url(q.prefix, q.sortBy, q.dir, p.PrevPage)
	}
	if p.NextPage > 0 {
		nav.NextURL = q.url(q.prefix, q.sortBy, q.dir, p.NextPage)
	}

	return nav
}

// sortLabel is a children table column's header.
func sortLabel(col string) string {
	switch col {
	case inventory.SortColCompareStatus:
		return "Status"
	case inventory.SortColSegment:
		return "Segment"
	case inventory.SortColSize:
		return "Size"
	case inventory.SortColObjects:
		return "Objects"
	case inventory.SortColCost:
		return "Cost / mo"
	}

	return col
}

// rootLabel names a run's root prefix: its source bucket, or "Root" for
// an ID that doesn't name one.
func rootLabel(id inventory.ID) string {
	if p := id.Split(); p.OK {
		return p.Source
	}

	return "Root"
}
