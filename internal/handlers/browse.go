package handlers

import (
	"errors"
	"net/http"
	"sort"
	"strings"

	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/pkg/humanfmt"
	"github.com/eunmann/s3-inv-db/pkg/indexread"
	"github.com/eunmann/s3-inv-db/pkg/pricing"
)

// Supported browse-level sort columns.
const (
	sortColSegment = "segment"
	sortColObjects = "objects"
	sortColSize    = "size"
	sortColCost    = "cost"
	sortDirAsc     = "asc"
	sortDirDesc    = "desc"
)

// BrowseCrumb is one segment of the breadcrumb trail rendered in the browse
// partial. Prefix is the full prefix up to and including this segment, ready
// to be passed back to /partials/browse-level as ?prefix=...
type BrowseCrumb struct {
	Label  string `json:"label"`
	Prefix string `json:"prefix"`
}

// BrowseChild is one immediate-child prefix shown in the browse partial.
// Segment is the bit after the parent prefix and before the trailing slash
// — what the user sees in the list. Prefix is the full prefix the user
// navigates into when they click.
type BrowseChild struct {
	Segment                 string `json:"segment"`
	Prefix                  string `json:"prefix"`
	ObjectCount             uint64 `json:"object_count"`
	ObjectCountH            string `json:"object_count_human"`
	TotalBytes              uint64 `json:"total_bytes"`
	TotalBytesH             string `json:"total_bytes_human"`
	MonthlyCostMicrodollars uint64 `json:"monthly_cost_microdollars,omitempty"`
	MonthlyCostFormatted    string `json:"monthly_cost_formatted,omitempty"`
	HasChildren             bool   `json:"has_children"`
}

// BrowseSortLink carries the {sort, dir} pair the template should embed in
// a column header click, plus an Indicator (↑/↓/"") to show the current
// sort direction. Computed server-side so the template stays declarative.
type BrowseSortLink struct {
	Sort      string `json:"sort"`
	Dir       string `json:"dir"`
	Indicator string `json:"indicator"`
}

// BrowseLevel is the data the browse_level.html partial renders.
type BrowseLevel struct {
	InventoryID   string                    `json:"inventory_id"`
	Prefix        string                    `json:"prefix"`
	Breadcrumbs   []BrowseCrumb             `json:"breadcrumbs"`
	ObjectCount   uint64                    `json:"object_count"`
	ObjectCountH  string                    `json:"object_count_human"`
	TotalBytes    uint64                    `json:"total_bytes"`
	TotalBytesH   string                    `json:"total_bytes_human"`
	TierBreakdown []TierStats               `json:"tier_breakdown,omitempty"`
	CostEstimate  *CostEstimate             `json:"cost_estimate,omitempty"`
	HasTierData   bool                      `json:"has_tier_data"`
	Children      []BrowseChild             `json:"children"`
	Sort          string                    `json:"sort"`
	Dir           string                    `json:"dir"`
	SortLinks     map[string]BrowseSortLink `json:"sort_links"`
	NotFound      bool                      `json:"not_found,omitempty"`
}

// BrowsePage renders the explorer page shell.
func (h *Handlers) BrowsePage(w http.ResponseWriter, _ *http.Request) {
	inventories := h.manager.List()
	loaded := make([]inventory.Info, 0, len(inventories))
	for i := range inventories {
		if inventories[i].State == inventory.StateLoaded {
			loaded = append(loaded, inventories[i])
		}
	}
	data := map[string]any{
		"Title":       "Browse",
		"Inventories": loaded,
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := h.renderer.Render(w, "browse.html", data); err != nil {
		h.logger.Error().Err(err).Msg("failed to render browse page")
		http.Error(w, "failed to render page", http.StatusInternalServerError)
	}
}

// BrowseLevelPartial returns one level of the explorer: stats for the
// requested prefix + its immediate child prefixes + breadcrumbs.
func (h *Handlers) BrowseLevelPartial(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	inventoryID := q.Get("inventory_id")
	if inventoryID == "" || !q.Has("prefix") {
		http.Error(w, "inventory_id and prefix are required", http.StatusBadRequest)
		return
	}
	prefix := q.Get("prefix")
	sortBy, dir := normalizeSort(q.Get("sort"), q.Get("dir"))

	idx, err := h.manager.GetIndex(inventoryID)
	if err != nil {
		switch {
		case errors.Is(err, inventory.ErrNotFound):
			http.Error(w, "inventory not found", http.StatusNotFound)
		case errors.Is(err, inventory.ErrNotLoaded):
			http.Error(w, "inventory not loaded — build & load it on the Inventories page first", http.StatusConflict)
		default:
			h.logger.Error().Err(err).Msg("get index")
			http.Error(w, "failed to get index", http.StatusInternalServerError)
		}
		return
	}

	level := h.buildBrowseLevel(idx, inventoryID, prefix, sortBy, dir)

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := h.renderer.RenderPartial(w, "browse_level.html", level); err != nil {
		h.logger.Error().Err(err).Msg("failed to render browse level")
		http.Error(w, "failed to render partial", http.StatusInternalServerError)
	}
}

// normalizeSort coerces user-supplied sort/dir params to known values.
// Unknown column → segment. Unknown direction → the column's default
// (asc for segment, desc for numeric columns — biggest first is the
// usual ask for the latter).
func normalizeSort(sortBy, dir string) (col, direction string) {
	switch sortBy {
	case sortColObjects, sortColSize, sortColCost, sortColSegment:
		col = sortBy
	default:
		col = sortColSegment
	}
	switch dir {
	case sortDirAsc, sortDirDesc:
		direction = dir
	default:
		if col == sortColSegment {
			direction = sortDirAsc
		} else {
			direction = sortDirDesc
		}
	}
	return col, direction
}

// buildBrowseLevel computes the stats + children for a single prefix. When
// the prefix doesn't exist in the trie we still return a valid level with
// NotFound=true so the partial can render an inline "no such prefix"
// message in the explorer; bouncing the user back to an HTTP error would
// lose the rest of the page state.
func (h *Handlers) buildBrowseLevel(idx *indexread.Index, inventoryID, prefix, sortBy, dir string) BrowseLevel {
	level := BrowseLevel{
		InventoryID: inventoryID,
		Prefix:      prefix,
		Breadcrumbs: breadcrumbs(prefix),
		HasTierData: idx.HasTierData(),
		Sort:        sortBy,
		Dir:         dir,
		SortLinks:   sortLinks(sortBy, dir),
	}

	pos, ok := idx.Lookup(prefix)
	if !ok {
		level.NotFound = true
		return level
	}

	stats := idx.Stats(pos)
	level.ObjectCount = stats.ObjectCount
	level.ObjectCountH = humanfmt.CountUint64(stats.ObjectCount)
	level.TotalBytes = stats.TotalBytes
	level.TotalBytesH = humanfmt.BytesUint64(stats.TotalBytes)

	if idx.HasTierData() {
		breakdown := idx.TierBreakdown(pos)
		level.TierBreakdown = make([]TierStats, 0, len(breakdown))
		for _, tb := range breakdown {
			level.TierBreakdown = append(level.TierBreakdown, TierStats{
				TierName:     tb.TierName,
				ObjectCount:  tb.ObjectCount,
				ObjectCountH: humanfmt.CountUint64(tb.ObjectCount),
				Bytes:        tb.Bytes,
				BytesH:       humanfmt.BytesUint64(tb.Bytes),
			})
		}
		if len(breakdown) > 0 {
			cost := pricing.ComputeMonthlyCost(breakdown, h.priceTable)
			level.CostEstimate = &CostEstimate{
				TotalMicrodollars:           cost.TotalMicrodollars,
				TotalFormatted:              pricing.FormatCost(cost.TotalMicrodollars),
				MonitoringMicrodollars:      cost.MonitoringMicrodollars,
				MinObjectSizeMicrodollars:   cost.MinObjectSizeMicrodollars,
				GlacierOverheadMicrodollars: cost.GlacierOverheadMicrodollars,
			}
			if len(cost.PerTierMicrodollars) > 0 {
				level.CostEstimate.PerTierMicrodollars = cost.PerTierMicrodollars
				level.CostEstimate.PerTierFormatted = make(map[string]string, len(cost.PerTierMicrodollars))
				for tier, microdollars := range cost.PerTierMicrodollars {
					level.CostEstimate.PerTierFormatted[tier] = pricing.FormatCost(microdollars)
				}
			}
		}
	}

	level.Children = h.buildChildren(idx, pos, prefix)
	sortChildren(level.Children, sortBy, dir)
	return level
}

func (h *Handlers) buildChildren(idx *indexread.Index, pos uint64, prefix string) []BrowseChild {
	positions, err := idx.DescendantsAtDepthFiltered(pos, 1, indexread.Filter{})
	if err != nil {
		h.logger.Warn().Err(err).Msg("descendants at depth 1")
		return nil
	}
	hasTier := idx.HasTierData()
	out := make([]BrowseChild, 0, len(positions))
	for _, p := range positions {
		fullPrefix, err := idx.PrefixString(p)
		if err != nil {
			continue
		}
		stats := idx.Stats(p)
		child := BrowseChild{
			Segment:      segmentOf(prefix, fullPrefix),
			Prefix:       fullPrefix,
			ObjectCount:  stats.ObjectCount,
			ObjectCountH: humanfmt.CountUint64(stats.ObjectCount),
			TotalBytes:   stats.TotalBytes,
			TotalBytesH:  humanfmt.BytesUint64(stats.TotalBytes),
			HasChildren:  idx.MaxDepthInSubtree(p) > idx.Depth(p),
		}
		if hasTier {
			breakdown := idx.TierBreakdown(p)
			cost := pricing.ComputeMonthlyCost(breakdown, h.priceTable)
			child.MonthlyCostMicrodollars = cost.TotalMicrodollars
			child.MonthlyCostFormatted = pricing.FormatCost(cost.TotalMicrodollars)
		} else {
			child.MonthlyCostFormatted = "—"
		}
		out = append(out, child)
	}
	return out
}

// sortChildren orders the children slice in place by (sortBy, dir). For
// equal-key entries it falls back to segment-asc so the order is stable
// regardless of how the index hands them to us.
func sortChildren(children []BrowseChild, sortBy, dir string) {
	less := func(i, j int) bool {
		a, b := &children[i], &children[j]
		var primary bool
		var equal bool
		switch sortBy {
		case sortColObjects:
			primary = a.ObjectCount < b.ObjectCount
			equal = a.ObjectCount == b.ObjectCount
		case sortColSize:
			primary = a.TotalBytes < b.TotalBytes
			equal = a.TotalBytes == b.TotalBytes
		case sortColCost:
			primary = a.MonthlyCostMicrodollars < b.MonthlyCostMicrodollars
			equal = a.MonthlyCostMicrodollars == b.MonthlyCostMicrodollars
		default:
			primary = a.Segment < b.Segment
			equal = a.Segment == b.Segment
		}
		if equal {
			return a.Segment < b.Segment
		}
		if dir == sortDirDesc {
			return !primary
		}
		return primary
	}
	sort.SliceStable(children, less)
}

// sortLinks builds the per-column {sort, dir, indicator} bundle the
// template embeds in each header's hx-vals. Clicking the active column
// toggles direction; clicking a different column applies that column's
// default direction (asc for segment, desc for numerics).
func sortLinks(currentSort, currentDir string) map[string]BrowseSortLink {
	cols := []struct {
		key        string
		defaultDir string
	}{
		{sortColSegment, sortDirAsc},
		{sortColObjects, sortDirDesc},
		{sortColSize, sortDirDesc},
		{sortColCost, sortDirDesc},
	}
	links := make(map[string]BrowseSortLink, len(cols))
	for _, c := range cols {
		link := BrowseSortLink{Sort: c.key, Dir: c.defaultDir}
		if c.key == currentSort {
			if currentDir == sortDirAsc {
				link.Dir = sortDirDesc
				link.Indicator = "↑"
			} else {
				link.Dir = sortDirAsc
				link.Indicator = "↓"
			}
		}
		links[c.key] = link
	}
	return links
}

// breadcrumbs returns Root + one Crumb per slash-separated segment of prefix.
//
//	""           -> [Root]
//	"foo/"       -> [Root, foo]
//	"foo/bar/"   -> [Root, foo, bar]
func breadcrumbs(prefix string) []BrowseCrumb {
	crumbs := []BrowseCrumb{{Label: "Root", Prefix: ""}}
	if prefix == "" {
		return crumbs
	}
	trimmed := strings.TrimSuffix(prefix, "/")
	parts := strings.Split(trimmed, "/")
	cum := ""
	for _, p := range parts {
		cum += p + "/"
		crumbs = append(crumbs, BrowseCrumb{Label: p, Prefix: cum})
	}
	return crumbs
}

// segmentOf strips parent from child and the trailing slash, yielding the
// display label for one child row. For root parent ("") the segment is the
// child with its trailing slash trimmed.
func segmentOf(parent, child string) string {
	return strings.TrimSuffix(strings.TrimPrefix(child, parent), "/")
}
