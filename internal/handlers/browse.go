package handlers

import (
	"errors"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/pkg/humanfmt"
	"github.com/eunmann/s3-inv-db/pkg/indexread"
	"github.com/eunmann/s3-inv-db/pkg/pricing"
)

const (
	sortColSegment  = "segment"
	sortColObjects  = "objects"
	sortColSize     = "size"
	sortColCost     = "cost"
	sortDirAsc      = "asc"
	sortDirDesc     = "desc"
	defaultPageSize = 100
	maxPageSize     = 500
)

// BrowseCrumb is one segment of the breadcrumb trail.
type BrowseCrumb struct {
	Label  string `json:"label"`
	Prefix string `json:"prefix"`
}

// BrowseChild is one immediate-child prefix shown in the explorer.
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

// BrowseSortLink carries the {sort, dir, indicator} bundle a column
// header click should send and display.
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
	TotalChildren int                       `json:"total_children"`
	Sort          string                    `json:"sort"`
	Dir           string                    `json:"dir"`
	SortLinks     map[string]BrowseSortLink `json:"sort_links"`
	Pagination    BrowsePagination          `json:"pagination"`
	NotFound      bool                      `json:"not_found,omitempty"`
}

// BrowsePagination describes the slice of children rendered for one page.
type BrowsePagination struct {
	Page     int `json:"page"`
	PageSize int `json:"page_size"`
	Pages    int `json:"pages"`
	FirstRow int `json:"first_row"`
	LastRow  int `json:"last_row"`
	PrevPage int `json:"prev_page"`
	NextPage int `json:"next_page"`
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
	page, pageSize := normalizePage(q.Get("page"), q.Get("page_size"))

	var level BrowseLevel
	err := h.manager.WithIndex(inventoryID, func(idx *indexread.Index) error {
		level = h.buildBrowseLevel(idx, inventoryID, prefix, sortBy, dir, page, pageSize)
		return nil
	})
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

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := h.renderer.RenderPartial(w, "browse_level.html", level); err != nil {
		h.logger.Error().Err(err).Msg("failed to render browse level")
		http.Error(w, "failed to render partial", http.StatusInternalServerError)
	}
}

// normalizePage clamps page (≥1) and size (1..maxPageSize) from user input.
func normalizePage(pageStr, sizeStr string) (page, size int) {
	page = 1
	if v, err := strconv.Atoi(pageStr); err == nil && v >= 1 {
		page = v
	}
	size = defaultPageSize
	if v, err := strconv.Atoi(sizeStr); err == nil && v >= 1 {
		size = v
	}
	if size > maxPageSize {
		size = maxPageSize
	}
	return page, size
}

// normalizeSort clamps sort/dir from user input to known values.
// Unknown column → segment. Unknown direction → segment uses asc, others desc.
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

// buildBrowseLevel computes the stats + children for a single prefix.
// Missing prefixes return NotFound=true so the partial can render an
// inline message rather than failing the whole request.
func (h *Handlers) buildBrowseLevel(idx *indexread.Index, inventoryID, prefix, sortBy, dir string, page, pageSize int) BrowseLevel {
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

	all := h.buildChildren(idx, pos, prefix, sortBy == sortColCost)
	sortChildren(all, sortBy, dir)
	level.TotalChildren = len(all)
	level.Pagination = paginate(len(all), page, pageSize)

	from, to := level.Pagination.FirstRow, level.Pagination.LastRow
	if from > 0 {
		level.Children = all[from-1 : to]
		if sortBy != sortColCost && idx.HasTierData() {
			h.fillChildCosts(idx, level.Children)
		}
	}
	return level
}

func (h *Handlers) buildChildren(idx *indexread.Index, pos uint64, prefix string, computeCost bool) []BrowseChild {
	positions, err := idx.DescendantsAtDepthFiltered(pos, 1, indexread.Filter{})
	if err != nil {
		h.logger.Warn().Err(err).Msg("descendants at depth 1")
		return nil
	}
	hasTier := idx.HasTierData()
	children := make([]BrowseChild, 0, len(positions))
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
		if hasTier && computeCost {
			breakdown := idx.TierBreakdown(p)
			cost := pricing.ComputeMonthlyCost(breakdown, h.priceTable)
			child.MonthlyCostMicrodollars = cost.TotalMicrodollars
			child.MonthlyCostFormatted = pricing.FormatCost(cost.TotalMicrodollars)
		} else if !hasTier {
			child.MonthlyCostFormatted = "—"
		}
		children = append(children, child)
	}
	return children
}

func (h *Handlers) fillChildCosts(idx *indexread.Index, visible []BrowseChild) {
	for i := range visible {
		p, ok := idx.Lookup(visible[i].Prefix)
		if !ok {
			continue
		}
		breakdown := idx.TierBreakdown(p)
		cost := pricing.ComputeMonthlyCost(breakdown, h.priceTable)
		visible[i].MonthlyCostMicrodollars = cost.TotalMicrodollars
		visible[i].MonthlyCostFormatted = pricing.FormatCost(cost.TotalMicrodollars)
	}
}

// paginate returns the 1-indexed window for page of total items at pageSize.
func paginate(total, page, pageSize int) BrowsePagination {
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
	last := first + pageSize - 1
	if last > total {
		last = total
	}
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

// sortChildren orders children in place. Ties resolve by segment-asc.
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

// sortLinks builds the per-column {sort, dir, indicator} bundle for the
// column headers. The active column toggles direction on click; other
// columns get their default (asc for segment, desc for numerics).
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

// breadcrumbs returns Root + one Crumb per slash-separated segment.
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

// segmentOf returns the child segment, with the parent prefix and
// trailing slash trimmed.
func segmentOf(parent, child string) string {
	return strings.TrimSuffix(strings.TrimPrefix(child, parent), "/")
}
