package handlers

import (
	"context"
	"errors"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/internal/logctx"
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

// Browse* types below are render-only structures consumed by the
// browse_level.html template. They are not JSON-serialized — the browse
// view is HTML-only — so no `json:"…"` tags.

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

// BrowseLevel is the data the browse_level.html partial renders.
type BrowseLevel struct {
	InventoryID   string
	Prefix        string
	Breadcrumbs   []BrowseCrumb
	ObjectCount   uint64
	ObjectCountH  string
	TotalBytes    uint64
	TotalBytesH   string
	TierBreakdown []TierStats
	CostEstimate  *CostEstimate
	HasTierData   bool
	Children      []BrowseChild
	TotalChildren int
	Sort          string
	Dir           string
	SortLinks     map[string]BrowseSortLink
	Pagination    BrowsePagination
	NotFound      bool
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

// BrowsePage renders the explorer page shell, including the initial
// browse level inline when the URL carries inventory_id + prefix. That
// makes the page server-renderable (a fresh GET shows the full state
// without a second AJAX round-trip).
func (h *Handlers) BrowsePage(w http.ResponseWriter, r *http.Request) {
	inventories := h.manager.List()
	loaded := make([]inventory.Info, 0, len(inventories))
	for i := range inventories {
		if inventories[i].State == inventory.StateLoaded {
			loaded = append(loaded, inventories[i])
		}
	}

	q := r.URL.Query()
	inventoryID := q.Get("inventory_id")
	prefix := q.Get("prefix")
	sortBy, dir := normalizeSort(q.Get("sort"), q.Get("dir"))
	page, pageSize := normalizePage(q.Get("page"), q.Get("page_size"))

	data := map[string]any{
		"Title":       "Browse",
		"Inventories": loaded,
		"InventoryID": inventoryID,
		"Prefix":      prefix,
	}

	if inventoryID != "" {
		ctx := r.Context()
		var level BrowseLevel
		err := h.manager.WithIndex(inventoryID, func(idx *indexread.Index) error {
			level = h.buildBrowseLevel(ctx, idx, inventoryID, prefix, sortBy, dir, page, pageSize)
			return nil
		})
		// Either render or — for ErrNotLoaded/ErrNotFound — fall through
		// to the empty placeholder; the user can pick a different inventory.
		if err == nil {
			data["InitialLevel"] = level
		}
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := h.renderer.Render(w, "browse.html", data); err != nil {
		logctx.FromContext(r.Context()).Error().Err(err).Msg("failed to render browse page")
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

	ctx := r.Context()
	logger := logctx.FromContext(ctx)
	var level BrowseLevel
	err := h.manager.WithIndex(inventoryID, func(idx *indexread.Index) error {
		level = h.buildBrowseLevel(ctx, idx, inventoryID, prefix, sortBy, dir, page, pageSize)
		return nil
	})
	if err != nil {
		switch {
		case errors.Is(err, inventory.ErrNotFound):
			http.Error(w, "inventory not found", http.StatusNotFound)
		case errors.Is(err, inventory.ErrNotLoaded):
			http.Error(w, "inventory not loaded — build & load it on the Inventories page first", http.StatusConflict)
		default:
			logger.Error().Err(err).Msg("get index")
			http.Error(w, "failed to get index", http.StatusInternalServerError)
		}
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := h.renderer.RenderPartial(w, "browse_level.html", level); err != nil {
		logger.Error().Err(err).Msg("failed to render browse level")
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
func (h *Handlers) buildBrowseLevel(ctx context.Context, idx *indexread.Index, inventoryID, prefix, sortBy, dir string, page, pageSize int) BrowseLevel {
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

	all := h.buildChildren(ctx, idx, pos, prefix, sortBy == sortColCost)
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

func (h *Handlers) buildChildren(ctx context.Context, idx *indexread.Index, pos uint64, prefix string, computeCost bool) []BrowseChild {
	positions, err := idx.DescendantsAtDepthFiltered(pos, 1, indexread.Filter{})
	if err != nil {
		logctx.FromContext(ctx).Warn().Err(err).Msg("descendants at depth 1")
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
