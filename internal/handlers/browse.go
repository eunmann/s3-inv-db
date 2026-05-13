package handlers

import (
	"context"
	"errors"
	"net/http"

	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/pkg/humanfmt"
	"github.com/eunmann/s3-inv-db/pkg/indexread"
	"github.com/rs/zerolog"
)

// Type aliases re-exporting the domain browse types so templates and
// existing handler code can continue to refer to handlers.BrowseLevel
// while the underlying values live in the inventory package.
type (
	BrowseCrumb      = inventory.BrowseCrumb
	BrowseChild      = inventory.BrowseChild
	BrowseSortLink   = inventory.BrowseSortLink
	BrowsePagination = inventory.BrowsePagination
)

// BrowseLevel is the data the browse_level.html partial renders.
// It still lives in the HTTP layer because it composes TierStats and
// CostEstimate (JSON-API types), which the domain shouldn't know about.
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

// BrowsePage serves both the full Browse page and the inner level
// partial, content-negotiated via the HX-Request header. A plain GET
// (browser nav, full reload, back/forward restore) returns the whole
// page with the level inlined; an htmx GET returns just the level
// partial that swaps into #browse-target.
//
// Single URL means htmx's natural request URL is the page URL, so
// hx-push-url="true" pushes the right thing without server-side
// HX-Push-Url headers, and a full reload of the pushed URL renders
// the full page (not a stray partial).
func (h *Handlers) BrowsePage(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	inventoryID := q.Get("inventory_id")
	prefix := q.Get("prefix")
	sortBy, dir := inventory.NormalizeSort(q.Get("sort"), q.Get("dir"))
	page, pageSize := inventory.NormalizePage(q.Get("page"), q.Get("page_size"))

	// HX-History-Restore-Request is sent when htmx is restoring a
	// cached page on back/forward navigation; in that case we want the
	// full layout, not a bare partial.
	htmxPartial := r.Header.Get("HX-Request") == "true" &&
		r.Header.Get("HX-History-Restore-Request") != "true"

	if htmxPartial {
		h.renderBrowseLevelPartial(w, r, inventoryID, prefix, sortBy, dir, page, pageSize)
		return
	}
	h.renderBrowsePage(w, r, inventoryID, prefix, sortBy, dir, page, pageSize)
}

// renderBrowsePage emits the full page with the level inlined for
// non-htmx requests (plain navigation, reload, history restore).
func (h *Handlers) renderBrowsePage(w http.ResponseWriter, r *http.Request,
	inventoryID, prefix, sortBy, dir string, page, pageSize int,
) {
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
		// On ErrNotLoaded / ErrNotFound, fall through to the empty
		// placeholder — the user can pick a different inventory.
		if err == nil {
			data["InitialLevel"] = level
		}
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := h.renderer.Render(w, "browse.html", data); err != nil {
		zerolog.Ctx(r.Context()).Error().Err(err).Msg("failed to render browse page")
		http.Error(w, "failed to render page", http.StatusInternalServerError)
	}
}

// renderBrowseLevelPartial emits just the level partial for htmx
// requests. An empty inventoryID is rejected with 400 — the partial
// path has nothing to render without it.
func (h *Handlers) renderBrowseLevelPartial(w http.ResponseWriter, r *http.Request,
	inventoryID, prefix, sortBy, dir string, page, pageSize int,
) {
	if inventoryID == "" {
		http.Error(w, "inventory_id is required", http.StatusBadRequest)
		return
	}
	ctx := r.Context()
	logger := zerolog.Ctx(ctx)
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

// buildBrowseLevel composes the domain-level prefix view with HTTP-shape
// stats/tier/cost annotations. Pure prefix math lives in the inventory
// package; this method just plumbs index reads + price-table-aware cost
// formatting into the render struct.
func (h *Handlers) buildBrowseLevel(ctx context.Context, idx *indexread.Index, inventoryID, prefix, sortBy, dir string, page, pageSize int) BrowseLevel {
	level := BrowseLevel{
		InventoryID: inventoryID,
		Prefix:      prefix,
		Breadcrumbs: inventory.Breadcrumbs(prefix),
		HasTierData: idx.HasTierData(),
		Sort:        sortBy,
		Dir:         dir,
		SortLinks:   inventory.SortLinks(sortBy, dir),
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
		level.CostEstimate = h.computeCostEstimate(breakdown, true)
	}

	all := h.buildChildren(ctx, idx, pos, prefix, sortBy == inventory.SortColCost)
	inventory.SortChildren(all, sortBy, dir)
	level.TotalChildren = len(all)
	level.Pagination = inventory.Paginate(len(all), page, pageSize)

	from, to := level.Pagination.FirstRow, level.Pagination.LastRow
	if from > 0 {
		level.Children = all[from-1 : to]
		if sortBy != inventory.SortColCost && idx.HasTierData() {
			h.fillChildCosts(idx, level.Children)
		}
	}
	return level
}

func (h *Handlers) buildChildren(ctx context.Context, idx *indexread.Index, pos uint64, prefix string, computeCost bool) []BrowseChild {
	positions, err := idx.DescendantsAtDepthFiltered(pos, 1, indexread.Filter{})
	if err != nil {
		zerolog.Ctx(ctx).Warn().Err(err).Msg("descendants at depth 1")
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
			Segment:      inventory.SegmentOf(prefix, fullPrefix),
			Prefix:       fullPrefix,
			ObjectCount:  stats.ObjectCount,
			ObjectCountH: humanfmt.CountUint64(stats.ObjectCount),
			TotalBytes:   stats.TotalBytes,
			TotalBytesH:  humanfmt.BytesUint64(stats.TotalBytes),
			HasChildren:  idx.MaxDepthInSubtree(p) > idx.Depth(p),
		}
		if hasTier && computeCost {
			est := h.computeCostEstimate(idx.TierBreakdown(p), false)
			if est != nil {
				child.MonthlyCostMicrodollars = est.TotalMicrodollars
				child.MonthlyCostFormatted = est.TotalFormatted
			}
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
		est := h.computeCostEstimate(idx.TierBreakdown(p), false)
		if est != nil {
			visible[i].MonthlyCostMicrodollars = est.TotalMicrodollars
			visible[i].MonthlyCostFormatted = est.TotalFormatted
		}
	}
}
