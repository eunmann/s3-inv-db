package handlers

import (
	"context"
	"errors"
	"net/http"
	"sort"

	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/pkg/humanfmt"
	"github.com/eunmann/s3-inv-db/pkg/indexread"
	"github.com/rs/zerolog"
)

// missingValueGlyph is the em-dash rendered in browse/compare cells
// when a metric is unavailable on that side of the comparison.
const missingValueGlyph = "—"

type (
	BrowseCrumb      = inventory.BrowseCrumb
	BrowseChild      = inventory.BrowseChild
	BrowseSortLink   = inventory.BrowseSortLink
	BrowsePagination = inventory.BrowsePagination
)

// BrowseInventoryGroup is one (sourceBucket, inventoryID) configuration
// with all of its loaded runs. The Browse page's inventory <select>
// renders one <optgroup> per group so the user navigates configuration
// first, then picks a run within it.
type BrowseInventoryGroup struct {
	ConfigLabel string // "<src>/<inv>" — the group header
	Options     []BrowseInventoryOption
}

// BrowseInventoryOption is one loaded run inside a BrowseInventoryGroup.
// Label is the full "<src>/<inv> · <run>" string and is what the closed
// <select> displays once the user picks a value — native HTML doesn't
// surface the surrounding <optgroup> in the chip, so the option text
// has to be self-identifying.
type BrowseInventoryOption struct {
	ID    inventory.ID
	Label string
}

// BrowseLevel is the data the browse_level.html partial renders. Lives
// in the HTTP layer because it composes TierStats and CostEstimate.
type BrowseLevel struct {
	InventoryID   inventory.ID
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

// BrowsePage serves the full page and the inner level partial at the
// same URL, dispatched via wantsHTMXPartial.
func (h *Handlers) BrowsePage(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	inventoryID := inventory.ID(q.Get("inventory_id"))
	prefix := q.Get("prefix")
	sortParams := inventory.NormalizeSort(q.Get("sort"), q.Get("dir"))
	sortBy, dir := sortParams.Col, sortParams.Dir
	pageParams := inventory.NormalizePage(q.Get("page"), q.Get("page_size"))
	page, pageSize := pageParams.Page, pageParams.Size

	if wantsHTMXPartial(r) {
		h.renderBrowseLevelPartial(w, r, inventoryID, prefix, sortBy, dir, page, pageSize)

		return
	}
	h.renderBrowsePage(w, r, inventoryID, prefix, sortBy, dir, page, pageSize)
}

func (h *Handlers) renderBrowsePage(w http.ResponseWriter, r *http.Request,
	inventoryID inventory.ID, prefix, sortBy, dir string, page, pageSize int,
) {
	data := map[string]any{
		"Title":           "Browse",
		"InventoryGroups": groupLoadedInventories(h.manager.List()),
		"InventoryID":     inventoryID,
		"Prefix":          prefix,
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

func (h *Handlers) renderBrowseLevelPartial(w http.ResponseWriter, r *http.Request,
	inventoryID inventory.ID, prefix, sortBy, dir string, page, pageSize int,
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
func (h *Handlers) buildBrowseLevel(ctx context.Context, idx *indexread.Index, inventoryID inventory.ID, prefix, sortBy, dir string, page, pageSize int) BrowseLevel {
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
			child.MonthlyCostFormatted = missingValueGlyph
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

// BrowseLevelResponse is the JSON shape returned by BrowseLevelAPI.
// Mirrors BrowseLevel but drops the Tailwind/HTML-only fields and
// converts the numeric fields to JSON-tagged structs.
type BrowseLevelResponse struct {
	InventoryID   inventory.ID      `json:"inventory_id"`
	Prefix        string            `json:"prefix"`
	Breadcrumbs   []BrowseCrumbJSON `json:"breadcrumbs"`
	Stats         PrefixStatsJSON   `json:"stats"`
	Children      []BrowseChildJSON `json:"children"`
	TotalChildren int               `json:"total_children"`
	Sort          string            `json:"sort"`
	Dir           string            `json:"dir"`
	Pagination    PaginationJSON    `json:"pagination"`
	NotFound      bool              `json:"not_found,omitempty"`
}

// BrowseCrumbJSON is one breadcrumb entry in the browse path.
type BrowseCrumbJSON struct {
	Label  string `json:"label"`
	Prefix string `json:"prefix"`
}

// PrefixStatsJSON is the aggregated stats at the current prefix.
type PrefixStatsJSON struct {
	ObjectCount   uint64        `json:"object_count"`
	TotalBytes    uint64        `json:"total_bytes"`
	HasTierData   bool          `json:"has_tier_data"`
	TierBreakdown []TierStats   `json:"tier_breakdown,omitempty"`
	CostEstimate  *CostEstimate `json:"cost_estimate,omitempty"`
}

// BrowseChildJSON is one immediate-child prefix.
type BrowseChildJSON struct {
	Segment                 string `json:"segment"`
	Prefix                  string `json:"prefix"`
	ObjectCount             uint64 `json:"object_count"`
	TotalBytes              uint64 `json:"total_bytes"`
	MonthlyCostMicrodollars uint64 `json:"monthly_cost_microdollars,omitempty"`
	HasChildren             bool   `json:"has_children"`
}

// PaginationJSON is the typed pagination block returned by paginated
// list endpoints. Mirrors inventory.BrowsePagination minus the
// First/LastRow human-friendly fields callers can derive themselves.
type PaginationJSON struct {
	Page     int `json:"page"`
	PageSize int `json:"page_size"`
	Pages    int `json:"pages"`
	Total    int `json:"total"`
}

// BrowseLevelAPI returns the same data the Browse page renders, in
// JSON form. Accepts the same query parameters as the page: inventory_id,
// prefix, sort, dir, page, page_size. Returns 400 on missing inventory_id,
// 404 when the inventory isn't registered, 409 when not loaded.
func (h *Handlers) BrowseLevelAPI(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	inventoryID := inventory.ID(q.Get("inventory_id"))
	prefix := q.Get("prefix")
	sortParams := inventory.NormalizeSort(q.Get("sort"), q.Get("dir"))
	sortBy, dir := sortParams.Col, sortParams.Dir
	pageParams := inventory.NormalizePage(q.Get("page"), q.Get("page_size"))
	page, pageSize := pageParams.Page, pageParams.Size

	if inventoryID == "" {
		WriteJSONError(w, http.StatusBadRequest, "inventory_id is required")

		return
	}

	ctx := r.Context()
	var level BrowseLevel
	err := h.manager.WithIndex(inventoryID, func(idx *indexread.Index) error {
		level = h.buildBrowseLevel(ctx, idx, inventoryID, prefix, sortBy, dir, page, pageSize)

		return nil
	})
	if err != nil {
		resp := managerErrorStatus(err)
		WriteJSONError(w, resp.Status, resp.Message)

		return
	}

	resp := BrowseLevelResponse{
		InventoryID:   inventoryID,
		Prefix:        prefix,
		Sort:          sortBy,
		Dir:           dir,
		TotalChildren: level.TotalChildren,
		NotFound:      level.NotFound,
		Stats: PrefixStatsJSON{
			ObjectCount:   level.ObjectCount,
			TotalBytes:    level.TotalBytes,
			HasTierData:   level.HasTierData,
			TierBreakdown: level.TierBreakdown,
			CostEstimate:  level.CostEstimate,
		},
		Pagination: PaginationJSON{
			Page:     level.Pagination.Page,
			PageSize: level.Pagination.PageSize,
			Pages:    level.Pagination.Pages,
			Total:    level.TotalChildren,
		},
	}
	for _, b := range level.Breadcrumbs {
		resp.Breadcrumbs = append(resp.Breadcrumbs, BrowseCrumbJSON{Label: b.Label, Prefix: b.Prefix})
	}
	resp.Children = make([]BrowseChildJSON, 0, len(level.Children))
	for i := range level.Children {
		c := &level.Children[i]
		resp.Children = append(resp.Children, BrowseChildJSON{
			Segment:                 c.Segment,
			Prefix:                  c.Prefix,
			ObjectCount:             c.ObjectCount,
			TotalBytes:              c.TotalBytes,
			MonthlyCostMicrodollars: c.MonthlyCostMicrodollars,
			HasChildren:             c.HasChildren,
		})
	}
	WriteJSON(w, http.StatusOK, resp)
}

// groupLoadedInventories splits the manager's inventory list into one
// BrowseInventoryGroup per configuration, keeping only StateLoaded runs.
// Groups are sorted alphabetically by ConfigLabel; runs inside each
// group come back newest-first (ISO run timestamps sort lexicographically).
//
// Composite IDs that don't split into the expected 3 parts (legacy
// 2-part entries or hand-registered inventories) collapse into a
// single "Other" group so they remain selectable.
func groupLoadedInventories(all []inventory.Info) []BrowseInventoryGroup {
	groups := map[string]*BrowseInventoryGroup{}
	for i := range all {
		if all[i].State != inventory.StateLoaded {
			continue
		}
		label, opt := splitForGroup(all[i])
		g, ok := groups[label]
		if !ok {
			g = &BrowseInventoryGroup{ConfigLabel: label}
			groups[label] = g
		}
		g.Options = append(g.Options, opt)
	}
	out := make([]BrowseInventoryGroup, 0, len(groups))
	for _, g := range groups {
		sort.Slice(g.Options, func(i, j int) bool {
			// Within a group every Label has the same config prefix so
			// lex-descending sorts by run timestamp newest-first.
			return g.Options[i].Label > g.Options[j].Label
		})
		out = append(out, *g)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ConfigLabel < out[j].ConfigLabel })

	return out
}

func splitForGroup(info inventory.Info) (string, BrowseInventoryOption) {
	if src, inv, run, ok := info.ID.Split(); ok {
		config := src + "/" + inv

		return config, BrowseInventoryOption{
			ID:    info.ID,
			Label: config + " · " + humanfmt.RunTimestamp(run),
		}
	}

	return "Other", BrowseInventoryOption{ID: info.ID, Label: info.Name}
}
