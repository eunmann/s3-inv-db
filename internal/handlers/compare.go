package handlers

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sort"

	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/pkg/humanfmt"
	"github.com/eunmann/s3-inv-db/pkg/indexread"
	"github.com/eunmann/s3-inv-db/pkg/pricing"
	"github.com/rs/zerolog"
)

// ComparePicker is the per-configuration run list driving the from/to
// dropdowns. Each configuration's loaded runs are grouped under their
// "<src>/<inv>" label, mirroring the Browse picker's <optgroup>
// presentation so the two pages feel coherent.
type ComparePicker struct {
	Groups []ComparePickerGroup
}

// ComparePickerGroup is one configuration with all of its loaded runs.
type ComparePickerGroup struct {
	ConfigLabel string
	Options     []ComparePickerOption
}

// ComparePickerOption is one loaded run inside a ComparePickerGroup.
type ComparePickerOption struct {
	ID    inventory.ID
	Label string // "<src>/<inv> · <run>" — self-identifying in the closed <select>
}

// ComparePageData is the typed page data for compare.html.
type ComparePageData struct {
	Title       string
	Picker      ComparePicker
	From        inventory.ID
	To          inventory.ID
	ConfigLabel string // "<src>/<inv>" when both IDs share the config
	FromRun     string // formatted run timestamp (or empty)
	ToRun       string // formatted run timestamp (or empty)
	Prefix      string
	Breadcrumbs []BrowseCrumb // shared with Browse; same template idiom
	Error       string        // user-facing setup error (empty when level is shown)
	Level       *CompareLevelView
	Partial     ComparePartialData // shape passed to the inner partial
}

// ComparePartialData carries the compare_level partial's inputs. Used for
// both the htmx-driven swap and the initial full-page render so the
// partial template has one stable shape.
type ComparePartialData struct {
	Prefix      string
	Breadcrumbs []BrowseCrumb
	From, To    inventory.ID
	Level       *CompareLevelView
}

// CompareLevelView is the rendered comparison at one prefix.
type CompareLevelView struct {
	Self CompareSelfView

	Children      []CompareChildView
	TotalChildren int // children before pagination, after hide-unchanged filter
	Status        CompareStatusCounts
	HideUnchanged bool // toggle state for the template
	Pagination    BrowsePagination
	NotFound      bool // prefix missing on both sides — empty state

	Sort      string // current sort column (empty = default)
	Dir       string // current direction (asc/desc)
	SortLinks map[string]inventory.BrowseSortLink
}

// CompareStatusCounts summarises the change set at this prefix.
type CompareStatusCounts struct {
	Added, Removed, Changed, Unchanged int
}

// CompareSelfView is the prefix-level summary card.
type CompareSelfView struct {
	Prefix string

	ObjectsBeforeH, ObjectsAfterH, ObjectsDeltaH, ObjectsPct string
	ObjectsSign                                              int

	BytesBeforeH, BytesAfterH, BytesDeltaH, BytesPct string
	BytesSign                                        int

	HasCost                                      bool
	CostBeforeH, CostAfterH, CostDeltaH, CostPct string
	CostSign                                     int

	// One-time PUT cost — the API charge to ingest the run's objects.
	// Always populated (object counts are always known); the sign carries
	// the direction of the move between runs.
	APICostBeforeH, APICostAfterH, APICostDeltaH, APICostPct string
	APICostSign                                              int
}

// CompareChildView is one row in the children table.
type CompareChildView struct {
	Segment, Prefix string
	Status          string // human label — template runs compareStatusClass on it
	StatusOrder     int    // stable rank for status-column sort
	HasChildren     bool

	ObjectsDelta                                             int64
	ObjectsBeforeH, ObjectsAfterH, ObjectsDeltaH, ObjectsPct string
	ObjectsSign                                              int

	BytesDelta                                       int64
	BytesBeforeH, BytesAfterH, BytesDeltaH, BytesPct string
	BytesSign                                        int

	HasCost                                      bool
	CostDelta                                    int64
	CostBeforeH, CostAfterH, CostDeltaH, CostPct string
	CostSign                                     int

	APICostDelta                                             int64
	APICostBeforeH, APICostAfterH, APICostDeltaH, APICostPct string
	APICostSign                                              int

	// AbsByteDelta drives the default "biggest absolute mover" sort.
	AbsByteDelta uint64
}

// ComparePage renders the comparison page. Same URL serves the full page
// or the inner level partial dispatched via wantsHTMXPartial, matching
// the Browse handler's pattern.
func (h *Handlers) ComparePage(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	from := inventory.ID(q.Get("from"))
	to := inventory.ID(q.Get("to"))
	prefix := q.Get("prefix")
	hideUnchanged := q.Get("show_unchanged") != trueLiteral
	pageParams := inventory.NormalizePage(q.Get("page"), q.Get("page_size"))
	page, pageSize := pageParams.Page, pageParams.Size
	sortParams := inventory.NormalizeCompareSort(q.Get("sort"), q.Get("dir"))
	sortBy, dir := sortParams.Col, sortParams.Dir

	opts := compareViewOptions{
		from: from, to: to, prefix: prefix,
		hideUnchanged: hideUnchanged,
		page:          page, pageSize: pageSize,
		sortBy: sortBy, dir: dir,
	}
	if wantsHTMXPartial(r) {
		h.renderCompareLevelPartial(w, r, opts)

		return
	}
	h.renderCompareFullPage(w, r, opts)
}

// compareViewOptions bundles the query-string knobs ComparePage parses so
// the inner render functions and computeCompareLevel don't drown in
// positional parameters.
type compareViewOptions struct {
	from, to       inventory.ID
	prefix         string
	hideUnchanged  bool
	page, pageSize int
	sortBy, dir    string
}

func (h *Handlers) renderCompareFullPage(w http.ResponseWriter, r *http.Request, opts compareViewOptions) {
	data := ComparePageData{
		Title:       "Compare runs",
		Picker:      buildComparePicker(h.manager.List()),
		From:        opts.from,
		To:          opts.to,
		Prefix:      opts.prefix,
		Breadcrumbs: inventory.Breadcrumbs(opts.prefix),
	}

	switch {
	case opts.from == "" || opts.to == "":
		// First visit — no validation error, just the picker.
	case !sameConfig(opts.from, opts.to):
		data.Error = "Both runs must belong to the same inventory configuration."
	default:
		fromDesc := describeRun(opts.from)
		data.ConfigLabel, data.FromRun = fromDesc.ConfigLabel, fromDesc.RunLabel
		data.ToRun = describeRun(opts.to).RunLabel
		level, err := h.computeCompareLevel(r.Context(), opts)
		switch {
		case errors.Is(err, inventory.ErrNotFound):
			data.Error = "One of the runs is no longer registered. Refresh the Inventories page."
		case errors.Is(err, inventory.ErrNotLoaded):
			data.Error = "Both runs must be loaded before they can be compared."
		case err != nil:
			zerolog.Ctx(r.Context()).Error().Err(err).Msg("compare level")
			data.Error = "Failed to compute the comparison. See server logs for details."
		default:
			data.Level = level
			data.Partial = ComparePartialData{
				Prefix:      opts.prefix,
				Breadcrumbs: data.Breadcrumbs,
				From:        opts.from,
				To:          opts.to,
				Level:       level,
			}
		}
	}

	h.renderHTML(w, r, "compare.html", "render compare page", data)
}

func (h *Handlers) renderCompareLevelPartial(w http.ResponseWriter, r *http.Request, opts compareViewOptions) {
	if opts.from == "" || opts.to == "" {
		http.Error(w, "from and to are required", http.StatusBadRequest)

		return
	}
	if !sameConfig(opts.from, opts.to) {
		http.Error(w, "both runs must belong to the same inventory configuration", http.StatusBadRequest)

		return
	}
	level, err := h.computeCompareLevel(r.Context(), opts)
	if err != nil {
		switch {
		case errors.Is(err, inventory.ErrNotFound):
			http.Error(w, "one of the runs is no longer registered", http.StatusNotFound)
		case errors.Is(err, inventory.ErrNotLoaded):
			http.Error(w, "both runs must be loaded before they can be compared", http.StatusConflict)
		default:
			zerolog.Ctx(r.Context()).Error().Err(err).Msg("compare level partial")
			http.Error(w, "failed to compute comparison", http.StatusInternalServerError)
		}

		return
	}
	view := ComparePartialData{
		Prefix:      opts.prefix,
		Breadcrumbs: inventory.Breadcrumbs(opts.prefix),
		From:        opts.from,
		To:          opts.to,
		Level:       level,
	}
	h.renderHTMLPartial(w, r, "compare_level.html", "render compare level partial", view)
}

// computeCompareLevel borrows both indexes and assembles the rendered view
// with cost deltas, signs, and human-formatted numbers. Filters, sorts,
// and paginates the children before returning so the caller doesn't
// have to.
func (h *Handlers) computeCompareLevel(_ context.Context, opts compareViewOptions) (*CompareLevelView, error) {
	var view CompareLevelView
	err := h.manager.WithTwoIndexes(opts.from, opts.to, func(a, b *indexread.Index) error {
		data := inventory.CompareLevel(a, b, opts.prefix)
		view.NotFound = data.Self.NotFoundInA && data.Self.NotFoundInB
		view.Self = h.buildCompareSelfView(data.Self)
		view.Children = make([]CompareChildView, 0, len(data.Children))
		for i := range data.Children {
			c := h.buildCompareChildView(&data.Children[i])
			view.Children = append(view.Children, c)
			switch data.Children[i].Status {
			case inventory.CompareAdded:
				view.Status.Added++
			case inventory.CompareRemoved:
				view.Status.Removed++
			case inventory.CompareChanged:
				view.Status.Changed++
			case inventory.CompareUnchanged:
				view.Status.Unchanged++
			}
		}

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("compute compare level: %w", err)
	}
	view.HideUnchanged = opts.hideUnchanged
	if opts.hideUnchanged {
		filtered := view.Children[:0]
		for i := range view.Children {
			if view.Children[i].Status != inventory.CompareUnchanged.String() {
				filtered = append(filtered, view.Children[i])
			}
		}
		view.Children = filtered
	}
	view.Sort = opts.sortBy
	view.Dir = opts.dir
	view.SortLinks = inventory.CompareSortLinks(opts.sortBy, opts.dir)
	sortCompareChildView(view.Children, opts.sortBy, opts.dir)
	view.TotalChildren = len(view.Children)
	view.Pagination = inventory.Paginate(view.TotalChildren, opts.page, opts.pageSize)
	from1, to1 := view.Pagination.FirstRow, view.Pagination.LastRow
	if from1 > 0 {
		view.Children = view.Children[from1-1 : to1]
	} else {
		view.Children = nil
	}

	return &view, nil
}

// sortCompareChildView orders the table rows in place. When sortBy is
// empty the default "biggest absolute byte mover" applies — handy on
// first visit because users mostly want to see what moved. Explicit
// sorts use signed deltas so direction picks growers vs shrinkers.
func sortCompareChildView(rows []CompareChildView, sortBy, dir string) {
	desc := dir == inventory.SortDirDesc
	less := func(i, j int) bool {
		a, b := &rows[i], &rows[j]
		var primary, equal bool
		switch sortBy {
		case inventory.SortColCompareStatus:
			primary = a.StatusOrder < b.StatusOrder
			equal = a.StatusOrder == b.StatusOrder
		case inventory.SortColObjects:
			primary = a.ObjectsDelta < b.ObjectsDelta
			equal = a.ObjectsDelta == b.ObjectsDelta
		case inventory.SortColSize:
			primary = a.BytesDelta < b.BytesDelta
			equal = a.BytesDelta == b.BytesDelta
		case inventory.SortColCost:
			primary = a.CostDelta < b.CostDelta
			equal = a.CostDelta == b.CostDelta
		case inventory.SortColSegment:
			primary = a.Segment < b.Segment
			equal = a.Segment == b.Segment
		default:
			// No explicit sort — default to biggest absolute byte
			// delta first so the most interesting rows land on top.
			primary = a.AbsByteDelta < b.AbsByteDelta
			equal = a.AbsByteDelta == b.AbsByteDelta
			desc = true
		}
		if equal {
			return a.Segment < b.Segment
		}
		if desc {
			return !primary
		}

		return primary
	}
	sort.SliceStable(rows, less)
}

func (h *Handlers) buildCompareSelfView(self inventory.CompareSelf) CompareSelfView {
	v := CompareSelfView{
		Prefix:         self.Prefix,
		ObjectsBeforeH: numericLabel(self.Objects.Before, self.NotFoundInA, humanfmt.CountUint64),
		ObjectsAfterH:  numericLabel(self.Objects.After, self.NotFoundInB, humanfmt.CountUint64),
		BytesBeforeH:   numericLabel(self.Bytes.Before, self.NotFoundInA, humanfmt.BytesUint64),
		BytesAfterH:    numericLabel(self.Bytes.After, self.NotFoundInB, humanfmt.BytesUint64),
	}
	objDelta := formatDelta(self.Objects.Before, self.Objects.After, self.Objects.Delta, humanfmt.CountUint64)
	v.ObjectsDeltaH, v.ObjectsPct, v.ObjectsSign = objDelta.DeltaH, objDelta.Pct, objDelta.Sign
	bDelta := formatDelta(self.Bytes.Before, self.Bytes.After, self.Bytes.Delta, humanfmt.BytesUint64)
	v.BytesDeltaH, v.BytesPct, v.BytesSign = bDelta.DeltaH, bDelta.Pct, bDelta.Sign
	if self.HasTierDataA || self.HasTierDataB {
		v.HasCost = true
		costBefore := tierMapCost(self.TierBeforeMap, h.priceTable)
		costAfter := tierMapCost(self.TierAfterMap, h.priceTable)
		v.CostBeforeH = pricing.FormatCost(costBefore)
		v.CostAfterH = pricing.FormatCost(costAfter)
		cDelta := formatCostDelta(costBefore, costAfter)
		v.CostDeltaH, v.CostPct, v.CostSign = cDelta.DeltaH, cDelta.Pct, cDelta.Sign
	}
	apiBefore := pricing.ComputePutCost(self.Objects.Before, h.priceTable)
	apiAfter := pricing.ComputePutCost(self.Objects.After, h.priceTable)
	v.APICostBeforeH = pricing.FormatCost(apiBefore)
	v.APICostAfterH = pricing.FormatCost(apiAfter)
	apiDelta := formatCostDelta(apiBefore, apiAfter)
	v.APICostDeltaH, v.APICostPct, v.APICostSign = apiDelta.DeltaH, apiDelta.Pct, apiDelta.Sign

	return v
}

func (h *Handlers) buildCompareChildView(c *inventory.CompareChild) CompareChildView {
	v := CompareChildView{
		Segment:        c.Segment,
		Prefix:         c.Prefix,
		Status:         c.Status.String(),
		StatusOrder:    inventory.StatusOrder(c.Status),
		HasChildren:    c.HasChildren,
		ObjectsDelta:   c.Objects.Delta,
		BytesDelta:     c.Bytes.Delta,
		ObjectsBeforeH: numericLabel(c.Objects.Before, c.Status == inventory.CompareAdded, humanfmt.CountUint64),
		ObjectsAfterH:  numericLabel(c.Objects.After, c.Status == inventory.CompareRemoved, humanfmt.CountUint64),
		BytesBeforeH:   numericLabel(c.Bytes.Before, c.Status == inventory.CompareAdded, humanfmt.BytesUint64),
		BytesAfterH:    numericLabel(c.Bytes.After, c.Status == inventory.CompareRemoved, humanfmt.BytesUint64),
		AbsByteDelta:   absInt64(c.Bytes.Delta),
	}
	objDelta := formatDelta(c.Objects.Before, c.Objects.After, c.Objects.Delta, humanfmt.CountUint64)
	v.ObjectsDeltaH, v.ObjectsPct, v.ObjectsSign = objDelta.DeltaH, objDelta.Pct, objDelta.Sign
	bDelta := formatDelta(c.Bytes.Before, c.Bytes.After, c.Bytes.Delta, humanfmt.BytesUint64)
	v.BytesDeltaH, v.BytesPct, v.BytesSign = bDelta.DeltaH, bDelta.Pct, bDelta.Sign
	if len(c.TierBefore) > 0 || len(c.TierAfter) > 0 {
		v.HasCost = true
		costBefore := tierMapCost(c.TierBefore, h.priceTable)
		costAfter := tierMapCost(c.TierAfter, h.priceTable)
		v.CostBeforeH = pricing.FormatCost(costBefore)
		v.CostAfterH = pricing.FormatCost(costAfter)
		v.CostDelta = int64(costAfter) - int64(costBefore)
		cDelta := formatCostDelta(costBefore, costAfter)
		v.CostDeltaH, v.CostPct, v.CostSign = cDelta.DeltaH, cDelta.Pct, cDelta.Sign
	}
	apiBefore := pricing.ComputePutCost(c.Objects.Before, h.priceTable)
	apiAfter := pricing.ComputePutCost(c.Objects.After, h.priceTable)
	v.APICostBeforeH = pricing.FormatCost(apiBefore)
	v.APICostAfterH = pricing.FormatCost(apiAfter)
	v.APICostDelta = int64(apiAfter) - int64(apiBefore)
	apiDelta := formatCostDelta(apiBefore, apiAfter)
	v.APICostDeltaH, v.APICostPct, v.APICostSign = apiDelta.DeltaH, apiDelta.Pct, apiDelta.Sign

	return v
}

// numericLabel formats a uint64 with humanfmt unless the prefix was
// missing on that side, in which case the cell shows "—".
func numericLabel(n uint64, missing bool, fn func(uint64) string) string {
	if missing {
		return missingValueGlyph
	}

	return fn(n)
}

// DeltaParts is the formatted-delta triple shared by Browse/Compare:
// the rendered delta string ("+200K"), the percentage label ("+20%"),
// and the sign (-1/0/+1) for template styling.
type DeltaParts struct {
	DeltaH string
	Pct    string
	Sign   int
}

// formatDelta returns ("+200K", "+20%", +1) style triples. A zero
// delta returns ("±0", "", 0) so the template can hide the percentage.
func formatDelta(before, after uint64, delta int64, fn func(uint64) string) DeltaParts {
	switch {
	case delta > 0:
		return DeltaParts{DeltaH: "+" + fn(uint64(delta)), Pct: pctChange(before, after), Sign: 1}
	case delta < 0:
		return DeltaParts{DeltaH: "−" + fn(uint64(-delta)), Pct: pctChange(before, after), Sign: -1}
	default:
		return DeltaParts{DeltaH: "±0"}
	}
}

func formatCostDelta(before, after uint64) DeltaParts {
	switch {
	case after > before:
		return DeltaParts{DeltaH: "+" + pricing.FormatCost(after-before), Pct: pctChange(before, after), Sign: 1}
	case after < before:
		return DeltaParts{DeltaH: "−" + pricing.FormatCost(before-after), Pct: pctChange(before, after), Sign: -1}
	default:
		return DeltaParts{DeltaH: "±0"}
	}
}

// pctChange returns "+20%" or "−13%". When the before-value is zero
// the percentage is undefined; we render "new" / "gone" instead.
func pctChange(before, after uint64) string {
	switch {
	case before == 0 && after > 0:
		return "new"
	case before > 0 && after == 0:
		return "gone"
	case before == 0:
		return ""
	}
	pct := float64(int64(after)-int64(before)) / float64(before) * 100
	if pct > 0 {
		return fmt.Sprintf("+%.0f%%", pct)
	}

	return fmt.Sprintf("%.0f%%", pct)
}

func absInt64(v int64) uint64 {
	if v < 0 {
		return uint64(-v)
	}

	return uint64(v)
}

// tierMapCost flattens a map keyed by tier name back into the price
// engine's slice form so we can reuse pricing.ComputeMonthlyCost.
func tierMapCost(m map[string]indexread.TierBreakdown, prices pricing.PriceTable) uint64 {
	if len(m) == 0 {
		return 0
	}
	breakdown := make([]indexread.TierBreakdown, 0, len(m))
	for _, v := range m {
		breakdown = append(breakdown, v)
	}

	return pricing.ComputeMonthlyCost(breakdown, prices).TotalMicrodollars
}

// sameConfig reports whether two inventory IDs share their first two
// segments. Both inputs must already be 3-part for compare to apply.
func sameConfig(idA, idB inventory.ID) bool {
	a := idA.Split()
	b := idB.Split()

	return a.OK && b.OK && a.Source == b.Source && a.Inventory == b.Inventory
}

// RunDescription is the parsed label/run pair produced by describeRun.
type RunDescription struct {
	ConfigLabel string
	RunLabel    string
}

// describeRun extracts the configuration label ("<src>/<inv>") and the
// formatted run timestamp from an inventory ID. Returns ("", id) when
// the ID isn't 3-part so the page still renders something sensible.
func describeRun(id inventory.ID) RunDescription {
	p := id.Split()
	if !p.OK {
		return RunDescription{RunLabel: string(id)}
	}

	return RunDescription{ConfigLabel: p.Source + "/" + p.Inventory, RunLabel: humanfmt.RunTimestamp(p.Run)}
}

// CompareLevelResponse is the JSON shape returned by CompareLevelAPI. Carries
// the same numeric facts the Compare page renders but drops the Tailwind/
// HTML-only fields and keeps raw int64 deltas so clients can format
// however they like.
type CompareLevelResponse struct {
	From          inventory.ID            `json:"from"`
	To            inventory.ID            `json:"to"`
	Prefix        string                  `json:"prefix"`
	Breadcrumbs   []BrowseCrumbJSON       `json:"breadcrumbs"`
	Self          CompareSelfResponse     `json:"self"`
	Children      []CompareChildResponse  `json:"children"`
	StatusCounts  CompareStatusCountsJSON `json:"status_counts"`
	TotalChildren int                     `json:"total_children"`
	Sort          string                  `json:"sort"`
	Dir           string                  `json:"dir"`
	Pagination    PaginationJSON          `json:"pagination"`
	HideUnchanged bool                    `json:"hide_unchanged"`
	NotFound      bool                    `json:"not_found,omitempty"`
}

// CompareSelfResponse is the prefix-level before/after triple.
type CompareSelfResponse struct {
	ObjectsBefore uint64 `json:"objects_before"`
	ObjectsAfter  uint64 `json:"objects_after"`
	ObjectsDelta  int64  `json:"objects_delta"`
	BytesBefore   uint64 `json:"bytes_before"`
	BytesAfter    uint64 `json:"bytes_after"`
	BytesDelta    int64  `json:"bytes_delta"`

	HasCost                bool   `json:"has_cost"`
	CostBeforeMicrodollars uint64 `json:"cost_before_microdollars,omitempty"`
	CostAfterMicrodollars  uint64 `json:"cost_after_microdollars,omitempty"`
	CostDeltaMicrodollars  int64  `json:"cost_delta_microdollars,omitempty"`

	APICostBeforeMicrodollars uint64 `json:"api_cost_before_microdollars"`
	APICostAfterMicrodollars  uint64 `json:"api_cost_after_microdollars"`
	APICostDeltaMicrodollars  int64  `json:"api_cost_delta_microdollars"`

	NotFoundInFrom bool `json:"not_found_in_from,omitempty"`
	NotFoundInTo   bool `json:"not_found_in_to,omitempty"`
}

// CompareChildResponse is one row in the children comparison.
type CompareChildResponse struct {
	Segment     string `json:"segment"`
	Prefix      string `json:"prefix"`
	Status      string `json:"status"` // added / removed / changed / unchanged
	HasChildren bool   `json:"has_children"`

	ObjectsBefore uint64 `json:"objects_before"`
	ObjectsAfter  uint64 `json:"objects_after"`
	ObjectsDelta  int64  `json:"objects_delta"`

	BytesBefore uint64 `json:"bytes_before"`
	BytesAfter  uint64 `json:"bytes_after"`
	BytesDelta  int64  `json:"bytes_delta"`

	HasCost                bool   `json:"has_cost"`
	CostBeforeMicrodollars uint64 `json:"cost_before_microdollars,omitempty"`
	CostAfterMicrodollars  uint64 `json:"cost_after_microdollars,omitempty"`
	CostDeltaMicrodollars  int64  `json:"cost_delta_microdollars,omitempty"`

	APICostBeforeMicrodollars uint64 `json:"api_cost_before_microdollars"`
	APICostAfterMicrodollars  uint64 `json:"api_cost_after_microdollars"`
	APICostDeltaMicrodollars  int64  `json:"api_cost_delta_microdollars"`
}

// CompareStatusCountsJSON mirrors CompareStatusCounts with JSON tags.
type CompareStatusCountsJSON struct {
	Added     int `json:"added"`
	Removed   int `json:"removed"`
	Changed   int `json:"changed"`
	Unchanged int `json:"unchanged"`
}

// CompareLevelAPI returns the same data the Compare page renders, in JSON.
// Same query parameters: from, to, prefix, sort, dir, page, page_size,
// show_unchanged. 400 on mismatched configurations, 404 when either run
// is unregistered, 409 when either isn't loaded.
//
// The API builds the response directly from inventory.CompareLevel rather
// than reusing the page's view types so the JSON carries raw uint64
// counts and microdollar costs — no formatted strings to parse back.
func (h *Handlers) CompareLevelAPI(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	from := inventory.ID(q.Get("from"))
	to := inventory.ID(q.Get("to"))
	prefix := q.Get("prefix")
	hideUnchanged := q.Get("show_unchanged") != trueLiteral
	pageParams := inventory.NormalizePage(q.Get("page"), q.Get("page_size"))
	page, pageSize := pageParams.Page, pageParams.Size
	sortParams := inventory.NormalizeCompareSort(q.Get("sort"), q.Get("dir"))
	sortBy, dir := sortParams.Col, sortParams.Dir

	if from == "" || to == "" {
		WriteJSONError(w, http.StatusBadRequest, "from and to are required")

		return
	}
	if !sameConfig(from, to) {
		WriteJSONError(w, http.StatusBadRequest, "from and to must belong to the same inventory configuration")

		return
	}

	var data inventory.CompareLevelData
	err := h.manager.WithTwoIndexes(from, to, func(a, b *indexread.Index) error {
		data = inventory.CompareLevel(a, b, prefix)

		return nil
	})
	if err != nil {
		resp := managerErrorStatus(err)
		WriteJSONError(w, resp.Status, resp.Message)

		return
	}

	resp := h.buildCompareAPIResponse(from, to, prefix, sortBy, dir, hideUnchanged, page, pageSize, data)
	WriteJSON(w, http.StatusOK, resp)
}

// buildCompareAPIResponse turns a CompareLevelData into the JSON payload,
// applying the same filter→sort→paginate pipeline as the HTML view but
// with raw numeric output.
func (h *Handlers) buildCompareAPIResponse(from, to inventory.ID, prefix, sortBy, dir string, hideUnchanged bool, page, pageSize int, data inventory.CompareLevelData) CompareLevelResponse {
	resp := CompareLevelResponse{
		From:          from,
		To:            to,
		Prefix:        prefix,
		Sort:          sortBy,
		Dir:           dir,
		HideUnchanged: hideUnchanged,
		NotFound:      data.Self.NotFoundInA && data.Self.NotFoundInB,
	}
	for _, b := range inventory.Breadcrumbs(prefix) {
		resp.Breadcrumbs = append(resp.Breadcrumbs, BrowseCrumbJSON{Label: b.Label, Prefix: b.Prefix})
	}
	resp.Self = h.buildCompareSelfJSON(data.Self)

	rows := make([]CompareChildResponse, 0, len(data.Children))
	for i := range data.Children {
		c := &data.Children[i]
		row := CompareChildResponse{
			Segment:       c.Segment,
			Prefix:        c.Prefix,
			Status:        c.Status.String(),
			HasChildren:   c.HasChildren,
			ObjectsBefore: c.Objects.Before,
			ObjectsAfter:  c.Objects.After,
			ObjectsDelta:  c.Objects.Delta,
			BytesBefore:   c.Bytes.Before,
			BytesAfter:    c.Bytes.After,
			BytesDelta:    c.Bytes.Delta,
		}
		if len(c.TierBefore) > 0 || len(c.TierAfter) > 0 {
			row.HasCost = true
			row.CostBeforeMicrodollars = tierMapCost(c.TierBefore, h.priceTable)
			row.CostAfterMicrodollars = tierMapCost(c.TierAfter, h.priceTable)
			row.CostDeltaMicrodollars = int64(row.CostAfterMicrodollars) - int64(row.CostBeforeMicrodollars)
		}
		row.APICostBeforeMicrodollars = pricing.ComputePutCost(c.Objects.Before, h.priceTable)
		row.APICostAfterMicrodollars = pricing.ComputePutCost(c.Objects.After, h.priceTable)
		row.APICostDeltaMicrodollars = int64(row.APICostAfterMicrodollars) - int64(row.APICostBeforeMicrodollars)
		switch c.Status {
		case inventory.CompareAdded:
			resp.StatusCounts.Added++
		case inventory.CompareRemoved:
			resp.StatusCounts.Removed++
		case inventory.CompareChanged:
			resp.StatusCounts.Changed++
		case inventory.CompareUnchanged:
			resp.StatusCounts.Unchanged++
		}
		rows = append(rows, row)
	}
	if hideUnchanged {
		filtered := rows[:0]
		for i := range rows {
			if rows[i].Status != inventory.CompareUnchanged.String() {
				filtered = append(filtered, rows[i])
			}
		}
		rows = filtered
	}
	sortCompareAPIChildren(rows, sortBy, dir)
	resp.TotalChildren = len(rows)
	p := inventory.Paginate(len(rows), page, pageSize)
	resp.Pagination = paginationFromBrowse(p, len(rows))
	if p.FirstRow > 0 {
		resp.Children = rows[p.FirstRow-1 : p.LastRow]
	} else {
		resp.Children = []CompareChildResponse{}
	}

	return resp
}

func (h *Handlers) buildCompareSelfJSON(self inventory.CompareSelf) CompareSelfResponse {
	r := CompareSelfResponse{
		ObjectsBefore:  self.Objects.Before,
		ObjectsAfter:   self.Objects.After,
		ObjectsDelta:   self.Objects.Delta,
		BytesBefore:    self.Bytes.Before,
		BytesAfter:     self.Bytes.After,
		BytesDelta:     self.Bytes.Delta,
		NotFoundInFrom: self.NotFoundInA,
		NotFoundInTo:   self.NotFoundInB,
	}
	if self.HasTierDataA || self.HasTierDataB {
		r.HasCost = true
		r.CostBeforeMicrodollars = tierMapCost(self.TierBeforeMap, h.priceTable)
		r.CostAfterMicrodollars = tierMapCost(self.TierAfterMap, h.priceTable)
		r.CostDeltaMicrodollars = int64(r.CostAfterMicrodollars) - int64(r.CostBeforeMicrodollars)
	}
	r.APICostBeforeMicrodollars = pricing.ComputePutCost(self.Objects.Before, h.priceTable)
	r.APICostAfterMicrodollars = pricing.ComputePutCost(self.Objects.After, h.priceTable)
	r.APICostDeltaMicrodollars = int64(r.APICostAfterMicrodollars) - int64(r.APICostBeforeMicrodollars)

	return r
}

// sortCompareAPIChildren mirrors sortCompareChildView for the JSON row shape.
// When sortBy is empty the default falls through to biggest |Δ bytes|.
func sortCompareAPIChildren(rows []CompareChildResponse, sortBy, dir string) {
	desc := dir == inventory.SortDirDesc
	less := func(i, j int) bool {
		a, b := &rows[i], &rows[j]
		var primary, equal bool
		switch sortBy {
		case inventory.SortColCompareStatus:
			oa, ob := statusRank(a.Status), statusRank(b.Status)
			primary = oa < ob
			equal = oa == ob
		case inventory.SortColObjects:
			primary = a.ObjectsDelta < b.ObjectsDelta
			equal = a.ObjectsDelta == b.ObjectsDelta
		case inventory.SortColSize:
			primary = a.BytesDelta < b.BytesDelta
			equal = a.BytesDelta == b.BytesDelta
		case inventory.SortColCost:
			primary = a.CostDeltaMicrodollars < b.CostDeltaMicrodollars
			equal = a.CostDeltaMicrodollars == b.CostDeltaMicrodollars
		case inventory.SortColSegment:
			primary = a.Segment < b.Segment
			equal = a.Segment == b.Segment
		default:
			ax, bx := absInt64(a.BytesDelta), absInt64(b.BytesDelta)
			primary = ax < bx
			equal = ax == bx
			desc = true
		}
		if equal {
			return a.Segment < b.Segment
		}
		if desc {
			return !primary
		}

		return primary
	}
	sort.SliceStable(rows, less)
}

// statusRank mirrors inventory.StatusOrder for the JSON status strings.
func statusRank(s string) int {
	switch s {
	case "added":
		return 1
	case "removed":
		return 2
	case statusChangedString:
		return 3
	case "unchanged":
		return 4
	}

	return statusUnknownRank
}

// Status sort keys mirrored from the inventory package; lifted out as
// constants so goconst and mnd don't flag the literals.
const (
	statusChangedString = "changed"
	statusUnknownRank   = 5
)

// paginationFromBrowse converts the domain pagination into the JSON
// shape — keeps the field names aligned with PaginationJSON.
func paginationFromBrowse(p inventory.BrowsePagination, total int) PaginationJSON {
	return PaginationJSON{
		Page:     p.Page,
		PageSize: p.PageSize,
		Pages:    p.Pages,
		Total:    total,
	}
}

// buildComparePicker collects every StateLoaded inventory into per-config
// optgroups. Configurations with only one loaded run still appear so
// the user sees they're available — comparison just isn't possible
// until a second run is loaded.
func buildComparePicker(all []inventory.Info) ComparePicker {
	groups := map[string]*ComparePickerGroup{}
	for i := range all {
		if all[i].State != inventory.StateLoaded {
			continue
		}
		p := all[i].ID.Split()
		if !p.OK {
			continue
		}
		src, inv, run := p.Source, p.Inventory, p.Run
		config := src + "/" + inv
		g, gok := groups[config]
		if !gok {
			g = &ComparePickerGroup{ConfigLabel: config}
			groups[config] = g
		}
		g.Options = append(g.Options, ComparePickerOption{
			ID:    all[i].ID,
			Label: config + " · " + humanfmt.RunTimestamp(run),
		})
	}
	out := ComparePicker{Groups: make([]ComparePickerGroup, 0, len(groups))}
	for _, g := range groups {
		sort.Slice(g.Options, func(i, j int) bool { return g.Options[i].Label > g.Options[j].Label })
		out.Groups = append(out.Groups, *g)
	}
	sort.Slice(out.Groups, func(i, j int) bool { return out.Groups[i].ConfigLabel < out.Groups[j].ConfigLabel })

	return out
}
