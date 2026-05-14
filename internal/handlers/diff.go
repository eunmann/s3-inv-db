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

// DiffPicker is the per-configuration run list driving the from/to
// dropdowns. Each configuration's loaded runs are grouped under their
// "<src>/<inv>" label, mirroring the Browse picker's <optgroup>
// presentation so the two pages feel coherent.
type DiffPicker struct {
	Groups []DiffPickerGroup
}

// DiffPickerGroup is one configuration with all of its loaded runs.
type DiffPickerGroup struct {
	ConfigLabel string
	Options     []DiffPickerOption
}

// DiffPickerOption is one loaded run inside a DiffPickerGroup.
type DiffPickerOption struct {
	ID    inventory.ID
	Label string // "<src>/<inv> · <run>" — self-identifying in the closed <select>
}

// DiffPageData is the typed page data for diff.html.
type DiffPageData struct {
	Title       string
	Picker      DiffPicker
	From        inventory.ID
	To          inventory.ID
	ConfigLabel string // "<src>/<inv>" when both IDs share the config
	FromRun     string // formatted run timestamp (or empty)
	ToRun       string // formatted run timestamp (or empty)
	Prefix      string
	Breadcrumbs []BrowseCrumb // shared with Browse; same template idiom
	Error       string        // user-facing setup error (empty when level is shown)
	Level       *DiffLevelView
	Partial     DiffPartialData // shape passed to the inner partial
}

// DiffPartialData carries the diff_level partial's inputs. Used for
// both the htmx-driven swap and the initial full-page render so the
// partial template has one stable shape.
type DiffPartialData struct {
	Prefix      string
	Breadcrumbs []BrowseCrumb
	From, To    inventory.ID
	Level       *DiffLevelView
}

// DiffLevelView is the rendered comparison at one prefix.
type DiffLevelView struct {
	Self DiffSelfView

	Children      []DiffChildView
	TotalChildren int // children before pagination, after hide-unchanged filter
	Status        DiffStatusCounts
	HideUnchanged bool // toggle state for the template
	Pagination    BrowsePagination
	NotFound      bool // prefix missing on both sides — empty state

	Sort      string // current sort column (empty = default)
	Dir       string // current direction (asc/desc)
	SortLinks map[string]inventory.BrowseSortLink
}

// DiffStatusCounts summarises the change set at this prefix.
type DiffStatusCounts struct {
	Added, Removed, Changed, Unchanged int
}

// DiffSelfView is the prefix-level summary card.
type DiffSelfView struct {
	Prefix string

	ObjectsBeforeH, ObjectsAfterH, ObjectsDeltaH, ObjectsPct string
	ObjectsSign                                              int

	BytesBeforeH, BytesAfterH, BytesDeltaH, BytesPct string
	BytesSign                                        int

	HasCost                                      bool
	CostBeforeH, CostAfterH, CostDeltaH, CostPct string
	CostSign                                     int
}

// DiffChildView is one row in the children table.
type DiffChildView struct {
	Segment, Prefix string
	Status          string // human label — template runs diffStatusClass on it
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

	// AbsByteDelta drives the default "biggest absolute mover" sort.
	AbsByteDelta uint64
}

// DiffPage renders the comparison page. Same URL serves the full page
// or the inner level partial dispatched via wantsHTMXPartial, matching
// the Browse handler's pattern.
func (h *Handlers) DiffPage(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	from := inventory.ID(q.Get("from"))
	to := inventory.ID(q.Get("to"))
	prefix := q.Get("prefix")
	hideUnchanged := q.Get("show_unchanged") != "true"
	page, pageSize := inventory.NormalizePage(q.Get("page"), q.Get("page_size"))
	sortBy, dir := inventory.NormalizeDiffSort(q.Get("sort"), q.Get("dir"))

	opts := diffViewOptions{
		from: from, to: to, prefix: prefix,
		hideUnchanged: hideUnchanged,
		page:          page, pageSize: pageSize,
		sortBy: sortBy, dir: dir,
	}
	if wantsHTMXPartial(r) {
		h.renderDiffLevelPartial(w, r, opts)
		return
	}
	h.renderDiffFullPage(w, r, opts)
}

// diffViewOptions bundles the query-string knobs DiffPage parses so
// the inner render functions and computeDiffLevel don't drown in
// positional parameters.
type diffViewOptions struct {
	from, to       inventory.ID
	prefix         string
	hideUnchanged  bool
	page, pageSize int
	sortBy, dir    string
}

func (h *Handlers) renderDiffFullPage(w http.ResponseWriter, r *http.Request, opts diffViewOptions) {
	data := DiffPageData{
		Title:       "Compare runs",
		Picker:      buildDiffPicker(h.manager.List()),
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
		data.ConfigLabel, data.FromRun = describeRun(opts.from)
		_, data.ToRun = describeRun(opts.to)
		level, err := h.computeDiffLevel(r.Context(), opts)
		switch {
		case errors.Is(err, inventory.ErrNotFound):
			data.Error = "One of the runs is no longer registered. Refresh the Inventories page."
		case errors.Is(err, inventory.ErrNotLoaded):
			data.Error = "Both runs must be loaded before they can be compared."
		case err != nil:
			zerolog.Ctx(r.Context()).Error().Err(err).Msg("diff level")
			data.Error = "Failed to compute the diff. See server logs for details."
		default:
			data.Level = level
			data.Partial = DiffPartialData{
				Prefix:      opts.prefix,
				Breadcrumbs: data.Breadcrumbs,
				From:        opts.from,
				To:          opts.to,
				Level:       level,
			}
		}
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := h.renderer.Render(w, "diff.html", data); err != nil {
		zerolog.Ctx(r.Context()).Error().Err(err).Msg("render diff page")
		http.Error(w, "failed to render page", http.StatusInternalServerError)
	}
}

func (h *Handlers) renderDiffLevelPartial(w http.ResponseWriter, r *http.Request, opts diffViewOptions) {
	if opts.from == "" || opts.to == "" {
		http.Error(w, "from and to are required", http.StatusBadRequest)
		return
	}
	if !sameConfig(opts.from, opts.to) {
		http.Error(w, "both runs must belong to the same inventory configuration", http.StatusBadRequest)
		return
	}
	level, err := h.computeDiffLevel(r.Context(), opts)
	if err != nil {
		switch {
		case errors.Is(err, inventory.ErrNotFound):
			http.Error(w, "one of the runs is no longer registered", http.StatusNotFound)
		case errors.Is(err, inventory.ErrNotLoaded):
			http.Error(w, "both runs must be loaded before they can be compared", http.StatusConflict)
		default:
			zerolog.Ctx(r.Context()).Error().Err(err).Msg("diff level partial")
			http.Error(w, "failed to compute diff", http.StatusInternalServerError)
		}
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	view := DiffPartialData{
		Prefix:      opts.prefix,
		Breadcrumbs: inventory.Breadcrumbs(opts.prefix),
		From:        opts.from,
		To:          opts.to,
		Level:       level,
	}
	if err := h.renderer.RenderPartial(w, "diff_level.html", view); err != nil {
		zerolog.Ctx(r.Context()).Error().Err(err).Msg("render diff level partial")
		http.Error(w, "failed to render partial", http.StatusInternalServerError)
	}
}

// computeDiffLevel borrows both indexes and assembles the rendered view
// with cost deltas, signs, and human-formatted numbers. Filters, sorts,
// and paginates the children before returning so the caller doesn't
// have to.
func (h *Handlers) computeDiffLevel(_ context.Context, opts diffViewOptions) (*DiffLevelView, error) {
	var view DiffLevelView
	err := h.manager.WithTwoIndexes(opts.from, opts.to, func(a, b *indexread.Index) error {
		data := inventory.DiffLevel(a, b, opts.prefix)
		view.NotFound = data.Self.NotFoundInA && data.Self.NotFoundInB
		view.Self = h.buildDiffSelfView(data.Self)
		view.Children = make([]DiffChildView, 0, len(data.Children))
		for i := range data.Children {
			c := h.buildDiffChildView(&data.Children[i])
			view.Children = append(view.Children, c)
			switch data.Children[i].Status {
			case inventory.DiffAdded:
				view.Status.Added++
			case inventory.DiffRemoved:
				view.Status.Removed++
			case inventory.DiffChanged:
				view.Status.Changed++
			case inventory.DiffUnchanged:
				view.Status.Unchanged++
			}
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("compute diff: %w", err)
	}
	view.HideUnchanged = opts.hideUnchanged
	if opts.hideUnchanged {
		filtered := view.Children[:0]
		for i := range view.Children {
			if view.Children[i].Status != inventory.DiffUnchanged.String() {
				filtered = append(filtered, view.Children[i])
			}
		}
		view.Children = filtered
	}
	view.Sort = opts.sortBy
	view.Dir = opts.dir
	view.SortLinks = inventory.DiffSortLinks(opts.sortBy, opts.dir)
	sortDiffChildView(view.Children, opts.sortBy, opts.dir)
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

// sortDiffChildView orders the table rows in place. When sortBy is
// empty the default "biggest absolute byte mover" applies — handy on
// first visit because users mostly want to see what moved. Explicit
// sorts use signed deltas so direction picks growers vs shrinkers.
func sortDiffChildView(rows []DiffChildView, sortBy, dir string) {
	desc := dir == inventory.SortDirDesc
	less := func(i, j int) bool {
		a, b := &rows[i], &rows[j]
		var primary, equal bool
		switch sortBy {
		case inventory.SortColDiffStatus:
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

func (h *Handlers) buildDiffSelfView(self inventory.DiffSelf) DiffSelfView {
	v := DiffSelfView{
		Prefix:         self.Prefix,
		ObjectsBeforeH: numericLabel(self.Objects.Before, self.NotFoundInA, humanfmt.CountUint64),
		ObjectsAfterH:  numericLabel(self.Objects.After, self.NotFoundInB, humanfmt.CountUint64),
		BytesBeforeH:   numericLabel(self.Bytes.Before, self.NotFoundInA, humanfmt.BytesUint64),
		BytesAfterH:    numericLabel(self.Bytes.After, self.NotFoundInB, humanfmt.BytesUint64),
	}
	v.ObjectsDeltaH, v.ObjectsPct, v.ObjectsSign = formatDelta(self.Objects.Before, self.Objects.After, self.Objects.Delta, humanfmt.CountUint64)
	v.BytesDeltaH, v.BytesPct, v.BytesSign = formatDelta(self.Bytes.Before, self.Bytes.After, self.Bytes.Delta, humanfmt.BytesUint64)
	if self.HasTierDataA || self.HasTierDataB {
		v.HasCost = true
		costBefore := tierMapCost(self.TierBeforeMap, h.priceTable)
		costAfter := tierMapCost(self.TierAfterMap, h.priceTable)
		v.CostBeforeH = pricing.FormatCost(costBefore)
		v.CostAfterH = pricing.FormatCost(costAfter)
		v.CostDeltaH, v.CostPct, v.CostSign = formatCostDelta(costBefore, costAfter)
	}
	return v
}

func (h *Handlers) buildDiffChildView(c *inventory.DiffChild) DiffChildView {
	v := DiffChildView{
		Segment:        c.Segment,
		Prefix:         c.Prefix,
		Status:         c.Status.String(),
		StatusOrder:    inventory.StatusOrder(c.Status),
		HasChildren:    c.HasChildren,
		ObjectsDelta:   c.Objects.Delta,
		BytesDelta:     c.Bytes.Delta,
		ObjectsBeforeH: numericLabel(c.Objects.Before, c.Status == inventory.DiffAdded, humanfmt.CountUint64),
		ObjectsAfterH:  numericLabel(c.Objects.After, c.Status == inventory.DiffRemoved, humanfmt.CountUint64),
		BytesBeforeH:   numericLabel(c.Bytes.Before, c.Status == inventory.DiffAdded, humanfmt.BytesUint64),
		BytesAfterH:    numericLabel(c.Bytes.After, c.Status == inventory.DiffRemoved, humanfmt.BytesUint64),
		AbsByteDelta:   absInt64(c.Bytes.Delta),
	}
	v.ObjectsDeltaH, v.ObjectsPct, v.ObjectsSign = formatDelta(c.Objects.Before, c.Objects.After, c.Objects.Delta, humanfmt.CountUint64)
	v.BytesDeltaH, v.BytesPct, v.BytesSign = formatDelta(c.Bytes.Before, c.Bytes.After, c.Bytes.Delta, humanfmt.BytesUint64)
	if len(c.TierBefore) > 0 || len(c.TierAfter) > 0 {
		v.HasCost = true
		costBefore := tierMapCost(c.TierBefore, h.priceTable)
		costAfter := tierMapCost(c.TierAfter, h.priceTable)
		v.CostBeforeH = pricing.FormatCost(costBefore)
		v.CostAfterH = pricing.FormatCost(costAfter)
		v.CostDelta = int64(costAfter) - int64(costBefore)
		v.CostDeltaH, v.CostPct, v.CostSign = formatCostDelta(costBefore, costAfter)
	}
	return v
}

// numericLabel formats a uint64 with humanfmt unless the prefix was
// missing on that side, in which case the cell shows "—".
func numericLabel(n uint64, missing bool, fn func(uint64) string) string {
	if missing {
		return "—"
	}
	return fn(n)
}

// formatDelta returns ("+200K", "+20%", +1) style triples. A zero
// delta returns ("±0", "", 0) so the template can hide the percentage.
func formatDelta(before, after uint64, delta int64, fn func(uint64) string) (deltaH, pct string, sign int) {
	switch {
	case delta > 0:
		return "+" + fn(uint64(delta)), pctChange(before, after), 1
	case delta < 0:
		return "−" + fn(uint64(-delta)), pctChange(before, after), -1
	default:
		return "±0", "", 0
	}
}

func formatCostDelta(before, after uint64) (deltaH, pct string, sign int) {
	switch {
	case after > before:
		return "+" + pricing.FormatCost(after-before), pctChange(before, after), 1
	case after < before:
		return "−" + pricing.FormatCost(before-after), pctChange(before, after), -1
	default:
		return "±0", "", 0
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
// segments. Both inputs must already be 3-part for the diff to apply.
func sameConfig(idA, idB inventory.ID) bool {
	srcA, invA, _, okA := idA.Split()
	srcB, invB, _, okB := idB.Split()
	return okA && okB && srcA == srcB && invA == invB
}

// describeRun extracts the configuration label ("<src>/<inv>") and the
// formatted run timestamp from an inventory ID. Returns ("", id) when
// the ID isn't 3-part so the page still renders something sensible.
func describeRun(id inventory.ID) (configLabel, runLabel string) {
	src, inv, run, ok := id.Split()
	if !ok {
		return "", string(id)
	}
	return src + "/" + inv, humanfmt.RunTimestamp(run)
}

// DiffLevelResponse is the JSON shape returned by DiffLevelAPI. Carries
// the same numeric facts the Diff page renders but drops the Tailwind/
// HTML-only fields and keeps raw int64 deltas so clients can format
// however they like.
type DiffLevelResponse struct {
	From          inventory.ID         `json:"from"`
	To            inventory.ID         `json:"to"`
	Prefix        string               `json:"prefix"`
	Breadcrumbs   []BrowseCrumbJSON    `json:"breadcrumbs"`
	Self          DiffSelfResponse     `json:"self"`
	Children      []DiffChildResponse  `json:"children"`
	StatusCounts  DiffStatusCountsJSON `json:"status_counts"`
	TotalChildren int                  `json:"total_children"`
	Sort          string               `json:"sort"`
	Dir           string               `json:"dir"`
	Pagination    PaginationJSON       `json:"pagination"`
	HideUnchanged bool                 `json:"hide_unchanged"`
	NotFound      bool                 `json:"not_found,omitempty"`
}

// DiffSelfResponse is the prefix-level before/after triple.
type DiffSelfResponse struct {
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

	NotFoundInFrom bool `json:"not_found_in_from,omitempty"`
	NotFoundInTo   bool `json:"not_found_in_to,omitempty"`
}

// DiffChildResponse is one row in the children diff.
type DiffChildResponse struct {
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
}

// DiffStatusCountsJSON mirrors DiffStatusCounts with JSON tags.
type DiffStatusCountsJSON struct {
	Added     int `json:"added"`
	Removed   int `json:"removed"`
	Changed   int `json:"changed"`
	Unchanged int `json:"unchanged"`
}

// DiffLevelAPI returns the same data the Diff page renders, in JSON.
// Same query parameters: from, to, prefix, sort, dir, page, page_size,
// show_unchanged. 400 on mismatched configurations, 404 when either run
// is unregistered, 409 when either isn't loaded.
//
// The API builds the response directly from inventory.DiffLevel rather
// than reusing the page's view types so the JSON carries raw uint64
// counts and microdollar costs — no formatted strings to parse back.
func (h *Handlers) DiffLevelAPI(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	from := inventory.ID(q.Get("from"))
	to := inventory.ID(q.Get("to"))
	prefix := q.Get("prefix")
	hideUnchanged := q.Get("show_unchanged") != "true"
	page, pageSize := inventory.NormalizePage(q.Get("page"), q.Get("page_size"))
	sortBy, dir := inventory.NormalizeDiffSort(q.Get("sort"), q.Get("dir"))

	if from == "" || to == "" {
		WriteJSONError(w, http.StatusBadRequest, "from and to are required")
		return
	}
	if !sameConfig(from, to) {
		WriteJSONError(w, http.StatusBadRequest, "from and to must belong to the same inventory configuration")
		return
	}

	var data inventory.DiffLevelData
	err := h.manager.WithTwoIndexes(from, to, func(a, b *indexread.Index) error {
		data = inventory.DiffLevel(a, b, prefix)
		return nil
	})
	if err != nil {
		status, msg := managerErrorStatus(err)
		WriteJSONError(w, status, msg)
		return
	}

	resp := h.buildDiffAPIResponse(from, to, prefix, sortBy, dir, hideUnchanged, page, pageSize, data)
	WriteJSON(w, http.StatusOK, resp)
}

// buildDiffAPIResponse turns a DiffLevelData into the JSON payload,
// applying the same filter→sort→paginate pipeline as the HTML view but
// with raw numeric output.
func (h *Handlers) buildDiffAPIResponse(from, to inventory.ID, prefix, sortBy, dir string, hideUnchanged bool, page, pageSize int, data inventory.DiffLevelData) DiffLevelResponse {
	resp := DiffLevelResponse{
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
	resp.Self = h.buildDiffSelfJSON(data.Self)

	rows := make([]DiffChildResponse, 0, len(data.Children))
	for i := range data.Children {
		c := &data.Children[i]
		row := DiffChildResponse{
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
		switch c.Status {
		case inventory.DiffAdded:
			resp.StatusCounts.Added++
		case inventory.DiffRemoved:
			resp.StatusCounts.Removed++
		case inventory.DiffChanged:
			resp.StatusCounts.Changed++
		case inventory.DiffUnchanged:
			resp.StatusCounts.Unchanged++
		}
		rows = append(rows, row)
	}
	if hideUnchanged {
		filtered := rows[:0]
		for i := range rows {
			if rows[i].Status != inventory.DiffUnchanged.String() {
				filtered = append(filtered, rows[i])
			}
		}
		rows = filtered
	}
	sortDiffAPIChildren(rows, sortBy, dir)
	resp.TotalChildren = len(rows)
	p := inventory.Paginate(len(rows), page, pageSize)
	resp.Pagination = paginationFromBrowse(p, len(rows))
	if p.FirstRow > 0 {
		resp.Children = rows[p.FirstRow-1 : p.LastRow]
	} else {
		resp.Children = []DiffChildResponse{}
	}
	return resp
}

func (h *Handlers) buildDiffSelfJSON(self inventory.DiffSelf) DiffSelfResponse {
	r := DiffSelfResponse{
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
	return r
}

// sortDiffAPIChildren mirrors sortDiffChildView for the JSON row shape.
// When sortBy is empty the default falls through to biggest |Δ bytes|.
func sortDiffAPIChildren(rows []DiffChildResponse, sortBy, dir string) {
	desc := dir == inventory.SortDirDesc
	less := func(i, j int) bool {
		a, b := &rows[i], &rows[j]
		var primary, equal bool
		switch sortBy {
		case inventory.SortColDiffStatus:
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
	case "changed":
		return 3
	case "unchanged":
		return 4
	}
	return 5
}

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

// buildDiffPicker collects every StateLoaded inventory into per-config
// optgroups. Configurations with only one loaded run still appear so
// the user sees they're available — comparison just isn't possible
// until a second run is loaded.
func buildDiffPicker(all []inventory.Info) DiffPicker {
	groups := map[string]*DiffPickerGroup{}
	for i := range all {
		if all[i].State != inventory.StateLoaded {
			continue
		}
		src, inv, run, ok := all[i].ID.Split()
		if !ok {
			continue
		}
		config := src + "/" + inv
		g, gok := groups[config]
		if !gok {
			g = &DiffPickerGroup{ConfigLabel: config}
			groups[config] = g
		}
		g.Options = append(g.Options, DiffPickerOption{
			ID:    all[i].ID,
			Label: config + " · " + humanfmt.RunTimestamp(run),
		})
	}
	out := DiffPicker{Groups: make([]DiffPickerGroup, 0, len(groups))}
	for _, g := range groups {
		sort.Slice(g.Options, func(i, j int) bool { return g.Options[i].Label > g.Options[j].Label })
		out.Groups = append(out.Groups, *g)
	}
	sort.Slice(out.Groups, func(i, j int) bool { return out.Groups[i].ConfigLabel < out.Groups[j].ConfigLabel })
	return out
}
