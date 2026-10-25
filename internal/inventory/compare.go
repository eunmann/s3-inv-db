package inventory

import (
	"cmp"
	"slices"
	"strings"

	"github.com/eunmann/s3-inv-db/pkg/format"
	"github.com/eunmann/s3-inv-db/pkg/indexread"
)

// Compare-specific sort column identifiers. The numeric ones (objects /
// size / cost) deliberately reuse the Browse keys so the URLs read
// the same — only the comparator differs. Status is compare-only.
const (
	SortColCompareStatus = "status"
)

// Stable, distinct ranks for each CompareStatus so column sort can
// order rows by status. The numbers carry no meaning beyond keeping
// like-statuses together.
const (
	statusOrderAdded     = 1
	statusOrderRemoved   = 2
	statusOrderChanged   = 3
	statusOrderUnchanged = 4
	statusOrderUnknown   = 5
)

// StatusOrder returns a stable, distinct integer for each CompareStatus
// so the handler can sort rows by status column. Like-statuses cluster.
func StatusOrder(s CompareStatus) int {
	switch s {
	case CompareAdded:
		return statusOrderAdded
	case CompareRemoved:
		return statusOrderRemoved
	case CompareChanged:
		return statusOrderChanged
	case CompareUnchanged:
		return statusOrderUnchanged
	default:
		return statusOrderUnknown
	}
}

// NormalizeCompareSort clamps sort/dir from user input to the compare
// view's known sort columns. Unknown column falls back to "" which the
// handler treats as the "biggest absolute byte mover" default —
// preserves the current first-visit experience.
func NormalizeCompareSort(sortBy, dir string) SortParams {
	return normalizeSortParams(sortBy, dir,
		"",
		[]string{SortColSegment, SortColObjects, SortColSize, SortColCost, SortColCompareStatus},
		[]string{SortColSegment, SortColCompareStatus},
	)
}

// CompareSortLinks builds the per-column {sort, dir, indicator} bundle
// for the compare children table.
func CompareSortLinks(currentSort, currentDir string) map[string]BrowseSortLink {
	cols := []struct {
		key        string
		defaultDir string
	}{
		{SortColCompareStatus, SortDirAsc},
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
				link.Indicator = sortIndicatorAsc
			} else {
				link.Dir = SortDirAsc
				link.Indicator = sortIndicatorDesc
			}
		}
		links[c.key] = link
	}

	return links
}

// Compare-view types and pure helpers. Two loaded indexes are compared at
// one prefix to surface the deltas: how the totals moved, and which
// immediate-child segments grew, shrank, or appeared.

// CompareStatus classifies a child relative to the two runs.
//
//	CompareAdded     — present only in B (the "to" side)
//	CompareRemoved   — present only in A (the "from" side)
//	CompareChanged   — present in both, with a non-zero delta in objects,
//	                bytes, or tier mix (which surfaces as a cost delta
//	                even when objects/bytes are unchanged)
//	CompareUnchanged — present in both with all measured fields equal
type CompareStatus uint8

// Compare status constants. String values match the user-facing badge
// labels in the template so a rename catches both ends. CompareUnknown
// is the zero value: an unclassified row would otherwise default to
// "unchanged" silently. The classifier always returns one of the four
// real states; CompareUnknown only fires if a row is constructed
// without classification, which surfaces immediately in the UI.
const (
	CompareUnknown CompareStatus = iota
	CompareUnchanged
	CompareAdded
	CompareRemoved
	CompareChanged
)

// String returns the user-facing label for the status. Templates render
// {{.Status}} directly into the badge text.
func (s CompareStatus) String() string {
	switch s {
	case CompareAdded:
		return "added"
	case CompareRemoved:
		return "removed"
	case CompareChanged:
		return "changed"
	case CompareUnchanged:
		return "unchanged"
	default:
		return "unknown"
	}
}

// CompareNumeric pairs a before/after value with its signed delta.
// Delta is the int64 of (after - before) so callers can format growth
// (positive, green) or shrinkage (negative, red) symmetrically.
type CompareNumeric struct {
	Before uint64
	After  uint64
	Delta  int64
}

// CompareSelf is the prefix-level comparison: how the totals moved between runs
// at this exact prefix. NotFoundInA/NotFoundInB report whether the
// prefix exists in either side — when both are false the page renders
// the empty state. CostBefore/CostAfter are the monthly storage costs in
// microdollars (zero without a cost function or tier data).
type CompareSelf struct {
	Prefix       string
	Objects      CompareNumeric
	Bytes        CompareNumeric
	CostBefore   uint64
	CostAfter    uint64
	NotFoundInA  bool
	NotFoundInB  bool
	HasTierDataA bool
	HasTierDataB bool
}

// CompareChild is one immediate-child segment comparison. CostBefore and
// CostAfter are its monthly storage costs in microdollars; HasCost is set
// when either side has a tier breakdown to price.
type CompareChild struct {
	Segment     string
	Prefix      string
	Objects     CompareNumeric
	Bytes       CompareNumeric
	CostBefore  uint64
	CostAfter   uint64
	Status      CompareStatus
	HasChildren bool
	HasCost     bool
}

// CompareQuery selects the filter, order and page of one compare level.
type CompareQuery struct {
	SortBy        string
	Dir           string
	Page          int
	PageSize      int
	HideUnchanged bool
}

// CompareCostFunc returns the monthly storage cost of pos in idx, in
// microdollars. CompareLevel prices every child with it when sorting by
// cost, and otherwise only the prefix itself and the visible rows.
type CompareCostFunc func(idx *indexread.Index, pos uint64) uint64

// CompareStatusCounts counts every child of the level by status, before
// HideUnchanged filtering.
type CompareStatusCounts struct {
	Added, Removed, Changed, Unchanged int
}

// CompareLevelData is one compare level: the prefix's own deltas, the
// requested page of children (fully built), and totals over all children.
type CompareLevelData struct {
	Self       CompareSelf
	Children   []CompareChild
	Pagination BrowsePagination
	Counts     CompareStatusCounts
	// Total is the number of children after HideUnchanged filtering.
	Total int
}

// compareRow is one child as positions, its status and its sort key;
// stats, strings and costs are re-read for the visible page alone.
type compareRow struct {
	posA, posB uint64
	// key orders rows by the sort column, ascending; see sortKey.
	key      uint64
	inA, inB bool
	status   CompareStatus
}

// CompareLevel compares two indexes at one prefix and returns the
// self-delta plus the requested page of children present in either side.
// Both child lists are walked in place and merged into one slim row per
// child, classified, filtered and keyed in the same pass, so the cost of
// a page view does not include building a string, tier map or stats
// copy per child. Cost prices every child only when sorting by cost.
func CompareLevel(a, b *indexread.Index, prefix string, q CompareQuery, cost CompareCostFunc) CompareLevelData {
	out := CompareLevelData{Self: CompareSelf{Prefix: prefix}}
	posA, okA := a.Lookup(prefix)
	posB, okB := b.Lookup(prefix)
	out.Self = compareSelf(a, b, prefix, posA, okA, posB, okB, cost)
	if !okA && !okB {
		return out
	}

	m := levelMerge{a: a, b: b, q: q, cost: cost}
	if q.SortBy != SortColCost {
		m.cost = nil
	}
	rows := m.merge(childSpan(a, posA, okA), childSpan(b, posB, okB))
	out.Counts = m.counts
	sortCompareRows(rows, q.SortBy, q.Dir)

	out.Total = len(rows)
	out.Pagination = Paginate(len(rows), q.Page, q.PageSize)
	if p := out.Pagination; p.FirstRow > 0 {
		var tiers tierScratch
		out.Children = make([]CompareChild, 0, p.LastRow-p.FirstRow+1)
		for i := range rows[p.FirstRow-1 : p.LastRow] {
			if c, ok := buildCompareChild(a, b, &rows[p.FirstRow-1+i], cost, &tiers); ok {
				out.Children = append(out.Children, c)
			}
		}
	}

	return out
}

func compareSelf(a, b *indexread.Index, prefix string, posA uint64, okA bool, posB uint64, okB bool, cost CompareCostFunc) CompareSelf {
	self := CompareSelf{Prefix: prefix, NotFoundInA: !okA, NotFoundInB: !okB}
	if !okA && !okB {
		return self
	}
	self.HasTierDataA = a.HasTierData()
	self.HasTierDataB = b.HasTierData()
	if okA {
		st := a.Stats(posA)
		self.Objects.Before = st.ObjectCount
		self.Bytes.Before = st.TotalBytes
		if self.HasTierDataA && cost != nil {
			self.CostBefore = cost(a, posA)
		}
	}
	if okB {
		st := b.Stats(posB)
		self.Objects.After = st.ObjectCount
		self.Bytes.After = st.TotalBytes
		if self.HasTierDataB && cost != nil {
			self.CostAfter = cost(b, posB)
		}
	}
	self.Objects.Delta = int64(self.Objects.After) - int64(self.Objects.Before)
	self.Bytes.Delta = int64(self.Bytes.After) - int64(self.Bytes.Before)

	return self
}

func (c *CompareStatusCounts) add(s CompareStatus) {
	switch s {
	case CompareAdded:
		c.Added++
	case CompareRemoved:
		c.Removed++
	case CompareChanged:
		c.Changed++
	case CompareUnchanged:
		c.Unchanged++
	case CompareUnknown:
	}
}

// childSpan is the depth-index span of parent's children; empty when the
// prefix is missing on that side.
func childSpan(idx *indexread.Index, parent uint64, ok bool) format.Span {
	if !ok {
		return format.Span{}
	}
	span, err := idx.DescendantSpan(parent, 1)
	if err != nil {
		return format.Span{}
	}

	return span
}

// levelMerge holds what one merge pass needs besides the two spans.
type levelMerge struct {
	a, b   *indexread.Index
	q      CompareQuery
	cost   CompareCostFunc // nil unless sorting by cost
	tiers  tierScratch
	counts CompareStatusCounts
}

// merge joins two child spans, each in index (preorder) order, into one
// row per child name. Preorder is byte order of the full prefix, i.e. of
// each name with its trailing slash: "2024-x/" < "2024/" because '-' <
// '/'. Comparing the bare names would split such siblings into an
// "added" and a "removed". Rows come out in that order; every row is
// counted, and unchanged rows are dropped when the query hides them.
func (m *levelMerge) merge(sa, sb format.Span) []compareRow {
	a, b := m.a, m.b
	na, nb := sa.Len(), sb.Len()
	out := make([]compareRow, 0, max(na, nb))
	var i, j uint64
	for i < na || j < nb {
		var pa, pb uint64
		if i < na {
			pa = a.SpanPosition(sa, i)
		}
		if j < nb {
			pb = b.SpanPosition(sb, j)
		}
		var c int
		switch {
		case i == na:
			c = 1
		case j == nb:
			c = -1
		default:
			c = compareSlashTerminated(a.ChildSegment(pa), b.ChildSegment(pb))
		}
		var r compareRow
		var stA, stB indexread.Stats
		if c <= 0 {
			r.posA, r.inA = pa, true
			stA = a.Stats(pa)
			i++
		}
		if c >= 0 {
			r.posB, r.inB = pb, true
			stB = b.Stats(pb)
			j++
		}
		objDelta := int64(stB.ObjectCount) - int64(stA.ObjectCount)
		byteDelta := int64(stB.TotalBytes) - int64(stA.TotalBytes)
		r.status = classifyRow(a, b, &r, objDelta, byteDelta, &m.tiers)
		m.counts.add(r.status)
		if m.q.HideUnchanged && r.status == CompareUnchanged {
			continue
		}
		r.key = m.sortKey(&r, objDelta, byteDelta)
		out = append(out, r)
	}

	return out
}

// sortKey maps the row's value in the sort column to a uint64 whose
// ascending order is the column's ascending order: signed deltas are
// offset by 2^63, the default column is the absolute byte delta.
func (m *levelMerge) sortKey(r *compareRow, objDelta, byteDelta int64) uint64 {
	switch m.q.SortBy {
	case SortColCompareStatus:
		return uint64(StatusOrder(r.status))
	case SortColObjects:
		return signedKey(objDelta)
	case SortColSize:
		return signedKey(byteDelta)
	case SortColCost:
		if m.cost == nil {
			return signedKey(0)
		}
		after := rowCost(m.b, r.posB, r.inB, m.cost)
		before := rowCost(m.a, r.posA, r.inA, m.cost)

		return signedKey(int64(after) - int64(before))
	case SortColSegment:
		return 0
	default:
		return absDelta(byteDelta)
	}
}

// signBit is the int64 sign bit as a uint64.
const signBit = uint64(1) << 63

// signedKey maps int64 order onto uint64 order.
func signedKey(v int64) uint64 { return uint64(v) ^ signBit }

// compareSlashTerminated compares x+"/" with y+"/" without building them.
func compareSlashTerminated(x, y string) int {
	n := min(len(x), len(y))
	if c := strings.Compare(x[:n], y[:n]); c != 0 {
		return c
	}
	switch {
	case len(x) == len(y):
		return 0
	case len(x) < len(y):
		return cmp.Compare(byte('/'), y[n])
	default:
		return cmp.Compare(x[n], byte('/'))
	}
}

// tierScratch holds two reusable tier breakdowns, so classifying and
// pricing rows allocates nothing per row.
type tierScratch struct {
	a, b []indexread.TierBreakdown
}

// classifyRow: present on one side only is Added/Removed; present on
// both is Changed if objects, bytes or the tier mix moved. The tier mix
// is read only when objects and bytes are equal.
func classifyRow(a, b *indexread.Index, r *compareRow, objDelta, byteDelta int64, tiers *tierScratch) CompareStatus {
	switch {
	case !r.inA:
		return CompareAdded
	case !r.inB:
		return CompareRemoved
	case objDelta != 0, byteDelta != 0:
		return CompareChanged
	}
	tiers.a = a.AppendTierBreakdown(tiers.a[:0], r.posA)
	tiers.b = b.AppendTierBreakdown(tiers.b[:0], r.posB)
	if !indexread.TiersEqual(tiers.a, tiers.b) {
		return CompareChanged
	}

	return CompareUnchanged
}

func rowCost(idx *indexread.Index, pos uint64, present bool, cost CompareCostFunc) uint64 {
	if !present {
		return 0
	}

	return cost(idx, pos)
}

// sortCompareRows orders rows for display. With no explicit column the
// biggest absolute byte movers come first. Names, and ties in every
// column, sort in S3 key order, ascending ("2024-01" before "2024").
func sortCompareRows(rows []compareRow, sortBy, dir string) {
	desc := dir == SortDirDesc
	switch sortBy {
	case SortColSegment:
		// rows arrive in S3 key order of their names (see merge), and
		// names are distinct: ascending is that order, descending its
		// reverse.
		if desc {
			slices.Reverse(rows)
		}

		return
	case SortColCompareStatus, SortColObjects, SortColSize, SortColCost:
	default:
		desc = true
	}
	// The sort is stable, so ties keep merge order: name, ascending.
	if desc {
		slices.SortStableFunc(rows, func(x, y compareRow) int { return cmp.Compare(y.key, x.key) })
	} else {
		slices.SortStableFunc(rows, func(x, y compareRow) int { return cmp.Compare(x.key, y.key) })
	}
}

func absDelta(v int64) uint64 {
	if v < 0 {
		return uint64(-v)
	}

	return uint64(v)
}

// buildCompareChild materialises one visible row: its strings, stats and
// costs.
func buildCompareChild(a, b *indexread.Index, r *compareRow, cost CompareCostFunc, tiers *tierScratch) (CompareChild, bool) {
	idx, pos := a, r.posA
	if !r.inA {
		idx, pos = b, r.posB
	}
	full, err := idx.PrefixString(pos)
	if err != nil {
		return CompareChild{}, false
	}
	c := CompareChild{
		Segment: idx.ChildSegment(pos),
		Prefix:  full,
		Status:  r.status,
	}
	if r.inA {
		st := a.Stats(r.posA)
		c.Objects.Before, c.Bytes.Before = st.ObjectCount, st.TotalBytes
		c.HasChildren = a.HasChildren(r.posA)
		tiers.a = a.AppendTierBreakdown(tiers.a[:0], r.posA)
		if len(tiers.a) > 0 && cost != nil {
			c.HasCost = true
			c.CostBefore = cost(a, r.posA)
		}
	}
	if r.inB {
		st := b.Stats(r.posB)
		c.Objects.After, c.Bytes.After = st.ObjectCount, st.TotalBytes
		c.HasChildren = c.HasChildren || b.HasChildren(r.posB)
		tiers.b = b.AppendTierBreakdown(tiers.b[:0], r.posB)
		if len(tiers.b) > 0 && cost != nil {
			c.HasCost = true
			c.CostAfter = cost(b, r.posB)
		}
	}
	c.Objects.Delta = int64(c.Objects.After) - int64(c.Objects.Before)
	c.Bytes.Delta = int64(c.Bytes.After) - int64(c.Bytes.Before)

	return c, true
}
