package inventory

import (
	"cmp"
	"slices"
	"strings"

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
// the empty state.
type CompareSelf struct {
	TierBeforeMap map[string]indexread.TierBreakdown
	TierAfterMap  map[string]indexread.TierBreakdown
	Prefix        string
	Objects       CompareNumeric
	Bytes         CompareNumeric
	NotFoundInA   bool
	NotFoundInB   bool
	HasTierDataA  bool
	HasTierDataB  bool
}

// CompareChild is one immediate-child segment comparison.
type CompareChild struct {
	TierBefore  map[string]indexread.TierBreakdown
	TierAfter   map[string]indexread.TierBreakdown
	Segment     string
	Prefix      string
	Objects     CompareNumeric
	Bytes       CompareNumeric
	Status      CompareStatus
	HasChildren bool
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
// microdollars. Used only to sort by cost.
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

// compareRow is one child as numbers only; strings and tier maps are
// built for the visible page alone.
type compareRow struct {
	posA, posB uint64
	objects    CompareNumeric
	bytes      CompareNumeric
	costDelta  int64
	inA, inB   bool
	status     CompareStatus
}

// CompareLevel compares two indexes at one prefix and returns the
// self-delta plus the requested page of children present in either side.
// Children are merged, classified, filtered and sorted from numbers and
// cached segment names, so the cost of a page view does not include
// building a string or tier map per child. Cost is used only when
// sorting by cost.
func CompareLevel(a, b *indexread.Index, prefix string, q CompareQuery, cost CompareCostFunc) CompareLevelData {
	out := CompareLevelData{Self: CompareSelf{Prefix: prefix}}
	posA, okA := a.Lookup(prefix)
	posB, okB := b.Lookup(prefix)
	out.Self = compareSelf(a, b, prefix, posA, okA, posB, okB)
	if !okA && !okB {
		return out
	}

	rows := mergeChildren(a, b, childPositions(a, posA, okA), childPositions(b, posB, okB))
	var tiers tierScratch
	for i := range rows {
		r := &rows[i]
		r.status = classifyRow(a, b, r, &tiers)
		out.Counts.add(r.status)
		if q.SortBy == SortColCost && cost != nil {
			r.costDelta = int64(rowCost(b, r.posB, r.inB, cost)) - int64(rowCost(a, r.posA, r.inA, cost))
		}
	}
	if q.HideUnchanged {
		rows = slices.DeleteFunc(rows, func(r compareRow) bool { return r.status == CompareUnchanged })
	}
	sortCompareRows(a, b, rows, q.SortBy, q.Dir)

	out.Total = len(rows)
	out.Pagination = Paginate(len(rows), q.Page, q.PageSize)
	if p := out.Pagination; p.FirstRow > 0 {
		out.Children = make([]CompareChild, 0, p.LastRow-p.FirstRow+1)
		for i := range rows[p.FirstRow-1 : p.LastRow] {
			if c, ok := buildCompareChild(a, b, &rows[p.FirstRow-1+i]); ok {
				out.Children = append(out.Children, c)
			}
		}
	}

	return out
}

func compareSelf(a, b *indexread.Index, prefix string, posA uint64, okA bool, posB uint64, okB bool) CompareSelf {
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
		if self.HasTierDataA {
			self.TierBeforeMap = a.TierBreakdownMap(posA)
		}
	}
	if okB {
		st := b.Stats(posB)
		self.Objects.After = st.ObjectCount
		self.Bytes.After = st.TotalBytes
		if self.HasTierDataB {
			self.TierAfterMap = b.TierBreakdownMap(posB)
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

func childPositions(idx *indexread.Index, parent uint64, ok bool) []uint64 {
	if !ok {
		return nil
	}
	positions, err := idx.DescendantsAtDepth(parent, 1)
	if err != nil {
		return nil
	}

	return positions
}

// mergeChildren joins two child lists, each in index (preorder) order.
// Preorder is byte order of the full prefix, i.e. of each name with its
// trailing slash: "2024-x/" < "2024/" because '-' < '/'. Comparing the
// bare names would split such siblings into an "added" and a "removed".
func mergeChildren(a, b *indexread.Index, pa, pb []uint64) []compareRow {
	out := make([]compareRow, 0, max(len(pa), len(pb)))
	i, j := 0, 0
	for i < len(pa) || j < len(pb) {
		var c int
		switch {
		case i == len(pa):
			c = 1
		case j == len(pb):
			c = -1
		default:
			c = compareSlashTerminated(a.ChildSegment(pa[i]), b.ChildSegment(pb[j]))
		}
		var r compareRow
		if c <= 0 {
			r.posA, r.inA = pa[i], true
			st := a.Stats(pa[i])
			r.objects.Before, r.bytes.Before = st.ObjectCount, st.TotalBytes
			i++
		}
		if c >= 0 {
			r.posB, r.inB = pb[j], true
			st := b.Stats(pb[j])
			r.objects.After, r.bytes.After = st.ObjectCount, st.TotalBytes
			j++
		}
		r.objects.Delta = int64(r.objects.After) - int64(r.objects.Before)
		r.bytes.Delta = int64(r.bytes.After) - int64(r.bytes.Before)
		out = append(out, r)
	}

	return out
}

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

// tierScratch holds the two tier breakdowns classifyRow compares,
// reused across a level's rows so classifying allocates nothing per row.
type tierScratch struct {
	a, b []indexread.TierBreakdown
}

// classifyRow: present on one side only is Added/Removed; present on
// both is Changed if objects, bytes or the tier mix moved. The tier mix
// is read only when objects and bytes are equal.
func classifyRow(a, b *indexread.Index, r *compareRow, tiers *tierScratch) CompareStatus {
	switch {
	case !r.inA:
		return CompareAdded
	case !r.inB:
		return CompareRemoved
	case r.objects.Delta != 0, r.bytes.Delta != 0:
		return CompareChanged
	}
	tiers.a = a.AppendTierBreakdown(tiers.a[:0], r.posA)
	tiers.b = b.AppendTierBreakdown(tiers.b[:0], r.posB)
	if !tiersEqual(tiers.a, tiers.b) {
		return CompareChanged
	}

	return CompareUnchanged
}

// tiersEqual compares two non-zero tier breakdowns; both come in tier-ID
// order, so a positional comparison is exact.
func tiersEqual(x, y []indexread.TierBreakdown) bool {
	return slices.EqualFunc(x, y, func(p, q indexread.TierBreakdown) bool {
		return p.TierID == q.TierID && p.ObjectCount == q.ObjectCount && p.Bytes == q.Bytes
	})
}

func rowCost(idx *indexread.Index, pos uint64, present bool, cost CompareCostFunc) uint64 {
	if !present {
		return 0
	}

	return cost(idx, pos)
}

func rowName(a, b *indexread.Index, r *compareRow) string {
	if r.inA {
		return a.ChildSegment(r.posA)
	}

	return b.ChildSegment(r.posB)
}

// sortCompareRows orders rows for display. With no explicit column the
// biggest absolute byte movers come first. Names, and ties in every
// column, sort in S3 key order, ascending ("2024-01" before "2024").
func sortCompareRows(a, b *indexread.Index, rows []compareRow, sortBy, dir string) {
	desc := dir == SortDirDesc
	metric := func(r *compareRow) int64 {
		switch sortBy {
		case SortColCompareStatus:
			return int64(StatusOrder(r.status))
		case SortColObjects:
			return r.objects.Delta
		case SortColSize:
			return r.bytes.Delta
		case SortColCost:
			return r.costDelta
		case SortColSegment:
			return 0
		default:
			desc = true

			return int64(absDelta(r.bytes.Delta))
		}
	}
	// rows arrive in merge order, S3 key order of the names (see
	// mergeChildren), and the sort is stable: ties keep that order.
	slices.SortStableFunc(rows, func(x, y compareRow) int {
		c := cmp.Compare(metric(&x), metric(&y))
		if sortBy == SortColSegment {
			c = compareSlashTerminated(rowName(a, b, &x), rowName(a, b, &y))
		}
		if desc {
			c = -c
		}

		return c
	})
}

func absDelta(v int64) uint64 {
	if v < 0 {
		return uint64(-v)
	}

	return uint64(v)
}

// buildCompareChild materialises one visible row: its strings and tier
// maps.
func buildCompareChild(a, b *indexread.Index, r *compareRow) (CompareChild, bool) {
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
		Objects: r.objects,
		Bytes:   r.bytes,
		Status:  r.status,
	}
	if r.inA {
		c.HasChildren = c.HasChildren || a.HasChildren(r.posA)
		if a.HasTierData() {
			c.TierBefore = a.TierBreakdownMap(r.posA)
		}
	}
	if r.inB {
		c.HasChildren = c.HasChildren || b.HasChildren(r.posB)
		if b.HasTierData() {
			c.TierAfter = b.TierBreakdownMap(r.posB)
		}
	}

	return c, true
}
