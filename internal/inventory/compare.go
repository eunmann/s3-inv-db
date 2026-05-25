package inventory

import (
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
// for the compare children table. Same shape as inventory.SortLinks so
// the template can reuse the helper pattern.
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
// one prefix to surface the deltas a user can act on: how the totals
// moved, and which immediate-child segments grew, shrank, or appeared.
// HTTP-shape concerns (cost formatting, sort links, pagination wiring)
// live in the handler that composes these values.

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
// labels in the template so a rename catches both ends.
const (
	CompareUnchanged CompareStatus = iota
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
	default:
		return "unchanged"
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

// NewCompareNumeric computes the delta for a before/after pair.
func NewCompareNumeric(before, after uint64) CompareNumeric {
	return CompareNumeric{Before: before, After: after, Delta: int64(after) - int64(before)}
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

// CompareLevelData is the full set of inputs the template needs to render
// one prefix's comparison.
type CompareLevelData struct {
	Self     CompareSelf
	Children []CompareChild
}

// CompareLevel compares two indexes at one prefix and returns the self-
// delta plus a row per child segment present in either side. The
// caller is responsible for filtering (e.g. hide-unchanged), sorting,
// and pagination — keeps this helper pure and order-independent.
func CompareLevel(a, b *indexread.Index, prefix string) CompareLevelData {
	out := CompareLevelData{Self: CompareSelf{Prefix: prefix}}
	posA, okA := a.Lookup(prefix)
	posB, okB := b.Lookup(prefix)
	out.Self.NotFoundInA = !okA
	out.Self.NotFoundInB = !okB
	if !okA && !okB {
		return out
	}

	out.Self.HasTierDataA = a.HasTierData()
	out.Self.HasTierDataB = b.HasTierData()
	if okA {
		st := a.Stats(posA)
		out.Self.Objects.Before = st.ObjectCount
		out.Self.Bytes.Before = st.TotalBytes
		if out.Self.HasTierDataA {
			out.Self.TierBeforeMap = a.TierBreakdownMap(posA)
		}
	}
	if okB {
		st := b.Stats(posB)
		out.Self.Objects.After = st.ObjectCount
		out.Self.Bytes.After = st.TotalBytes
		if out.Self.HasTierDataB {
			out.Self.TierAfterMap = b.TierBreakdownMap(posB)
		}
	}
	out.Self.Objects.Delta = int64(out.Self.Objects.After) - int64(out.Self.Objects.Before)
	out.Self.Bytes.Delta = int64(out.Self.Bytes.After) - int64(out.Self.Bytes.Before)

	childrenA := indexChildren(a, posA, okA, prefix)
	childrenB := indexChildren(b, posB, okB, prefix)
	out.Children = mergeChildren(childrenA, childrenB)

	return out
}

// mergeChildren performs an O(n+m) sorted merge of two pre-sorted
// child-record slices, emitting one CompareChild per distinct
// segment. Replaces the previous map-based join (one map alloc per
// child × two sides) which dominated allocation cost on wide
// prefixes.
func mergeChildren(a, b []segChildRec) []CompareChild {
	out := make([]CompareChild, 0, len(a)+len(b))
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		switch {
		case a[i].seg < b[j].seg:
			out = append(out, fromBefore(a[i]))
			i++
		case a[i].seg > b[j].seg:
			out = append(out, fromAfter(b[j]))
			j++
		default:
			out = append(out, fromBoth(a[i], b[j]))
			i++
			j++
		}
	}
	for ; i < len(a); i++ {
		out = append(out, fromBefore(a[i]))
	}
	for ; j < len(b); j++ {
		out = append(out, fromAfter(b[j]))
	}

	return out
}

func fromBefore(r segChildRec) CompareChild {
	c := CompareChild{
		Segment:     r.seg,
		Prefix:      r.rec.fullPrefix,
		Objects:     CompareNumeric{Before: r.rec.stats.ObjectCount},
		Bytes:       CompareNumeric{Before: r.rec.stats.TotalBytes},
		HasChildren: r.rec.hasChildren,
		TierBefore:  r.rec.tiers,
	}
	c.Objects.Delta = -int64(c.Objects.Before)
	c.Bytes.Delta = -int64(c.Bytes.Before)
	c.Status = classify(c.Objects, c.Bytes, c.TierBefore, c.TierAfter)

	return c
}

func fromAfter(r segChildRec) CompareChild {
	c := CompareChild{
		Segment:     r.seg,
		Prefix:      r.rec.fullPrefix,
		Objects:     CompareNumeric{After: r.rec.stats.ObjectCount},
		Bytes:       CompareNumeric{After: r.rec.stats.TotalBytes},
		HasChildren: r.rec.hasChildren,
		TierAfter:   r.rec.tiers,
	}
	c.Objects.Delta = int64(c.Objects.After)
	c.Bytes.Delta = int64(c.Bytes.After)
	c.Status = classify(c.Objects, c.Bytes, c.TierBefore, c.TierAfter)

	return c
}

func fromBoth(a, b segChildRec) CompareChild {
	c := CompareChild{
		Segment: a.seg,
		// Prefer A's fullPrefix; both are identical to a.seg for the
		// same segment under the same parent.
		Prefix:      a.rec.fullPrefix,
		Objects:     CompareNumeric{Before: a.rec.stats.ObjectCount, After: b.rec.stats.ObjectCount},
		Bytes:       CompareNumeric{Before: a.rec.stats.TotalBytes, After: b.rec.stats.TotalBytes},
		HasChildren: a.rec.hasChildren || b.rec.hasChildren,
		TierBefore:  a.rec.tiers,
		TierAfter:   b.rec.tiers,
	}
	c.Objects.Delta = int64(c.Objects.After) - int64(c.Objects.Before)
	c.Bytes.Delta = int64(c.Bytes.After) - int64(c.Bytes.Before)
	c.Status = classify(c.Objects, c.Bytes, c.TierBefore, c.TierAfter)

	return c
}

type childRec struct {
	tiers       map[string]indexread.TierBreakdown
	fullPrefix  string
	stats       indexread.Stats
	hasChildren bool
}

// segChildRec pairs a child's segment label with its record. Returned
// from indexChildren in segment-sorted order so callers can do an
// O(n+m) sorted merge across two indexes.
type segChildRec struct {
	seg string
	rec childRec
}

// indexChildren returns the children of `parent` as a slice sorted by
// segment. Sortedness comes for free: DescendantsAtDepthFiltered
// returns positions in subtree-preorder, and the underlying prefixes
// are stored in preorder which is lex-sorted, so the derived
// segments are already in order.
func indexChildren(idx *indexread.Index, parent uint64, ok bool, prefix string) []segChildRec {
	if !ok {
		return nil
	}
	positions, err := idx.DescendantsAtDepthFiltered(parent, 1, indexread.Filter{})
	if err != nil {
		return nil
	}
	hasTier := idx.HasTierData()
	out := make([]segChildRec, 0, len(positions))
	parentDepth := idx.Depth(parent)
	for _, p := range positions {
		full, err := idx.PrefixString(p)
		if err != nil {
			continue
		}
		seg := SegmentOf(prefix, full)
		rec := childRec{
			fullPrefix:  full,
			stats:       idx.Stats(p),
			hasChildren: idx.MaxDepthInSubtree(p) > parentDepth+1,
		}
		if hasTier {
			rec.tiers = idx.TierBreakdownMap(p)
		}
		out = append(out, segChildRec{seg: seg, rec: rec})
	}

	return out
}

// classify decides the status of a child given its numeric deltas and
// per-tier maps. A child present only on one side is Added/Removed.
// One present on both is Changed if any measurable field — including
// the tier mix — moved, otherwise Unchanged.
func classify(objects, bytes CompareNumeric, tierBefore, tierAfter map[string]indexread.TierBreakdown) CompareStatus {
	onlyAfter := objects.Before == 0 && bytes.Before == 0 && len(tierBefore) == 0 &&
		(objects.After > 0 || bytes.After > 0 || len(tierAfter) > 0)
	onlyBefore := objects.After == 0 && bytes.After == 0 && len(tierAfter) == 0 &&
		(objects.Before > 0 || bytes.Before > 0 || len(tierBefore) > 0)
	switch {
	case onlyAfter:
		return CompareAdded
	case onlyBefore:
		return CompareRemoved
	case objects.Delta != 0, bytes.Delta != 0, !tierMapsEqual(tierBefore, tierAfter):
		return CompareChanged
	default:
		return CompareUnchanged
	}
}

func tierMapsEqual(a, b map[string]indexread.TierBreakdown) bool {
	if len(a) != len(b) {
		return false
	}
	for k, va := range a {
		vb, ok := b[k]
		if !ok || va.Bytes != vb.Bytes || va.ObjectCount != vb.ObjectCount {
			return false
		}
	}

	return true
}
