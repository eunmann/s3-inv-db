package inventory

import (
	"sort"

	"github.com/eunmann/s3-inv-db/pkg/indexread"
)

// Diff-specific sort column identifiers. The numeric ones (objects /
// size / cost) deliberately reuse the Browse keys so the URLs read
// the same — only the comparator differs. Status is diff-only.
const (
	SortColDiffStatus = "status"
)

// statusOrder orders the four DiffStatus values for column sort. The
// numbers don't have a meaning beyond "stable, distinct" — they just
// keep added together, changed together, etc.
func statusOrder(s DiffStatus) int {
	switch s {
	case DiffAdded:
		return 1
	case DiffRemoved:
		return 2
	case DiffChanged:
		return 3
	case DiffUnchanged:
		return 4
	default:
		return 5
	}
}

// NormalizeDiffSort clamps sort/dir from user input to the diff's
// known sort columns. Unknown column falls back to "" which the
// handler treats as the "biggest absolute byte mover" default —
// preserves the current first-visit experience.
func NormalizeDiffSort(sortBy, dir string) (col, direction string) {
	switch sortBy {
	case SortColSegment, SortColObjects, SortColSize, SortColCost, SortColDiffStatus:
		col = sortBy
	default:
		col = ""
	}
	switch dir {
	case SortDirAsc, SortDirDesc:
		direction = dir
	default:
		if col == SortColSegment || col == SortColDiffStatus {
			direction = SortDirAsc
		} else {
			direction = SortDirDesc
		}
	}
	return col, direction
}

// DiffSortLinks builds the per-column {sort, dir, indicator} bundle
// for the diff children table. Same shape as inventory.SortLinks so
// the template can reuse the helper pattern.
func DiffSortLinks(currentSort, currentDir string) map[string]BrowseSortLink {
	cols := []struct {
		key        string
		defaultDir string
	}{
		{SortColDiffStatus, SortDirAsc},
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
				link.Indicator = "↑"
			} else {
				link.Dir = SortDirAsc
				link.Indicator = "↓"
			}
		}
		links[c.key] = link
	}
	return links
}

// StatusOrder returns a stable, distinct integer for each status so
// the handler can sort rows by status column. The numbers have no
// semantic meaning beyond keeping like-statuses together.
func StatusOrder(s DiffStatus) int { return statusOrder(s) }

// Diff-view types and pure helpers. Two loaded indexes are compared at
// one prefix to surface the deltas a user can act on: how the totals
// moved, and which immediate-child segments grew, shrank, or appeared.
// HTTP-shape concerns (cost formatting, sort links, pagination wiring)
// live in the handler that composes these values.

// DiffStatus classifies a child relative to the two runs.
//
//	DiffAdded     — present only in B (the "to" side)
//	DiffRemoved   — present only in A (the "from" side)
//	DiffChanged   — present in both, with a non-zero delta in objects,
//	                bytes, or tier mix (which surfaces as a cost delta
//	                even when objects/bytes are unchanged)
//	DiffUnchanged — present in both with all measured fields equal
type DiffStatus uint8

// Diff status constants. String values match the user-facing badge
// labels in the template so a rename catches both ends.
const (
	DiffUnchanged DiffStatus = iota
	DiffAdded
	DiffRemoved
	DiffChanged
)

// String returns the user-facing label for the status. Templates render
// {{.Status}} directly into the badge text.
func (s DiffStatus) String() string {
	switch s {
	case DiffAdded:
		return "added"
	case DiffRemoved:
		return "removed"
	case DiffChanged:
		return "changed"
	default:
		return "unchanged"
	}
}

// DiffNumeric pairs a before/after value with its signed delta.
// Delta is the int64 of (after - before) so callers can format growth
// (positive, green) or shrinkage (negative, red) symmetrically.
type DiffNumeric struct {
	Before uint64
	After  uint64
	Delta  int64
}

// NewDiffNumeric computes the delta for a before/after pair.
func NewDiffNumeric(before, after uint64) DiffNumeric {
	return DiffNumeric{Before: before, After: after, Delta: int64(after) - int64(before)}
}

// DiffSelf is the prefix-level diff: how the totals moved between runs
// at this exact prefix. NotFoundInA/NotFoundInB report whether the
// prefix exists in either side — when both are false the page renders
// the empty state.
type DiffSelf struct {
	Prefix        string
	Objects       DiffNumeric
	Bytes         DiffNumeric
	NotFoundInA   bool
	NotFoundInB   bool
	HasTierDataA  bool
	HasTierDataB  bool
	TierBeforeMap map[string]indexread.TierBreakdown
	TierAfterMap  map[string]indexread.TierBreakdown
}

// DiffChild is one immediate-child segment diff.
type DiffChild struct {
	Segment     string
	Prefix      string
	Status      DiffStatus
	Objects     DiffNumeric
	Bytes       DiffNumeric
	HasChildren bool // in either A or B — drill-in is possible if true

	// Per-tier breakdown for cost computation downstream. Keyed by
	// tier name. Empty when neither side has tier data.
	TierBefore map[string]indexread.TierBreakdown
	TierAfter  map[string]indexread.TierBreakdown
}

// DiffLevelData is the full set of inputs the template needs to render
// one prefix's comparison.
type DiffLevelData struct {
	Self     DiffSelf
	Children []DiffChild
}

// DiffLevel compares two indexes at one prefix and returns the self-
// delta plus a row per child segment present in either side. The
// caller is responsible for filtering (e.g. hide-unchanged), sorting,
// and pagination — keeps this helper pure and order-independent.
func DiffLevel(a, b *indexread.Index, prefix string) DiffLevelData {
	out := DiffLevelData{Self: DiffSelf{Prefix: prefix}}
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

	merged := map[string]*DiffChild{}
	order := []string{}
	for seg, rec := range childrenA {
		merged[seg] = &DiffChild{
			Segment:     seg,
			Prefix:      rec.fullPrefix,
			Objects:     DiffNumeric{Before: rec.stats.ObjectCount},
			Bytes:       DiffNumeric{Before: rec.stats.TotalBytes},
			HasChildren: rec.hasChildren,
			TierBefore:  rec.tiers,
		}
		order = append(order, seg)
	}
	for seg, rec := range childrenB {
		child, ok := merged[seg]
		if !ok {
			child = &DiffChild{
				Segment:     seg,
				Prefix:      rec.fullPrefix,
				HasChildren: rec.hasChildren,
			}
			merged[seg] = child
			order = append(order, seg)
		}
		child.Objects.After = rec.stats.ObjectCount
		child.Bytes.After = rec.stats.TotalBytes
		if rec.hasChildren {
			child.HasChildren = true
		}
		child.TierAfter = rec.tiers
	}

	sort.Strings(order)
	out.Children = make([]DiffChild, 0, len(order))
	for _, seg := range order {
		c := merged[seg]
		c.Objects.Delta = int64(c.Objects.After) - int64(c.Objects.Before)
		c.Bytes.Delta = int64(c.Bytes.After) - int64(c.Bytes.Before)
		c.Status = classify(c.Objects, c.Bytes, c.TierBefore, c.TierAfter)
		out.Children = append(out.Children, *c)
	}
	return out
}

type childRec struct {
	fullPrefix  string
	stats       indexread.Stats
	tiers       map[string]indexread.TierBreakdown
	hasChildren bool
}

func indexChildren(idx *indexread.Index, parent uint64, ok bool, prefix string) map[string]childRec {
	if !ok {
		return nil
	}
	positions, err := idx.DescendantsAtDepthFiltered(parent, 1, indexread.Filter{})
	if err != nil {
		return nil
	}
	hasTier := idx.HasTierData()
	out := make(map[string]childRec, len(positions))
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
		out[seg] = rec
	}
	return out
}

// classify decides the status of a child given its numeric deltas and
// per-tier maps. A child present only on one side is Added/Removed.
// One present on both is Changed if any measurable field — including
// the tier mix — moved, otherwise Unchanged.
func classify(objects, bytes DiffNumeric, tierBefore, tierAfter map[string]indexread.TierBreakdown) DiffStatus {
	onlyAfter := objects.Before == 0 && bytes.Before == 0 && len(tierBefore) == 0 &&
		(objects.After > 0 || bytes.After > 0 || len(tierAfter) > 0)
	onlyBefore := objects.After == 0 && bytes.After == 0 && len(tierAfter) == 0 &&
		(objects.Before > 0 || bytes.Before > 0 || len(tierBefore) > 0)
	switch {
	case onlyAfter:
		return DiffAdded
	case onlyBefore:
		return DiffRemoved
	case objects.Delta != 0, bytes.Delta != 0, !tierMapsEqual(tierBefore, tierAfter):
		return DiffChanged
	default:
		return DiffUnchanged
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
