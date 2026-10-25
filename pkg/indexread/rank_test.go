package indexread_test

import (
	"cmp"
	"fmt"
	"math/rand/v2"
	"path/filepath"
	"slices"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/indexread"
)

// TestRankDescendants_MatchesSortedReference checks RankDescendants,
// which skips depth blocks by their bounds, against sorting every
// descendant: both metrics, both directions, with and without filters,
// on spans of many blocks that start and end mid-block. The shapes
// cover heavy tails, heavy ties (most ranks decided by position) and
// values rising or falling with position, where a plain scan order is
// the worst case for pruning.
func TestRankDescendants_MatchesSortedReference(t *testing.T) {
	shapes := map[string]func(r *rand.Rand, i int) uint64{
		"heavy_tail": func(r *rand.Rand, _ int) uint64 { return 1 + r.Uint64N(1<<40)>>r.UintN(40) },
		"ties":       func(r *rand.Rand, _ int) uint64 { return 100 * (1 + r.Uint64N(2)) },
		"rising":     func(_ *rand.Rand, i int) uint64 { return uint64(1000 + i) },
		"falling":    func(_ *rand.Rand, i int) uint64 { return uint64(1_000_000 - i) },
	}
	for name, size := range shapes {
		t.Run(name, func(t *testing.T) {
			r := rand.New(rand.NewPCG(5, 9))
			keys := make([]string, 0, 16_000)
			sizes := make([]uint64, 0, 16_000)
			child := 0
			for g := range 7 {
				for c := range 300 + r.IntN(900) {
					for f := range 1 + r.IntN(3) {
						keys = append(keys, fmt.Sprintf("g%02d/c%05d/f%d", g, c, f))
						sizes = append(sizes, size(r, child))
					}
					child++
				}
			}
			dir := filepath.Join(t.TempDir(), "index")
			if err := buildIndexFromKeysWithSizes(t, dir, keys, sizes); err != nil {
				t.Fatal(err)
			}
			idx, err := indexread.Open(dir)
			if err != nil {
				t.Fatal(err)
			}
			defer idx.Close()
			checkRankAgainstSort(t, idx)
		})
	}
}

func checkRankAgainstSort(t *testing.T, idx *indexread.Index) {
	t.Helper()
	root, _ := idx.Lookup("")
	type query struct {
		parent   uint64
		relDepth int
	}
	groups, err := idx.DescendantsAtDepth(root, 1)
	if err != nil {
		t.Fatal(err)
	}
	queries := make([]query, 0, 3+2*len(groups))
	queries = append(queries, query{root, 1}, query{root, 2}, query{root, 3})
	for _, g := range groups {
		queries = append(queries, query{g, 1}, query{g, 2})
	}
	for _, q := range queries {
		all, err := idx.DescendantsAtDepth(q.parent, q.relDepth)
		if err != nil {
			t.Fatal(err)
		}
		medianBytes := uint64(0)
		if len(all) > 0 {
			b := make([]uint64, len(all))
			for i, pos := range all {
				b[i] = idx.Stats(pos).TotalBytes
			}
			slices.Sort(b)
			medianBytes = b[len(b)/2]
		}
		for _, filter := range []indexread.Filter{{MinCount: 2}, {MinBytes: medianBytes}, {MinCount: 2, MinBytes: medianBytes}} {
			checkPageAgainstFilter(t, idx, q.parent, q.relDepth, filter)
		}
		for _, metric := range []indexread.TopMetric{indexread.TopByBytesMetric, indexread.TopByCountMetric} {
			for _, asc := range []bool{false, true} {
				for _, filter := range []indexread.Filter{{}, {MinCount: 2}, {MinBytes: medianBytes}} {
					want := sortedReference(idx, all, metric, asc, filter)
					for _, limit := range []int{1, 7, 64, 65, 500, len(all), len(all) + 10} {
						got, err := idx.RankDescendants(q.parent, q.relDepth, limit, metric, asc, filter)
						if err != nil {
							t.Fatal(err)
						}
						n := min(limit, len(want))
						if !slices.Equal(got, want[:n]) {
							t.Fatalf("parent %d depth +%d metric %d asc %v filter %+v limit %d: got %d results, want %d; first diff at %d",
								q.parent, q.relDepth, metric, asc, filter, limit, len(got), n, firstDiff(got, want[:n]))
						}
					}
				}
			}
		}
	}
}

// checkPageAgainstFilter checks DescendantsPage, which settles whole
// depth blocks by their bounds, against windows of the filtered list.
func checkPageAgainstFilter(t *testing.T, idx *indexread.Index, parent uint64, relDepth int, filter indexread.Filter) {
	t.Helper()
	all, err := idx.DescendantsAtDepthFiltered(parent, relDepth, filter)
	if err != nil {
		t.Fatal(err)
	}
	n := len(all)
	for _, w := range []struct{ offset, limit int }{{0, 10}, {0, n}, {n / 3, 100}, {n / 2, 1}, {max(n-5, 0), 10}, {n + 5, 10}, {3, 0}} {
		page, err := idx.DescendantsPage(parent, relDepth, filter, w.offset, w.limit)
		if err != nil {
			t.Fatal(err)
		}
		start := min(w.offset, n)
		want := all[start:min(start+w.limit, n)]
		if page.Total != n || !slices.Equal(page.Positions, want) {
			t.Fatalf("parent %d depth +%d filter %+v window %+v: got total %d and %d positions, want %d and %d",
				parent, relDepth, filter, w, page.Total, len(page.Positions), n, len(want))
		}
	}
}

func sortedReference(idx *indexread.Index, positions []uint64, metric indexread.TopMetric, asc bool, filter indexread.Filter) []indexread.TopResult {
	value := func(s indexread.Stats) uint64 {
		if metric == indexread.TopByCountMetric {
			return s.ObjectCount
		}

		return s.TotalBytes
	}
	var out []indexread.TopResult
	for _, pos := range positions {
		s := idx.Stats(pos)
		if s.ObjectCount >= filter.MinCount && s.TotalBytes >= filter.MinBytes {
			out = append(out, indexread.TopResult{Pos: pos, Stats: s})
		}
	}
	slices.SortFunc(out, func(a, b indexread.TopResult) int {
		c := cmp.Compare(value(b.Stats), value(a.Stats))
		if asc {
			c = -c
		}
		if c != 0 {
			return c
		}

		return cmp.Compare(a.Pos, b.Pos)
	})

	return out
}

func firstDiff(a, b []indexread.TopResult) int {
	for i := range min(len(a), len(b)) {
		if a[i] != b[i] {
			return i
		}
	}

	return min(len(a), len(b))
}
