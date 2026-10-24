package indexread_test

import (
	"fmt"
	"runtime"
	"slices"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/indexread"
)

const pageTestChildren = 20000

// openWideIndex builds an index whose root has pageTestChildren
// children; every second child holds 1000 bytes, the rest 1.
func openWideIndex(t *testing.T) *indexread.Index {
	t.Helper()
	keys := make([]string, pageTestChildren)
	sizes := make([]uint64, pageTestChildren)
	for i := range keys {
		keys[i] = fmt.Sprintf("p%05d/f", i)
		sizes[i] = 1
		if i%2 == 0 {
			sizes[i] = 1000
		}
	}
	dir := setupTestIndex(t)
	if err := buildIndexFromKeysWithSizes(t, dir, keys, sizes); err != nil {
		t.Fatalf("build: %v", err)
	}
	idx, err := indexread.Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = idx.Close() })

	return idx
}

func TestDescendantsPage_MatchesFilteredWindow(t *testing.T) {
	idx := openWideIndex(t)
	root, _ := idx.Lookup("")
	for _, filter := range []indexread.Filter{{}, {MinBytes: 1000}} {
		all, err := idx.DescendantsAtDepthFiltered(root, 1, filter)
		if err != nil {
			t.Fatal(err)
		}
		for _, w := range []struct{ offset, limit int }{{0, 10}, {7, 3}, {len(all) - 2, 10}, {len(all) + 5, 10}, {3, 0}} {
			page, err := idx.DescendantsPage(root, 1, filter, w.offset, w.limit)
			if err != nil {
				t.Fatal(err)
			}
			start := min(w.offset, len(all))
			want := all[start:min(start+w.limit, len(all))]
			if page.Total != len(all) || !slices.Equal(page.Positions, want) {
				t.Errorf("filter %+v window %+v: got %+v, want total=%d positions=%v", filter, w, page, len(all), want)
			}
		}
	}
	if page, err := idx.DescendantsPage(root, 9, indexread.Filter{}, 0, 10); err != nil || page.Total != 0 || len(page.Positions) != 0 {
		t.Errorf("depth past max: page=%+v err=%v, want empty", page, err)
	}
}

// A page must cost memory in proportion to its limit, not to the number
// of descendants: every request materialised all of them under the
// index read lock, twice with a filter.
func TestDescendantsPage_AllocatesOnlyThePage(t *testing.T) {
	idx := openWideIndex(t)
	root, _ := idx.Lookup("")
	const budget = pageTestChildren * 8 / 4
	for _, filter := range []indexread.Filter{{}, {MinBytes: 1000}} {
		var before, after runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&before)
		page, err := idx.DescendantsPage(root, 1, filter, 100, 10)
		runtime.ReadMemStats(&after)
		if err != nil || len(page.Positions) != 10 || page.Total == 0 {
			t.Fatalf("filter %+v: %d positions, total %d, err %v", filter, len(page.Positions), page.Total, err)
		}
		if got := after.TotalAlloc - before.TotalAlloc; got > budget {
			t.Errorf("filter %+v: one 10-row page allocated %d bytes over %d descendants, want <= %d", filter, got, pageTestChildren, budget)
		}
	}
}
