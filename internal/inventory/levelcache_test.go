package inventory_test

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/pkg/indexread"
	"github.com/eunmann/s3-inv-db/pkg/tiers"
	"github.com/google/go-cmp/cmp"
)

// wideTieredIndex has more children at the root than a LevelCache's
// threshold, with many equal sizes so ties decide much of every order.
// With drift, some children change, some go and new ones appear.
func wideTieredIndex(t *testing.T, seed int64, drift bool) *indexread.Index {
	t.Helper()
	rng := rand.New(rand.NewSource(seed))
	n := inventory.LevelCacheMinChildren + 1500
	objs := make([]tieredObj, 0, 2*n)
	for i := range n {
		name := fmt.Sprintf("c%05d", i)
		if i%7 == 0 {
			name += "-x" // sorts before "c....../" siblings' slash
		}
		size := uint64(rng.Intn(50) + 1)
		tier := tiers.Standard
		if rng.Intn(3) == 0 {
			tier = tiers.GlacierFR
		}
		if drift {
			switch rng.Intn(10) {
			case 0:
				size += 3
			case 1:
				continue
			case 2:
				tier = tiers.StandardIA
			}
		}
		objs = append(objs, tieredObj{name + "/a", size, tier}, tieredObj{name + "/b", size * 2, tiers.Standard})
	}
	if drift {
		for i := range 300 {
			objs = append(objs, tieredObj{fmt.Sprintf("new%04d/a", i), uint64(i%9 + 1), tiers.Standard})
		}
	}

	return openTieredIndex(t, objs...)
}

func sizeCost(idx *indexread.Index, p uint64) uint64 {
	return idx.Stats(p).TotalBytes*3 + idx.Stats(p).ObjectCount
}

// TestLevelCache_PageChildrenMatchesUncached: every view of a cached
// level (the first, which only marks it; the second, which orders and
// keeps it; and later hits) returns exactly the uncached page.
func TestLevelCache_PageChildrenMatchesUncached(t *testing.T) {
	idx := wideTieredIndex(t, 1, false)
	root, _ := idx.Lookup("")
	cost := func(p uint64) uint64 { return sizeCost(idx, p) }
	cache := inventory.NewLevelCache(64 << 20)
	for _, sortBy := range []string{inventory.SortColObjects, inventory.SortColSize, inventory.SortColCost, inventory.SortColSegment} {
		for _, dir := range []string{inventory.SortDirAsc, inventory.SortDirDesc} {
			for view := range 3 {
				for _, page := range []int{1, 2, 17, 999} {
					q := inventory.ChildQuery{SortBy: sortBy, Dir: dir, Page: page, PageSize: 100}
					want, err := inventory.PageChildren(idx, root, q, cost)
					if err != nil {
						t.Fatal(err)
					}
					got, err := cache.PageChildren(idx, root, q, cost)
					if err != nil {
						t.Fatal(err)
					}
					if diff := cmp.Diff(want, got); diff != "" {
						t.Fatalf("%s %s view %d page %d (-uncached +cached):\n%s", sortBy, dir, view, page, diff)
					}
				}
			}
		}
	}
	if cache.Bytes() == 0 {
		t.Fatal("nothing was cached")
	}
}

// TestLevelCache_CompareMatchesUncached: cached compare levels, first
// view and repeats, equal the uncached result for every order and filter.
func TestLevelCache_CompareMatchesUncached(t *testing.T) {
	before := wideTieredIndex(t, 2, false)
	after := wideTieredIndex(t, 2, true)
	cache := inventory.NewLevelCache(64 << 20)
	for _, sortBy := range []string{"", inventory.SortColCompareStatus, inventory.SortColSegment, inventory.SortColObjects, inventory.SortColSize, inventory.SortColCost} {
		for _, dir := range []string{inventory.SortDirAsc, inventory.SortDirDesc} {
			for _, hide := range []bool{false, true} {
				for view := range 2 {
					for _, page := range []int{1, 3, 999} {
						q := inventory.CompareQuery{SortBy: sortBy, Dir: dir, HideUnchanged: hide, Page: page, PageSize: 50}
						want := inventory.CompareLevel(before, after, "", q, sizeCost)
						got := cache.CompareLevel(before, after, "", q, sizeCost)
						if diff := cmp.Diff(want, got); diff != "" {
							t.Fatalf("%q %s hide=%v view %d page %d (-uncached +cached):\n%s", sortBy, dir, hide, view, page, diff)
						}
					}
				}
			}
		}
	}
}

// TestLevelCache_IndexesDoNotShareEntries: two indexes with the same
// shape at the same positions never see each other's order.
func TestLevelCache_IndexesDoNotShareEntries(t *testing.T) {
	a := wideTieredIndex(t, 3, false)
	b := wideTieredIndex(t, 4, false) // same names, other sizes
	cache := inventory.NewLevelCache(64 << 20)
	q := inventory.ChildQuery{SortBy: inventory.SortColSize, Dir: inventory.SortDirDesc, Page: 1, PageSize: 100}
	for _, idx := range []*indexread.Index{a, a, a, b, b, b} {
		root, _ := idx.Lookup("")
		want, _ := inventory.PageChildren(idx, root, q, nil)
		got, _ := cache.PageChildren(idx, root, q, nil)
		if diff := cmp.Diff(want, got); diff != "" {
			t.Fatalf("(-uncached +cached):\n%s", diff)
		}
	}
}

// TestLevelCache_StaysWithinBudget: a budget smaller than two levels'
// orders keeps the cache within it while every answer stays exact.
func TestLevelCache_StaysWithinBudget(t *testing.T) {
	idx := wideTieredIndex(t, 5, false)
	root, _ := idx.Lookup("")
	budget := int64(inventory.LevelCacheMinChildren+1500)*4 + 4096
	cache := inventory.NewLevelCache(budget)
	for range 3 {
		for _, sortBy := range []string{inventory.SortColObjects, inventory.SortColSize} {
			for _, dir := range []string{inventory.SortDirAsc, inventory.SortDirDesc} {
				q := inventory.ChildQuery{SortBy: sortBy, Dir: dir, Page: 2, PageSize: 100}
				want, _ := inventory.PageChildren(idx, root, q, nil)
				got, _ := cache.PageChildren(idx, root, q, nil)
				if diff := cmp.Diff(want, got); diff != "" {
					t.Fatalf("(-uncached +cached):\n%s", diff)
				}
				if used := cache.Bytes(); used > budget {
					t.Fatalf("cache holds %d bytes, budget %d", used, budget)
				}
			}
		}
	}
}

func TestLevelCache_NilAndDisabledCacheNothing(t *testing.T) {
	if c := inventory.NewLevelCache(0); c != nil {
		t.Fatal("a zero budget must disable the cache")
	}
	var c *inventory.LevelCache
	idx := wideTieredIndex(t, 6, false)
	root, _ := idx.Lookup("")
	q := inventory.ChildQuery{SortBy: inventory.SortColSize, Page: 1, PageSize: 10}
	want, _ := inventory.PageChildren(idx, root, q, nil)
	for range 3 {
		got, _ := c.PageChildren(idx, root, q, nil)
		if diff := cmp.Diff(want, got); diff != "" {
			t.Fatal(diff)
		}
	}
	if c.Bytes() != 0 {
		t.Fatal("nil cache reports bytes")
	}
}
