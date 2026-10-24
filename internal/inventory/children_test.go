package inventory_test

import (
	"path/filepath"
	"testing"

	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/pkg/extsort"
	"github.com/eunmann/s3-inv-db/pkg/indexread"
	"github.com/eunmann/s3-inv-db/pkg/tiers"
	"github.com/google/go-cmp/cmp"
)

// childSpec is one child of the root: count objects totalling bytes.
type childSpec struct {
	name           string
	count, bytes   uint64
	monthlyCostUSD uint64
}

func openChildrenIndex(t *testing.T, children []childSpec) *indexread.Index {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "index")
	agg := extsort.NewAggregator(64, 0)
	for _, c := range children {
		for i := range c.count {
			size := uint64(1)
			if i == c.count-1 {
				size = c.bytes - (c.count - 1)
			}
			agg.AddObject(c.name+"/obj"+string(rune('a'+i%26))+string(rune('a'+i/26)), size, tiers.Standard)
		}
	}
	rows := agg.Drain()
	extsort.SortPrefixRows(rows)
	b, err := extsort.NewIndexBuilder(dir, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := b.SetPresentTiers(agg.PresentTiers()); err != nil {
		t.Fatal(err)
	}
	for _, r := range rows {
		if err := b.Add(r); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.Finalize(); err != nil {
		t.Fatal(err)
	}
	idx, err := indexread.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = idx.Close() })

	return idx
}

func pageNames(t *testing.T, idx *indexread.Index, q inventory.ChildQuery, cost inventory.CostFunc) []string {
	t.Helper()
	root, ok := idx.Lookup("")
	if !ok {
		t.Fatal("root missing")
	}
	page, err := inventory.PageChildren(idx, root, q, cost)
	if err != nil {
		t.Fatal(err)
	}
	names := make([]string, len(page.Positions))
	for i, p := range page.Positions {
		names[i] = idx.ChildSegment(p)
	}

	return names
}

func TestPageChildren_SortOrders(t *testing.T) {
	// Deliberately misaligned orderings so each sort key is visible.
	specs := []childSpec{{"c", 10, 100, 300}, {"a", 30, 200, 100}, {"b", 20, 300, 200}}
	idx := openChildrenIndex(t, specs)
	costByName := map[string]uint64{}
	for _, s := range specs {
		costByName[s.name] = s.monthlyCostUSD
	}
	cost := func(p uint64) uint64 { return costByName[idx.ChildSegment(p)] }

	tests := []struct {
		sortBy, dir string
		want        []string
	}{
		{"segment", "asc", []string{"a", "b", "c"}},
		{"segment", "desc", []string{"c", "b", "a"}},
		{"objects", "desc", []string{"a", "b", "c"}},
		{"objects", "asc", []string{"c", "b", "a"}},
		{"size", "desc", []string{"b", "a", "c"}},
		{"size", "asc", []string{"c", "a", "b"}},
		{"cost", "desc", []string{"c", "b", "a"}},
		{"cost", "asc", []string{"a", "b", "c"}},
	}
	for _, tt := range tests {
		t.Run(tt.sortBy+"_"+tt.dir, func(t *testing.T) {
			got := pageNames(t, idx, inventory.ChildQuery{SortBy: tt.sortBy, Dir: tt.dir, Page: 1, PageSize: 10}, cost)
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("order (-want +got):\n%s", diff)
			}
		})
	}
}

func TestPageChildren_TiesBreakByNameAscending(t *testing.T) {
	idx := openChildrenIndex(t, []childSpec{{"c", 5, 5, 0}, {"a", 5, 5, 0}, {"b", 5, 5, 0}})
	got := pageNames(t, idx, inventory.ChildQuery{SortBy: "objects", Dir: "desc", Page: 1, PageSize: 10}, nil)
	if diff := cmp.Diff([]string{"a", "b", "c"}, got); diff != "" {
		t.Errorf("tie-break (-want +got):\n%s", diff)
	}
}

// Name order is by segment ("2024" < "2024-x"), not by the index's
// full-prefix order ("2024-x/" < "2024/"), and only the page is returned.
func TestPageChildren_NameOrderAndPaging(t *testing.T) {
	idx := openChildrenIndex(t, []childSpec{{"2024-x", 1, 1, 0}, {"2024", 1, 1, 0}, {"2025", 1, 1, 0}, {"2023", 1, 1, 0}})
	got := pageNames(t, idx, inventory.ChildQuery{SortBy: "segment", Dir: "asc", Page: 2, PageSize: 2}, nil)
	if diff := cmp.Diff([]string{"2024-x", "2025"}, got); diff != "" {
		t.Errorf("page 2 (-want +got):\n%s", diff)
	}
}
