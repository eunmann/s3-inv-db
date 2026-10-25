package inventory_test

import (
	"math/rand"
	"path/filepath"
	"sort"
	"strconv"
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

// Name order is S3 key order: each name with its trailing slash, as S3
// lists them ("2024-x/" < "2024/" because '-' < '/'). It is the index's
// own order, so only the page is read.
func TestPageChildren_NameOrderAndPaging(t *testing.T) {
	idx := openChildrenIndex(t, []childSpec{{"2024-x", 1, 1, 0}, {"2024", 1, 1, 0}, {"2025", 1, 1, 0}, {"2023", 1, 1, 0}})
	for _, tc := range []struct {
		dir  string
		page int
		want []string
	}{
		{"asc", 1, []string{"2023", "2024-x"}},
		{"asc", 2, []string{"2024", "2025"}},
		{"desc", 1, []string{"2025", "2024"}},
		{"desc", 2, []string{"2024-x", "2023"}},
	} {
		got := pageNames(t, idx, inventory.ChildQuery{SortBy: "segment", Dir: tc.dir, Page: tc.page, PageSize: 2}, nil)
		if diff := cmp.Diff(tc.want, got); diff != "" {
			t.Errorf("%s page %d (-want +got):\n%s", tc.dir, tc.page, diff)
		}
	}
}

// randomTieredIndex builds an index from n random keys whose first
// segment mixes characters that sort just below and above '/'. The same
// seed gives the same objects; with drift, some sizes change, some
// objects are dropped and a few new ones are added, so a compare of the
// two sides has every status.
func randomTieredIndex(t *testing.T, seed int64, n int, drift bool) *indexread.Index {
	t.Helper()
	rng := rand.New(rand.NewSource(seed))
	alphabet := []string{"a", "b", "a-", "a.", "a ", "a!", "a0", "ab", "a_", "b-x", "b.y"}
	objs := make([]tieredObj, 0, n)
	for range n {
		name := alphabet[rng.Intn(len(alphabet))] + alphabet[rng.Intn(len(alphabet))]
		size := uint64(rng.Intn(1000) + 1)
		tier := tiers.Standard
		if rng.Intn(4) == 0 {
			tier = tiers.GlacierFR
		}
		file := "/f" + strconv.Itoa(rng.Intn(5))
		switch r := rng.Intn(20); {
		case drift && r == 0:
			size += 7
		case drift && r == 1:
			continue
		}
		objs = append(objs, tieredObj{key: name + file, size: size, tier: tier})
	}
	if drift {
		for i := range 5 {
			objs = append(objs, tieredObj{key: "new" + alphabet[i] + "/x", size: 1, tier: tiers.Standard})
		}
	}

	return openTieredIndex(t, objs...)
}

type refChild struct {
	name                 string
	objects, bytes, cost uint64
}

// s3Less orders names the way S3 lists keys: each with its trailing
// slash, so "2024-x" sorts before "2024".
func s3Less(a, b string) bool { return a+"/" < b+"/" }

// referenceChildren materialises every child and sorts it by the
// column, ties by name ascending in S3 key order.
func referenceChildren(idx *indexread.Index, cost func(uint64) uint64, sortBy, dir string) []string {
	root, _ := idx.Lookup("")
	positions, _ := idx.DescendantsAtDepth(root, 1)
	rows := make([]refChild, 0, len(positions))
	for _, p := range positions {
		full, _ := idx.PrefixString(p)
		st := idx.Stats(p)
		rows = append(rows, refChild{name: inventory.SegmentOf("", full), objects: st.ObjectCount, bytes: st.TotalBytes, cost: cost(p)})
	}
	sort.SliceStable(rows, func(i, j int) bool {
		a, b := rows[i], rows[j]
		var primary, equal bool
		switch sortBy {
		case inventory.SortColObjects:
			primary, equal = a.objects < b.objects, a.objects == b.objects
		case inventory.SortColSize:
			primary, equal = a.bytes < b.bytes, a.bytes == b.bytes
		case inventory.SortColCost:
			primary, equal = a.cost < b.cost, a.cost == b.cost
		default:
			primary, equal = s3Less(a.name, b.name), a.name == b.name
		}
		if equal {
			return s3Less(a.name, b.name)
		}
		if dir == inventory.SortDirDesc {
			return !primary
		}

		return primary
	})
	out := make([]string, len(rows))
	for i, r := range rows {
		out[i] = r.name
	}

	return out
}

func TestPageChildren_MatchesMaterialisingReference(t *testing.T) {
	for trial := range 20 {
		idx := randomTieredIndex(t, int64(trial), 300, false)
		cost := func(p uint64) uint64 { return idx.Stats(p).TotalBytes*3 + uint64(len(idx.ChildSegment(p))) }
		for _, sortBy := range []string{inventory.SortColSegment, inventory.SortColObjects, inventory.SortColSize, inventory.SortColCost} {
			for _, dir := range []string{inventory.SortDirAsc, inventory.SortDirDesc} {
				want := referenceChildren(idx, cost, sortBy, dir)
				var got []string
				for page := 1; ; page++ {
					names := pageNames(t, idx, inventory.ChildQuery{SortBy: sortBy, Dir: dir, Page: page, PageSize: 7}, cost)
					got = append(got, names...)
					if len(got) >= len(want) || len(names) == 0 {
						break
					}
				}
				if diff := cmp.Diff(want, got); diff != "" {
					t.Fatalf("trial %d sort %s %s (-reference +PageChildren):\n%s", trial, sortBy, dir, diff)
				}
			}
		}
	}
}
