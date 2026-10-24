package inventory_test

import (
	"path/filepath"
	"sort"
	"testing"

	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/internal/seeder"
	"github.com/eunmann/s3-inv-db/pkg/extsort"
	"github.com/eunmann/s3-inv-db/pkg/indexread"
	"github.com/eunmann/s3-inv-db/pkg/tiers"
	"github.com/google/go-cmp/cmp"
	"github.com/rs/zerolog"
)

type tieredObj struct {
	key  string
	size uint64
	tier tiers.ID
}

func openTieredIndex(t *testing.T, objs ...tieredObj) *indexread.Index {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "index")
	agg := extsort.NewAggregator(len(objs), 0)
	for _, o := range objs {
		agg.AddObject(o.key, o.size, o.tier)
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

// comparePair is a before/after pair of indexes.
type comparePair struct{ before, after *indexread.Index }

// compareFixture: one child per status, including a tier-mix-only change.
func compareFixture(t *testing.T) comparePair {
	t.Helper()
	before := openTieredIndex(t,
		tieredObj{"gone/x", 5, tiers.Standard},
		tieredObj{"obj/a", 50, tiers.Standard}, tieredObj{"obj/b", 50, tiers.Standard},
		tieredObj{"same/x", 7, tiers.Standard},
		tieredObj{"tier/x", 9, tiers.Standard},
	)
	after := openTieredIndex(t,
		tieredObj{"new/x", 3, tiers.Standard},
		tieredObj{"obj/a", 40, tiers.Standard}, tieredObj{"obj/b", 40, tiers.Standard}, tieredObj{"obj/c", 20, tiers.Standard},
		tieredObj{"same/x", 7, tiers.Standard},
		tieredObj{"tier/x", 9, tiers.GlacierFR},
	)

	return comparePair{before: before, after: after}
}

func TestCompareLevel_Statuses(t *testing.T) {
	f := compareFixture(t)
	before, after := f.before, f.after
	got := inventory.CompareLevel(before, after, "", inventory.CompareQuery{SortBy: inventory.SortColSegment, Page: 1, PageSize: 50}, nil)

	want := map[string]inventory.CompareStatus{
		"gone": inventory.CompareRemoved,
		"new":  inventory.CompareAdded,
		"obj":  inventory.CompareChanged, // objects moved, bytes equal
		"same": inventory.CompareUnchanged,
		"tier": inventory.CompareChanged, // only the tier mix moved
	}
	if len(got.Children) != len(want) {
		t.Fatalf("got %d children, want %d", len(got.Children), len(want))
	}
	for _, c := range got.Children {
		if c.Status != want[c.Segment] {
			t.Errorf("%s: status %s, want %s", c.Segment, c.Status, want[c.Segment])
		}
	}
	wantCounts := inventory.CompareStatusCounts{Added: 1, Removed: 1, Changed: 2, Unchanged: 1}
	if got.Counts != wantCounts {
		t.Errorf("counts = %+v, want %+v", got.Counts, wantCounts)
	}
}

func TestCompareLevel_FilterSortPage(t *testing.T) {
	f := compareFixture(t)
	before, after := f.before, f.after
	q := inventory.CompareQuery{SortBy: inventory.SortColSegment, Dir: inventory.SortDirDesc, HideUnchanged: true, Page: 2, PageSize: 2}
	got := inventory.CompareLevel(before, after, "", q, nil)

	if got.Total != 4 {
		t.Errorf("Total = %d, want 4 (unchanged hidden)", got.Total)
	}
	if got.Counts.Unchanged != 1 {
		t.Errorf("Counts must cover hidden rows too: %+v", got.Counts)
	}
	names := make([]string, 0, len(got.Children))
	for _, c := range got.Children {
		names = append(names, c.Segment)
	}
	// Desc by name without "same": tier, obj | new, gone.
	if diff := cmp.Diff([]string{"new", "gone"}, names); diff != "" {
		t.Errorf("page 2 (-want +got):\n%s", diff)
	}
}

func TestCompareLevel_CostSortUsesCostFunc(t *testing.T) {
	f := compareFixture(t)
	before, after := f.before, f.after
	// Make "same" the biggest cost grower although nothing else moved.
	cost := func(idx *indexread.Index, pos uint64) uint64 {
		if idx == after && idx.ChildSegment(pos) == "same" {
			return 1000
		}

		return 0
	}
	got := inventory.CompareLevel(before, after, "", inventory.CompareQuery{SortBy: inventory.SortColCost, Dir: inventory.SortDirDesc, Page: 1, PageSize: 1}, cost)
	if len(got.Children) != 1 || got.Children[0].Segment != "same" {
		t.Errorf("top cost mover = %+v, want same", got.Children)
	}
}

func TestCompareLevel_AgainstSeededIndexes(t *testing.T) {
	a := openSeededIndex(t, 42, 100)
	b := openSeededIndex(t, 42, 200) // same shape, twice as many objects

	got := inventory.CompareLevel(a, b, "", inventory.CompareQuery{Page: 1, PageSize: 500}, nil)
	if got.Self.NotFoundInA || got.Self.NotFoundInB {
		t.Fatalf("root must exist in both indexes; got %+v", got.Self)
	}
	if got.Self.Objects.Before == 0 || got.Self.Objects.After == 0 {
		t.Errorf("expected non-zero object counts on both sides: %+v", got.Self.Objects)
	}
	if got.Self.Objects.Delta <= 0 {
		t.Errorf("After has more objects so Delta should be positive, got %d", got.Self.Objects.Delta)
	}
	if len(got.Children) == 0 {
		t.Fatal("root should have child segments")
	}
	for _, c := range got.Children {
		if c.Status == inventory.CompareUnchanged {
			continue // ok
		}
		if c.Objects.Delta == 0 && c.Bytes.Delta == 0 && cmp.Equal(c.TierBefore, c.TierAfter) {
			t.Errorf("child %q classified %s but all deltas are zero", c.Segment, c.Status)
		}
	}
}

func TestCompareLevel_OneSideMissingPrefix(t *testing.T) {
	a := openSeededIndex(t, 42, 100)
	got := inventory.CompareLevel(a, a, "this/does/not/exist/", inventory.CompareQuery{Page: 1, PageSize: 500}, nil)
	if !got.Self.NotFoundInA || !got.Self.NotFoundInB {
		t.Errorf("missing prefix should mark both sides not-found: %+v", got.Self)
	}
}

func TestNormalizeCompareSort(t *testing.T) {
	cases := []struct {
		name              string
		sort, dir         string
		wantSort, wantDir string
	}{
		{"empty falls through (default sort)", "", "", "", "desc"},
		{"unknown column falls through", "garbage", "", "", "desc"},
		{"status defaults asc", "status", "", "status", "asc"},
		{"segment defaults asc", "segment", "", "segment", "asc"},
		{"objects defaults desc", "objects", "", "objects", "desc"},
		{"size defaults desc", "size", "", "size", "desc"},
		{"cost defaults desc", "cost", "", "cost", "desc"},
		{"explicit asc on numeric wins", "size", "asc", "size", "asc"},
		{"unknown dir falls back per column", "objects", "sideways", "objects", "desc"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := inventory.NormalizeCompareSort(tc.sort, tc.dir)
			gotS, gotD := got.Col, got.Dir
			if gotS != tc.wantSort || gotD != tc.wantDir {
				t.Errorf("inventory.NormalizeCompareSort(%q,%q) = (%q,%q), want (%q,%q)", tc.sort, tc.dir, gotS, gotD, tc.wantSort, tc.wantDir)
			}
		})
	}
}

func TestCompareStatusString(t *testing.T) {
	cases := []struct {
		want string
		s    inventory.CompareStatus
	}{
		{s: inventory.CompareAdded, want: "added"},
		{s: inventory.CompareRemoved, want: "removed"},
		{s: inventory.CompareChanged, want: "changed"},
		{s: inventory.CompareUnchanged, want: "unchanged"},
		{s: inventory.CompareUnknown, want: "unknown"},
		{s: inventory.CompareStatus(99), want: "unknown"},
	}
	for _, tc := range cases {
		if got := tc.s.String(); got != tc.want {
			t.Errorf("inventory.CompareStatus(%d).String() = %q, want %q", tc.s, got, tc.want)
		}
	}
}

func TestStatusOrder_StableDistinctPerStatus(t *testing.T) {
	statuses := []inventory.CompareStatus{inventory.CompareAdded, inventory.CompareRemoved, inventory.CompareChanged, inventory.CompareUnchanged}
	seen := map[int]inventory.CompareStatus{}
	for _, s := range statuses {
		got := inventory.StatusOrder(s)
		if prev, dup := seen[got]; dup {
			t.Errorf("rank %d collides for %s and %s", got, prev, s)
		}
		seen[got] = s
	}
	bogus := inventory.StatusOrder(inventory.CompareStatus(99))
	if _, dup := seen[bogus]; dup {
		t.Errorf("bogus status rank %d collides with a real status", bogus)
	}
}

func TestCompareSortLinks_ClickedColumnFlipsDirection(t *testing.T) {
	links := inventory.CompareSortLinks("size", "desc")
	if links["size"].Dir != "asc" || links["size"].Indicator != "↓" {
		t.Errorf("active column = %+v, want Dir=asc Indicator=↓", links["size"])
	}
	if links["objects"].Dir != "desc" || links["objects"].Indicator != "" {
		t.Errorf("inactive column = %+v, want Dir=desc Indicator=''", links["objects"])
	}
}

func openSeededIndex(t *testing.T, seed int64, objects int) *indexread.Index {
	t.Helper()
	tmp := t.TempDir()
	cfg := seeder.Config{
		OutputDir: tmp,
		Count:     1,
		Objects:   objects,
		Preset:    "small",
		Seed:      seed,
		Logger:    zerolog.Nop(),
	}
	if err := seeder.Run(t.Context(), cfg); err != nil {
		t.Fatalf("seed: %v", err)
	}
	idx, err := indexread.Open(filepath.Join(tmp, "inv-001"))
	if err != nil {
		t.Fatalf("indexread.Open: %v", err)
	}
	t.Cleanup(func() { _ = idx.Close() })

	return idx
}

// buildKeysIndexDir builds an index directory from explicit object keys
// (1 byte each, STANDARD tier).
func buildKeysIndexDir(t *testing.T, keys ...string) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "index")
	agg := extsort.NewAggregator(len(keys), 0)
	for _, k := range keys {
		agg.AddObject(k, 1, tiers.Standard)
	}
	rows := agg.Drain()
	extsort.SortPrefixRows(rows)
	b, err := extsort.NewIndexBuilder(dir, t.TempDir())
	if err != nil {
		t.Fatalf("NewIndexBuilder: %v", err)
	}
	if err := b.SetPresentTiers(agg.PresentTiers()); err != nil {
		t.Fatalf("SetPresentTiers: %v", err)
	}
	for _, r := range rows {
		if err := b.Add(r); err != nil {
			t.Fatalf("Add: %v", err)
		}
	}
	if err := b.Finalize(); err != nil {
		t.Fatalf("Finalize: %v", err)
	}

	return dir
}

// openKeysIndex opens an index built from explicit object keys.
func openKeysIndex(t *testing.T, keys ...string) *indexread.Index {
	t.Helper()
	idx, err := indexread.Open(buildKeysIndexDir(t, keys...))
	if err != nil {
		t.Fatalf("indexread.Open: %v", err)
	}
	t.Cleanup(func() { _ = idx.Close() })

	return idx
}

// Prefixes are stored in byte order of the full prefix, where "2024-x/"
// sorts before "2024/" ('-' < '/') even though segment "2024" sorts
// before "2024-x". Merging on segments split such siblings.
func TestCompareLevel_PunctuatedSiblingsMergeOnce(t *testing.T) {
	before := openKeysIndex(t, "2024-archive/a", "2024/a", "2024.old/a", "2024 copy/a", "2024!/a")
	after := openKeysIndex(t, "2024/a", "2024/b", "2024 copy/a", "2025/a")

	got := inventory.CompareLevel(before, after, "", inventory.CompareQuery{Page: 1, PageSize: 500}, nil)

	want := map[string]inventory.CompareStatus{
		"2024-archive": inventory.CompareRemoved,
		"2024":         inventory.CompareChanged,
		"2024.old":     inventory.CompareRemoved,
		"2024 copy":    inventory.CompareUnchanged,
		"2024!":        inventory.CompareRemoved,
		"2025":         inventory.CompareAdded,
	}
	seen := map[string]int{}
	for _, c := range got.Children {
		seen[c.Segment]++
		if w, ok := want[c.Segment]; !ok || c.Status != w {
			t.Errorf("child %q status %s, want %s", c.Segment, c.Status, w)
		}
	}
	for seg := range want {
		if seen[seg] != 1 {
			t.Errorf("child %q appears %d times, want once", seg, seen[seg])
		}
	}
}

func compareOrder(t *testing.T, before, after *indexread.Index, sortBy, dir string) []string {
	t.Helper()
	got := inventory.CompareLevel(before, after, "", inventory.CompareQuery{SortBy: sortBy, Dir: dir, Page: 1, PageSize: 50}, nil)
	names := make([]string, len(got.Children))
	for i, c := range got.Children {
		names[i] = c.Segment
	}

	return names
}

func TestCompareLevel_SortOrders(t *testing.T) {
	// a grows by 10 bytes, b shrinks by 1000, c grows by 500.
	before := openTieredIndex(t, tieredObj{"a/x", 100, tiers.Standard}, tieredObj{"b/x", 1100, tiers.Standard}, tieredObj{"c/x", 100, tiers.Standard})
	after := openTieredIndex(t, tieredObj{"a/x", 110, tiers.Standard}, tieredObj{"b/x", 100, tiers.Standard}, tieredObj{"c/x", 600, tiers.Standard})

	tests := []struct {
		name, sortBy, dir string
		want              []string
	}{
		{"default biggest absolute mover first", "", "", []string{"b", "c", "a"}},
		{"size asc puts shrinkers first", inventory.SortColSize, inventory.SortDirAsc, []string{"b", "a", "c"}},
		{"size desc puts growers first", inventory.SortColSize, inventory.SortDirDesc, []string{"c", "a", "b"}},
		{"name asc", inventory.SortColSegment, inventory.SortDirAsc, []string{"a", "b", "c"}},
		{"name desc", inventory.SortColSegment, inventory.SortDirDesc, []string{"c", "b", "a"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if diff := cmp.Diff(tt.want, compareOrder(t, before, after, tt.sortBy, tt.dir)); diff != "" {
				t.Errorf("order (-want +got):\n%s", diff)
			}
		})
	}
}

func TestCompareLevel_StatusSort(t *testing.T) {
	f := compareFixture(t)
	before, after := f.before, f.after
	// added, removed, changed (ties by name), unchanged.
	want := []string{"new", "gone", "obj", "tier", "same"}
	if diff := cmp.Diff(want, compareOrder(t, before, after, inventory.SortColCompareStatus, inventory.SortDirAsc)); diff != "" {
		t.Errorf("status order (-want +got):\n%s", diff)
	}
}

type refCompareRow struct {
	name                           string
	status                         inventory.CompareStatus
	objDelta, byteDelta, costDelta int64
}

// referenceCompare joins both sides' children by full prefix through maps
// (no ordering assumptions), classifies, filters and sorts with the
// pre-#26 comparator.
func referenceCompare(a, b *indexread.Index, q inventory.CompareQuery, cost inventory.CompareCostFunc) ([]string, inventory.CompareStatusCounts) {
	type side struct {
		pos uint64
		ok  bool
	}
	byName := map[string][2]side{}
	for i, idx := range []*indexread.Index{a, b} {
		root, _ := idx.Lookup("")
		ps, _ := idx.DescendantsAtDepth(root, 1)
		for _, p := range ps {
			full, _ := idx.PrefixString(p)
			e := byName[full]
			e[i] = side{p, true}
			byName[full] = e
		}
	}
	var rows []refCompareRow
	var counts inventory.CompareStatusCounts
	for full, e := range byName {
		var oa, ob, ba, bb, ca, cbb uint64
		if e[0].ok {
			st := a.Stats(e[0].pos)
			oa, ba, ca = st.ObjectCount, st.TotalBytes, cost(a, e[0].pos)
		}
		if e[1].ok {
			st := b.Stats(e[1].pos)
			ob, bb, cbb = st.ObjectCount, st.TotalBytes, cost(b, e[1].pos)
		}
		r := refCompareRow{name: inventory.SegmentOf("", full), objDelta: int64(ob) - int64(oa), byteDelta: int64(bb) - int64(ba), costDelta: int64(cbb) - int64(ca)}
		switch {
		case !e[0].ok:
			r.status = inventory.CompareAdded
			counts.Added++
		case !e[1].ok:
			r.status = inventory.CompareRemoved
			counts.Removed++
		case r.objDelta != 0 || r.byteDelta != 0 || !cmp.Equal(a.TierBreakdownMap(e[0].pos), b.TierBreakdownMap(e[1].pos)):
			r.status = inventory.CompareChanged
			counts.Changed++
		default:
			r.status = inventory.CompareUnchanged
			counts.Unchanged++
		}
		if q.HideUnchanged && r.status == inventory.CompareUnchanged {
			continue
		}
		rows = append(rows, r)
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].name < rows[j].name }) // deterministic base
	desc := q.Dir == inventory.SortDirDesc
	sort.SliceStable(rows, func(i, j int) bool {
		x, y := rows[i], rows[j]
		var primary, equal bool
		switch q.SortBy {
		case inventory.SortColCompareStatus:
			primary, equal = inventory.StatusOrder(x.status) < inventory.StatusOrder(y.status), x.status == y.status
		case inventory.SortColObjects:
			primary, equal = x.objDelta < y.objDelta, x.objDelta == y.objDelta
		case inventory.SortColSize:
			primary, equal = x.byteDelta < y.byteDelta, x.byteDelta == y.byteDelta
		case inventory.SortColCost:
			primary, equal = x.costDelta < y.costDelta, x.costDelta == y.costDelta
		case inventory.SortColSegment:
			primary, equal = x.name < y.name, x.name == y.name
		default:
			ax, ay := abs64(x.byteDelta), abs64(y.byteDelta)
			primary, equal = ax < ay, ax == ay
			desc = true
		}
		if equal {
			return x.name < y.name
		}
		if desc {
			return !primary
		}

		return primary
	})
	names := make([]string, len(rows))
	for i, r := range rows {
		names[i] = r.name
	}

	return names, counts
}

func abs64(v int64) int64 {
	if v < 0 {
		return -v
	}

	return v
}

func TestCompareLevel_MatchesMaterialisingReference(t *testing.T) {
	cost := func(idx *indexread.Index, p uint64) uint64 { return idx.Stats(p).TotalBytes * 2 }
	for trial := range 15 {
		before := randomTieredIndex(t, int64(100+trial), 250, false)
		after := randomTieredIndex(t, int64(100+trial), 250, true)
		for _, sortBy := range []string{"", inventory.SortColCompareStatus, inventory.SortColSegment, inventory.SortColObjects, inventory.SortColSize, inventory.SortColCost} {
			for _, dir := range []string{inventory.SortDirAsc, inventory.SortDirDesc} {
				for _, hide := range []bool{false, true} {
					q := inventory.CompareQuery{SortBy: sortBy, Dir: dir, HideUnchanged: hide, PageSize: 6}
					want, wantCounts := referenceCompare(before, after, q, cost)
					var got []string
					var data inventory.CompareLevelData
					for page := 1; ; page++ {
						q.Page = page
						data = inventory.CompareLevel(before, after, "", q, cost)
						for _, c := range data.Children {
							got = append(got, c.Segment)
						}
						if len(got) >= data.Total || len(data.Children) == 0 {
							break
						}
					}
					if data.Counts != wantCounts || data.Total != len(want) {
						t.Fatalf("trial %d %q %s hide=%v: counts %+v total %d, want %+v total %d", trial, sortBy, dir, hide, data.Counts, data.Total, wantCounts, len(want))
					}
					if diff := cmp.Diff(want, got); diff != "" {
						t.Fatalf("trial %d %q %s hide=%v (-reference +CompareLevel):\n%s", trial, sortBy, dir, hide, diff)
					}
				}
			}
		}
	}
}
