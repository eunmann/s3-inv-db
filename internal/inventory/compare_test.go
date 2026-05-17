package inventory_test

import (
	"path/filepath"
	"testing"

	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/internal/seeder"
	"github.com/eunmann/s3-inv-db/pkg/indexread"
	"github.com/rs/zerolog"
)

func TestClassify(t *testing.T) {
	tb := func(bytes, count uint64) map[string]indexread.TierBreakdown {
		return map[string]indexread.TierBreakdown{"STANDARD": {TierName: "STANDARD", Bytes: bytes, ObjectCount: count}}
	}
	cases := []struct {
		ta    map[string]indexread.TierBreakdown
		tb    map[string]indexread.TierBreakdown
		name  string
		obj   inventory.CompareNumeric
		bytes inventory.CompareNumeric
		want  inventory.CompareStatus
	}{
		{
			name: "only after = added",
			obj:  inventory.NewCompareNumeric(0, 100), bytes: inventory.NewCompareNumeric(0, 100),
			ta: nil, tb: tb(100, 100),
			want: inventory.CompareAdded,
		},
		{
			name: "only before = removed",
			obj:  inventory.NewCompareNumeric(100, 0), bytes: inventory.NewCompareNumeric(100, 0),
			ta: tb(100, 100), tb: nil,
			want: inventory.CompareRemoved,
		},
		{
			name: "objects moved but bytes same = changed",
			obj:  inventory.NewCompareNumeric(10, 12), bytes: inventory.NewCompareNumeric(100, 100),
			want: inventory.CompareChanged,
		},
		{
			name: "all fields identical = unchanged",
			obj:  inventory.NewCompareNumeric(10, 10), bytes: inventory.NewCompareNumeric(100, 100),
			ta: tb(100, 10), tb: tb(100, 10),
			want: inventory.CompareUnchanged,
		},
		{
			name: "tier mix changed (objects+bytes identical) = changed",
			obj:  inventory.NewCompareNumeric(10, 10), bytes: inventory.NewCompareNumeric(100, 100),
			ta:   map[string]indexread.TierBreakdown{"STANDARD": {Bytes: 100, ObjectCount: 10}},
			tb:   map[string]indexread.TierBreakdown{"GLACIER": {Bytes: 100, ObjectCount: 10}},
			want: inventory.CompareChanged,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := inventory.ClassifyForTest(tc.obj, tc.bytes, tc.ta, tc.tb)
			if got != tc.want {
				t.Errorf("inventory.ClassifyForTest = %s, want %s", got, tc.want)
			}
		})
	}
}

func TestTierMapsEqual(t *testing.T) {
	cases := []struct {
		a    map[string]indexread.TierBreakdown
		b    map[string]indexread.TierBreakdown
		name string
		want bool
	}{
		{name: "both nil", a: nil, b: nil, want: true},
		{name: "same content", a: map[string]indexread.TierBreakdown{"S": {Bytes: 1, ObjectCount: 1}}, b: map[string]indexread.TierBreakdown{"S": {Bytes: 1, ObjectCount: 1}}, want: true},
		{name: "different bytes", a: map[string]indexread.TierBreakdown{"S": {Bytes: 1}}, b: map[string]indexread.TierBreakdown{"S": {Bytes: 2}}, want: false},
		{name: "different keys", a: map[string]indexread.TierBreakdown{"S": {Bytes: 1}}, b: map[string]indexread.TierBreakdown{"G": {Bytes: 1}}, want: false},
		{name: "length differs", a: map[string]indexread.TierBreakdown{"S": {Bytes: 1}}, b: map[string]indexread.TierBreakdown{}, want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := inventory.TierMapsEqualForTest(tc.a, tc.b); got != tc.want {
				t.Errorf("inventory.TierMapsEqualForTest = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestCompareLevel_AgainstSeededIndexes(t *testing.T) {
	a := openSeededIndex(t, 42, 100)
	b := openSeededIndex(t, 42, 200) // same shape, twice as many objects

	got := inventory.CompareLevel(a, b, "")
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
		if c.Objects.Delta == 0 && c.Bytes.Delta == 0 && inventory.TierMapsEqualForTest(c.TierBefore, c.TierAfter) {
			t.Errorf("child %q classified %s but all deltas are zero", c.Segment, c.Status)
		}
	}
}

func TestCompareLevel_OneSideMissingPrefix(t *testing.T) {
	a := openSeededIndex(t, 42, 100)
	got := inventory.CompareLevel(a, a, "this/does/not/exist/")
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
		{s: inventory.CompareStatus(99), want: "unchanged"},
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
	if err := seeder.Run(cfg); err != nil {
		t.Fatalf("seed: %v", err)
	}
	idx, err := indexread.Open(filepath.Join(tmp, "inv-001"))
	if err != nil {
		t.Fatalf("indexread.Open: %v", err)
	}
	t.Cleanup(func() { _ = idx.Close() })

	return idx
}
