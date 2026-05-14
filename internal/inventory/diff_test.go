package inventory

import (
	"path/filepath"
	"testing"

	"github.com/eunmann/s3-inv-db/internal/seeder"
	"github.com/eunmann/s3-inv-db/pkg/indexread"
	"github.com/rs/zerolog"
)

func TestClassify(t *testing.T) {
	tb := func(bytes, count uint64) map[string]indexread.TierBreakdown {
		return map[string]indexread.TierBreakdown{"STANDARD": {TierName: "STANDARD", Bytes: bytes, ObjectCount: count}}
	}
	cases := []struct {
		name       string
		obj, bytes DiffNumeric
		ta, tb     map[string]indexread.TierBreakdown
		want       DiffStatus
	}{
		{
			name: "only after = added",
			obj:  NewDiffNumeric(0, 100), bytes: NewDiffNumeric(0, 100),
			ta: nil, tb: tb(100, 100),
			want: DiffAdded,
		},
		{
			name: "only before = removed",
			obj:  NewDiffNumeric(100, 0), bytes: NewDiffNumeric(100, 0),
			ta: tb(100, 100), tb: nil,
			want: DiffRemoved,
		},
		{
			name: "objects moved but bytes same = changed",
			obj:  NewDiffNumeric(10, 12), bytes: NewDiffNumeric(100, 100),
			want: DiffChanged,
		},
		{
			name: "all fields identical = unchanged",
			obj:  NewDiffNumeric(10, 10), bytes: NewDiffNumeric(100, 100),
			ta: tb(100, 10), tb: tb(100, 10),
			want: DiffUnchanged,
		},
		{
			name: "tier mix changed (objects+bytes identical) = changed",
			obj:  NewDiffNumeric(10, 10), bytes: NewDiffNumeric(100, 100),
			ta:   map[string]indexread.TierBreakdown{"STANDARD": {Bytes: 100, ObjectCount: 10}},
			tb:   map[string]indexread.TierBreakdown{"GLACIER": {Bytes: 100, ObjectCount: 10}},
			want: DiffChanged,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := classify(tc.obj, tc.bytes, tc.ta, tc.tb)
			if got != tc.want {
				t.Errorf("classify = %s, want %s", got, tc.want)
			}
		})
	}
}

func TestTierMapsEqual(t *testing.T) {
	cases := []struct {
		name string
		a, b map[string]indexread.TierBreakdown
		want bool
	}{
		{"both nil", nil, nil, true},
		{"same content", map[string]indexread.TierBreakdown{"S": {Bytes: 1, ObjectCount: 1}}, map[string]indexread.TierBreakdown{"S": {Bytes: 1, ObjectCount: 1}}, true},
		{"different bytes", map[string]indexread.TierBreakdown{"S": {Bytes: 1}}, map[string]indexread.TierBreakdown{"S": {Bytes: 2}}, false},
		{"different keys", map[string]indexread.TierBreakdown{"S": {Bytes: 1}}, map[string]indexread.TierBreakdown{"G": {Bytes: 1}}, false},
		{"length differs", map[string]indexread.TierBreakdown{"S": {Bytes: 1}}, map[string]indexread.TierBreakdown{}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tierMapsEqual(tc.a, tc.b); got != tc.want {
				t.Errorf("tierMapsEqual = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestDiffLevel_AgainstSeededIndexes(t *testing.T) {
	a := openSeededIndex(t, 42, 100)
	b := openSeededIndex(t, 42, 200) // same shape, twice as many objects

	got := DiffLevel(a, b, "")
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
		if c.Status == DiffUnchanged {
			continue // ok
		}
		if c.Objects.Delta == 0 && c.Bytes.Delta == 0 && tierMapsEqual(c.TierBefore, c.TierAfter) {
			t.Errorf("child %q classified %s but all deltas are zero", c.Segment, c.Status)
		}
	}
}

func TestDiffLevel_OneSideMissingPrefix(t *testing.T) {
	a := openSeededIndex(t, 42, 100)
	got := DiffLevel(a, a, "this/does/not/exist/")
	if !got.Self.NotFoundInA || !got.Self.NotFoundInB {
		t.Errorf("missing prefix should mark both sides not-found: %+v", got.Self)
	}
}

func TestNormalizeDiffSort(t *testing.T) {
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
			gotS, gotD := NormalizeDiffSort(tc.sort, tc.dir)
			if gotS != tc.wantSort || gotD != tc.wantDir {
				t.Errorf("NormalizeDiffSort(%q,%q) = (%q,%q), want (%q,%q)", tc.sort, tc.dir, gotS, gotD, tc.wantSort, tc.wantDir)
			}
		})
	}
}

func TestDiffStatusString(t *testing.T) {
	cases := []struct {
		s    DiffStatus
		want string
	}{
		{DiffAdded, "added"},
		{DiffRemoved, "removed"},
		{DiffChanged, "changed"},
		{DiffUnchanged, "unchanged"},
		{DiffStatus(99), "unchanged"}, // out-of-range falls through to the safe default
	}
	for _, tc := range cases {
		if got := tc.s.String(); got != tc.want {
			t.Errorf("DiffStatus(%d).String() = %q, want %q", tc.s, got, tc.want)
		}
	}
}

func TestStatusOrder_StableDistinctPerStatus(t *testing.T) {
	// Pin that every concrete status gets its own rank and that the public
	// wrapper matches the unexported helper. The exact integers are an
	// implementation detail — what matters is "stable and distinct".
	statuses := []DiffStatus{DiffAdded, DiffRemoved, DiffChanged, DiffUnchanged}
	seen := map[int]DiffStatus{}
	for _, s := range statuses {
		got := StatusOrder(s)
		if got != statusOrder(s) {
			t.Errorf("public StatusOrder(%s)=%d disagrees with unexported helper", s, got)
		}
		if prev, dup := seen[got]; dup {
			t.Errorf("rank %d collides for %s and %s", got, prev, s)
		}
		seen[got] = s
	}
	// Unknown values must not collide with the four real statuses.
	bogus := StatusOrder(DiffStatus(99))
	if _, dup := seen[bogus]; dup {
		t.Errorf("bogus status rank %d collides with a real status", bogus)
	}
}

func TestDiffSortLinks_ClickedColumnFlipsDirection(t *testing.T) {
	links := DiffSortLinks("size", "desc")
	if links["size"].Dir != "asc" || links["size"].Indicator != "↓" {
		t.Errorf("active column should toggle dir + show indicator, got %+v", links["size"])
	}
	if links["objects"].Dir != "desc" || links["objects"].Indicator != "" {
		t.Errorf("inactive column gets its default dir + no indicator, got %+v", links["objects"])
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
