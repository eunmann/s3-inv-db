package handlers

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestNormalizeSort(t *testing.T) {
	tests := []struct {
		name        string
		sort, dir   string
		wantCol     string
		wantDirCode string
	}{
		{"defaults", "", "", "segment", "asc"},
		{"unknown column falls back to segment", "garbage", "", "segment", "asc"},
		{"objects defaults to desc", "objects", "", "objects", "desc"},
		{"size defaults to desc", "size", "", "size", "desc"},
		{"cost defaults to desc", "cost", "", "cost", "desc"},
		{"explicit asc on numeric column wins", "objects", "asc", "objects", "asc"},
		{"unknown dir falls back to default", "size", "sideways", "size", "desc"},
		{"unknown dir falls back to asc for segment", "segment", "garbage", "segment", "asc"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotCol, gotDir := normalizeSort(tt.sort, tt.dir)
			if gotCol != tt.wantCol {
				t.Errorf("col = %q, want %q", gotCol, tt.wantCol)
			}
			if gotDir != tt.wantDirCode {
				t.Errorf("dir = %q, want %q", gotDir, tt.wantDirCode)
			}
		})
	}
}

func TestSortChildren(t *testing.T) {
	// Three children with deliberately misaligned orderings so we can tell
	// which sort key was applied.
	mkInput := func() []BrowseChild {
		return []BrowseChild{
			{Segment: "c", ObjectCount: 10, TotalBytes: 100, MonthlyCostMicrodollars: 300},
			{Segment: "a", ObjectCount: 30, TotalBytes: 200, MonthlyCostMicrodollars: 100},
			{Segment: "b", ObjectCount: 20, TotalBytes: 300, MonthlyCostMicrodollars: 200},
		}
	}

	tests := []struct {
		name        string
		sortBy, dir string
		wantOrder   []string // segments in expected order
	}{
		{"segment asc", "segment", "asc", []string{"a", "b", "c"}},
		{"segment desc", "segment", "desc", []string{"c", "b", "a"}},
		{"objects desc", "objects", "desc", []string{"a", "b", "c"}},
		{"objects asc", "objects", "asc", []string{"c", "b", "a"}},
		{"size desc", "size", "desc", []string{"b", "a", "c"}},
		{"size asc", "size", "asc", []string{"c", "a", "b"}},
		{"cost desc", "cost", "desc", []string{"c", "b", "a"}},
		{"cost asc", "cost", "asc", []string{"a", "b", "c"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			children := mkInput()
			sortChildren(children, tt.sortBy, tt.dir)
			got := make([]string, len(children))
			for i, c := range children {
				got[i] = c.Segment
			}
			if diff := cmp.Diff(tt.wantOrder, got); diff != "" {
				t.Errorf("order mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestSortChildren_TieBreakOnSegment(t *testing.T) {
	// All three have the same ObjectCount — the tie-breaker should be
	// alphabetical segment, regardless of direction.
	children := []BrowseChild{
		{Segment: "c", ObjectCount: 5},
		{Segment: "a", ObjectCount: 5},
		{Segment: "b", ObjectCount: 5},
	}
	sortChildren(children, "objects", "desc")
	got := []string{children[0].Segment, children[1].Segment, children[2].Segment}
	want := []string{"a", "b", "c"}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("tie-break order (-want +got):\n%s", diff)
	}
}

func TestSortLinks_IndicatorsAndToggles(t *testing.T) {
	// Currently sorted by objects desc: that column shows ↓, clicking it
	// would flip to asc; other columns offer their defaults with no indicator.
	got := sortLinks("objects", "desc")

	want := map[string]BrowseSortLink{
		"segment": {Sort: "segment", Dir: "asc", Indicator: ""},
		"objects": {Sort: "objects", Dir: "asc", Indicator: "↓"},
		"size":    {Sort: "size", Dir: "desc", Indicator: ""},
		"cost":    {Sort: "cost", Dir: "desc", Indicator: ""},
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("sortLinks (-want +got):\n%s", diff)
	}
}

func TestSortLinks_AscArrowAndAscFlip(t *testing.T) {
	// Currently sorted by segment asc: that column shows ↑, clicking it
	// would flip to desc.
	got := sortLinks("segment", "asc")
	if got["segment"].Indicator != "↑" {
		t.Errorf("segment indicator = %q, want ↑", got["segment"].Indicator)
	}
	if got["segment"].Dir != "desc" {
		t.Errorf("segment next dir = %q, want desc", got["segment"].Dir)
	}
}

func TestBreadcrumbs(t *testing.T) {
	tests := []struct {
		in   string
		want []BrowseCrumb
	}{
		{"", []BrowseCrumb{{Label: "Root", Prefix: ""}}},
		{"foo/", []BrowseCrumb{{Label: "Root", Prefix: ""}, {Label: "foo", Prefix: "foo/"}}},
		{"foo/bar/", []BrowseCrumb{
			{Label: "Root", Prefix: ""},
			{Label: "foo", Prefix: "foo/"},
			{Label: "bar", Prefix: "foo/bar/"},
		}},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			got := breadcrumbs(tt.in)
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("breadcrumbs(%q) (-want +got):\n%s", tt.in, diff)
			}
		})
	}
}

func TestSegmentOf(t *testing.T) {
	tests := []struct {
		parent, child, want string
	}{
		{"", "foo/", "foo"},
		{"foo/", "foo/bar/", "bar"},
		{"foo/bar/", "foo/bar/baz/", "baz"},
	}
	for _, tt := range tests {
		t.Run(tt.parent+"->"+tt.child, func(t *testing.T) {
			if got := segmentOf(tt.parent, tt.child); got != tt.want {
				t.Errorf("segmentOf(%q,%q) = %q, want %q", tt.parent, tt.child, got, tt.want)
			}
		})
	}
}
