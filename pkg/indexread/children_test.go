package indexread_test

import (
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/indexread"
)

func TestChildSegment_MatchesTrimmedPrefix(t *testing.T) {
	setup := setupIndexFromKeys(t, []string{"data/2024/a.csv", "data/2024-old/b", "data//c", "x y/+z/f", "top.txt"})
	idx, err := indexread.Open(setup.IndexDir)
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()

	want := map[string]string{"": "", "data/": "data", "data/2024/": "2024", "data/2024-old/": "2024-old", "data//": "", "x y/": "x y", "x y/+z/": "+z"}
	for pos := range idx.Count() {
		full, err := idx.PrefixString(pos)
		if err != nil {
			t.Fatal(err)
		}
		w, ok := want[full]
		if !ok {
			t.Fatalf("unexpected prefix %q in index", full)
		}
		if got := idx.ChildSegment(pos); got != w {
			t.Errorf("ChildSegment(%q) = %q, want %q", full, got, w)
		}
	}
	if got := testing.AllocsPerRun(100, func() { _ = idx.ChildSegment(1) }); got != 0 {
		t.Errorf("ChildSegment allocates %.0f times per call, want 0", got)
	}
}
