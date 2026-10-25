package cli

import (
	"path/filepath"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/extsort"
	"github.com/eunmann/s3-inv-db/pkg/indexread"
	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

type cmpObj struct {
	key  string
	size uint64
	tier tiers.ID
}

func buildCmpIndex(t *testing.T, objs []cmpObj) *indexread.Index {
	t.Helper()
	agg := extsort.NewAggregator(len(objs)*4, 0)
	for _, o := range objs {
		agg.AddObject(o.key, o.size, o.tier)
	}
	rows := agg.Drain()
	extsort.SortPrefixRows(rows)
	dir := filepath.Join(t.TempDir(), "index")
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

// TestDiffLevels_StatusesMatchTheServer: a tier-only change is
// "changed" (it was reported "unchanged"), and siblings whose names
// sort around '/' ("2024-01/" before "2024/") are matched, not split
// into an added and a removed row.
func TestDiffLevels_StatusesMatchTheServer(t *testing.T) {
	from := buildCmpIndex(t, []cmpObj{
		{"2024/a", 10, tiers.Standard},
		{"2024-01/a", 5, tiers.Standard},
		{"tier/a", 7, tiers.Standard},
		{"gone/a", 1, tiers.Standard},
	})
	to := buildCmpIndex(t, []cmpObj{
		{"2024/a", 10, tiers.Standard},
		{"2024-01/a", 6, tiers.Standard},
		{"tier/a", 7, tiers.GlacierFR},
		{"new/a", 1, tiers.Standard},
	})
	fromLevel, err := levelAt(from, "", 1)
	if err != nil {
		t.Fatal(err)
	}
	toLevel, err := levelAt(to, "", 1)
	if err != nil {
		t.Fatal(err)
	}
	rows := diffLevels(fromLevel, toLevel)
	got := map[string]string{}
	order := make([]string, 0, len(rows))
	for _, r := range rows {
		got[r.Prefix] = r.Status
		order = append(order, r.Prefix)
	}
	want := map[string]string{
		"2024-01/": "changed",
		"2024/":    "unchanged",
		"gone/":    "removed",
		"new/":     "added",
		"tier/":    "changed",
	}
	if len(got) != len(want) {
		t.Fatalf("rows = %v, want %v", got, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%s = %q, want %q (all: %v)", k, got[k], v, got)
		}
	}
	if order[0] != "2024-01/" || order[1] != "2024/" {
		t.Errorf("order = %v, want S3 key order", order)
	}
}
