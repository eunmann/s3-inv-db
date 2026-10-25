package handlers_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"testing"

	"github.com/eunmann/s3-inv-db/internal/handlers"
	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/pkg/extsort"
	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

const gib = 1 << 30

type costObj struct {
	key  string
	size uint64
	tier tiers.ID
}

// loadCostIndex builds an index from objs and loads it into a catalog
// under id.
func loadCostIndex(t *testing.T, id inventory.ID, objs []costObj) *inventory.Catalog {
	t.Helper()
	agg := extsort.NewAggregator(len(objs)*4, 0)
	for _, o := range objs {
		agg.AddObject(o.key, o.size, o.tier)
	}
	rows := agg.Drain()
	extsort.SortPrefixRows(rows)
	dir := filepath.Join(t.TempDir(), "index")
	builder, err := extsort.NewIndexBuilder(dir, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := builder.SetPresentTiers(agg.PresentTiers()); err != nil {
		t.Fatal(err)
	}
	for _, r := range rows {
		if err := builder.Add(r); err != nil {
			t.Fatal(err)
		}
	}
	if err := builder.Finalize(); err != nil {
		t.Fatal(err)
	}
	mgr := inventory.NewCatalog(nil)
	t.Cleanup(func() { _ = mgr.Close() })
	if err := mgr.Register(t.Context(), id, "n", dir); err != nil {
		t.Fatal(err)
	}
	if err := mgr.Load(t.Context(), id); err != nil {
		t.Fatal(err)
	}

	return mgr
}

// TestBrowseLevelAPI_SortByCost: sorting by cost orders children by
// their priced tier mix, not their size. "big" is the largest but sits
// in Glacier, so it costs less than the smaller Standard "mid".
func TestBrowseLevelAPI_SortByCost(t *testing.T) {
	const id = "src/inv/2026-01-01T00-00Z"
	mgr := loadCostIndex(t, id, []costObj{
		{"big/a", 10 * gib, tiers.GlacierFR},
		{"mid/a", 2 * gib, tiers.Standard},
		{"small/a", 1 * gib, tiers.Standard},
	})
	h := newWiredHandlers(t, mgr)

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/browse?inventory_id="+id+"&sort=cost&dir=desc", http.NoBody)
	w := httptest.NewRecorder()
	h.BrowseLevelAPI(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status %d: %s", w.Code, w.Body.String())
	}
	var resp handlers.BrowseLevelResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	got := make([]string, 0, len(resp.Children))
	for _, c := range resp.Children {
		got = append(got, c.Segment)
	}
	want := []string{"mid", "big", "small"}
	if !slices.Equal(got, want) {
		t.Fatalf("children = %v, want %v", got, want)
	}
	for i := 1; i < len(resp.Children); i++ {
		if resp.Children[i].MonthlyCostMicrodollars > resp.Children[i-1].MonthlyCostMicrodollars {
			t.Errorf("costs not descending: %+v", resp.Children)
		}
	}
}
