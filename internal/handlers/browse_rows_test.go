package handlers_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/eunmann/s3-inv-db/internal/handlers"
	"github.com/eunmann/s3-inv-db/pkg/indexread"
	"github.com/eunmann/s3-inv-db/pkg/pricing"
	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

// TestBrowseLevelAPI_RowsMatchIndex: every row's prefix, numbers and
// cost equal what the index gives for that child directly (its full
// prefix and a full cost computation), for names with empty, punctuated
// and multi-byte segments, at the root and one level down.
func TestBrowseLevelAPI_RowsMatchIndex(t *testing.T) {
	const id = "src/inv/2026-01-01T00-00Z"
	mgr := loadCostIndex(t, id, []costObj{
		{"a//x", 3 * gib, tiers.Standard},
		{"a/b/x", 1 * gib, tiers.GlacierFR},
		{"a-b/x", 2 * gib, tiers.StandardIA},
		{"a/b-c/y", 5, tiers.Standard},
		{"émoji/ü/z", 7 * gib, tiers.Standard},
		{"top", 1, tiers.Standard},
	})
	h := newWiredHandlers(t, mgr)
	prices := pricing.DefaultUSEast1Prices()
	for _, prefix := range []string{"", "a/", "émoji/"} {
		req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/browse?inventory_id="+id+"&prefix="+url.QueryEscape(prefix), http.NoBody)
		w := httptest.NewRecorder()
		h.BrowseLevelAPI(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("%q: status %d: %s", prefix, w.Code, w.Body.String())
		}
		var resp handlers.BrowseLevelResponse
		if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
			t.Fatal(err)
		}
		if len(resp.Children) == 0 {
			t.Fatalf("%q: no children", prefix)
		}
		err := mgr.WithIndex(id, func(idx *indexread.Index) error {
			for _, c := range resp.Children {
				pos, ok := idx.Lookup(c.Prefix)
				if !ok {
					t.Fatalf("%q: row prefix %q is not in the index", prefix, c.Prefix)
				}
				full, err := idx.PrefixString(pos)
				if err != nil || full != c.Prefix {
					t.Errorf("row %q: index prefix %q (%v)", c.Prefix, full, err)
				}
				st := idx.Stats(pos)
				want := pricing.ComputeMonthlyCost(idx.TierBreakdown(pos), prices).TotalMicrodollars
				if c.ObjectCount != st.ObjectCount || c.TotalBytes != st.TotalBytes || c.MonthlyCostMicrodollars != want || c.HasChildren != idx.HasChildren(pos) {
					t.Errorf("row %q: got %+v, want stats %+v cost %d children %v", c.Prefix, c, st, want, idx.HasChildren(pos))
				}
			}

			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}
