package handlers_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/PuerkitoBio/goquery"
	"github.com/eunmann/s3-inv-db/internal/handlers"
	"github.com/eunmann/s3-inv-db/pkg/humanfmt"
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

// TestBrowseLevelAPI_DefaultSortIsSizeDescending: with no sort param a
// level lists its largest children first.
func TestBrowseLevelAPI_DefaultSortIsSizeDescending(t *testing.T) {
	const id = "src/inv/2026-01-01T00-00Z"
	mgr := loadCostIndex(t, id, []costObj{
		{"a/x", 1, tiers.Standard},
		{"b/x", 3 * gib, tiers.Standard},
		{"c/x", 2 * gib, tiers.Standard},
	})
	h := newWiredHandlers(t, mgr)
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/browse?inventory_id="+id, http.NoBody)
	w := httptest.NewRecorder()
	h.BrowseLevelAPI(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status %d: %s", w.Code, w.Body.String())
	}
	var resp handlers.BrowseLevelResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Sort != "size" || resp.Dir != "desc" {
		t.Errorf("sort = %q %q, want size desc", resp.Sort, resp.Dir)
	}
	got := make([]string, 0, len(resp.Children))
	for _, c := range resp.Children {
		got = append(got, c.Segment)
	}
	if want := "b,c,a"; strings.Join(got, ",") != want {
		t.Errorf("children = %v, want %s", got, want)
	}
}

// TestBrowsePartial_RowsShowFormattedNumbers: the HTML rows render each
// child's formatted count, size and cost, which only the HTML path
// builds.
func TestBrowsePartial_RowsShowFormattedNumbers(t *testing.T) {
	const id = "src/inv/2026-01-01T00-00Z"
	mgr := loadCostIndex(t, id, []costObj{
		{"a/x", 3 * gib, tiers.Standard},
		{"b/x", 1 * gib, tiers.GlacierFR},
		{"b/y", 5, tiers.Standard},
		{"c/d/x", 7 * gib, tiers.StandardIA},
	})
	h := newWiredHandlers(t, mgr)
	w := httptest.NewRecorder()
	h.BrowsePage(w, newHXRequest(t, http.MethodGet, "/browse?inventory_id="+url.QueryEscape(id)))
	if w.Code != http.StatusOK {
		t.Fatalf("status %d: %s", w.Code, w.Body.String())
	}
	rows := parseHTMLFragment(t, w.Body.String()).Find("table").Last().Find("tbody tr") // the children table follows the tier table
	if rows.Length() != 3 {
		t.Fatalf("rows = %d, want 3", rows.Length())
	}
	prices := pricing.DefaultUSEast1Prices()
	err := mgr.WithIndex(id, func(idx *indexread.Index) error {
		rows.Each(func(_ int, tr *goquery.Selection) {
			cells := tr.Find("td")
			seg := strings.TrimSuffix(strings.TrimSpace(cells.Eq(0).Text()), "/")
			pos, ok := idx.Lookup(seg + "/")
			if !ok {
				t.Fatalf("row %q not in index", seg)
			}
			st := idx.Stats(pos)
			want := []string{
				humanfmt.CountUint64(st.ObjectCount),
				humanfmt.BytesUint64(st.TotalBytes),
				pricing.FormatCost(pricing.ComputeMonthlyCost(idx.TierBreakdown(pos), prices).TotalMicrodollars),
			}
			for i, w := range want {
				if got := strings.TrimSpace(cells.Eq(i + 1).Text()); got != w {
					t.Errorf("row %q cell %d = %q, want %q", seg, i+1, got, w)
				}
			}
		})

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
