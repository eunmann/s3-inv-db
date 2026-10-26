package handlers_test

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/PuerkitoBio/goquery"
	"github.com/eunmann/s3-inv-db/internal/handlers"
	"github.com/eunmann/s3-inv-db/pkg/humanfmt"
	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

func TestShareOf(t *testing.T) {
	tests := []struct {
		name         string
		part, whole  uint64
		label, width string
	}{
		{"zero-byte parent", 0, 0, "—", "0"},
		{"zero-byte child", 0, 100, "0%", "0"},
		{"whole", 100, 100, "100%", "100"},
		{"part", 421, 1000, "42.1%", "42.1"},
		{"rounds half up", 1, 3, "33.3%", "33.3"},
		{"tiny but nonzero", 1, 1_000_000, "<0.1%", "0.0"},
		{"just under whole", 999_999, 1_000_000, ">99.9%", "100.0"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			label, width := handlers.ShareOfForTest(tt.part, tt.whole)
			if label != tt.label || width != tt.width {
				t.Errorf("shareOf(%d, %d) = %q, %q; want %q, %q", tt.part, tt.whole, label, width, tt.label, tt.width)
			}
		})
	}
}

// TestBrowsePartial_ZeroByteParentShowsNoShare: a level whose objects
// are all empty has no share to show, not a division by zero.
func TestBrowsePartial_ZeroByteParentShowsNoShare(t *testing.T) {
	const id = "bucket/inv/2026-01-01T00-00Z"
	mgr := loadCostIndex(t, id, []costObj{
		{"a/x", 0, tiers.Standard},
		{"b/x", 0, tiers.Standard},
	})
	h := newWiredHandlers(t, mgr)
	w := httptest.NewRecorder()
	h.BrowsePage(w, newHXRequest(t, http.MethodGet, "/browse?inventory_id="+url.QueryEscape(id)))
	if w.Code != http.StatusOK {
		t.Fatalf("status %d: %s", w.Code, w.Body.String())
	}
	rows := parseHTMLFragment(t, w.Body.String()).Find("table").Last().Find("tbody tr")
	if rows.Length() != 2 {
		t.Fatalf("rows = %d, want 2", rows.Length())
	}
	for i := range rows.Length() {
		if got := strings.TrimSpace(rows.Eq(i).Find("td").Eq(2).Text()); got != "—" {
			t.Errorf("row %d share = %q, want —", i, got)
		}
	}
}

// TestBrowsePartial_SummaryHeadline: the level's size, objects and cost
// are the headline; the root is titled with the bucket, not "Root",
// and has no one-item breadcrumb; the tier breakdown is open, largest
// tier first, each with its share.
func TestBrowsePartial_SummaryHeadline(t *testing.T) {
	const id = "my-bucket/inv/2026-01-01T00-00Z"
	mgr := loadCostIndex(t, id, []costObj{
		{"a/x", 1 * gib, tiers.Standard},
		{"b/x", 3 * gib, tiers.GlacierFR},
	})
	h := newWiredHandlers(t, mgr)
	w := httptest.NewRecorder()
	h.BrowsePage(w, newHXRequest(t, http.MethodGet, "/browse?inventory_id="+url.QueryEscape(id)))
	if w.Code != http.StatusOK {
		t.Fatalf("status %d: %s", w.Code, w.Body.String())
	}
	doc := parseHTMLFragment(t, w.Body.String())
	if got := strings.TrimSpace(doc.Find("#browse-level-title").Text()); got != "my-bucket" {
		t.Errorf("title = %q, want my-bucket", got)
	}
	if doc.Find(`nav[aria-label="Breadcrumb"]`).Length() != 0 {
		t.Error("root shows a breadcrumb repeating its title")
	}
	if strings.Contains(w.Body.String(), "Root") {
		t.Error(`root still says "Root"`)
	}
	var terms []string
	doc.Find("dl dt").Each(func(_ int, s *goquery.Selection) { terms = append(terms, strings.TrimSpace(s.Text())) })
	if got := strings.Join(terms, ","); got != "Size,Objects,Est. cost / month" {
		t.Errorf("headline terms = %s", got)
	}
	if got, want := strings.TrimSpace(doc.Find("dl dd").First().Text()), humanfmt.BytesUint64(4*gib); got != want {
		t.Errorf("headline size = %q, want %q", got, want)
	}
	details := doc.Find("details")
	if _, open := details.Attr("open"); !open {
		t.Error("tier breakdown is collapsed; want it open")
	}
	tierRows := details.Find("tbody tr")
	if tierRows.Length() != 2 {
		t.Fatalf("tier rows = %d, want 2", tierRows.Length())
	}
	for i, want := range []string{"75.0%", "25.0%"} {
		if got := strings.TrimSpace(tierRows.Eq(i).Find("td").Eq(2).Text()); got != want {
			t.Errorf("tier row %d share = %q, want %q", i, got, want)
		}
	}
}
