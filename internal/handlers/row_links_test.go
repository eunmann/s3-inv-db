package handlers_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/PuerkitoBio/goquery"
	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

// Row drill-down links are built in Go, not by template funcs. These
// tests pin them, through the real templates, to what a url.Values
// query and a json.Marshal'd map give, for prefixes that need escaping
// in every layer (URL, JSON and HTML attribute).

const (
	linkFromID inventory.ID = "src/inv/2026-01-01T00-00Z"
	linkToID   inventory.ID = "src/inv/2026-01-02T00-00Z"
)

var linkObjs = []costObj{
	{`we&ird "q" <t>' x%2F+y/sub/o`, 1, tiers.Standard},
	{"plain/sub/o", 2, tiers.Standard},
	{"sep /sub/o", 3, tiers.Standard},
	{"leaf/o", 4, tiers.Standard},
}

// rowLinks maps each linked row's segment to its link element.
func rowLinks(t *testing.T, body, hxGet string) map[string]*goquery.Selection {
	t.Helper()
	doc := parseHTMLFragment(t, body)
	links := map[string]*goquery.Selection{}
	doc.Find(`tbody [hx-get="` + hxGet + `"]`).Each(func(_ int, s *goquery.Selection) {
		links[strings.TrimSuffix(strings.TrimSpace(s.Text()), "/")] = s
	})

	return links
}

func wantHxVals(t *testing.T, m map[string]string) string {
	t.Helper()
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}

	return string(b)
}

func TestBrowse_RowLinksEscapeEveryLayer(t *testing.T) {
	mgr := loadCostIndex(t, linkFromID, linkObjs)
	h := newWiredHandlers(t, mgr)
	target := "/browse?inventory_id=" + url.QueryEscape(string(linkFromID))
	requests := map[string]func(t *testing.T) *http.Request{
		"partial": func(t *testing.T) *http.Request {
			t.Helper()

			return newHXRequest(t, http.MethodGet, target)
		},
		"full page": func(t *testing.T) *http.Request {
			t.Helper()

			return httptest.NewRequestWithContext(t.Context(), http.MethodGet, target, http.NoBody)
		},
	}
	for name, newReq := range requests {
		t.Run(name, func(t *testing.T) {
			w := httptest.NewRecorder()
			h.BrowsePage(w, newReq(t))
			if w.Code != http.StatusOK {
				t.Fatalf("status %d: %s", w.Code, w.Body.String())
			}
			links := rowLinks(t, w.Body.String(), "/browse")
			if _, ok := links["leaf"]; ok || len(links) != 3 {
				t.Fatalf("linked rows = %d (leaf linked: %v), want the 3 with children", len(links), ok)
			}
			for _, o := range linkObjs[:3] {
				seg, _, _ := strings.Cut(o.key, "/")
				s, ok := links[seg]
				if !ok {
					t.Fatalf("no link for %q", seg)
				}
				prefix := seg + "/"
				wantHref := "/browse?" + url.Values{"inventory_id": {string(linkFromID)}, "prefix": {prefix}}.Encode()
				if got, _ := s.Attr("href"); got != wantHref {
					t.Errorf("%q href\n got %q\nwant %q", seg, got, wantHref)
				}
				want := wantHxVals(t, map[string]string{"inventory_id": string(linkFromID), "prefix": prefix})
				if got, _ := s.Attr("hx-vals"); got != want {
					t.Errorf("%q hx-vals\n got %q\nwant %q", seg, got, want)
				}
			}
		})
	}
}

func TestComparePartial_RowLinksEscapeEveryLayer(t *testing.T) {
	mgr := inventory.NewCatalog(nil)
	t.Cleanup(func() { _ = mgr.Close() })
	registerCostIndex(t, mgr, linkFromID, linkObjs)
	registerCostIndex(t, mgr, linkToID, linkObjs[1:])
	h := newWiredHandlers(t, mgr)
	w := httptest.NewRecorder()
	q := url.Values{"from": {string(linkFromID)}, "to": {string(linkToID)}, "show_unchanged": {"true"}}
	h.ComparePage(w, newHXRequest(t, http.MethodGet, "/compare?"+q.Encode()))
	if w.Code != http.StatusOK {
		t.Fatalf("status %d: %s", w.Code, w.Body.String())
	}
	links := rowLinks(t, w.Body.String(), "/compare")
	if _, ok := links["leaf"]; ok || len(links) != 3 {
		t.Fatalf("linked rows = %d (leaf linked: %v), want the 3 with children", len(links), ok)
	}
	for _, o := range linkObjs[:3] {
		seg, _, _ := strings.Cut(o.key, "/")
		s, ok := links[seg]
		if !ok {
			t.Fatalf("no link for %q", seg)
		}
		want := wantHxVals(t, map[string]string{"from": string(linkFromID), "to": string(linkToID), "prefix": seg + "/"})
		if got, _ := s.Attr("hx-vals"); got != want {
			t.Errorf("%q hx-vals\n got %q\nwant %q", seg, got, want)
		}
	}
}
