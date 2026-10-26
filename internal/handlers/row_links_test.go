package handlers_test

import (
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
// query gives, for prefixes that need escaping in every layer (URL and
// HTML attribute).

const (
	linkFromID inventory.ID = "src/inv/2026-01-01T00-00Z"
	linkToID   inventory.ID = "src/inv/2026-01-02T00-00Z"
)

var linkObjs = []costObj{
	{`we&ird "q" <t>' x%2F+y/sub/o`, 1, tiers.Standard},
	{"plain/sub/o", 2, tiers.Standard},
	{"sep /sub/o", 3, tiers.Standard},
	{"leaf/o", 4, tiers.Standard},
}

// rowLinks maps each linked row's segment to its link element.
func rowLinks(t *testing.T, body string) map[string]*goquery.Selection {
	t.Helper()
	doc := parseHTMLFragment(t, body)
	links := map[string]*goquery.Selection{}
	doc.Find(`tbody [hx-get]`).Each(func(_ int, s *goquery.Selection) {
		links[strings.TrimSuffix(strings.TrimSpace(s.Text()), "/")] = s
	})

	return links
}

// assertRealLinks checks that body's level (the target element on a
// full page, all of a partial) has at least htmx GETs, each an
// anchor to path whose href is its hx-get and that pushes that URL.
func assertRealLinks(t *testing.T, body, target, path string, least int) {
	t.Helper()
	level := parseHTMLFragment(t, body).Selection
	if s := level.Find(target); s.Length() > 0 {
		level = s
	}
	n := 0
	level.Find("[hx-get]").Each(func(_ int, s *goquery.Selection) {
		n++
		get, _ := s.Attr("hx-get")
		href, ok := s.Attr("href")
		if goquery.NodeName(s) != "a" || !ok || href != get {
			t.Errorf("<%s hx-get=%q href=%q>: want an <a> whose href is its hx-get", goquery.NodeName(s), get, href)
		}
		if !strings.HasPrefix(get, path+"?") {
			t.Errorf("hx-get %q: want %s?…", get, path)
		}
		if push, _ := s.Attr("hx-push-url"); push != "true" {
			t.Errorf("hx-get %q: hx-push-url = %q, want true", get, push)
		}
		if _, ok := s.Attr("hx-vals"); ok {
			t.Errorf("hx-get %q carries hx-vals; the URL alone must name the view", get)
		}
	})
	if n < least {
		t.Errorf("found %d htmx links, want at least %d", n, least)
	}
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
			links := rowLinks(t, w.Body.String())
			if _, ok := links["leaf"]; ok || len(links) != 3 {
				t.Fatalf("linked rows = %d (leaf linked: %v), want the 3 with children", len(links), ok)
			}
			for _, o := range linkObjs[:3] {
				seg, _, _ := strings.Cut(o.key, "/")
				s, ok := links[seg]
				if !ok {
					t.Fatalf("no link for %q", seg)
				}
				want := "/browse?" + url.Values{"inventory_id": {string(linkFromID)}, "prefix": {seg + "/"}}.Encode()
				for _, attr := range []string{"href", "hx-get"} {
					if got, _ := s.Attr(attr); got != want {
						t.Errorf("%q %s\n got %q\nwant %q", seg, attr, got, want)
					}
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
	links := rowLinks(t, w.Body.String())
	if _, ok := links["leaf"]; ok || len(links) != 3 {
		t.Fatalf("linked rows = %d (leaf linked: %v), want the 3 with children", len(links), ok)
	}
	for _, o := range linkObjs[:3] {
		seg, _, _ := strings.Cut(o.key, "/")
		s, ok := links[seg]
		if !ok {
			t.Fatalf("no link for %q", seg)
		}
		want := "/compare?" + url.Values{"from": {string(linkFromID)}, "to": {string(linkToID)}, "prefix": {seg + "/"}, "show_unchanged": {"true"}}.Encode()
		for _, attr := range []string{"href", "hx-get"} {
			if got, _ := s.Attr(attr); got != want {
				t.Errorf("%q %s\n got %q\nwant %q", seg, attr, got, want)
			}
		}
	}
}

// navObjs make a level two deep with enough children to page through at
// page_size=1, so breadcrumbs, sort headers, pager and rows all render.
var navObjs = []costObj{
	{"a/b/x/c/o", 1, tiers.Standard},
	{"a/b/y/c/o", 2, tiers.Standard},
	{"a/b/z/c/o", 3, tiers.Standard},
}

func TestBrowse_NavigationIsRealLinks(t *testing.T) {
	mgr := loadCostIndex(t, linkFromID, navObjs)
	h := newWiredHandlers(t, mgr)
	q := url.Values{"inventory_id": {string(linkFromID)}, "prefix": {"a/b/"}, "page": {"2"}, "page_size": {"1"}}
	for _, hx := range []bool{false, true} {
		w := httptest.NewRecorder()
		req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/browse?"+q.Encode(), http.NoBody)
		if hx {
			req.Header.Set("HX-Request", "true")
		}
		h.BrowsePage(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("status %d: %s", w.Code, w.Body.String())
		}
		// 2 crumbs + 4 sort headers + prev + next + 1 row.
		assertRealLinks(t, w.Body.String(), "#browse-target", "/browse", 9)
		doc := parseHTMLFragment(t, w.Body.String())
		next, _ := doc.Find(`a[rel="next"]`).Attr("href")
		wantNext := "/browse?" + url.Values{"inventory_id": {string(linkFromID)}, "prefix": {"a/b/"}, "sort": {"size"}, "dir": {"desc"}, "page": {"3"}, "page_size": {"1"}}.Encode()
		if next != wantNext {
			t.Errorf("next page\n got %q\nwant %q", next, wantNext)
		}
		if got := doc.Find(`th[aria-sort="descending"]`).Text(); !strings.Contains(got, "Size") {
			t.Errorf("aria-sort=descending header = %q, want Size", got)
		}
	}
}

func TestCompare_NavigationIsRealLinks(t *testing.T) {
	mgr := inventory.NewCatalog(nil)
	t.Cleanup(func() { _ = mgr.Close() })
	registerCostIndex(t, mgr, linkFromID, navObjs)
	registerCostIndex(t, mgr, linkToID, append([]costObj{{"a/b/w/o", 9, tiers.Standard}}, navObjs...))
	h := newWiredHandlers(t, mgr)
	q := url.Values{"from": {string(linkFromID)}, "to": {string(linkToID)}, "prefix": {"a/b/"}, "page": {"2"}, "page_size": {"1"}, "show_unchanged": {"true"}}
	for _, hx := range []bool{false, true} {
		w := httptest.NewRecorder()
		req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/compare?"+q.Encode(), http.NoBody)
		if hx {
			req.Header.Set("HX-Request", "true")
		}
		h.ComparePage(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("status %d: %s", w.Code, w.Body.String())
		}
		// 2 crumbs + unchanged toggle + 5 sort headers + prev + next + 1 row.
		assertRealLinks(t, w.Body.String(), "#compare-target", "/compare", 11)
		doc := parseHTMLFragment(t, w.Body.String())
		next, _ := doc.Find(`a[rel="next"]`).Attr("href")
		if u, err := url.Parse(next); err != nil || u.Query().Get("show_unchanged") != "true" || u.Query().Get("page") != "3" {
			t.Errorf("next page %q: want page 3 keeping show_unchanged", next)
		}
	}
}
