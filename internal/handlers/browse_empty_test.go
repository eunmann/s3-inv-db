package handlers_test

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/PuerkitoBio/goquery"
	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/pkg/humanfmt"
	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

// TestBrowsePage_EmptyStateListsLoadedRuns: with no run picked, Browse
// lists every loaded run by configuration, newest first, with its size
// and object count, each one click from its root.
func TestBrowsePage_EmptyStateListsLoadedRuns(t *testing.T) {
	mgr := inventory.NewCatalog(nil)
	t.Cleanup(func() { _ = mgr.Close() })
	const (
		older inventory.ID = "bucket-a/inv/2026-01-01T00-00Z"
		newer inventory.ID = "bucket-a/inv/2026-01-02T00-00Z"
		other inventory.ID = "bucket-b/inv/2026-01-01T00-00Z"
	)
	registerCostIndex(t, mgr, older, []costObj{{"a/x", 1 * gib, tiers.Standard}})
	registerCostIndex(t, mgr, newer, []costObj{{"a/x", 2 * gib, tiers.Standard}, {"b/x", 1, tiers.Standard}})
	registerCostIndex(t, mgr, other, []costObj{{"a/x", 5, tiers.Standard}})
	h := newWiredHandlers(t, mgr)
	w := httptest.NewRecorder()
	h.BrowsePage(w, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/browse", http.NoBody))
	if w.Code != http.StatusOK {
		t.Fatalf("status %d: %s", w.Code, w.Body.String())
	}
	doc := parseHTMLFragment(t, w.Body.String())
	list := doc.Find(`section[aria-labelledby="browse-runs-title"]`)
	var configs []string
	list.Find("h3").Each(func(_ int, s *goquery.Selection) { configs = append(configs, strings.TrimSpace(s.Text())) })
	if got := strings.Join(configs, ","); got != "bucket-a/inv,bucket-b/inv" {
		t.Errorf("configurations = %s", got)
	}
	links := list.Find("li a")
	want := []struct {
		id      inventory.ID
		size    uint64
		objects uint64
	}{{newer, 2*gib + 1, 2}, {older, 1 * gib, 1}, {other, 5, 1}}
	if links.Length() != len(want) {
		t.Fatalf("runs listed = %d, want %d", links.Length(), len(want))
	}
	for i, w := range want {
		a := links.Eq(i)
		wantHref := "/browse?" + url.Values{"inventory_id": {string(w.id)}}.Encode()
		if href, _ := a.Attr("href"); href != wantHref {
			t.Errorf("run %d href = %q, want %q", i, href, wantHref)
		}
		text := strings.Join(strings.Fields(a.Text()), " ")
		wantText := humanfmt.RunTimestamp(w.id.Split().Run) + " " + humanfmt.BytesUint64(w.size) + " · " + humanfmt.CountUint64(w.objects) + " objects"
		if text != wantText {
			t.Errorf("run %d text = %q, want %q", i, text, wantText)
		}
	}
	if doc.Find("#inventory_id optgroup").Length() != 2 {
		t.Error("picker no longer groups runs by configuration")
	}
}

func TestBrowsePage_EmptyStateWithNothingLoaded(t *testing.T) {
	f := newTestFixture(t)
	w := httptest.NewRecorder()
	f.h.BrowsePage(w, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/browse", http.NoBody))
	if w.Code != http.StatusOK {
		t.Fatalf("status %d", w.Code)
	}
	target := parseHTMLFragment(t, w.Body.String()).Find("#browse-target")
	if !strings.Contains(target.Text(), "No runs are loaded") {
		t.Errorf("empty state = %q, want it to say nothing is loaded", strings.TrimSpace(target.Text()))
	}
	if href, _ := target.Find("a").Attr("href"); href != "/inventories" {
		t.Errorf("empty state link = %q, want /inventories", href)
	}
}

// TestBrowsePage_UnavailableRunListsLoadedRuns: a link to a run that
// isn't loaded says so above the loaded runs, each with its totals.
func TestBrowsePage_UnavailableRunListsLoadedRuns(t *testing.T) {
	mgr := inventory.NewCatalog(nil)
	t.Cleanup(func() { _ = mgr.Close() })
	const loaded inventory.ID = "bucket-a/inv/2026-01-02T00-00Z"
	registerCostIndex(t, mgr, loaded, []costObj{{"a/x", 3 * gib, tiers.Standard}, {"b/x", 1, tiers.Standard}})
	h := newWiredHandlers(t, mgr)
	q := url.Values{"inventory_id": {"bucket-a/inv/2026-01-01T00-00Z"}, "prefix": {"a/"}}
	w := httptest.NewRecorder()
	h.BrowsePage(w, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/browse?"+q.Encode(), http.NoBody))
	if w.Code != http.StatusOK {
		t.Fatalf("status %d: %s", w.Code, w.Body.String())
	}
	target := parseHTMLFragment(t, w.Body.String()).Find("#browse-target")
	if !strings.Contains(target.Text(), "That run isn't loaded") {
		t.Errorf("results = %q, want the run-not-loaded warning", strings.Join(strings.Fields(target.Text()), " "))
	}
	runs := target.Find(`section[aria-labelledby="browse-runs-title"] li a`)
	if runs.Length() != 1 {
		t.Fatalf("runs listed = %d, want 1", runs.Length())
	}
	want := humanfmt.RunTimestamp(loaded.Split().Run) + " " + humanfmt.BytesUint64(3*gib+1) + " · 2 objects"
	if text := strings.Join(strings.Fields(runs.Text()), " "); text != want {
		t.Errorf("run = %q, want %q", text, want)
	}
}
