package handlers_test

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/PuerkitoBio/goquery"
	"github.com/eunmann/s3-inv-db/internal/handlers"
	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

const (
	sideRun1 inventory.ID = "b/inv/2026-01-01T00-00Z"
	sideRun2 inventory.ID = "b/inv/2026-01-02T00-00Z"
	sideRun3 inventory.ID = "b/inv/2026-01-03T00-00Z"
	sideLone inventory.ID = "c/inv/2026-01-01T00-00Z"
)

func sidesPicker() handlers.ComparePicker {
	infos := []inventory.Info{
		{ID: sideRun2, State: inventory.StateLoaded},
		{ID: sideLone, State: inventory.StateLoaded},
		{ID: sideRun3, State: inventory.StateLoaded},
		{ID: sideRun1, State: inventory.StateLoaded},
		{ID: "b/inv/2026-01-04T00-00Z", State: inventory.StateNotLoaded},
	}

	return handlers.BuildComparePickerForTest(infos)
}

func TestResolveCompareSides(t *testing.T) {
	tests := []struct {
		name             string
		from, to         inventory.ID
		wantFrom, wantTo inventory.ID
		wantNote         bool
		wantSingle       bool
	}{
		{"baseline gets the next newer run", sideRun1, "", sideRun1, sideRun2, false, false},
		{"middle baseline gets the next newer run", sideRun2, "", sideRun2, sideRun3, false, false},
		{"newest baseline becomes the compared run", sideRun3, "", sideRun2, sideRun3, true, false},
		{"compared run gets the next older baseline", "", sideRun3, sideRun2, sideRun3, false, false},
		{"oldest compared run becomes the baseline", "", sideRun1, sideRun1, sideRun2, true, false},
		{"only run of its configuration", sideLone, "", sideLone, "", false, true},
		{"run not loaded is left alone", "b/inv/2026-01-04T00-00Z", "", "b/inv/2026-01-04T00-00Z", "", false, false},
		{"both given are left alone", sideRun3, sideRun1, sideRun3, sideRun1, false, false},
		{"neither given", "", "", "", "", false, false},
	}
	picker := sidesPicker()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := handlers.ResolveCompareSidesForTest(picker, tt.from, tt.to)
			if got.From != tt.wantFrom || got.To != tt.wantTo {
				t.Errorf("sides = %q → %q, want %q → %q", got.From, got.To, tt.wantFrom, tt.wantTo)
			}
			if (got.Note != "") != tt.wantNote {
				t.Errorf("note = %q, want note: %v", got.Note, tt.wantNote)
			}
			if got.SingleRun != tt.wantSingle {
				t.Errorf("single run = %v, want %v", got.SingleRun, tt.wantSingle)
			}
		})
	}
}

// TestComparePage_OneSideComparesWithItsNeighbour: a link naming only a
// baseline opens the comparison with the next run, both selected.
func TestComparePage_OneSideComparesWithItsNeighbour(t *testing.T) {
	mgr := inventory.NewCatalog(nil)
	t.Cleanup(func() { _ = mgr.Close() })
	for _, id := range []inventory.ID{sideRun1, sideRun2} {
		registerCostIndex(t, mgr, id, []costObj{{"a/x", 1, tiers.Standard}})
	}
	h := newWiredHandlers(t, mgr)
	for _, q := range []url.Values{{"from": {string(sideRun1)}}, {"to": {string(sideRun2)}}, {"from": {string(sideRun2)}}} {
		w := httptest.NewRecorder()
		h.ComparePage(w, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/compare?"+q.Encode(), http.NoBody))
		if w.Code != http.StatusOK {
			t.Fatalf("%v: status %d", q, w.Code)
		}
		doc := parseHTMLFragment(t, w.Body.String())
		from, _ := doc.Find("#from option[selected]").Attr("value")
		to, _ := doc.Find("#to option[selected]").Attr("value")
		if from != string(sideRun1) || to != string(sideRun2) {
			t.Errorf("%v: selected %q → %q, want %q → %q", q, from, to, sideRun1, sideRun2)
		}
		if doc.Find("#compare-level-title").Length() != 1 {
			t.Errorf("%v: no comparison rendered", q)
		}
	}
}

func TestComparePage_EmptyStateSuggestsLatestPairs(t *testing.T) {
	mgr := inventory.NewCatalog(nil)
	t.Cleanup(func() { _ = mgr.Close() })
	for _, id := range []inventory.ID{sideRun1, sideRun2, sideRun3, sideLone} {
		registerCostIndex(t, mgr, id, []costObj{{"a/x", 1, tiers.Standard}})
	}
	h := newWiredHandlers(t, mgr)
	w := httptest.NewRecorder()
	h.ComparePage(w, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/compare", http.NoBody))
	if w.Code != http.StatusOK {
		t.Fatalf("status %d", w.Code)
	}
	links := parseHTMLFragment(t, w.Body.String()).Find(`section[aria-labelledby="compare-suggestions-title"] a`)
	if links.Length() != 1 {
		t.Fatalf("suggestions = %d, want 1 (only b/inv has two runs)", links.Length())
	}
	want := "/compare?" + url.Values{"from": {string(sideRun2)}, "to": {string(sideRun3)}}.Encode()
	if href, _ := links.Attr("href"); href != want {
		t.Errorf("suggestion href = %q, want %q", href, want)
	}
	if text := strings.Join(strings.Fields(links.Text()), " "); !strings.Contains(text, "b/inv") || !strings.Contains(text, "Compare 2026-01-02") || !strings.Contains(text, "→ 2026-01-03") {
		t.Errorf("suggestion text = %q", text)
	}
}

func TestComparePage_EmptyStateExplainsTwoRunsNeeded(t *testing.T) {
	mgr := inventory.NewCatalog(nil)
	t.Cleanup(func() { _ = mgr.Close() })
	registerCostIndex(t, mgr, sideLone, []costObj{{"a/x", 1, tiers.Standard}})
	for name, h := range map[string]*handlers.Handlers{"one run": newWiredHandlers(t, mgr), "nothing loaded": newTestFixture(t).h} {
		w := httptest.NewRecorder()
		h.ComparePage(w, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/compare", http.NoBody))
		target := parseHTMLFragment(t, w.Body.String()).Find("#compare-target")
		if !strings.Contains(target.Text(), "two loaded runs of one configuration") {
			t.Errorf("%s: empty state = %q", name, strings.TrimSpace(target.Text()))
		}
		if href, _ := target.Find("a").Attr("href"); href != "/inventories" {
			t.Errorf("%s: link = %q, want /inventories", name, href)
		}
	}
}

// optgroupLabels lists the optgroup labels of the select sel.
func optgroupLabels(doc *goquery.Document, sel string) []string {
	var labels []string
	doc.Find(sel + " optgroup").Each(func(_ int, g *goquery.Selection) {
		l, _ := g.Attr("label")
		labels = append(labels, l)
	})

	return labels
}

func TestComparePage_PickersGroupByConfiguration(t *testing.T) {
	mgr := inventory.NewCatalog(nil)
	t.Cleanup(func() { _ = mgr.Close() })
	for _, id := range []inventory.ID{sideRun1, sideRun2, sideLone} {
		registerCostIndex(t, mgr, id, []costObj{{"a/x", 1, tiers.Standard}})
	}
	h := newWiredHandlers(t, mgr)
	w := httptest.NewRecorder()
	h.ComparePage(w, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/compare?from="+url.QueryEscape(string(sideRun1)), http.NoBody))
	doc := parseHTMLFragment(t, w.Body.String())
	if got := strings.Join(optgroupLabels(doc, "#from"), ","); got != "b/inv,c/inv" {
		t.Errorf("from optgroups = %s, want b/inv,c/inv", got)
	}
	if got := strings.Join(optgroupLabels(doc, "#to"), ","); got != "b/inv" {
		t.Errorf("to optgroups = %s, want only the baseline's b/inv", got)
	}
}
