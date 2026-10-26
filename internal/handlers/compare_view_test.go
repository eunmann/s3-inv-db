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
	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

const (
	viewFromID inventory.ID = "my-bucket/inv/2026-01-01T00-00Z"
	viewToID   inventory.ID = "my-bucket/inv/2026-01-02T00-00Z"
)

// compareViewHandlers loads two runs of one configuration whose root
// children grew ("grew"), shrank ("shrank"), stayed ("same"), appeared
// ("new") and went away ("gone"). The root as a whole grew.
func compareViewHandlers(t *testing.T) *handlers.Handlers {
	t.Helper()
	mgr := inventory.NewCatalog(nil)
	t.Cleanup(func() { _ = mgr.Close() })
	registerCostIndex(t, mgr, viewFromID, []costObj{
		{"grew/x", 1 * gib, tiers.Standard},
		{"shrank/x", 2 * gib, tiers.Standard},
		{"same/x", 1, tiers.Standard},
		{"gone/x", 5, tiers.Standard},
	})
	registerCostIndex(t, mgr, viewToID, []costObj{
		{"grew/x", 4 * gib, tiers.Standard},
		{"shrank/x", 1 * gib, tiers.Standard},
		{"same/x", 1, tiers.Standard},
		{"new/x", 7, tiers.Standard},
	})

	return newWiredHandlers(t, mgr)
}

func compareViewGet(t *testing.T, h *handlers.Handlers, target string) *goquery.Document {
	t.Helper()
	w := httptest.NewRecorder()
	h.ComparePage(w, newHXRequest(t, http.MethodGet, target))
	if w.Code != http.StatusOK {
		t.Fatalf("status %d: %s", w.Code, w.Body.String())
	}

	return parseHTMLFragment(t, w.Body.String())
}

func compareViewTarget() string {
	return "/compare?" + url.Values{"from": {string(viewFromID)}, "to": {string(viewToID)}, "show_unchanged": {"true"}}.Encode()
}

// compareRows maps each child row's segment to the row.
func compareRows(doc *goquery.Document) map[string]*goquery.Selection {
	rows := map[string]*goquery.Selection{}
	doc.Find("tbody tr").Each(func(_ int, tr *goquery.Selection) {
		seg := strings.TrimSuffix(strings.TrimSpace(tr.Find("td").Eq(1).Text()), "/")
		rows[seg] = tr
	})

	return rows
}

func TestComparePartial_GrowthIsRedShrinkageGreen(t *testing.T) {
	doc := compareViewGet(t, compareViewHandlers(t), compareViewTarget())
	rows := compareRows(doc)
	for seg, colour := range map[string]string{"grew": "rose", "shrank": "emerald", "same": "gray", "new": "rose", "gone": "emerald"} {
		tr, ok := rows[seg]
		if !ok {
			t.Fatalf("no row for %q", seg)
		}
		size := tr.Find("td").Eq(2).Find("div").First()
		if class, _ := size.Attr("class"); !strings.Contains(class, "text-"+colour+"-") {
			t.Errorf("%s size delta %q: class %q, want %s", seg, strings.TrimSpace(size.Text()), class, colour)
		}
	}
	headline := doc.Find("dl dd").First()
	if class, _ := headline.Attr("class"); !strings.Contains(class, "text-rose-") {
		t.Errorf("headline size delta %q: class %q, want rose (the root grew)", strings.TrimSpace(headline.Text()), class)
	}
	if legend := doc.Text(); !strings.Contains(legend, "Red: grew") || !strings.Contains(legend, "Green: shrank") {
		t.Error("colour legend missing")
	}
}

func TestComparePartial_BadgesOnlyForAddedAndRemoved(t *testing.T) {
	rows := compareRows(compareViewGet(t, compareViewHandlers(t), compareViewTarget()))
	for seg, badge := range map[string]string{"grew": "", "shrank": "", "same": "", "new": "added", "gone": "removed"} {
		tr, ok := rows[seg]
		if !ok {
			t.Fatalf("no row for %q", seg)
		}
		got := strings.TrimSpace(tr.Find("td").First().Find("span.rounded-full").Text())
		if got != badge {
			t.Errorf("%s badge = %q, want %q", seg, got, badge)
		}
	}
}

func TestCompare_PutCostOnlyInJSON(t *testing.T) {
	h := compareViewHandlers(t)
	doc := compareViewGet(t, h, compareViewTarget())
	if html, _ := doc.Html(); strings.Contains(html, "PUT") {
		t.Error("compare HTML still shows the PUT API cost")
	}

	w := httptest.NewRecorder()
	h.CompareLevelAPI(w, httptest.NewRequestWithContext(t.Context(), http.MethodGet, compareViewTarget(), http.NoBody))
	if w.Code != http.StatusOK {
		t.Fatalf("status %d: %s", w.Code, w.Body.String())
	}
	var resp map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	self, _ := resp["self"].(map[string]any)
	for _, k := range []string{"api_cost_before_microdollars", "api_cost_after_microdollars", "api_cost_delta_microdollars"} {
		if _, ok := self[k]; !ok {
			t.Errorf("JSON self lacks %s", k)
		}
	}
}

func TestComparePartial_RootTitledWithBucket(t *testing.T) {
	doc := compareViewGet(t, compareViewHandlers(t), compareViewTarget())
	if got := strings.TrimSpace(doc.Find("#compare-level-title").Text()); got != "my-bucket" {
		t.Errorf("title = %q, want my-bucket", got)
	}
	if html, _ := doc.Html(); strings.Contains(html, "Root") || strings.Contains(html, "the root") {
		t.Error(`root still says "Root" or "the root"`)
	}
}
