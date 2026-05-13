package handlers

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/PuerkitoBio/goquery"
	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/internal/templates"
	"github.com/eunmann/s3-inv-db/pkg/pricing"
	"github.com/go-chi/chi/v5"
)

// testFixture provides a test environment for HTML handler tests.
type testFixture struct {
	h   *Handlers
	mgr *inventory.Manager
}

func newTestFixture(t *testing.T) *testFixture {
	t.Helper()
	mgr := inventory.NewManager()
	renderer, err := templates.New()
	if err != nil {
		t.Fatalf("failed to create renderer: %v", err)
	}
	h := New(mgr, renderer, pricing.DefaultUSEast1Prices())
	return &testFixture{h: h, mgr: mgr}
}

func (f *testFixture) registerInventory(t *testing.T, id, name, path string) {
	t.Helper()
	if err := f.mgr.Register(id, name, path); err != nil {
		t.Fatalf("register: %v", err)
	}
}

// parseHTML creates a goquery document from response body.
func parseHTML(t *testing.T, body string) *goquery.Document {
	t.Helper()
	doc, err := goquery.NewDocumentFromReader(strings.NewReader(body))
	if err != nil {
		t.Fatalf("parse HTML: %v", err)
	}
	return doc
}

// parseHTMLFragment creates a goquery document from an HTML fragment.
// Wraps tr elements in table/tbody for proper parsing.
func parseHTMLFragment(t *testing.T, body string) *goquery.Document {
	t.Helper()
	// Wrap in minimal HTML structure to help parser
	wrapped := body
	if strings.HasPrefix(strings.TrimSpace(body), "<tr") {
		wrapped = "<table><tbody>" + body + "</tbody></table>"
	}
	doc, err := goquery.NewDocumentFromReader(strings.NewReader(wrapped))
	if err != nil {
		t.Fatalf("parse HTML fragment: %v", err)
	}
	return doc
}

// assertElementExists checks selector finds at least one element.
func assertElementExists(t *testing.T, doc *goquery.Document, selector string) {
	t.Helper()
	if doc.Find(selector).Length() == 0 {
		t.Errorf("expected element %q to exist", selector)
	}
}

// assertElementText checks exact text content.
func assertElementText(t *testing.T, doc *goquery.Document, selector, expected string) {
	t.Helper()
	el := doc.Find(selector).First()
	if el.Length() == 0 {
		t.Errorf("element %q not found", selector)
		return
	}
	got := strings.TrimSpace(el.Text())
	if got != expected {
		t.Errorf("element %q text = %q, want %q", selector, got, expected)
	}
}

// assertElementContainsText checks text contains substring.
func assertElementContainsText(t *testing.T, doc *goquery.Document, selector, contains string) {
	t.Helper()
	el := doc.Find(selector).First()
	if el.Length() == 0 {
		t.Errorf("element %q not found", selector)
		return
	}
	got := el.Text()
	if !strings.Contains(got, contains) {
		t.Errorf("element %q text = %q, want substring %q", selector, got, contains)
	}
}

// assertElementCount checks number of matching elements.
func assertElementCount(t *testing.T, doc *goquery.Document, selector string, expected int) {
	t.Helper()
	got := doc.Find(selector).Length()
	if got != expected {
		t.Errorf("element %q count = %d, want %d", selector, got, expected)
	}
}

// Dashboard Tests

func TestDashboard_EmptyState(t *testing.T) {
	f := newTestFixture(t)

	req := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
	w := httptest.NewRecorder()
	f.h.Dashboard(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body: %s", w.Code, http.StatusOK, w.Body.String())
	}

	doc := parseHTML(t, w.Body.String())

	// Verify heading
	assertElementText(t, doc, "h1", "Dashboard")

	// Verify 4 summary cards
	assertElementCount(t, doc, ".grid > div", 4)

	// Verify counts are 0
	assertElementContainsText(t, doc, "dd", "0")

	// Verify empty state message
	assertElementContainsText(t, doc, ".text-center h3", "No inventories")

	// No table rows since no inventories
	assertElementCount(t, doc, "tbody tr", 0)
}

func TestDashboard_WithInventories(t *testing.T) {
	f := newTestFixture(t)
	f.registerInventory(t, "inv1", "Test Inventory", "/path/to/index")

	req := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
	w := httptest.NewRecorder()
	f.h.Dashboard(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusOK)
	}

	doc := parseHTML(t, w.Body.String())

	// Verify heading
	assertElementText(t, doc, "h1", "Dashboard")

	// Verify 4 summary cards
	assertElementCount(t, doc, ".grid > div", 4)

	// Verify inventory appears in table
	assertElementCount(t, doc, "tbody tr", 1)
	assertElementContainsText(t, doc, "tbody tr td:first-child", "Test Inventory")

	// Verify state badge has pending class (bg-yellow-100)
	badge := doc.Find("tbody tr span.px-2").First()
	classAttr, _ := badge.Attr("class")
	if !strings.Contains(classAttr, "bg-yellow-100") {
		t.Errorf("expected pending state badge with bg-yellow-100, got class=%q", classAttr)
	}
}

func TestDashboard_CountsReflectState(t *testing.T) {
	f := newTestFixture(t)
	f.registerInventory(t, "inv1", "Inventory 1", "/path1")
	f.registerInventory(t, "inv2", "Inventory 2", "/path2")
	f.registerInventory(t, "inv3", "Inventory 3", "/path3")

	req := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
	w := httptest.NewRecorder()
	f.h.Dashboard(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusOK)
	}

	doc := parseHTML(t, w.Body.String())

	// Verify total count is 3
	// The first dd should be the total count
	dds := doc.Find("dl dd")
	if dds.Length() < 1 {
		t.Fatal("no dd elements found")
	}
	firstDD := strings.TrimSpace(dds.First().Text())
	if firstDD != "3" {
		t.Errorf("total count = %q, want %q", firstDD, "3")
	}

	// Verify 3 rows in table
	assertElementCount(t, doc, "tbody tr", 3)
}

// Inventories Page Tests

func TestInventoriesPage_EmptyState(t *testing.T) {
	f := newTestFixture(t)

	req := httptest.NewRequest(http.MethodGet, "/inventories", http.NoBody)
	w := httptest.NewRecorder()
	f.h.InventoriesPage(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusOK)
	}

	doc := parseHTML(t, w.Body.String())
	assertElementText(t, doc, "h1", "Inventories")
	assertElementContainsText(t, doc, ".text-center h3", "No inventories discovered")
	assertElementCount(t, doc, "tbody tr", 0)
}

func TestInventoriesPage_NoDiscoveryMessage(t *testing.T) {
	f := newTestFixture(t)

	req := httptest.NewRequest(http.MethodGet, "/inventories", http.NoBody)
	w := httptest.NewRecorder()
	f.h.InventoriesPage(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusOK)
	}

	// Without --s3-source the page should tell the user discovery is
	// disabled. The legacy register form is gone.
	body := w.Body.String()
	if !strings.Contains(body, "--s3-source") {
		t.Errorf("expected page to mention --s3-source when discovery disabled; body=%s", body)
	}
	doc := parseHTML(t, body)
	assertElementText(t, doc, "h1", "Inventories")
	if doc.Find("input#id").Length() != 0 {
		t.Error("legacy register form should not be present")
	}
}

// Browse Page Tests

func TestBrowsePage_NoLoadedInventories(t *testing.T) {
	f := newTestFixture(t)

	req := httptest.NewRequest(http.MethodGet, "/browse", http.NoBody)
	w := httptest.NewRecorder()
	f.h.BrowsePage(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusOK)
	}

	doc := parseHTML(t, w.Body.String())
	assertElementText(t, doc, "h1", "Browse")
	assertElementExists(t, doc, "form#browse-form")
	assertElementContainsText(t, doc, "form p", "No loaded inventories")
	options := doc.Find("select#inventory_id option")
	if options.Length() != 1 {
		t.Errorf("expected 1 option (placeholder), got %d", options.Length())
	}
}

func TestBrowsePage_FormStructure(t *testing.T) {
	f := newTestFixture(t)

	req := httptest.NewRequest(http.MethodGet, "/browse", http.NoBody)
	w := httptest.NewRecorder()
	f.h.BrowsePage(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusOK)
	}

	doc := parseHTML(t, w.Body.String())
	assertElementExists(t, doc, "form#browse-form")
	assertElementExists(t, doc, "select#inventory_id")
	assertElementExists(t, doc, "input#prefix")
	assertElementExists(t, doc, "#browse-target")
}

func TestBrowsePage_WithPendingInventory(t *testing.T) {
	f := newTestFixture(t)
	f.registerInventory(t, "inv1", "Test Inventory", "/path")

	req := httptest.NewRequest(http.MethodGet, "/browse", http.NoBody)
	w := httptest.NewRecorder()
	f.h.BrowsePage(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusOK)
	}

	// Pending inventories shouldn't appear in the dropdown — only loaded.
	doc := parseHTML(t, w.Body.String())
	options := doc.Find("select#inventory_id option")
	if options.Length() != 1 {
		t.Errorf("expected 1 option (placeholder), got %d", options.Length())
	}
}

// Inventory Row Partial Tests

func TestInventoryRowPartial_PendingState(t *testing.T) {
	f := newTestFixture(t)
	f.registerInventory(t, "inv1", "Test Inventory", "/path")

	req := httptest.NewRequest(http.MethodGet, "/partials/inventory-row/inv1", http.NoBody)
	req = withChiContext(req, "inv1")
	w := httptest.NewRecorder()

	f.h.InventoryRowPartial(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body: %s", w.Code, http.StatusOK, w.Body.String())
	}

	body := w.Body.String()

	// Use fragment parser for partial (tr elements need wrapping)
	doc := parseHTMLFragment(t, body)

	// Verify row structure
	assertElementExists(t, doc, "tr")

	// Verify inventory name
	assertElementContainsText(t, doc, "td", "Test Inventory")

	// Verify pending state badge
	badge := doc.Find("span.px-2").First()
	classAttr, _ := badge.Attr("class")
	if !strings.Contains(classAttr, "bg-yellow-100") {
		t.Errorf("expected pending state badge, got class=%q", classAttr)
	}

	// Verify Load button exists
	assertElementContainsText(t, doc, "button", "Load")
}

func TestInventoryRowPartial_NotFound(t *testing.T) {
	f := newTestFixture(t)

	req := httptest.NewRequest(http.MethodGet, "/partials/inventory-row/nonexistent", http.NoBody)
	req = withChiContext(req, "nonexistent")
	w := httptest.NewRecorder()

	f.h.InventoryRowPartial(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("status = %d, want %d", w.Code, http.StatusNotFound)
	}

	// Partial routes must not return JSON; HTMX would swap the literal
	// JSON body into the DOM.
	ct := w.Header().Get("Content-Type")
	if strings.Contains(ct, "application/json") {
		t.Errorf("Content-Type = %q, must not be JSON for an HTML partial route", ct)
	}
}

// Browse-level partial tests. /browse content-negotiates on the
// HX-Request header — these tests pin the htmx-partial path.

func newHXRequest(method, target string) *http.Request {
	req := httptest.NewRequest(method, target, http.NoBody)
	req.Header.Set("HX-Request", "true")
	return req
}

func TestBrowsePage_PartialRequiresInventoryID(t *testing.T) {
	f := newTestFixture(t)
	req := newHXRequest(http.MethodGet, "/browse?prefix=data/")
	w := httptest.NewRecorder()

	f.h.BrowsePage(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d", w.Code, http.StatusBadRequest)
	}
}

func TestBrowsePage_PartialInventoryNotFound(t *testing.T) {
	f := newTestFixture(t)
	req := newHXRequest(http.MethodGet, "/browse?inventory_id=nonexistent&prefix=test/")
	w := httptest.NewRecorder()

	f.h.BrowsePage(w, req)
	if w.Code != http.StatusNotFound {
		t.Errorf("status = %d, want %d", w.Code, http.StatusNotFound)
	}
}

func TestBrowsePage_PartialNotLoaded(t *testing.T) {
	f := newTestFixture(t)
	f.registerInventory(t, "inv1", "Test", "/path")
	req := newHXRequest(http.MethodGet, "/browse?inventory_id=inv1&prefix=test/")
	w := httptest.NewRecorder()

	f.h.BrowsePage(w, req)
	if w.Code != http.StatusConflict {
		t.Errorf("status = %d, want %d", w.Code, http.StatusConflict)
	}
}

func TestBrowsePage_FullPageWithoutHXRequest(t *testing.T) {
	f := newTestFixture(t)
	// A plain GET (no HX-Request header) must return the full page,
	// not a bare partial — that's the whole point of content
	// negotiation. We just check the response includes the layout
	// shell (the <title> tag).
	req := httptest.NewRequest(http.MethodGet, "/browse", http.NoBody)
	w := httptest.NewRecorder()

	f.h.BrowsePage(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want %d", w.Code, http.StatusOK)
	}
	if !strings.Contains(w.Body.String(), "<title>") {
		t.Errorf("response missing <title> — looks like a partial, not the full page")
	}
}

// TestBrowsePage_BoostedNavReturnsFullPage: hx-boost on <body> makes
// nav clicks send HX-Request: true; those are still page navigations
// and must get the full layout, not the partial branch's 400.
func TestBrowsePage_BoostedNavReturnsFullPage(t *testing.T) {
	f := newTestFixture(t)
	req := httptest.NewRequest(http.MethodGet, "/browse", http.NoBody)
	req.Header.Set("HX-Request", "true")
	req.Header.Set("HX-Boosted", "true")
	w := httptest.NewRecorder()

	f.h.BrowsePage(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("boosted /browse without params: status = %d, want %d", w.Code, http.StatusOK)
	}
	if !strings.Contains(w.Body.String(), "<title>") {
		t.Error("boosted /browse: response missing <title>, looks like a partial swap was returned")
	}
}

// TestLayout_DisablesHTMXHistoryCache pins the layout's two history-cache
// safeties: the htmx-config meta tag and hx-history="false" on body.
// Both together guarantee back/forward goes through the server so form
// state reflects the URL — outerHTML snapshots can't preserve user-
// edited <input value> or <option selected>.
func TestLayout_DisablesHTMXHistoryCache(t *testing.T) {
	f := newTestFixture(t)
	req := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
	w := httptest.NewRecorder()
	f.h.Dashboard(w, req)

	body := w.Body.String()
	if !strings.Contains(body, `name="htmx-config"`) || !strings.Contains(body, `"historyCacheSize":0`) {
		t.Error("layout missing <meta name=\"htmx-config\"> with historyCacheSize:0")
	}
	if !strings.Contains(body, `hx-history="false"`) {
		t.Error("layout body missing hx-history=\"false\"")
	}
}

// TestBrowsePage_HistoryRestoreReturnsFullPage: HX-History-Restore-Request
// from htmx back/forward must get the full layout, not a bare partial.
func TestBrowsePage_HistoryRestoreReturnsFullPage(t *testing.T) {
	f := newTestFixture(t)
	req := httptest.NewRequest(http.MethodGet, "/browse", http.NoBody)
	req.Header.Set("HX-Request", "true")
	req.Header.Set("HX-History-Restore-Request", "true")
	w := httptest.NewRecorder()

	f.h.BrowsePage(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("history-restore /browse: status = %d, want %d", w.Code, http.StatusOK)
	}
	if !strings.Contains(w.Body.String(), "<title>") {
		t.Error("history-restore /browse: response missing <title>")
	}
}

// State Badge Color Tests

func TestStateBadgeColors(t *testing.T) {
	f := newTestFixture(t)
	f.registerInventory(t, "inv1", "Test Inventory", "/path")

	req := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
	w := httptest.NewRecorder()
	f.h.Dashboard(w, req)

	doc := parseHTML(t, w.Body.String())

	// Find the state badge
	badge := doc.Find("tbody tr span.px-2").First()
	classAttr, _ := badge.Attr("class")

	// Pending state should have yellow classes
	if !strings.Contains(classAttr, "bg-yellow-100") || !strings.Contains(classAttr, "text-yellow-800") {
		t.Errorf("pending badge should have yellow classes, got: %q", classAttr)
	}
}

// withChiContext adds a chi URL param context with the given id.
func withChiContext(r *http.Request, id string) *http.Request {
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("id", id)
	return r.WithContext(context.WithValue(r.Context(), chi.RouteCtxKey, rctx))
}
