package templates

import (
	"bytes"
	"strings"
	"testing"

	"github.com/eunmann/s3-inv-db/internal/inventory"
)

func TestRenderer_Dashboard(t *testing.T) {
	renderer, err := New()
	if err != nil {
		t.Fatalf("failed to create renderer: %v", err)
	}

	data := map[string]interface{}{
		"Title":        "Dashboard",
		"TotalCount":   0,
		"LoadedCount":  0,
		"PendingCount": 0,
		"ErrorCount":   0,
		"Inventories":  []interface{}{},
	}

	var buf bytes.Buffer
	if err := renderer.Render(&buf, "dashboard.html", data); err != nil {
		t.Fatalf("failed to render dashboard: %v", err)
	}

	if buf.Len() == 0 {
		t.Error("rendered output is empty")
	}

	html := buf.String()
	if !strings.Contains(html, "Dashboard") {
		t.Error("rendered output doesn't contain 'Dashboard'")
	}
	if !strings.Contains(html, "<h1") {
		t.Error("rendered output doesn't contain h1 tag")
	}
}

func TestRenderer_Inventories(t *testing.T) {
	renderer, err := New()
	if err != nil {
		t.Fatalf("failed to create renderer: %v", err)
	}

	data := map[string]interface{}{
		"Title":       "Inventories",
		"Inventories": []interface{}{},
	}

	var buf bytes.Buffer
	if err := renderer.Render(&buf, "inventories.html", data); err != nil {
		t.Fatalf("failed to render inventories: %v", err)
	}

	html := buf.String()
	if !strings.Contains(html, "Inventories") {
		t.Error("rendered output doesn't contain 'Inventories'")
	}
}

func TestRenderer_Browse(t *testing.T) {
	renderer, err := New()
	if err != nil {
		t.Fatalf("failed to create renderer: %v", err)
	}

	data := map[string]interface{}{
		"Title":       "Browse",
		"Inventories": []interface{}{},
	}

	var buf bytes.Buffer
	if err := renderer.Render(&buf, "browse.html", data); err != nil {
		t.Fatalf("failed to render browse: %v", err)
	}

	html := buf.String()
	if !strings.Contains(html, "Browse") {
		t.Error("rendered output doesn't contain 'Browse'")
	}
	if !strings.Contains(html, "browse-target") {
		t.Error("rendered output doesn't contain the partial target div")
	}
}

func TestRenderer_PageNotFound(t *testing.T) {
	renderer, err := New()
	if err != nil {
		t.Fatalf("failed to create renderer: %v", err)
	}

	var buf bytes.Buffer
	err = renderer.Render(&buf, "nonexistent.html", nil)
	if err == nil {
		t.Error("expected error for nonexistent template")
	}
}

// TestRenderer_RepeatedRenders is a regression for "html/template: cannot
// Clone after it has executed" — the renderer used to Clone a cached base
// template on every request, which worked once and failed forever after.
// Render each page (and a partial) twice and assert both calls succeed.
func TestRenderer_RepeatedRenders(t *testing.T) {
	renderer, err := New()
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	type renderCase struct {
		name string
		data any
	}
	cases := []renderCase{
		{"dashboard.html", map[string]any{
			"Title":        "Dashboard",
			"TotalCount":   0,
			"LoadedCount":  0,
			"PendingCount": 0,
			"ErrorCount":   0,
			"Inventories":  []any{},
		}},
		{"inventories.html", map[string]any{
			"Title":       "Inventories",
			"Inventories": []any{},
		}},
		{"browse.html", map[string]any{
			"Title":       "Browse",
			"Inventories": []any{},
		}},
	}

	for _, c := range cases {
		var first, second bytes.Buffer
		if err := renderer.Render(&first, c.name, c.data); err != nil {
			t.Errorf("Render(%s) first call: %v", c.name, err)
			continue
		}
		if err := renderer.Render(&second, c.name, c.data); err != nil {
			t.Errorf("Render(%s) second call: %v", c.name, err)
			continue
		}
		if first.Len() == 0 || second.Len() == 0 {
			t.Errorf("Render(%s) produced empty output", c.name)
		}
	}

	// Same for the partial path. Use the real Info type so the
	// template's .State.IsPending predicate resolves.
	partialData := inventory.Info{ID: "x", Name: "X", Path: "/p", State: inventory.StatePending}
	for i := range 2 {
		var buf bytes.Buffer
		if err := renderer.RenderPartial(&buf, "inventory_row.html", partialData); err != nil {
			t.Errorf("RenderPartial call %d: %v", i, err)
		}
	}
}

func TestRenderer_Partial(t *testing.T) {
	renderer, err := New()
	if err != nil {
		t.Fatalf("failed to create renderer: %v", err)
	}

	data := inventory.Info{
		ID:    "test-id",
		Name:  "Test Name",
		Path:  "/path/to/test",
		State: inventory.StatePending,
	}

	var buf bytes.Buffer
	if err := renderer.RenderPartial(&buf, "inventory_row.html", data); err != nil {
		t.Fatalf("failed to render inventory row partial: %v", err)
	}

	html := buf.String()
	if !strings.Contains(html, "test-id") {
		t.Error("rendered partial doesn't contain inventory ID")
	}
}

func TestHxValsAttr_QuotesInValueAreJSONEscaped(t *testing.T) {
	// The whole point of hxVals: a value containing `"` must not break
	// the JSON payload after html/template's attribute-context escaping.
	attr, err := hxValsAttr("prefix", `a"b`)
	if err != nil {
		t.Fatalf("hxValsAttr: %v", err)
	}
	got := string(attr)
	if !strings.Contains(got, `\"`) {
		t.Errorf("attr %q does not contain JSON-escaped quote", got)
	}
	if !strings.HasPrefix(got, `hx-vals='`) || !strings.HasSuffix(got, `'`) {
		t.Errorf("attr %q is not wrapped in single quotes", got)
	}
}

func TestHxValsAttr_SingleQuoteInValueIsHTMLEscaped(t *testing.T) {
	// A `'` in a value would otherwise terminate the single-quoted
	// attribute. Must be replaced with the HTML entity.
	attr, err := hxValsAttr("prefix", "it's")
	if err != nil {
		t.Fatalf("hxValsAttr: %v", err)
	}
	got := string(attr)
	if !strings.Contains(got, "&#39;") {
		t.Errorf("attr %q does not escape single quote", got)
	}
	// The outer single quotes of the attribute must remain.
	if !strings.HasPrefix(got, `hx-vals='`) || !strings.HasSuffix(got, `'`) {
		t.Errorf("attr %q broken outer single quotes", got)
	}
}

func TestHxValsAttr_OddPairs(t *testing.T) {
	if _, err := hxValsAttr("only-key"); err == nil {
		t.Error("expected error on odd number of args")
	}
}

func TestBrowseURL_EncodesSpecialChars(t *testing.T) {
	got, err := browseURL("inventory_id", "inv-1", "prefix", "a&b c#d/")
	if err != nil {
		t.Fatalf("browseURL: %v", err)
	}
	// url.Values.Encode percent-encodes `&`, ` `, `#`, `/`. Verify each.
	if !strings.Contains(got, "prefix=a%26b+c%23d%2F") {
		t.Errorf("URL %q missing percent-encoded prefix", got)
	}
}

func TestBrowseURL_SkipsEmpty(t *testing.T) {
	got, err := browseURL("inventory_id", "inv-1", "prefix", "", "sort", "")
	if err != nil {
		t.Fatalf("browseURL: %v", err)
	}
	if strings.Contains(got, "prefix=") || strings.Contains(got, "sort=") {
		t.Errorf("URL %q should not include empty params", got)
	}
	if !strings.Contains(got, "inventory_id=inv-1") {
		t.Errorf("URL %q missing inventory_id", got)
	}
}

func TestHxValsAttr_HTMLSpecialCharsInValueAreSafe(t *testing.T) {
	// json.Marshal in Go escapes <, >, & to JSON unicode escapes
	// (<, >, &) by default. So no literal `&`, `<`, or
	// `>` ever appears in the output, the surrounding single-quoted HTML
	// attribute can't terminate prematurely, and there's no half-formed
	// entity for browsers to interpret. Lock that contract in.
	attr, err := hxValsAttr("k", `a&b<c>d`)
	if err != nil {
		t.Fatalf("hxValsAttr: %v", err)
	}
	got := string(attr)
	for _, banned := range []string{"a&b", "<c", ">d"} {
		if strings.Contains(got, banned) {
			t.Errorf("attr %q contains literal %q — JSON HTML-safe escaping broken", got, banned)
		}
	}
	// JSON unicode escapes (6 chars each: backslash + u + 4 hex digits).
	for _, want := range []string{"\\u0026", "\\u003c", "\\u003e"} {
		if !strings.Contains(got, want) {
			t.Errorf("attr %q missing JSON unicode escape %q", got, want)
		}
	}
}
