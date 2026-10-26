package templates_test

import (
	"bytes"
	"regexp"
	"strings"
	"testing"

	"github.com/eunmann/s3-inv-db/internal/templates"
)

func TestRenderer_Dashboard(t *testing.T) {
	renderer, err := templates.New()
	if err != nil {
		t.Fatalf("failed to create renderer: %v", err)
	}

	data := map[string]any{
		"Title":        "Dashboard",
		"TotalCount":   0,
		"LoadedCount":  0,
		"PendingCount": 0,
		"ErrorCount":   0,
		"Inventories":  []any{},
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
	// A hidden dashboard tab must not hold one of the browser's few
	// connections to the origin: live.js closes a "while-visible" stream.
	if !strings.Contains(html, `data-live-jobs="while-visible"`) {
		t.Error(`dashboard doesn't mark its job stream data-live-jobs="while-visible"`)
	}
	if !strings.Contains(html, "data-live-status hidden") {
		t.Error("layout doesn't carry the hidden data-live-status notice live.js shows while the stream is down")
	}
}

func TestRenderer_Inventories(t *testing.T) {
	renderer, err := templates.New()
	if err != nil {
		t.Fatalf("failed to create renderer: %v", err)
	}

	data := map[string]any{
		"Title":       "Inventories",
		"Inventories": []any{},
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

// TestRenderer_InventoriesRunHashSegments: the #run= check that reopens
// a drawer on reload accepts every bucket, configuration and run name
// the server links to, and rejects path tricks.
func TestRenderer_InventoriesRunHashSegments(t *testing.T) {
	renderer, err := templates.New()
	if err != nil {
		t.Fatalf("failed to create renderer: %v", err)
	}
	var buf bytes.Buffer
	if err := renderer.Render(&buf, "inventories.html", map[string]any{"Title": "Inventories"}); err != nil {
		t.Fatalf("failed to render inventories: %v", err)
	}
	m := regexp.MustCompile(`var segment = /(.+)/;`).FindStringSubmatch(buf.String())
	if m == nil {
		t.Fatal("inventories page has no #run= segment pattern")
	}
	segment := regexp.MustCompile(m[1])
	for _, ok := range []string{"my-bucket.example", "daily_inventory", "_inv", "Inv-01", "2026-10-01T01-00Z"} {
		if !segment.MatchString(ok) {
			t.Errorf("segment pattern rejects %q", ok)
		}
	}
	for _, bad := range []string{"", ".", "..", "a/b", "a b", "a?b"} {
		if segment.MatchString(bad) {
			t.Errorf("segment pattern accepts %q", bad)
		}
	}
}

func TestRenderer_Browse(t *testing.T) {
	renderer, err := templates.New()
	if err != nil {
		t.Fatalf("failed to create renderer: %v", err)
	}

	data := map[string]any{
		"Title":       "Browse",
		"Inventories": []any{},
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
	renderer, err := templates.New()
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
	renderer, err := templates.New()
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	type renderCase struct {
		data any
		name string
	}
	cases := []renderCase{
		{name: "dashboard.html", data: map[string]any{
			"Title":        "Dashboard",
			"TotalCount":   0,
			"LoadedCount":  0,
			"PendingCount": 0,
			"ErrorCount":   0,
			"Inventories":  []any{},
		}},
		{name: "inventories.html", data: map[string]any{
			"Title":       "Inventories",
			"Inventories": []any{},
		}},
		{name: "browse.html", data: map[string]any{
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

	// Same for the partial path.
	for i := range 2 {
		var buf bytes.Buffer
		if err := renderer.RenderPartial(&buf, "notifications_banner.html", bannerData("x")); err != nil {
			t.Errorf("RenderPartial call %d: %v", i, err)
		}
	}
}

// bannerData is notifications_banner.html's data with one issue
// targeting target.
func bannerData(target string) map[string]any {
	return map[string]any{"Notifications": []map[string]string{
		{"Title": "Build failed", "Target": target, "Message": "boom"},
	}}
}

func TestRenderer_Partial(t *testing.T) {
	renderer, err := templates.New()
	if err != nil {
		t.Fatalf("failed to create renderer: %v", err)
	}

	var buf bytes.Buffer
	if err := renderer.RenderPartial(&buf, "notifications_banner.html", bannerData("test-id")); err != nil {
		t.Fatalf("failed to render notifications banner partial: %v", err)
	}

	html := buf.String()
	if !strings.Contains(html, "test-id") {
		t.Error("rendered partial doesn't contain the issue's target")
	}
}

func TestHxValsJSON_QuotesInValueAreJSONEscaped(t *testing.T) {
	// The whole point of hxValsJSON: a value containing `"` must not
	// break the JSON payload. json.Marshal escapes `"` to `\"`, and the
	// surrounding html/template attribute-context escaping then encodes
	// the literal `"` separators as `&#34;` (browsers decode that back
	// to `"` before HTMX reads the attribute).
	got, err := templates.HxValsJSON("prefix", `a"b`)
	if err != nil {
		t.Fatalf("hxValsJSON: %v", err)
	}
	if !strings.Contains(got, `\"`) {
		t.Errorf("output %q does not contain JSON-escaped quote", got)
	}
	if !strings.HasPrefix(got, `{`) || !strings.HasSuffix(got, `}`) {
		t.Errorf("output %q is not a JSON object literal", got)
	}
}

func TestHxValsJSON_OddPairs(t *testing.T) {
	if _, err := templates.HxValsJSON("only-key"); err == nil {
		t.Error("expected error on odd number of args")
	}
}

func TestHxValsJSON_HTMLSpecialCharsInValueAreSafe(t *testing.T) {
	// json.Marshal in Go escapes <, >, & to JSON unicode escapes
	// (<, >, &) by default. So no literal `&`, `<`, or
	// `>` ever appears in the output. Combined with html/template's
	// attribute-context escaping at the call site, an attacker-controlled
	// value cannot break out of the attribute or close the surrounding
	// tag. Lock that contract in.
	got, err := templates.HxValsJSON("k", `a&b<c>d`)
	if err != nil {
		t.Fatalf("hxValsJSON: %v", err)
	}
	for _, banned := range []string{"a&b", "<c", ">d"} {
		if strings.Contains(got, banned) {
			t.Errorf("output %q contains literal %q — JSON HTML-safe escaping broken", got, banned)
		}
	}
	// JSON unicode escapes (6 chars each: backslash + u + 4 hex digits).
	for _, want := range []string{"\\u0026", "\\u003c", "\\u003e"} {
		if !strings.Contains(got, want) {
			t.Errorf("output %q missing JSON unicode escape %q", got, want)
		}
	}
}

// The drawer body re-renders while a build runs. Only sections the user
// clicked keep their open state across a swap; the rest take the server's
// default, so Build details folds once the build ends.
func TestRenderer_InventoriesDrawerKeepsOnlyTouchedSections(t *testing.T) {
	renderer, err := templates.New()
	if err != nil {
		t.Fatalf("failed to create renderer: %v", err)
	}
	var buf bytes.Buffer
	if err := renderer.Render(&buf, "inventories.html", map[string]any{"Title": "Inventories"}); err != nil {
		t.Fatalf("failed to render inventories: %v", err)
	}
	var handler string
	for _, m := range regexp.MustCompile(`(?s)addEventListener\('htmx:beforeSwap', function \(e\) \{(.*?)\n\s*\}\);`).FindAllStringSubmatch(buf.String(), -1) {
		if strings.Contains(m[1], "'run-drawer-body'") {
			handler = m[1]
		}
	}
	if handler == "" {
		t.Fatal("inventories page has no htmx:beforeSwap handler for the drawer body")
	}
	for _, want := range []string{
		"!body._touched) return;",
		"if (body._touched[d.id]) open[d.id] = d.open;",
	} {
		if !strings.Contains(handler, want) {
			t.Errorf("beforeSwap handler missing %q; untouched sections would override the server default\n%s", want, handler)
		}
	}
	if !strings.Contains(buf.String(), "(body._touched = body._touched || {})[summary.parentElement.id] = true;") {
		t.Error("inventories page no longer records which drawer sections the user clicked")
	}
}
