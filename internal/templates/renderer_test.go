package templates

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRenderer_Dashboard(t *testing.T) {
	renderer, err := New(false)
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
	renderer, err := New(false)
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

func TestRenderer_Stats(t *testing.T) {
	renderer, err := New(false)
	if err != nil {
		t.Fatalf("failed to create renderer: %v", err)
	}

	data := map[string]interface{}{
		"Title":       "Query Stats",
		"Inventories": []interface{}{},
	}

	var buf bytes.Buffer
	if err := renderer.Render(&buf, "stats.html", data); err != nil {
		t.Fatalf("failed to render stats: %v", err)
	}

	html := buf.String()
	if !strings.Contains(html, "Query Stats") {
		t.Error("rendered output doesn't contain 'Query Stats'")
	}
}

func TestRenderer_PageNotFound(t *testing.T) {
	renderer, err := New(false)
	if err != nil {
		t.Fatalf("failed to create renderer: %v", err)
	}

	var buf bytes.Buffer
	err = renderer.Render(&buf, "nonexistent.html", nil)
	if err == nil {
		t.Error("expected error for nonexistent template")
	}
}

// TestRenderer_DevModeReloadsFromDisk verifies that devMode picks up
// edits to templates between renders. Without this, devMode silently
// served stale embedded content if the disk path resolved wrong.
func TestRenderer_DevModeReloadsFromDisk(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "templates", "partials"), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	mustWrite := func(path, body string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(root, path), []byte(body), 0o644); err != nil {
			t.Fatalf("write %s: %v", path, err)
		}
	}

	mustWrite("templates/layout.html",
		`{{define "layout"}}<html><body>{{template "content" .}}</body></html>{{end}}`)
	mustWrite("templates/page.html",
		`{{template "layout" .}}{{define "content"}}<p>first</p>{{end}}`)
	mustWrite("templates/partials/snippet.html",
		`<span>partial-first</span>`)

	renderer, err := NewWithRootDir(true, root)
	if err != nil {
		t.Fatalf("NewWithRootDir: %v", err)
	}

	var buf bytes.Buffer
	if err := renderer.Render(&buf, "page.html", map[string]any{"Title": "T"}); err != nil {
		t.Fatalf("render page (initial): %v", err)
	}
	if !strings.Contains(buf.String(), "first") {
		t.Fatalf("expected initial render to contain 'first', got: %s", buf.String())
	}

	// Edit the page template on disk; devMode should pick it up on the
	// next Render call.
	mustWrite("templates/page.html",
		`{{template "layout" .}}{{define "content"}}<p>second</p>{{end}}`)
	mustWrite("templates/partials/snippet.html",
		`<span>partial-second</span>`)

	buf.Reset()
	if err := renderer.Render(&buf, "page.html", map[string]any{"Title": "T"}); err != nil {
		t.Fatalf("render page (after edit): %v", err)
	}
	if !strings.Contains(buf.String(), "second") {
		t.Errorf("devMode did not pick up edited page; got: %s", buf.String())
	}

	buf.Reset()
	if err := renderer.RenderPartial(&buf, "snippet.html", nil); err != nil {
		t.Fatalf("render partial: %v", err)
	}
	if !strings.Contains(buf.String(), "partial-second") {
		t.Errorf("devMode did not pick up edited partial; got: %s", buf.String())
	}
}

func TestRenderer_Partial(t *testing.T) {
	renderer, err := New(false)
	if err != nil {
		t.Fatalf("failed to create renderer: %v", err)
	}

	data := map[string]interface{}{
		"ID":    "test-id",
		"Name":  "Test Name",
		"Path":  "/path/to/test",
		"State": "pending",
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
