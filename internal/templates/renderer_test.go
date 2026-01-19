package templates

import (
	"bytes"
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
