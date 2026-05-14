package handlers

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestHelpPage_Renders pins the basic contract: 200 + HTML body that
// surfaces the page title and at least one anchor target. Any future
// content reshuffle should still hit these.
func TestHelpPage_Renders(t *testing.T) {
	f := newTestFixture(t)
	req := httptest.NewRequest(http.MethodGet, "/help", http.NoBody)
	w := httptest.NewRecorder()
	f.h.HelpPage(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	if ct := w.Header().Get("Content-Type"); !strings.Contains(ct, "text/html") {
		t.Errorf("Content-Type = %q, want text/html", ct)
	}

	body := w.Body.String()
	if !strings.Contains(body, "Documentation") || !strings.Contains(body, "Help") {
		head := body
		if len(head) > 500 {
			head = head[:500]
		}
		t.Errorf("body missing header label\nbody[:500]=%q", head)
	}
	// A handful of meaningful section headings present.
	for _, want := range []string{"Overview", "HTTP routes", "SSE stream", "Persistence", "Configuration", "Glossary"} {
		if !strings.Contains(body, want) {
			t.Errorf("body missing section heading %q", want)
		}
	}
	for _, anchor := range []string{
		`id="overview"`, `id="pages"`, `id="workflows"`,
		`id="workflow-load"`, `id="workflow-browse"`, `id="workflow-recover"`,
		`id="chips"`, `id="buttons"`, `id="progress"`,
		`id="urls"`, `id="routes"`, `id="sse"`,
		`id="persistence"`, `id="config"`,
		`id="shortcuts"`, `id="troubleshooting"`, `id="glossary"`,
	} {
		if !strings.Contains(body, anchor) {
			t.Errorf("body missing TOC anchor %s", anchor)
		}
	}
}

// TestHelpPage_StateChipsUseHelpers asserts the in-page reference
// renders chip examples via the same stateClass/stateLabel helpers the
// rest of the UI uses — so a future palette change to those funcs
// updates the help page automatically.
func TestHelpPage_StateChipsUseHelpers(t *testing.T) {
	f := newTestFixture(t)
	req := httptest.NewRequest(http.MethodGet, "/help", http.NoBody)
	w := httptest.NewRecorder()
	f.h.HelpPage(w, req)
	body := w.Body.String()

	for _, label := range []string{"Not loaded", "Loading", "Loaded", "On disk", "Error"} {
		if !strings.Contains(body, label) {
			t.Errorf("chip reference missing label %q", label)
		}
	}
	for _, cls := range []string{"bg-green-100", "bg-yellow-100", "bg-blue-100", "bg-red-100", "bg-gray-100"} {
		if !strings.Contains(body, cls) {
			t.Errorf("chip reference missing class %q (helper changed?)", cls)
		}
	}
}
