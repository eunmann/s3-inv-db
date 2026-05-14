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
	// Hero check — distinctive enough to survive copy edits, broad
	// enough to not pin a single phrase.
	if !strings.Contains(body, "Everything you can") {
		head := body
		if len(head) > 500 {
			head = head[:500]
		}
		t.Errorf("body missing hero heading\nbody[:500]=%q", head)
	}
	// At least one section marker — pins the editorial section style.
	if !strings.Contains(body, "§ 01") {
		t.Error("body missing section marker '§ 01'")
	}
	for _, anchor := range []string{
		`id="pages"`, `id="workflow-load"`, `id="workflow-browse"`,
		`id="chips"`, `id="buttons"`, `id="progress"`,
		`id="persistence"`, `id="troubleshooting"`, `id="glossary"`,
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
