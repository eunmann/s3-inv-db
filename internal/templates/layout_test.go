package templates_test

import (
	"bytes"
	"strings"
	"testing"

	"github.com/PuerkitoBio/goquery"
	"github.com/eunmann/s3-inv-db/internal/templates"
)

func renderLayoutPage(t *testing.T) *goquery.Document {
	t.Helper()
	renderer, err := templates.New()
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	var buf bytes.Buffer
	if err := renderer.Render(&buf, "dashboard.html", map[string]any{"Title": "Dashboard"}); err != nil {
		t.Fatalf("Render: %v", err)
	}
	doc, err := goquery.NewDocumentFromReader(&buf)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	return doc
}

// TestLayout_LiveAnnounceRegion: live.js writes build endings into one
// polite, visually hidden live region on every page.
func TestLayout_LiveAnnounceRegion(t *testing.T) {
	region := renderLayoutPage(t).Find("[data-live-announce]")
	if region.Length() != 1 {
		t.Fatalf("found %d [data-live-announce], want 1", region.Length())
	}
	if got, _ := region.Attr("aria-live"); got != "polite" {
		t.Errorf("aria-live = %q, want polite", got)
	}
	if !region.HasClass("sr-only") {
		t.Error("announce region must be visually hidden (sr-only)")
	}
	if _, hidden := region.Attr("hidden"); hidden {
		t.Error("a hidden live region announces nothing")
	}
}

// TestLayout_ErrorToast: every page has one dismissible alert for
// failed htmx requests, hidden until a request fails, inside an alert
// region that stays rendered so its message is announced.
func TestLayout_ErrorToast(t *testing.T) {
	doc := renderLayoutPage(t)

	boxes := doc.Find("[data-error-toast]")
	if boxes.Length() != 1 {
		t.Fatalf("found %d [data-error-toast], want 1", boxes.Length())
	}
	if _, hidden := boxes.Attr("hidden"); !hidden {
		t.Error("toast must start hidden")
	}
	region := boxes.Parent()
	if role, _ := region.Attr("role"); role != "alert" {
		t.Errorf("toast's region role = %q, want alert", role)
	}
	if _, hidden := region.Attr("hidden"); hidden {
		t.Error("the alert region itself must stay rendered")
	}
	if boxes.Find("[data-error-toast-text]").Length() != 1 {
		t.Error("toast has no [data-error-toast-text]")
	}
	closer := boxes.Find("button[data-error-toast-close]")
	if label, _ := closer.Attr("aria-label"); label == "" {
		t.Error("toast needs a labelled close button")
	}

	var script string
	doc.Find("head script").Each(func(_ int, s *goquery.Selection) {
		if strings.Contains(s.Text(), "data-error-toast") {
			script = s.Text()
		}
	})
	if script == "" {
		t.Fatal("no head script drives the toast")
	}
	for _, want := range []string{"htmx:responseError", "htmx:sendError", "[data-own-errors]", "textContent"} {
		if !strings.Contains(script, want) {
			t.Errorf("toast script missing %q", want)
		}
	}
	if strings.Contains(script, "innerHTML") {
		t.Error("toast script must write messages with textContent, not innerHTML")
	}
}
