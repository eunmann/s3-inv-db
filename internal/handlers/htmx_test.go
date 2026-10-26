package handlers_test

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/eunmann/s3-inv-db/internal/handlers"
	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/internal/seeder"
	"github.com/rs/zerolog"
)

// newTwoRunHandlers loads one seeded index as two runs of one
// configuration, so both Browse and Compare have something to render.
func newTwoRunHandlers(t *testing.T) *handlers.Handlers {
	t.Helper()
	tmp := t.TempDir()
	if err := seeder.Run(t.Context(), seeder.Config{
		OutputDir: tmp, Count: 1, Objects: 200, Preset: "small",
		Seed: 42, Logger: zerolog.Nop(),
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	mgr := inventory.NewCatalog(nil)
	t.Cleanup(func() { _ = mgr.Close() })
	h := newWiredHandlers(t, mgr)
	indexPath := filepath.Join(tmp, "inv-001")
	for _, id := range []inventory.ID{"src/inv/runA", "src/inv/runB"} {
		if err := mgr.Register(t.Context(), id, string(id), indexPath); err != nil {
			t.Fatalf("register %s: %v", id, err)
		}
		if err := mgr.Load(t.Context(), id); err != nil {
			t.Fatalf("load %s: %v", id, err)
		}
	}

	return h
}

// varyTokens splits every Vary header value into its listed names.
func varyTokens(hdr http.Header) []string {
	var out []string
	for _, v := range hdr.Values("Vary") {
		for tok := range strings.SplitSeq(v, ",") {
			out = append(out, strings.TrimSpace(tok))
		}
	}

	return out
}

// TestHTMLResponses_VaryAndNoCache: /browse and /compare return a page
// or a fragment at one URL, so every HTML answer must say it varies on
// the htmx headers and must be revalidated; otherwise a browser can
// show a cached fragment as the page on Back. A Vary set before the
// handler runs is kept.
func TestHTMLResponses_VaryAndNoCache(t *testing.T) {
	h := newTwoRunHandlers(t)
	type mode struct {
		name    string
		headers map[string]string
		partial bool
	}
	modes := []mode{
		{name: "full"},
		{name: "partial", headers: map[string]string{"HX-Request": "true"}, partial: true},
		{name: "history-restore", headers: map[string]string{"HX-Request": "true", "HX-History-Restore-Request": "true"}},
	}
	pages := []struct {
		name    string
		url     string
		handler http.HandlerFunc
	}{
		{"browse", "/browse?inventory_id=src/inv/runA&prefix=", h.BrowsePage},
		{"compare", "/compare?from=src/inv/runA&to=src/inv/runB&prefix=", h.ComparePage},
	}
	for _, p := range pages {
		for _, m := range modes {
			t.Run(p.name+"/"+m.name, func(t *testing.T) {
				req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, p.url, http.NoBody)
				for k, v := range m.headers {
					req.Header.Set(k, v)
				}
				w := httptest.NewRecorder()
				w.Header().Set("Vary", "Accept-Encoding")
				p.handler(w, req)

				if w.Code != http.StatusOK {
					t.Fatalf("status = %d, want 200; body=%s", w.Code, w.Body.String())
				}
				if got := strings.Contains(w.Body.String(), "<title>"); got == m.partial {
					t.Fatalf("full page = %v, want %v", got, !m.partial)
				}
				if got := w.Header().Get("Cache-Control"); got != "no-cache" {
					t.Errorf("Cache-Control = %q, want no-cache", got)
				}
				vary := varyTokens(w.Header())
				for _, want := range []string{"Accept-Encoding", "HX-Request", "HX-Boosted", "HX-History-Restore-Request"} {
					if !slices.Contains(vary, want) {
						t.Errorf("Vary %q missing %q", vary, want)
					}
				}
				if n := len(vary); n != 4 {
					t.Errorf("Vary %q lists %d names, want 4 (no duplicates)", vary, n)
				}
			})
		}
	}
}

// TestInventoriesPage_AutoLoadFormReportsOwnErrors: the auto-load form
// shows its own "Not saved: …" line, so it opts out of the layout's
// error toast rather than reporting a failure twice.
func TestInventoriesPage_AutoLoadFormReportsOwnErrors(t *testing.T) {
	list := []inventory.Inventory{{SourceBucket: "src", Name: "inv", Run: "2026-01-01T03-00Z", ManifestKey: "k/inv/manifest.json"}}
	h := newWiredHandlers(t, nil,
		handlers.WithDiscoverer(&fakeDiscoverer{listResp: list, bucket: "dst"}),
		handlers.WithLoader(&fakeBuilder{}))
	w := httptest.NewRecorder()
	h.InventoriesPage(w, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/inventories", http.NoBody))
	if w.Code != http.StatusOK {
		t.Fatalf("status %d", w.Code)
	}
	forms := parseHTML(t, w.Body.String()).Find("form[data-autoload-form]")
	if forms.Length() == 0 {
		t.Fatal("no auto-load form rendered")
	}
	if forms.Not("[data-own-errors]").Length() != 0 {
		t.Error("auto-load form lacks data-own-errors")
	}
}

// TestCancelJob_FinishedJobSaysSo: Cancel on a job that has already
// ended answers 404 with a sentence the error toast can show as is.
func TestCancelJob_FinishedJobSaysSo(t *testing.T) {
	h := newWiredHandlers(t, nil)
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/api/jobs/gone/cancel", http.NoBody)
	w := httptest.NewRecorder()
	h.CancelJob(w, withURLParam(req, "id", "gone"))

	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", w.Code)
	}
	if got := strings.TrimSpace(w.Body.String()); got != "this job is no longer running" {
		t.Errorf("body = %q", got)
	}
	if ct := w.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
		t.Errorf("Content-Type = %q, want text/plain", ct)
	}
}

// TestComparePage_FullPagePrefixInput: Back and Forward refetch the
// page from the server, so the full page must render the prefix input
// with the URL's prefix.
func TestComparePage_FullPagePrefixInput(t *testing.T) {
	h := newTwoRunHandlers(t)
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/compare?from=src/inv/runA&to=src/inv/runB&prefix=data/2024/", http.NoBody)
	req.Header.Set("HX-Request", "true")
	req.Header.Set("HX-History-Restore-Request", "true")
	w := httptest.NewRecorder()
	h.ComparePage(w, req)

	doc := parseHTML(t, w.Body.String())
	if got, _ := doc.Find("input#prefix").Attr("value"); got != "data/2024/" {
		t.Errorf("prefix input value = %q, want %q", got, "data/2024/")
	}
}
