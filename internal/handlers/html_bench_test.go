package handlers_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"
)

// Benchmark*LevelPartialHTML time one HTMX level partial (what a click
// in the UI fetches) at the largest page size, so the per-row template
// work dominates the per-page index work. Every child has a child of
// its own, so every row renders its drill-down link.

func htmlRouter(b *testing.B) http.Handler {
	b.Helper()
	h := wideHandlersWith(b, func(b *testing.B, dir string, changed bool) {
		b.Helper()
		buildWideIndexKeys(b, dir, changed, "c%06d/d/a", "c%06d/d/b")
	})
	r := chi.NewRouter()
	r.Get("/browse", h.BrowsePage)
	r.Get("/compare", h.ComparePage)

	return r
}

func benchHTMXGet(b *testing.B, router http.Handler, target string) {
	b.Helper()
	b.ReportAllocs()
	for b.Loop() {
		rec := httptest.NewRecorder()
		req := httptest.NewRequestWithContext(b.Context(), http.MethodGet, target, http.NoBody)
		req.Header.Set("HX-Request", "true")
		router.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			b.Fatalf("status %d: %s", rec.Code, rec.Body.String())
		}
	}
}

func BenchmarkBrowseLevelPartialHTML(b *testing.B) {
	benchHTMXGet(b, htmlRouter(b), "/browse?inventory_id="+gateFromID+"&sort=segment&page=50&page_size=500")
}

func BenchmarkCompareLevelPartialHTML(b *testing.B) {
	benchHTMXGet(b, htmlRouter(b), "/compare?from="+gateFromID+"&to="+gateToID+"&show_unchanged=true&page_size=500")
}
