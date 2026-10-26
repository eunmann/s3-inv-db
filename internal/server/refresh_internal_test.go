package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/pkg/extsort/events"
	"github.com/eunmann/s3-inv-db/pkg/pricing"
	"github.com/rs/zerolog"
)

func TestRefreshIndexes_NoSourceIs503(t *testing.T) {
	srv, err := New(t.Context(), Config{Addr: ":0", Logger: zerolog.Nop(), PriceTable: pricing.DefaultUSEast1Prices(), DB: testDB(t)})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "http://localhost/api/indexes/refresh", http.NoBody)
	w := httptest.NewRecorder()
	srv.Router().ServeHTTP(w, req)
	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503 with no s3_source or index_source", w.Code)
	}
}

type emptySource struct{}

func (emptySource) List(context.Context) ([]inventory.Inventory, error) { return nil, nil }
func (emptySource) Find(context.Context, string, string, string) (inventory.Inventory, error) {
	return inventory.Inventory{}, inventory.ErrNotFound
}
func (emptySource) Bucket() string { return "b" }
func (emptySource) BuildWith(context.Context, inventory.CacheKey, string, events.ProgressFunc, *events.Bus) (string, error) {
	return "", nil
}
func (emptySource) RemoveCache(inventory.CacheKey) error             { return nil }
func (emptySource) CacheSizeBytes(inventory.CacheKey) (int64, error) { return 0, nil }

// TestRefreshIndexes_HTMXReloadsThePage: the inventories page's Refresh
// button (an htmx request) gets HX-Refresh so the page reloads with the
// new listing; API callers get only the JSON answer.
func TestRefreshIndexes_HTMXReloadsThePage(t *testing.T) {
	mgr := inventory.NewCatalog(nil)
	t.Cleanup(func() { _ = mgr.Close() })
	s := &Server{discovery: inventory.NewDiscovery(mgr, inventory.WithBackend(emptySource{}, emptySource{}))}
	for _, htmx := range []bool{false, true} {
		req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "http://localhost/api/indexes/refresh", http.NoBody)
		if htmx {
			req.Header.Set("HX-Request", "true")
		}
		w := httptest.NewRecorder()
		s.refreshIndexes(w, req)
		if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"auto_load":false`) {
			t.Fatalf("htmx=%v: %d %s", htmx, w.Code, w.Body.String())
		}
		if got, want := w.Header().Get("HX-Refresh") == "true", htmx; got != want {
			t.Errorf("htmx=%v: HX-Refresh set %v, want %v", htmx, got, want)
		}
	}
}
