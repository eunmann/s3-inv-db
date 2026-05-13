package handlers

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/internal/s3disco"
	"github.com/eunmann/s3-inv-db/internal/templates"
	"github.com/eunmann/s3-inv-db/pkg/pricing"
	"github.com/go-chi/chi/v5"
)

// fakeDiscoverer / fakeBuilder are minimal stubs implementing the
// inventory.Discoverer and inventory.IndexBuilder interfaces.
type fakeDiscoverer struct {
	listResp []s3disco.Inventory
	listErr  error
	findResp s3disco.Inventory
	findErr  error
	bucket   string
}

func (f *fakeDiscoverer) List(context.Context) ([]s3disco.Inventory, error) {
	return f.listResp, f.listErr
}
func (f *fakeDiscoverer) Find(_ context.Context, _, _ string) (s3disco.Inventory, error) {
	return f.findResp, f.findErr
}
func (f *fakeDiscoverer) Bucket() string { return f.bucket }

type fakeBuilder struct {
	buildResp string
	buildErr  error
	evictErr  error
	evicted   []string
}

func (f *fakeBuilder) Build(_ context.Context, _, _, _ string) (string, error) {
	return f.buildResp, f.buildErr
}
func (f *fakeBuilder) Evict(src, id string) error {
	f.evicted = append(f.evicted, src+"/"+id)
	return f.evictErr
}

func newDiscoveredHandlers(t *testing.T, disc inventory.Discoverer, ldr inventory.IndexBuilder) *Handlers {
	t.Helper()
	mgr := inventory.NewManager()
	t.Cleanup(func() { _ = mgr.Close() })
	renderer, err := templates.New(false)
	if err != nil {
		t.Fatalf("renderer: %v", err)
	}
	return NewWithConfig(Config{
		Manager:    mgr,
		Renderer:   renderer,
		PriceTable: pricing.DefaultUSEast1Prices(),
		Discoverer: disc,
		Loader:     ldr,
	})
}

func chiCtxWithParams(r *http.Request, pairs ...string) *http.Request {
	rctx := chi.NewRouteContext()
	for i := 0; i+1 < len(pairs); i += 2 {
		rctx.URLParams.Add(pairs[i], pairs[i+1])
	}
	return r.WithContext(context.WithValue(r.Context(), chi.RouteCtxKey, rctx))
}

func TestLoadDiscoveredRowPartial_FindError(t *testing.T) {
	h := newDiscoveredHandlers(t,
		&fakeDiscoverer{findErr: errors.New("s3: throttled")},
		&fakeBuilder{},
	)
	req := httptest.NewRequest(http.MethodPost, "/partials/discovered/b/i/load", http.NoBody)
	req = chiCtxWithParams(req, "src", "b", "id", "i")
	w := httptest.NewRecorder()
	h.LoadDiscoveredRowPartial(w, req)
	if w.Code != http.StatusBadGateway {
		t.Errorf("status = %d, want 502", w.Code)
	}
}

func TestLoadDiscoveredRowPartial_NoCompletedRuns(t *testing.T) {
	// Find returns an Inventory with empty ManifestKey — semantically
	// "discovered but no run has happened yet".
	h := newDiscoveredHandlers(t,
		&fakeDiscoverer{findResp: s3disco.Inventory{SourceBucket: "b", InventoryID: "i"}},
		&fakeBuilder{},
	)
	req := httptest.NewRequest(http.MethodPost, "/partials/discovered/b/i/load", http.NoBody)
	req = chiCtxWithParams(req, "src", "b", "id", "i")
	w := httptest.NewRecorder()
	h.LoadDiscoveredRowPartial(w, req)
	if w.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", w.Code)
	}
	if !strings.Contains(w.Body.String(), "no completed runs") {
		t.Errorf("body = %q, want mention of no completed runs", w.Body.String())
	}
}

func TestLoadDiscoveredRowPartial_BuildError(t *testing.T) {
	disc := s3disco.Inventory{SourceBucket: "b", InventoryID: "i", ManifestKey: "k/manifest.json"}
	h := newDiscoveredHandlers(t,
		&fakeDiscoverer{findResp: disc, bucket: "dst"},
		&fakeBuilder{buildErr: errors.New("network broken")},
	)
	req := httptest.NewRequest(http.MethodPost, "/partials/discovered/b/i/load", http.NoBody)
	req = chiCtxWithParams(req, "src", "b", "id", "i")
	w := httptest.NewRecorder()
	h.LoadDiscoveredRowPartial(w, req)
	if w.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", w.Code)
	}
}

func TestUnloadDiscoveredRowPartial_NotFound(t *testing.T) {
	h := newDiscoveredHandlers(t, &fakeDiscoverer{}, &fakeBuilder{})
	req := httptest.NewRequest(http.MethodPost, "/partials/discovered/b/i/unload", http.NoBody)
	req = chiCtxWithParams(req, "src", "b", "id", "i")
	w := httptest.NewRecorder()
	h.UnloadDiscoveredRowPartial(w, req)
	if w.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404 for missing inventory", w.Code)
	}
}

func TestEvictDiscoveredRowPartial_TolerantOfMissing(t *testing.T) {
	bld := &fakeBuilder{}
	h := newDiscoveredHandlers(t, &fakeDiscoverer{}, bld)
	req := httptest.NewRequest(http.MethodDelete, "/partials/discovered/b/i", http.NoBody)
	req = chiCtxWithParams(req, "src", "b", "id", "i")
	w := httptest.NewRecorder()
	h.EvictDiscoveredRowPartial(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want 200 even when missing", w.Code)
	}
	if len(bld.evicted) != 1 || bld.evicted[0] != "b/i" {
		t.Errorf("loader.Evict called %v, want [b/i]", bld.evicted)
	}
}
