package handlers

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/internal/jobs"
	"github.com/eunmann/s3-inv-db/internal/migrate"
	"github.com/eunmann/s3-inv-db/internal/templates"
	"github.com/eunmann/s3-inv-db/pkg/pricing"
	"github.com/go-chi/chi/v5"
	_ "modernc.org/sqlite"
)

type fakeDiscoverer struct {
	listResp []inventory.Inventory
	listErr  error
	findResp inventory.Inventory
	findErr  error
	bucket   string
}

func (f *fakeDiscoverer) List(context.Context) ([]inventory.Inventory, error) {
	return f.listResp, f.listErr
}

func (f *fakeDiscoverer) Find(_ context.Context, _, _, _ string) (inventory.Inventory, error) {
	return f.findResp, f.findErr
}
func (f *fakeDiscoverer) Bucket() string { return f.bucket }

type fakeBuilder struct {
	buildResp string
	buildErr  error
}

func (f *fakeBuilder) Build(_ context.Context, _, _, _, _ string) (string, error) {
	return f.buildResp, f.buildErr
}

func (f *fakeBuilder) BuildWith(_ context.Context, _, _, _, _ string, _ func(string, int64, int64)) (string, error) {
	return f.buildResp, f.buildErr
}

func newDiscoveredHandlers(t *testing.T, disc inventory.Discoverer, ldr inventory.IndexBuilder) *Handlers {
	t.Helper()
	mgr := inventory.NewManager()
	t.Cleanup(func() { _ = mgr.Close() })
	renderer, err := templates.New()
	if err != nil {
		t.Fatalf("renderer: %v", err)
	}

	db, err := sql.Open("sqlite", "file::memory:?cache=shared&_pragma=foreign_keys(1)")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := migrate.Apply(db); err != nil {
		t.Fatalf("migrate.Apply: %v", err)
	}
	invStore, err := inventory.NewStore(db)
	if err != nil {
		t.Fatalf("inventory.NewStore: %v", err)
	}
	mgr.SetStore(invStore)
	jobStore, err := jobs.NewStore(db)
	if err != nil {
		t.Fatalf("jobs.NewStore: %v", err)
	}
	bus := jobs.NewBus(8)
	jobMgr := jobs.NewManager(jobStore, bus)

	return NewWithConfig(Config{
		Manager:    mgr,
		Renderer:   renderer,
		PriceTable: pricing.DefaultUSEast1Prices(),
		Discoverer: disc,
		Loader:     ldr,
		JobMgr:     jobMgr,
		JobStore:   jobStore,
		JobBus:     bus,
	})
}

func waitForJobInState(t *testing.T, store *jobs.Store, invID inventory.ID, state jobs.State) jobs.Job {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		js, err := store.ListForInventory(invID)
		if err == nil {
			for i := range js {
				if js[i].State == state {
					return js[i]
				}
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("no job for %s reached state %s within deadline", invID, state)

	return jobs.Job{}
}

func chiCtxWithParams(r *http.Request, pairs ...string) *http.Request {
	rctx := chi.NewRouteContext()
	for i := 0; i+1 < len(pairs); i += 2 {
		rctx.URLParams.Add(pairs[i], pairs[i+1])
	}

	return r.WithContext(context.WithValue(r.Context(), chi.RouteCtxKey, rctx))
}

func (f *fakeBuilder) RemoveCache(_, _, _ string) error             { return nil }
func (f *fakeBuilder) CacheSizeBytes(_, _, _ string) (int64, error) { return 0, nil }

func TestLoadDiscoveredRowPartial_FindError(t *testing.T) {
	h := newDiscoveredHandlers(t,
		&fakeDiscoverer{findErr: errors.New("s3: throttled")},
		&fakeBuilder{},
	)
	req := httptest.NewRequest(http.MethodPost, "/partials/discovered/b/i/load", http.NoBody)
	req = chiCtxWithParams(req, "src", "b", "id", "i", "run", "2026-05-13T03-00Z")
	w := httptest.NewRecorder()
	h.LoadDiscoveredRowPartial(w, req)
	if w.Code != http.StatusBadGateway {
		t.Errorf("status = %d, want 502", w.Code)
	}
}

func TestLoadDiscoveredRowPartial_NoCompletedRuns(t *testing.T) {
	h := newDiscoveredHandlers(t,
		&fakeDiscoverer{findResp: inventory.Inventory{SourceBucket: "b", InventoryName: "i"}},
		&fakeBuilder{},
	)
	req := httptest.NewRequest(http.MethodPost, "/partials/discovered/b/i/load", http.NoBody)
	req = chiCtxWithParams(req, "src", "b", "id", "i", "run", "2026-05-13T03-00Z")
	w := httptest.NewRecorder()
	h.LoadDiscoveredRowPartial(w, req)
	if w.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", w.Code)
	}
	if !strings.Contains(w.Body.String(), "no completed run") {
		t.Errorf("body = %q, want mention of no completed run", w.Body.String())
	}
}

func TestLoadDiscoveredRowPartial_AcceptsBuildError(t *testing.T) {
	disc := inventory.Inventory{SourceBucket: "b", InventoryName: "i", Run: "2026-05-13T03-00Z", ManifestKey: "k/2026-05-13T03-00Z/manifest.json"}
	h := newDiscoveredHandlers(t,
		&fakeDiscoverer{findResp: disc, bucket: "dst"},
		&fakeBuilder{buildErr: errors.New("network broken")},
	)
	req := httptest.NewRequest(http.MethodPost, "/partials/discovered/b/i/load", http.NoBody)
	req = chiCtxWithParams(req, "src", "b", "id", "i", "run", "2026-05-13T03-00Z")
	w := httptest.NewRecorder()
	h.LoadDiscoveredRowPartial(w, req)
	if w.Code != http.StatusAccepted {
		t.Errorf("status = %d, want 202", w.Code)
	}

	final := waitForJobInState(t, h.jobStore, disc.CompositeID(), jobs.StateFailed)
	if !strings.Contains(final.Error, "network broken") {
		t.Errorf("job error = %q, want to contain 'network broken'", final.Error)
	}
}

func TestInventoriesPage_PlaceholderRowOmitsHTMXRefresh(t *testing.T) {
	// Placeholder rows (Run == "") must not emit hx-get / hx-trigger with
	// empty URL segments — those produce /partials/discovered/b/i// which
	// 404s, and SSE topics like "row-b/i/" never fire.
	h := newDiscoveredHandlers(t,
		&fakeDiscoverer{listResp: []inventory.Inventory{{SourceBucket: "b", InventoryName: "i"}}, bucket: "dst"},
		&fakeBuilder{},
	)
	req := httptest.NewRequest(http.MethodGet, "/inventories", http.NoBody)
	w := httptest.NewRecorder()
	h.InventoriesPage(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", w.Code, w.Body.String())
	}
	body := w.Body.String()
	for _, bad := range []string{
		`/partials/discovered/b/i/"`,
		`/partials/discovered/b/i/ `,
		`row-b/i/"`,
	} {
		if strings.Contains(body, bad) {
			t.Errorf("placeholder row emitted broken URL fragment %q\nbody: %s", bad, body)
		}
	}
	if !strings.Contains(body, "no run") {
		t.Errorf("placeholder row missing 'no run' label; body: %s", body)
	}
}

func TestUnloadDiscoveredRowPartial_NotFound(t *testing.T) {
	h := newDiscoveredHandlers(t, &fakeDiscoverer{}, &fakeBuilder{})
	req := httptest.NewRequest(http.MethodPost, "/partials/discovered/b/i/unload", http.NoBody)
	req = chiCtxWithParams(req, "src", "b", "id", "i", "run", "2026-05-13T03-00Z")
	w := httptest.NewRecorder()
	h.UnloadDiscoveredRowPartial(w, req)
	if w.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404 for missing inventory", w.Code)
	}
}

// PinDiscoveredRowPartial accepts a 3-segment composite ID via chi URL
// params (src/id/run) so the template-generated URL maps to a real
// route. The /api/inventories/{id}/pin route uses a single-segment {id}
// and would silently 404 for a multi-segment composite ID, so the
// partial route is the one HTMX templates must call.
func TestPinDiscoveredRowPartial_NotFound(t *testing.T) {
	h := newDiscoveredHandlers(t, &fakeDiscoverer{}, &fakeBuilder{})
	req := httptest.NewRequest(http.MethodPost,
		"/partials/discovered/b/i/2026-05-13T03-00Z/pin",
		strings.NewReader("pinned=true"))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req = chiCtxWithParams(req, "src", "b", "id", "i", "run", "2026-05-13T03-00Z")
	w := httptest.NewRecorder()
	h.PinDiscoveredRowPartial(w, req)
	if w.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404 for missing inventory", w.Code)
	}
}

func TestPinDiscoveredRowPartial_BadForm(t *testing.T) {
	h := newDiscoveredHandlers(t, &fakeDiscoverer{}, &fakeBuilder{})
	// Send a malformed form body (invalid url-encoded sequence).
	req := httptest.NewRequest(http.MethodPost,
		"/partials/discovered/b/i/run/pin",
		strings.NewReader("%ZZ"))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req = chiCtxWithParams(req, "src", "b", "id", "i", "run", "run")
	w := httptest.NewRecorder()
	h.PinDiscoveredRowPartial(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400 for malformed form", w.Code)
	}
}

func TestDiscoveryEnabled_ReflectsWiring(t *testing.T) {
	bare := newTestHandlers(t)
	if bare.DiscoveryEnabled() {
		t.Error("DiscoveryEnabled() = true on bare handler, want false")
	}
	wired := newDiscoveredHandlers(t, &fakeDiscoverer{}, &fakeBuilder{})
	if !wired.DiscoveryEnabled() {
		t.Error("DiscoveryEnabled() = false with discoverer+builder wired, want true")
	}
}

func TestListDiscoveredAPI_DisabledReturns503(t *testing.T) {
	h := newTestHandlers(t)
	req := httptest.NewRequest(http.MethodGet, "/api/discovered", http.NoBody)
	w := httptest.NewRecorder()
	h.ListDiscoveredAPI(w, req)
	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", w.Code)
	}
	if !strings.Contains(w.Body.String(), "discovery not configured") {
		t.Errorf("body missing reason: %s", w.Body.String())
	}
}

func TestListDiscoveredAPI_DiscovererErrorReturns502(t *testing.T) {
	h := newDiscoveredHandlers(t,
		&fakeDiscoverer{listErr: errors.New("s3: throttled")},
		&fakeBuilder{},
	)
	req := httptest.NewRequest(http.MethodGet, "/api/discovered", http.NoBody)
	w := httptest.NewRecorder()
	h.ListDiscoveredAPI(w, req)
	if w.Code != http.StatusBadGateway {
		t.Errorf("status = %d, want 502", w.Code)
	}
	if strings.Contains(w.Body.String(), "throttled") {
		t.Errorf("internal error leaked to client: %s", w.Body.String())
	}
}

func TestListDiscoveredAPI_SuccessReturnsJSON(t *testing.T) {
	h := newDiscoveredHandlers(t,
		&fakeDiscoverer{listResp: []inventory.Inventory{
			{SourceBucket: "b1", InventoryName: "i1", Run: "2026-05-13T03-00Z"},
			{SourceBucket: "b1", InventoryName: "i2"},
		}},
		&fakeBuilder{},
	)
	req := httptest.NewRequest(http.MethodGet, "/api/discovered", http.NoBody)
	w := httptest.NewRecorder()
	h.ListDiscoveredAPI(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", w.Code, w.Body.String())
	}
	if ct := w.Header().Get("Content-Type"); !strings.Contains(ct, "application/json") {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}
	if !strings.Contains(w.Body.String(), `"b1"`) {
		t.Errorf("body missing expected source bucket: %s", w.Body.String())
	}
}
