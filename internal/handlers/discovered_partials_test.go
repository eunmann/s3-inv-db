package handlers_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/eunmann/s3-inv-db/internal/handlers"
	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/internal/jobs"
	"github.com/eunmann/s3-inv-db/pkg/extsort"
	"github.com/eunmann/s3-inv-db/pkg/tiers"
	"github.com/go-chi/chi/v5"
)

// Sentinel errors for fake S3 / build failures used across multiple
// table tests. The err113 linter forbids errors.New("...") at call
// sites, so the sentinels are wrapped via fmt.Errorf where the caller
// needs to add context.
var (
	errFakeS3Throttled = errors.New("s3: throttled")
	errFakeNetwork     = errors.New("network broken")
)

type fakeDiscoverer struct {
	listErr  error
	findErr  error
	findResp inventory.Inventory
	bucket   string
	listResp []inventory.Inventory
}

func (f *fakeDiscoverer) List(context.Context) ([]inventory.Inventory, error) {
	return f.listResp, f.listErr
}

func (f *fakeDiscoverer) Find(_ context.Context, _, _, _ string) (inventory.Inventory, error) {
	return f.findResp, f.findErr
}
func (f *fakeDiscoverer) Bucket() string { return f.bucket }

type fakeBuilder struct {
	buildErr  error
	buildResp string
}

func (f *fakeBuilder) BuildWith(_ context.Context, _, _, _, _ string, _ func(string, int64, int64)) (string, error) {
	return f.buildResp, f.buildErr
}

func newDiscoveredHandlers(t *testing.T, disc inventory.Discoverer, ldr inventory.IndexBuilder) *handlers.Handlers {
	t.Helper()
	mgr := inventory.NewCatalog()
	t.Cleanup(func() { _ = mgr.Close() })
	opts := []handlers.Option{handlers.WithDiscoverer(disc)}
	if ldr != nil {
		opts = append(opts, handlers.WithLoader(ldr))
	}

	return newWiredHandlers(t, mgr, opts...)
}

func waitForJobInState(t *testing.T, store *jobs.Store, invID inventory.ID, state jobs.State) jobs.Job {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		js, err := store.ListForInventory(t.Context(), invID)
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
		&fakeDiscoverer{findErr: errFakeS3Throttled},
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
		&fakeDiscoverer{findResp: inventory.Inventory{SourceBucket: "b", Name: "i"}},
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
	disc := inventory.Inventory{SourceBucket: "b", Name: "i", Run: "2026-05-13T03-00Z", ManifestKey: "k/2026-05-13T03-00Z/manifest.json"}
	h := newDiscoveredHandlers(t,
		&fakeDiscoverer{findResp: disc, bucket: "dst"},
		&fakeBuilder{buildErr: errFakeNetwork},
	)
	req := httptest.NewRequest(http.MethodPost, "/partials/discovered/b/i/load", http.NoBody)
	req = chiCtxWithParams(req, "src", "b", "id", "i", "run", "2026-05-13T03-00Z")
	w := httptest.NewRecorder()
	h.LoadDiscoveredRowPartial(w, req)
	if w.Code != http.StatusAccepted {
		t.Errorf("status = %d, want 202", w.Code)
	}

	final := waitForJobInState(t, h.JobStoreForTest(), disc.CompositeID(), jobs.StateFailed)
	if !strings.Contains(final.Error, "network broken") {
		t.Errorf("job error = %q, want to contain 'network broken'", final.Error)
	}
}

// buildMinimalIndex builds a real, loadable index in a tmpdir with a
// single object, returning the index directory. Used to drive an
// inventory into StateLoaded for the short-circuit test below.
func buildMinimalIndex(t *testing.T) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "index")

	agg := extsort.NewAggregator(1, 0)
	agg.AddObject("only/object.bin", 100, tiers.Standard)
	rows := agg.Drain()
	extsort.SortPrefixRows(rows)

	builder, err := extsort.NewIndexBuilder(dir, "")
	if err != nil {
		t.Fatalf("NewIndexBuilder: %v", err)
	}
	for _, r := range rows {
		if err := builder.Add(r); err != nil {
			t.Fatalf("builder.Add: %v", err)
		}
	}
	if err := builder.Finalize(); err != nil {
		t.Fatalf("builder.Finalize: %v", err)
	}

	return dir
}

// TestLoadDiscoveredRowPartial_AlreadyLoadedShortCircuits pins the
// defense-in-depth branch: when the live Manager already holds the run
// in StateLoaded (typical cause: the page was rendered against a stale
// discovery snapshot before my overlay fix), hitting Load must NOT
// submit a job. Submitting one would race the SSE subscriber on the
// way out and could leave the row stuck on "Loading…".
func TestLoadDiscoveredRowPartial_AlreadyLoadedShortCircuits(t *testing.T) {
	disc := inventory.Inventory{
		SourceBucket: "b", Name: "i", Run: "2026-05-13T03-00Z",
		ManifestKey: "k/2026-05-13T03-00Z/manifest.json",
	}
	h := newDiscoveredHandlers(t,
		&fakeDiscoverer{findResp: disc, bucket: "dst"},
		&fakeBuilder{},
	)

	// Hydrate the manager into a real StateLoaded by pointing it at a
	// minimal built index. Anything less goes via the indexread.Open
	// failure path and lands in StateError instead.
	indexDir := buildMinimalIndex(t)
	composite := disc.CompositeID()
	if err := h.ManagerForTest().Hydrate(t.Context(), inventory.Info{
		ID:    composite,
		Name:  "b/i @ 2026-05-13T03-00Z",
		Path:  "s3://b/k/2026-05-13T03-00Z/manifest.json",
		State: inventory.StateLoaded,
	}, indexDir); err != nil {
		t.Fatalf("hydrate: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/partials/discovered/b/i/load", http.NoBody)
	req = chiCtxWithParams(req, "src", "b", "id", "i", "run", "2026-05-13T03-00Z")
	w := httptest.NewRecorder()
	h.LoadDiscoveredRowPartial(w, req)

	// 200 (row rendered directly), not 202 (job accepted).
	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want 200 (short-circuit, no job submitted); body=%s", w.Code, w.Body.String())
	}
	// And no build job should have been created.
	_, err := h.JobStoreForTest().LatestForInventory(t.Context(), composite)
	if !errors.Is(err, jobs.ErrStoreNotFound) {
		t.Errorf("LatestForInventory err = %v, want ErrStoreNotFound (no job submitted)", err)
	}
}

func TestInventoriesPage_PlaceholderRowOmitsHTMXRefresh(t *testing.T) {
	// Placeholder rows (Run == "") must not emit hx-get / hx-trigger with
	// empty URL segments — those produce /partials/discovered/b/i// which
	// 404s, and SSE topics like "row-b/i/" never fire.
	h := newDiscoveredHandlers(t,
		&fakeDiscoverer{listResp: []inventory.Inventory{{SourceBucket: "b", Name: "i"}}, bucket: "dst"},
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
		&fakeDiscoverer{listErr: errFakeS3Throttled},
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
			{SourceBucket: "b1", Name: "i1", Run: "2026-05-13T03-00Z"},
			{SourceBucket: "b1", Name: "i2"},
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
