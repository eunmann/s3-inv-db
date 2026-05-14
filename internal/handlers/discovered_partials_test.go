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
	"github.com/eunmann/s3-inv-db/internal/s3disco"
	"github.com/eunmann/s3-inv-db/internal/templates"
	"github.com/eunmann/s3-inv-db/pkg/pricing"
	"github.com/go-chi/chi/v5"
	_ "modernc.org/sqlite"
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

func (f *fakeBuilder) BuildWith(_ context.Context, _, _, _ string, _ func(string)) (string, error) {
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
	renderer, err := templates.New()
	if err != nil {
		t.Fatalf("renderer: %v", err)
	}

	db, err := sql.Open("sqlite", "file::memory:?cache=shared&_pragma=foreign_keys(1)")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
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

// waitForJobState polls jobStore for any job in the given state on the
// inventory. Convenience for tests that submit a job and wait for it to
// finish before asserting.
func waitForJobInState(t *testing.T, store *jobs.Store, invID string, state jobs.State) jobs.Job {
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

func TestLoadDiscoveredRowPartial_AcceptsBuildError(t *testing.T) {
	// The handler returns 202 immediately; the build error surfaces on
	// the job, not the HTTP response. Verify the job moves to failed.
	disc := s3disco.Inventory{SourceBucket: "b", InventoryID: "i", ManifestKey: "k/manifest.json"}
	h := newDiscoveredHandlers(t,
		&fakeDiscoverer{findResp: disc, bucket: "dst"},
		&fakeBuilder{buildErr: errors.New("network broken")},
	)
	req := httptest.NewRequest(http.MethodPost, "/partials/discovered/b/i/load", http.NoBody)
	req = chiCtxWithParams(req, "src", "b", "id", "i")
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
