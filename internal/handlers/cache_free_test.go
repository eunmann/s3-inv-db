package handlers_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"testing"

	"github.com/eunmann/s3-inv-db/internal/handlers"
	"github.com/eunmann/s3-inv-db/internal/inventory"
)

// recordingCacheStore records the runs whose cache the handlers free.
type recordingCacheStore struct {
	mu      sync.Mutex
	removed []inventory.CacheKey
}

func (s *recordingCacheStore) RemoveCache(key inventory.CacheKey) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.removed = append(s.removed, key)

	return nil
}

func (*recordingCacheStore) CacheSizeBytes(inventory.CacheKey) (int64, error) { return 0, nil }

func (s *recordingCacheStore) keys() []inventory.CacheKey {
	s.mu.Lock()
	defer s.mu.Unlock()

	return slices.Clone(s.removed)
}

const (
	cacheRunSrc = "b"
	cacheRunInv = "i"
	cacheRunRun = "2026-05-13T03-00Z"
	cacheRunID  = inventory.ID(cacheRunSrc + "/" + cacheRunInv + "/" + cacheRunRun)
)

func newCacheHandlers(t *testing.T) (*handlers.Handlers, *recordingCacheStore) {
	t.Helper()
	cs := &recordingCacheStore{}
	disc := &fakeDiscoverer{bucket: "dst", findResp: inventory.Inventory{
		SourceBucket: cacheRunSrc, Name: cacheRunInv, Run: cacheRunRun,
		ManifestKey: "k/" + cacheRunRun + "/manifest.json",
	}}

	return newWiredHandlers(t, nil, handlers.WithCacheStore(cs), handlers.WithDiscoverer(disc), handlers.WithLoader(&fakeBuilder{})), cs
}

func hydrateLoaded(t *testing.T, h *handlers.Handlers, id inventory.ID) {
	t.Helper()
	if err := h.ManagerForTest().Hydrate(t.Context(), inventory.Info{
		ID: id, Name: string(id), Path: "s3://dst/manifest.json", State: inventory.StateLoaded,
	}, buildMinimalIndex(t)); err != nil {
		t.Fatalf("hydrate: %v", err)
	}
	if got, _ := h.ManagerForTest().Get(id); got.State != inventory.StateLoaded {
		t.Fatalf("hydrated state = %s (%s), want loaded", got.State, got.Error)
	}
}

// cacheOps are the unload and delete handlers that must free a run's
// cache, each with the request that reaches it for cacheRunID.
func cacheOps() []struct {
	name    string
	handler func(*handlers.Handlers) http.HandlerFunc
	method  string
	params  []string
	deletes bool
} {
	idParams := []string{"id", string(cacheRunID)}

	return []struct {
		name    string
		handler func(*handlers.Handlers) http.HandlerFunc
		method  string
		params  []string
		deletes bool
	}{
		{"UnloadInventoryAPI", func(h *handlers.Handlers) http.HandlerFunc { return h.UnloadInventoryAPI }, http.MethodPost, idParams, false},
		{"DeleteInventoryAPI", func(h *handlers.Handlers) http.HandlerFunc { return h.DeleteInventoryAPI }, http.MethodDelete, idParams, true},
		{"UnloadInventoryRowPartial", func(h *handlers.Handlers) http.HandlerFunc { return h.UnloadInventoryRowPartial }, http.MethodPost, idParams, false},
		{"DeleteInventoryRowPartial", func(h *handlers.Handlers) http.HandlerFunc { return h.DeleteInventoryRowPartial }, http.MethodDelete, idParams, true},
		{"UnloadDiscoveredRowPartial", func(h *handlers.Handlers) http.HandlerFunc { return h.UnloadDiscoveredRowPartial }, http.MethodPost, []string{"src", cacheRunSrc, "id", cacheRunInv, "run", cacheRunRun}, false},
	}
}

func serveOp(t *testing.T, fn http.HandlerFunc, method string, params []string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequestWithContext(t.Context(), method, "/", http.NoBody)
	req = chiCtxWithParams(req, params...)
	w := httptest.NewRecorder()
	fn(w, req)

	return w
}

// TestUnloadAndDelete_FreeRunCache: every unload and delete path frees
// the run's cache, so an unloaded run never holds disk outside the
// budget.
func TestUnloadAndDelete_FreeRunCache(t *testing.T) {
	want := []inventory.CacheKey{{SourceBucket: cacheRunSrc, InventoryID: cacheRunInv, Run: cacheRunRun}}
	for _, op := range cacheOps() {
		t.Run(op.name, func(t *testing.T) {
			h, cs := newCacheHandlers(t)
			hydrateLoaded(t, h, cacheRunID)

			w := serveOp(t, op.handler(h), op.method, op.params)
			if w.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200; body=%s", w.Code, w.Body.String())
			}
			if got := cs.keys(); !slices.Equal(got, want) {
				t.Errorf("RemoveCache calls = %v, want %v", got, want)
			}
		})
	}
}

// TestUnloadAndDelete_KeepLocalPathDir: an inventory registered from a
// local path has no cache; unloading or deleting it leaves its
// directory alone.
func TestUnloadAndDelete_KeepLocalPathDir(t *testing.T) {
	for _, op := range cacheOps() {
		if op.params[0] != "id" {
			continue
		}
		t.Run(op.name, func(t *testing.T) {
			h, cs := newCacheHandlers(t)
			hydrateLoaded(t, h, "local")

			w := serveOp(t, op.handler(h), op.method, []string{"id", "local"})
			if w.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200; body=%s", w.Code, w.Body.String())
			}
			if got := cs.keys(); len(got) != 0 {
				t.Errorf("RemoveCache calls = %v, want none", got)
			}
		})
	}
}

// TestDelete_RefusedWhileLoading: deleting a run whose build is in
// flight answers 409 and frees nothing; otherwise the build would
// publish an index after the cache was freed, owned by no run.
func TestDelete_RefusedWhileLoading(t *testing.T) {
	for _, op := range cacheOps() {
		if !op.deletes {
			continue
		}
		t.Run(op.name, func(t *testing.T) {
			h, cs := newCacheHandlers(t)
			mgr := h.ManagerForTest()
			if err := mgr.Register(t.Context(), cacheRunID, "n", "s3://dst/manifest.json"); err != nil {
				t.Fatalf("Register: %v", err)
			}
			started := make(chan struct{})
			release := make(chan struct{})
			done := make(chan error, 1)
			go func() {
				done <- mgr.LoadWith(t.Context(), cacheRunID, func(context.Context, inventory.Info) (string, error) {
					close(started)
					<-release

					return "", errFakeNetwork
				})
			}()
			<-started

			w := serveOp(t, op.handler(h), op.method, op.params)
			close(release)
			<-done
			if w.Code != http.StatusConflict {
				t.Errorf("status = %d, want 409; body=%s", w.Code, w.Body.String())
			}
			if got := cs.keys(); len(got) != 0 {
				t.Errorf("RemoveCache calls = %v, want none", got)
			}
			if _, ok := mgr.Get(cacheRunID); !ok {
				t.Error("the loading run was removed")
			}
		})
	}
}
