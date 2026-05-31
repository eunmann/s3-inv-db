package handlers_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/internal/jobs"
)

func TestRetryJob_MissingJobReturns404(t *testing.T) {
	h := newDiscoveredHandlers(t,
		&fakeDiscoverer{},
		&fakeBuilder{},
	)
	req := httptest.NewRequest(http.MethodPost, "/api/jobs/does-not-exist/retry", http.NoBody)
	req = chiCtxWithParams(req, "id", "does-not-exist")
	w := httptest.NewRecorder()
	h.RetryJob(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404; body=%s", w.Code, w.Body.String())
	}
}

func TestRetryJob_NonBuildKindReturns409(t *testing.T) {
	disc := inventory.Inventory{SourceBucket: "b", Name: "i", Run: "r"}
	h := newDiscoveredHandlers(t,
		&fakeDiscoverer{findResp: disc, bucket: "dst"},
		&fakeBuilder{},
	)
	composite := disc.CompositeID()
	seedInventoryRow(t, h.ManagerForTest(), composite)
	prev := jobs.Job{
		ID: "j-unload", InventoryID: composite, Kind: jobs.KindUnload,
		State: jobs.StateFailed,
	}
	if err := h.JobStoreForTest().Upsert(t.Context(), prev); err != nil {
		t.Fatalf("seed: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/api/jobs/j-unload/retry", http.NoBody)
	req = chiCtxWithParams(req, "id", "j-unload")
	w := httptest.NewRecorder()
	h.RetryJob(w, req)
	if w.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409; body=%s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "build") {
		t.Errorf("body should mention build-jobs-only: %s", w.Body.String())
	}
}

func TestRetryJob_NonTerminalReturns409(t *testing.T) {
	disc := inventory.Inventory{SourceBucket: "b", Name: "i", Run: "r"}
	h := newDiscoveredHandlers(t,
		&fakeDiscoverer{findResp: disc, bucket: "dst"},
		&fakeBuilder{},
	)
	composite := disc.CompositeID()
	seedInventoryRow(t, h.ManagerForTest(), composite)
	prev := jobs.Job{
		ID: "j-run", InventoryID: composite, Kind: jobs.KindBuild,
		State: jobs.StateRunning,
	}
	if err := h.JobStoreForTest().Upsert(t.Context(), prev); err != nil {
		t.Fatalf("seed: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/api/jobs/j-run/retry", http.NoBody)
	req = chiCtxWithParams(req, "id", "j-run")
	w := httptest.NewRecorder()
	h.RetryJob(w, req)
	if w.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409; body=%s", w.Code, w.Body.String())
	}
}

func TestRetryJob_SucceededReturns409(t *testing.T) {
	disc := inventory.Inventory{SourceBucket: "b", Name: "i", Run: "r"}
	h := newDiscoveredHandlers(t,
		&fakeDiscoverer{findResp: disc, bucket: "dst"},
		&fakeBuilder{},
	)
	composite := disc.CompositeID()
	seedInventoryRow(t, h.ManagerForTest(), composite)
	prev := jobs.Job{
		ID: "j-ok", InventoryID: composite, Kind: jobs.KindBuild,
		State: jobs.StateSucceeded,
	}
	if err := h.JobStoreForTest().Upsert(t.Context(), prev); err != nil {
		t.Fatalf("seed: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/api/jobs/j-ok/retry", http.NoBody)
	req = chiCtxWithParams(req, "id", "j-ok")
	w := httptest.NewRecorder()
	h.RetryJob(w, req)
	if w.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409; body=%s", w.Code, w.Body.String())
	}
}

func TestRetryJob_MalformedInventoryIDReturns400(t *testing.T) {
	h := newDiscoveredHandlers(t,
		&fakeDiscoverer{},
		&fakeBuilder{},
	)
	// inventoryID with only one segment cannot be split into src/inv/run.
	bad := inventory.ID("two/parts")
	seedInventoryRow(t, h.ManagerForTest(), bad)
	prev := jobs.Job{
		ID: "j-bad", InventoryID: bad, Kind: jobs.KindBuild,
		State: jobs.StateFailed,
	}
	if err := h.JobStoreForTest().Upsert(t.Context(), prev); err != nil {
		t.Fatalf("seed: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/api/jobs/j-bad/retry", http.NoBody)
	req = chiCtxWithParams(req, "id", "j-bad")
	w := httptest.NewRecorder()
	h.RetryJob(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body=%s", w.Code, w.Body.String())
	}
}

func TestRetryJob_FailedJobSubmitsFollowOn(t *testing.T) {
	disc := inventory.Inventory{
		SourceBucket: "b", Name: "i", Run: "2026-05-13T03-00Z",
		ManifestKey: "k/2026-05-13T03-00Z/manifest.json",
	}
	h := newDiscoveredHandlers(t,
		&fakeDiscoverer{findResp: disc, bucket: "dst"},
		&fakeBuilder{},
	)
	composite := disc.CompositeID()
	seedInventoryRow(t, h.ManagerForTest(), composite)
	prev := jobs.Job{
		ID: "prev-fail", InventoryID: composite, Kind: jobs.KindBuild,
		State: jobs.StateFailed, AttemptCount: 1, Error: "synthetic",
	}
	if err := h.JobStoreForTest().Upsert(t.Context(), prev); err != nil {
		t.Fatalf("seed: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/api/jobs/prev-fail/retry", http.NoBody)
	req = chiCtxWithParams(req, "id", "prev-fail")
	w := httptest.NewRecorder()
	h.RetryJob(w, req)
	if w.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202; body=%s", w.Code, w.Body.String())
	}
}
