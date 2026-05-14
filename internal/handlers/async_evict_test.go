package handlers

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/eunmann/s3-inv-db/internal/jobs"
	"github.com/eunmann/s3-inv-db/internal/s3disco"
)

// TestEvictDiscoveredRowPartial_CancelsInFlightJob pins the evict-while-
// running behavior: the in-flight build's context must be cancelled
// before the cache dir is wiped, so the goroutine doesn't keep fetching
// into a freshly-deleted directory.
//
// We check the builder's cancellation signal rather than the job row,
// because Evict cascade-deletes the job row from the store before the
// goroutine can persist the final 'cancelled' state.
func TestEvictDiscoveredRowPartial_CancelsInFlightJob(t *testing.T) {
	disc := s3disco.Inventory{SourceBucket: "b", InventoryID: "i", ManifestKey: "k/manifest.json"}
	bld := &slowBuilder{delay: 5 * time.Second}
	h := newDiscoveredHandlers(t,
		&fakeDiscoverer{findResp: disc, bucket: "dst"},
		bld,
	)

	// Kick off a build.
	loadReq := httptest.NewRequest(http.MethodPost, "/partials/discovered/b/i/load", http.NoBody)
	loadReq = chiCtxWithParams(loadReq, "src", "b", "id", "i")
	h.LoadDiscoveredRowPartial(httptest.NewRecorder(), loadReq)
	waitForJobInState(t, h.jobStore, disc.CompositeID(), jobs.StateRunning)

	// Evict — should cancel the running job + complete cleanup.
	evictReq := httptest.NewRequest(http.MethodDelete, "/partials/discovered/b/i", http.NoBody)
	evictReq = chiCtxWithParams(evictReq, "src", "b", "id", "i")
	ew := httptest.NewRecorder()
	h.EvictDiscoveredRowPartial(ew, evictReq)
	if ew.Code != http.StatusOK {
		t.Fatalf("evict status = %d, want 200", ew.Code)
	}

	// The builder's ctx must have been cancelled, proving Cancel reached
	// the build goroutine.
	select {
	case <-bld.cancelled:
	case <-time.After(2 * time.Second):
		t.Fatal("builder ctx was never cancelled — Evict didn't reach the job")
	}
}

// TestLoadDiscoveredRowPartial_RejectsDoubleSubmit confirms a second
// Load while one is already running renders the row state instead of
// spawning a duplicate job that would fail with InvalidState.
func TestLoadDiscoveredRowPartial_RejectsDoubleSubmit(t *testing.T) {
	disc := s3disco.Inventory{SourceBucket: "b", InventoryID: "i", ManifestKey: "k/manifest.json"}
	h := newDiscoveredHandlers(t,
		&fakeDiscoverer{findResp: disc, bucket: "dst"},
		&slowBuilder{delay: 5 * time.Second},
	)

	req := func() *http.Request {
		r := httptest.NewRequest(http.MethodPost, "/partials/discovered/b/i/load", http.NoBody)
		return chiCtxWithParams(r, "src", "b", "id", "i")
	}

	w1 := httptest.NewRecorder()
	h.LoadDiscoveredRowPartial(w1, req())
	if w1.Code != http.StatusAccepted {
		t.Fatalf("first load status = %d, want 202", w1.Code)
	}
	waitForJobInState(t, h.jobStore, disc.CompositeID(), jobs.StateRunning)

	// Second Load while the first is still running.
	w2 := httptest.NewRecorder()
	h.LoadDiscoveredRowPartial(w2, req())
	// 200 — handler short-circuits and re-renders the row with the
	// existing job's state. No 202 because no new job was created.
	if w2.Code != http.StatusOK {
		t.Errorf("second load status = %d, want 200 (no new job)", w2.Code)
	}

	// Verify only one job exists for this inventory.
	all, err := h.jobStore.ListForInventory(disc.CompositeID())
	if err != nil {
		t.Fatalf("ListForInventory: %v", err)
	}
	if len(all) != 1 {
		t.Errorf("job count = %d, want 1 (double-submit rejected)", len(all))
	}

	// Clean up — cancel so the test doesn't leak goroutines.
	_ = h.jobMgr.Cancel(all[0].ID)
}

// TestJobManager_Shutdown verifies in-flight jobs are cancelled and
// their goroutines fully wind down on Shutdown.
func TestJobManager_Shutdown(t *testing.T) {
	disc := s3disco.Inventory{SourceBucket: "b", InventoryID: "i", ManifestKey: "k/manifest.json"}
	h := newDiscoveredHandlers(t,
		&fakeDiscoverer{findResp: disc, bucket: "dst"},
		&slowBuilder{delay: 5 * time.Second},
	)
	loadReq := httptest.NewRequest(http.MethodPost, "/partials/discovered/b/i/load", http.NoBody)
	loadReq = chiCtxWithParams(loadReq, "src", "b", "id", "i")
	h.LoadDiscoveredRowPartial(httptest.NewRecorder(), loadReq)
	waitForJobInState(t, h.jobStore, disc.CompositeID(), jobs.StateRunning)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := h.jobMgr.Shutdown(ctx); err != nil {
		t.Errorf("Shutdown: %v", err)
	}

	final := waitForJobInState(t, h.jobStore, disc.CompositeID(), jobs.StateCancelled)
	if final.FinishedAt.IsZero() {
		t.Errorf("shutdown didn't set FinishedAt: %+v", final)
	}
}
