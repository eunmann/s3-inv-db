package handlers

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/eunmann/s3-inv-db/internal/jobs"
	"github.com/eunmann/s3-inv-db/internal/s3disco"
)

// slowBuilder simulates a build that takes time and respects ctx
// cancellation. Used so the test can hit Cancel mid-flight.
type slowBuilder struct {
	delay     time.Duration
	cancelled chan struct{}
	once      sync.Once
}

func (b *slowBuilder) Build(ctx context.Context, _, _, _ string) (string, error) {
	return b.BuildWith(ctx, "", "", "", nil)
}

func (b *slowBuilder) BuildWith(ctx context.Context, _, _, _ string, _ func(string, int64, int64)) (string, error) {
	b.once.Do(func() { b.cancelled = make(chan struct{}) })
	select {
	case <-time.After(b.delay):
		return "", errors.New("not actually building anything")
	case <-ctx.Done():
		close(b.cancelled)
		return "", fmt.Errorf("slowBuilder ctx: %w", ctx.Err())
	}
}

func (*slowBuilder) Evict(string, string) error { return nil }

// TestAsyncLifecycle_LoadSucceeds covers the happy-path flow: submit
// returns 202 + queued, job moves through running, ends in failed (our
// fake builder always fails after the delay).
func TestAsyncLifecycle_LoadSucceeds(t *testing.T) {
	disc := s3disco.Inventory{SourceBucket: "b", InventoryID: "i", ManifestKey: "k/manifest.json"}
	h := newDiscoveredHandlers(t,
		&fakeDiscoverer{findResp: disc, bucket: "dst"},
		&slowBuilder{delay: 20 * time.Millisecond},
	)

	req := httptest.NewRequest(http.MethodPost, "/partials/discovered/b/i/load", http.NoBody)
	req = chiCtxWithParams(req, "src", "b", "id", "i")
	w := httptest.NewRecorder()
	h.LoadDiscoveredRowPartial(w, req)
	if w.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", w.Code)
	}

	final := waitForJobInState(t, h.jobStore, disc.CompositeID(), jobs.StateFailed)
	if final.FinishedAt.IsZero() {
		t.Errorf("failed job missing FinishedAt: %+v", final)
	}
}

// TestAsyncLifecycle_Cancel cancels mid-build and verifies the job
// reaches the cancelled state.
func TestAsyncLifecycle_Cancel(t *testing.T) {
	disc := s3disco.Inventory{SourceBucket: "b", InventoryID: "i", ManifestKey: "k/manifest.json"}
	h := newDiscoveredHandlers(t,
		&fakeDiscoverer{findResp: disc, bucket: "dst"},
		&slowBuilder{delay: 5 * time.Second}, // long enough that Cancel arrives mid-build
	)

	req := httptest.NewRequest(http.MethodPost, "/partials/discovered/b/i/load", http.NoBody)
	req = chiCtxWithParams(req, "src", "b", "id", "i")
	w := httptest.NewRecorder()
	h.LoadDiscoveredRowPartial(w, req)
	if w.Code != http.StatusAccepted {
		t.Fatalf("Load status = %d, want 202", w.Code)
	}

	// Wait for the job to leave queued (running state) before cancelling.
	running := waitForJobInState(t, h.jobStore, disc.CompositeID(), jobs.StateRunning)

	cancelReq := httptest.NewRequest(http.MethodPost, "/api/jobs/"+running.ID+"/cancel", http.NoBody)
	cancelReq = chiCtxWithParams(cancelReq, "id", running.ID)
	cw := httptest.NewRecorder()
	h.CancelJob(cw, cancelReq)
	if cw.Code != http.StatusAccepted {
		t.Fatalf("Cancel status = %d, want 202", cw.Code)
	}

	final := waitForJobInState(t, h.jobStore, disc.CompositeID(), jobs.StateCancelled)
	if final.FinishedAt.IsZero() {
		t.Errorf("cancelled job missing FinishedAt: %+v", final)
	}
}

// TestAsyncLifecycle_RowReflectsJobState renders the row after the job
// is in flight and verifies the spinner + Cancel button surface, then
// after failure verifies the Retry button surfaces. Pins the UI
// contract independent of htmx-sse (which needs a browser).
func TestAsyncLifecycle_RowReflectsJobState(t *testing.T) {
	disc := s3disco.Inventory{SourceBucket: "b", InventoryID: "i", ManifestKey: "k/manifest.json"}
	h := newDiscoveredHandlers(t,
		&fakeDiscoverer{findResp: disc, bucket: "dst"},
		&slowBuilder{delay: 5 * time.Second},
	)

	loadReq := httptest.NewRequest(http.MethodPost, "/partials/discovered/b/i/load", http.NoBody)
	loadReq = chiCtxWithParams(loadReq, "src", "b", "id", "i")
	lw := httptest.NewRecorder()
	h.LoadDiscoveredRowPartial(lw, loadReq)

	// Wait for running so the row should render with Cancel.
	running := waitForJobInState(t, h.jobStore, disc.CompositeID(), jobs.StateRunning)

	rowReq := httptest.NewRequest(http.MethodGet, "/partials/discovered/b/i", http.NoBody)
	rowReq = chiCtxWithParams(rowReq, "src", "b", "id", "i")
	rw := httptest.NewRecorder()
	h.DiscoveredRowPartial(rw, rowReq)
	if rw.Code != http.StatusOK {
		t.Fatalf("row status = %d, want 200", rw.Code)
	}
	body := rw.Body.String()
	if !strings.Contains(body, "Cancel") || !strings.Contains(body, "animate-spin") {
		t.Errorf("running row missing Cancel button or spinner:\n%s", body)
	}

	// Cancel and re-render — expect Retry to appear.
	cancelReq := httptest.NewRequest(http.MethodPost, "/api/jobs/"+running.ID+"/cancel", http.NoBody)
	cancelReq = chiCtxWithParams(cancelReq, "id", running.ID)
	h.CancelJob(httptest.NewRecorder(), cancelReq)
	waitForJobInState(t, h.jobStore, disc.CompositeID(), jobs.StateCancelled)

	rw2 := httptest.NewRecorder()
	h.DiscoveredRowPartial(rw2, rowReq)
	if !strings.Contains(rw2.Body.String(), "Retry") {
		t.Errorf("cancelled row missing Retry button:\n%s", rw2.Body.String())
	}
}
