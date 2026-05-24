package handlers_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/eunmann/s3-inv-db/internal/handlers"
	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/internal/jobs"
)

func newJobsHandlers(t *testing.T) (*handlers.Handlers, *jobs.Scheduler) {
	t.Helper()
	invMgr := inventory.NewCatalog(nil)
	t.Cleanup(func() { _ = invMgr.Close() })
	h := newWiredHandlers(t, invMgr)
	if err := invMgr.Register(t.Context(), "src/inv1", "n", "p"); err != nil {
		t.Fatal(err)
	}

	return h, h.JobManagerForTest()
}

// TestJobsStream_PushesEvents subscribes via the SSE handler, submits
// a job, and verifies the stream contains the job's ID and a state
// transition. Asserts contract: every Manager transition fires a frame.
func TestJobsStream_PushesEvents(t *testing.T) {
	h, mgr := newJobsHandlers(t)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	req := httptest.NewRequest(http.MethodGet, "/api/jobs/stream", http.NoBody).WithContext(ctx)
	w := httptest.NewRecorder()

	streamDone := make(chan struct{})
	go func() {
		h.JobsStream(w, req)
		close(streamDone)
	}()

	// Tiny sleep to let the handler register its subscription before
	// Submit fires events the test would otherwise miss.
	time.Sleep(20 * time.Millisecond)

	work := make(chan struct{})
	job, err := mgr.Submit(t.Context(), "src/inv1", jobs.KindBuild, func(_ context.Context, _ func(jobs.Update)) error {
		<-work

		return nil
	})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	close(work)

	// Give the stream a moment to receive and write events, then end it.
	time.Sleep(100 * time.Millisecond)
	cancel()
	<-streamDone

	body := w.Body.String()
	if !strings.Contains(body, "event: job-"+string(job.ID)) {
		t.Errorf("stream missing event for job %s\nbody:\n%s", job.ID, body)
	}
	if !strings.Contains(body, "event: row-"+string(job.InventoryID)) {
		t.Errorf("stream missing row event for inventory %s", job.InventoryID)
	}
	if !strings.Contains(body, `"State":"queued"`) && !strings.Contains(body, `"State":"running"`) {
		t.Errorf("stream missing state transitions\nbody:\n%s", body)
	}
	if !strings.Contains(body, `"State":"succeeded"`) {
		t.Errorf("stream missing final succeeded state\nbody:\n%s", body)
	}
}

// TestJobsStream_EmitsHeartbeat verifies the periodic ping that
// surfaces dead client connections quickly. Without it, idle SSE
// connections from departed browser tabs sit on the server until
// Chrome closes the TCP socket (~60s), eating per-origin HTTP/1.1
// slots and stalling other htmx requests.
func TestJobsStream_EmitsHeartbeat(t *testing.T) {
	h := newHandlersWithHeartbeat(t, 20*time.Millisecond)

	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()
	req := httptest.NewRequest(http.MethodGet, "/api/jobs/stream", http.NoBody).WithContext(ctx)
	w := httptest.NewRecorder()
	h.JobsStream(w, req)

	body := w.Body.String()
	if !strings.Contains(body, ": connected") {
		t.Error("missing initial connected comment")
	}
	if !strings.Contains(body, ": ping") {
		t.Errorf("heartbeat ping not emitted within 200ms\nbody:\n%s", body)
	}
}

// newHandlersWithHeartbeat is the same as newJobsHandlers but with a
// caller-chosen SSE heartbeat interval — exercises the configurable path.
func newHandlersWithHeartbeat(t *testing.T, hb time.Duration) *handlers.Handlers {
	t.Helper()
	invMgr := inventory.NewCatalog(nil)
	t.Cleanup(func() { _ = invMgr.Close() })
	if err := invMgr.Register(t.Context(), "src/inv1", "n", "p"); err != nil {
		t.Fatal(err)
	}

	return newWiredHandlers(t, invMgr, handlers.WithSSEHeartbeat(hb))
}

// TestJobsStream_PerIPCap guards the SSE rate limit. Each subscriber
// keeps a buffered channel in jobs.Bus alive for the lifetime of the
// connection; without a cap one client can pin unbounded memory.
func TestJobsStream_PerIPCap(t *testing.T) {
	h := newWiredHandlers(t, nil, handlers.WithSSEMaxConnsPerIP(1))

	srv := httptest.NewServer(http.HandlerFunc(h.JobsStream))
	t.Cleanup(srv.Close)

	// First connection: should succeed. Keep the body open to occupy a slot.
	//nolint:noctx // streaming connection that intentionally has no deadline
	resp1, err := http.Get(srv.URL)
	if err != nil {
		t.Fatalf("first connect: %v", err)
	}
	t.Cleanup(func() { _ = resp1.Body.Close() })
	if resp1.StatusCode != http.StatusOK {
		t.Fatalf("first connect status = %d, want 200", resp1.StatusCode)
	}
	// Wait until the first connection has actually been registered by
	// the server. Reading a byte forces a roundtrip past acquireSSESlot.
	buf := make([]byte, 1)
	if _, err := resp1.Body.Read(buf); err != nil {
		t.Fatalf("read first byte: %v", err)
	}

	// Second connection from same IP: must be refused with 429.
	//nolint:noctx // test-local HTTP client, immediately closed below
	resp2, err := http.Get(srv.URL)
	if err != nil {
		t.Fatalf("second connect: %v", err)
	}
	defer func() { _ = resp2.Body.Close() }()
	if resp2.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("second connect status = %d, want 429", resp2.StatusCode)
	}
}
