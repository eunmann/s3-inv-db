package handlers

import (
	"context"
	"database/sql"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/internal/jobs"
	"github.com/eunmann/s3-inv-db/internal/templates"
	"github.com/eunmann/s3-inv-db/pkg/pricing"
	_ "modernc.org/sqlite"
)

func openJobsTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", "file::memory:?cache=shared&_pragma=foreign_keys(1)")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func newJobsHandlers(t *testing.T) (*Handlers, *jobs.Manager) {
	t.Helper()
	db := openJobsTestDB(t)
	invStore, err := inventory.NewStore(db)
	if err != nil {
		t.Fatal(err)
	}
	if err := invStore.Upsert(inventory.Info{ID: "src/inv1", Name: "n", Path: "p", State: inventory.StatePending}); err != nil {
		t.Fatal(err)
	}
	jobStore, err := jobs.NewStore(db)
	if err != nil {
		t.Fatal(err)
	}
	bus := jobs.NewBus(8)
	mgr := jobs.NewManager(jobStore, bus)
	renderer, err := templates.New()
	if err != nil {
		t.Fatal(err)
	}
	h := NewWithConfig(Config{
		Manager:    inventory.NewManager(),
		Renderer:   renderer,
		PriceTable: pricing.DefaultUSEast1Prices(),
		JobMgr:     mgr,
		JobStore:   jobStore,
		JobBus:     bus,
	})
	return h, mgr
}

// TestJobsStream_PushesEvents subscribes via the SSE handler, submits
// a job, and verifies the stream contains the job's ID and a state
// transition. Asserts contract: every Manager transition fires a frame.
func TestJobsStream_PushesEvents(t *testing.T) {
	h, mgr := newJobsHandlers(t)

	ctx, cancel := context.WithCancel(context.Background())
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
	job, err := mgr.Submit("src/inv1", jobs.KindBuild, func(_ context.Context, _ func(jobs.Update)) error {
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
	if !strings.Contains(body, "event: job-"+job.ID) {
		t.Errorf("stream missing event for job %s\nbody:\n%s", job.ID, body)
	}
	if !strings.Contains(body, "event: row-"+job.InventoryID) {
		t.Errorf("stream missing row event for inventory %s", job.InventoryID)
	}
	if !strings.Contains(body, `"State":"queued"`) && !strings.Contains(body, `"State":"running"`) {
		t.Errorf("stream missing state transitions\nbody:\n%s", body)
	}
	if !strings.Contains(body, `"State":"succeeded"`) {
		t.Errorf("stream missing final succeeded state\nbody:\n%s", body)
	}
}

func TestJobsStream_DisabledWhenJobBusMissing(t *testing.T) {
	renderer, err := templates.New()
	if err != nil {
		t.Fatal(err)
	}
	h := NewWithConfig(Config{
		Manager:    inventory.NewManager(),
		Renderer:   renderer,
		PriceTable: pricing.DefaultUSEast1Prices(),
	})
	req := httptest.NewRequest(http.MethodGet, "/api/jobs/stream", http.NoBody)
	w := httptest.NewRecorder()
	h.JobsStream(w, req)
	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", w.Code)
	}
}
