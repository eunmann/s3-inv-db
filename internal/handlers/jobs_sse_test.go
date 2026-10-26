package handlers_test

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/eunmann/s3-inv-db/internal/handlers"
	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/internal/jobs"
	"github.com/go-chi/chi/v5/middleware"
)

func newJobsHandlers(t *testing.T) (*handlers.Handlers, *jobs.Scheduler) {
	t.Helper()
	invMgr := inventory.NewCatalog(nil)
	t.Cleanup(func() { _ = invMgr.Close() })
	h := newWiredHandlers(t, invMgr)
	if err := invMgr.Register(t.Context(), "src/inv1", "n", "p"); err != nil {
		t.Fatal(err)
	}
	jobMgr := h.JobManagerForTest()

	return h, jobMgr
}

// sseFrame is one parsed server-sent event.
type sseFrame struct {
	event string
	data  string
}

// openStream connects to the handler's stream through a real server and
// returns a channel of parsed frames, closed when the stream ends.
func openStream(t *testing.T, h *handlers.Handlers) <-chan sseFrame {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(h.JobsStream))
	t.Cleanup(srv.Close)
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, srv.URL, http.NoBody)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	frames := make(chan sseFrame, 256)
	go func() {
		defer close(frames)
		sc := bufio.NewScanner(resp.Body)
		var cur sseFrame
		for sc.Scan() {
			line := sc.Text()
			switch {
			case line == "":
				if cur.event != "" {
					frames <- cur
				}
				cur = sseFrame{}
			case strings.HasPrefix(line, "event: "):
				cur.event = strings.TrimPrefix(line, "event: ")
			case strings.HasPrefix(line, "data: "):
				cur.data = strings.TrimPrefix(line, "data: ")
			}
		}
	}()

	return frames
}

// nextFrame returns the next frame matching want, failing after 2s.
func nextFrame(t *testing.T, frames <-chan sseFrame, want func(sseFrame) bool) sseFrame {
	t.Helper()
	timeout := time.After(2 * time.Second)
	for {
		select {
		case f, ok := <-frames:
			if !ok {
				t.Fatal("stream ended before the expected frame")
			}
			if want(f) {
				return f
			}
		case <-timeout:
			t.Fatal("expected frame never arrived")
		}
	}
}

// TestJobsStream_PushesEvents: a job submitted while a client is
// connected reaches it as "job" frames, ending with the terminal state.
func TestJobsStream_PushesEvents(t *testing.T) {
	handlers.SetSSEPaceForTest(t, 5*time.Millisecond)
	h, mgr := newJobsHandlers(t)
	frames := openStream(t, h)
	nextFrame(t, frames, func(f sseFrame) bool { return f.event == "synced" })

	work := make(chan struct{})
	job, err := mgr.Submit(t.Context(), "src/inv1", jobs.KindBuild, func(_ context.Context, report func(jobs.Update)) error {
		<-work
		for i := range 500 {
			report(jobs.Update{Stage: "downloading", StageDone: int64(i + 1), StageTotal: 500})
		}

		return nil
	})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	close(work)

	f := nextFrame(t, frames, func(f sseFrame) bool {
		return f.event == "job" && strings.Contains(f.data, `"State":"succeeded"`)
	})
	var ev struct {
		ID          string `json:"ID"`
		InventoryID string `json:"InventoryID"`
		StageDone   int64  `json:"StageDone"`
	}
	if err := json.Unmarshal([]byte(f.data), &ev); err != nil {
		t.Fatal(err)
	}
	if ev.ID != string(job.ID) || ev.InventoryID != "src/inv1" || ev.StageDone != 500 {
		t.Errorf("terminal frame = %+v, want job %s on src/inv1 at 500", ev, job.ID)
	}
}

// TestJobsStream_SnapshotOnConnect: a client that connects (or
// reconnects) while a job runs gets its current state before "synced",
// so it never waits for the next change to learn about it.
func TestJobsStream_SnapshotOnConnect(t *testing.T) {
	h, mgr := newJobsHandlers(t)
	running := make(chan struct{})
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	job, err := mgr.Submit(t.Context(), "src/inv1", jobs.KindBuild, func(_ context.Context, report func(jobs.Update)) error {
		report(jobs.Update{Stage: "building", StageDone: 3, StageTotal: 9})
		close(running)
		<-release

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	<-running

	frames := openStream(t, h)
	first := nextFrame(t, frames, func(sseFrame) bool { return true })
	if first.event != "job" || !strings.Contains(first.data, string(job.ID)) || !strings.Contains(first.data, `"StageDone":3`) {
		t.Fatalf("first frame = %+v, want the running job's snapshot", first)
	}
	if f := nextFrame(t, frames, func(sseFrame) bool { return true }); f.event != "synced" {
		t.Fatalf("second frame = %+v, want synced", f)
	}
}

// TestJobsStream_InventoryNotice: a catalog change outside any job (an
// eviction, a pin) reaches the client as an "inventory" frame.
func TestJobsStream_InventoryNotice(t *testing.T) {
	handlers.SetSSEPaceForTest(t, 5*time.Millisecond)
	h, _ := newJobsHandlers(t)
	frames := openStream(t, h)
	nextFrame(t, frames, func(f sseFrame) bool { return f.event == "synced" })

	h.JobBusForTest().PublishInventory("src/inv1")
	f := nextFrame(t, frames, func(f sseFrame) bool { return f.event == "inventory" })
	if f.data != `{"InventoryID":"src/inv1"}` {
		t.Errorf("inventory frame data = %s", f.data)
	}
}

// TestJobsStream_SlowReaderIsDisconnected: a client so far behind that
// its queue overflows is dropped, so it reconnects and resyncs from the
// snapshot rather than silently missing changes.
func TestJobsStream_SlowReaderIsDisconnected(t *testing.T) {
	handlers.SetSSEPaceForTest(t, time.Second)
	h, _ := newJobsHandlers(t)
	frames := openStream(t, h)
	nextFrame(t, frames, func(f sseFrame) bool { return f.event == "synced" })

	// One publish starts the first batch and its one-second pause; the
	// rest pile up past the bus's cap meanwhile.
	bus := h.JobBusForTest()
	bus.PublishInventory("src/first")
	nextFrame(t, frames, func(f sseFrame) bool { return f.event == "inventory" })
	for i := range 64 {
		bus.PublishInventory(inventory.ID(fmt.Sprintf("src/inv/%d", i)))
	}
	timeout := time.After(3 * time.Second)
	for {
		select {
		case f, ok := <-frames:
			if !ok {
				return
			}
			t.Fatalf("got frame %+v after overflow, want the stream closed", f)
		case <-timeout:
			t.Fatal("stream still open after its queue overflowed")
		}
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
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/jobs/stream", http.NoBody).WithContext(ctx)
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
// caller-chosen SSE heartbeat interval — exercises the heartbeat
// emission path on a short cadence so the test isn't slow.
func newHandlersWithHeartbeat(t *testing.T, hb time.Duration) *handlers.Handlers {
	t.Helper()
	handlers.SetSSEHeartbeatForTest(t, hb)
	invMgr := inventory.NewCatalog(nil)
	t.Cleanup(func() { _ = invMgr.Close() })
	if err := invMgr.Register(t.Context(), "src/inv1", "n", "p"); err != nil {
		t.Fatal(err)
	}

	return newWiredHandlers(t, invMgr)
}

// TestJobsStream_PerIPCap guards the SSE rate limit. Each subscriber
// keeps a buffered channel in jobs.Bus alive for the lifetime of the
// connection; without a cap one client can pin unbounded memory.
func TestJobsStream_PerIPCap(t *testing.T) {
	handlers.SetSSEMaxConnsPerIPForTest(t, 1)
	h := newWiredHandlers(t, nil)

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

// TestJobsStream_PerIPCapKeysOnClientIP guards that the cap counts the
// client IP the server's client-IP middleware resolved, not RemoteAddr:
// behind a trusted proxy every browser shares the proxy's RemoteAddr, so
// keying on it would put all users in one slot bucket.
func TestJobsStream_PerIPCapKeysOnClientIP(t *testing.T) {
	handlers.SetSSEMaxConnsPerIPForTest(t, 1)
	h := newWiredHandlers(t, nil)

	// The test client connects over loopback, so loopback is the trusted proxy.
	srv := httptest.NewServer(middleware.ClientIPFromXFF("127.0.0.0/8", "::1/128")(http.HandlerFunc(h.JobsStream)))
	t.Cleanup(srv.Close)

	// open connects as client xff and returns the status; a 200 stream
	// stays open, holding its slot, until the test ends.
	open := func(xff string) int {
		t.Helper()
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, srv.URL, http.NoBody)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("X-Forwarded-For", xff)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("connect as %s: %v", xff, err)
		}
		t.Cleanup(func() { _ = resp.Body.Close() })
		if resp.StatusCode == http.StatusOK {
			// Reading a byte waits until the server holds the slot.
			if _, err := resp.Body.Read(make([]byte, 1)); err != nil {
				t.Fatalf("read first byte as %s: %v", xff, err)
			}
		}

		return resp.StatusCode
	}

	if got := open("203.0.113.1"); got != http.StatusOK {
		t.Fatalf("first client status = %d, want 200", got)
	}
	if got := open("203.0.113.2"); got != http.StatusOK {
		t.Fatalf("second client behind the same proxy status = %d, want 200", got)
	}
	if got := open("203.0.113.1"); got != http.StatusTooManyRequests {
		t.Fatalf("repeat of first client status = %d, want 429", got)
	}
}
