package memdiag_test

import (
	"context"
	"errors"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/eunmann/s3-inv-db/internal/memdiag"
)

// TestTracker_StopShutsDownPprofServer guards that Tracker.Stop drains
// the pprof server bound during Start. The previous code started a
// goroutine running ListenAndServe but never called Shutdown, so the
// port stayed bound and the goroutine leaked until process exit.
func TestTracker_StopShutsDownPprofServer(t *testing.T) {
	// Bind to a free OS-picked port so parallel test runs can't collide
	// and we don't depend on :6060 being free in CI.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("pick port: %v", err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatalf("free probe port: %v", err)
	}

	tr := memdiag.NewTracker(memdiag.Config{
		Enabled:      true,
		PprofEnabled: true,
		PprofAddr:    addr,
		LogInterval:  time.Hour, // long enough that the log loop never fires
	})
	tr.Start()

	// Wait until the pprof server is actually listening.
	deadline := time.Now().Add(2 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err == nil {
			_ = c.Close()
			lastErr = nil

			break
		}
		lastErr = err
		time.Sleep(20 * time.Millisecond)
	}
	if lastErr != nil {
		t.Fatalf("pprof server never accepted connections: %v", lastErr)
	}

	tr.Stop()

	// After Stop the port must be free — Shutdown returned, so a new
	// listener should bind without EADDRINUSE.
	ln2, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("port still held after Stop (pprof Shutdown not wired): %v", err)
	}
	_ = ln2.Close()

	// Second Stop must be a no-op (the original code would have panicked
	// on close-of-closed-channel).
	tr.Stop()
}

// TestTracker_StopWithoutStartIsNoop verifies the early-return guard.
func TestTracker_StopWithoutStartIsNoop(t *testing.T) {
	tr := memdiag.NewTracker(memdiag.Config{Enabled: false})
	tr.Stop()
	tr.Stop() // must not panic
}

// TestTracker_PprofServesDuringRun confirms the registered handlers
// actually answer requests — guards against accidentally returning a
// new mux that has no handlers wired in.
func TestTracker_PprofServesDuringRun(t *testing.T) {
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	addr := ln.Addr().String()
	_ = ln.Close()

	tr := memdiag.NewTracker(memdiag.Config{
		Enabled:      true,
		PprofEnabled: true,
		PprofAddr:    addr,
		LogInterval:  time.Hour,
	})
	tr.Start()
	t.Cleanup(tr.Stop)

	// Poll for the server to come up.
	deadline := time.Now().Add(2 * time.Second)
	var resp *http.Response
	var lastErr error
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+"/debug/pprof/", http.NoBody)
		r, err := http.DefaultClient.Do(req)
		cancel()
		if err == nil {
			resp = r
			break
		}
		lastErr = err
		if !errors.Is(err, context.DeadlineExceeded) {
			time.Sleep(20 * time.Millisecond)
		}
	}
	if resp == nil {
		t.Fatalf("pprof index never responded: %v", lastErr)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("pprof index status = %d, want 200", resp.StatusCode)
	}
}
