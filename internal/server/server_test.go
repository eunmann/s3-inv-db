package server

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/eunmann/s3-inv-db/pkg/pricing"
	"github.com/rs/zerolog"
)

func TestNewServer(t *testing.T) {
	cfg := Config{
		Addr:       ":8080",
		Logger:     zerolog.Nop(),
		DevMode:    false,
		PriceTable: pricing.DefaultUSEast1Prices(),
	}

	srv, err := New(cfg)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if srv.Router() == nil {
		t.Error("Router() returned nil")
	}

	if srv.Manager() == nil {
		t.Error("Manager() returned nil")
	}
}

func TestServerAPIRoutes(t *testing.T) {
	cfg := Config{
		Addr:       ":8080",
		Logger:     zerolog.Nop(),
		DevMode:    false,
		PriceTable: pricing.DefaultUSEast1Prices(),
	}

	srv, err := New(cfg)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	// Test API routes return JSON
	req := httptest.NewRequest(http.MethodGet, "/api/inventories", http.NoBody)
	w := httptest.NewRecorder()

	srv.Router().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want %d", w.Code, http.StatusOK)
	}

	contentType := w.Header().Get("Content-Type")
	if contentType != "application/json" {
		t.Errorf("Content-Type = %q, want %q", contentType, "application/json")
	}
}

// TestServerGracefulShutdown verifies Run returns nil when ctx is cancelled,
// so SIGINT/SIGTERM produce a clean exit, and that the inventory manager is
// closed (cleared) afterward.
func TestServerGracefulShutdown(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	ln.Close()

	cfg := Config{
		Addr:       addr,
		Logger:     zerolog.Nop(),
		DevMode:    false,
		PriceTable: pricing.DefaultUSEast1Prices(),
	}

	srv, err := New(cfg)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	// Register an inventory so Close has something to clear.
	if err := srv.Manager().Register("probe", "Probe", "/no/such/path"); err != nil {
		t.Fatalf("register: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() {
		runErr <- srv.Run(ctx)
	}()

	// Give the server a moment to start accepting.
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-runErr:
		if err != nil {
			t.Errorf("Run() returned %v on graceful shutdown, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run() did not return within 5s after context cancel")
	}

	if got := len(srv.Manager().List()); got != 0 {
		t.Errorf("Manager.List() length = %d after Run, want 0 (manager not closed)", got)
	}
}

// TestServerRun_ListenError verifies that when ListenAndServe fails (port
// already in use), Run returns the error and still closes the inventory
// manager on the way out.
func TestServerRun_ListenError(t *testing.T) {
	// Occupy a port so the server can't bind to it.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	cfg := Config{
		Addr:       ln.Addr().String(),
		Logger:     zerolog.Nop(),
		DevMode:    false,
		PriceTable: pricing.DefaultUSEast1Prices(),
	}

	srv, err := New(cfg)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if err := srv.Manager().Register("probe", "Probe", "/no/such/path"); err != nil {
		t.Fatalf("register: %v", err)
	}

	runErr := make(chan error, 1)
	go func() {
		runErr <- srv.Run(context.Background())
	}()

	select {
	case err := <-runErr:
		if err == nil {
			t.Error("Run() returned nil on port conflict, want non-nil error")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run() did not return within 5s on port conflict")
	}

	if got := len(srv.Manager().List()); got != 0 {
		t.Errorf("Manager.List() length = %d after ListenAndServe failure, want 0", got)
	}
}

func TestSameOriginMiddleware_RejectsCrossOriginMutation(t *testing.T) {
	cfg := Config{Addr: ":0", Logger: zerolog.Nop(), PriceTable: pricing.DefaultUSEast1Prices()}
	srv, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// POST with cross-origin Referer must be rejected.
	req := httptest.NewRequest(http.MethodDelete, "http://localhost/api/inventories/foo", http.NoBody)
	req.Host = "localhost"
	req.Header.Set("Origin", "http://attacker.example")
	w := httptest.NewRecorder()
	srv.Router().ServeHTTP(w, req)
	if w.Code != http.StatusForbidden {
		t.Errorf("cross-origin DELETE status = %d, want 403", w.Code)
	}
}

func TestSameOriginMiddleware_AllowsSameOriginMutation(t *testing.T) {
	cfg := Config{Addr: ":0", Logger: zerolog.Nop(), PriceTable: pricing.DefaultUSEast1Prices()}
	srv, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	req := httptest.NewRequest(http.MethodDelete, "http://localhost/api/inventories/foo", http.NoBody)
	req.Host = "localhost"
	req.Header.Set("Origin", "http://localhost")
	w := httptest.NewRecorder()
	srv.Router().ServeHTTP(w, req)
	// Will 404 because the inventory doesn't exist; we just care that
	// it got past the middleware (i.e., not 403).
	if w.Code == http.StatusForbidden {
		t.Errorf("same-origin DELETE was rejected as cross-origin")
	}
}

func TestSameOriginMiddleware_AllowsNoOrigin(t *testing.T) {
	cfg := Config{Addr: ":0", Logger: zerolog.Nop(), PriceTable: pricing.DefaultUSEast1Prices()}
	srv, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// No Origin and no Referer — typical curl/script request, allowed.
	req := httptest.NewRequest(http.MethodDelete, "http://localhost/api/inventories/foo", http.NoBody)
	req.Host = "localhost"
	w := httptest.NewRecorder()
	srv.Router().ServeHTTP(w, req)
	if w.Code == http.StatusForbidden {
		t.Errorf("origin-less DELETE was rejected")
	}
}

func TestSameOriginMiddleware_DoesNotBlockReads(t *testing.T) {
	cfg := Config{Addr: ":0", Logger: zerolog.Nop(), PriceTable: pricing.DefaultUSEast1Prices()}
	srv, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "http://localhost/api/inventories", http.NoBody)
	req.Host = "localhost"
	req.Header.Set("Origin", "http://attacker.example")
	w := httptest.NewRecorder()
	srv.Router().ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("cross-origin GET status = %d, want 200 (reads are public)", w.Code)
	}
}
