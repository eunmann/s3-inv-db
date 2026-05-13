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
// so SIGINT/SIGTERM produce a clean exit.
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
}
