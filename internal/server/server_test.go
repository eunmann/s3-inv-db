package server

import (
	"net/http"
	"net/http/httptest"
	"testing"

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
