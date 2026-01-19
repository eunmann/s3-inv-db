package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/internal/templates"
	"github.com/eunmann/s3-inv-db/pkg/pricing"
	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog"
)

func newTestHandlers(t *testing.T) *Handlers {
	t.Helper()

	mgr := inventory.NewManager()
	renderer, err := templates.New(false)
	if err != nil {
		t.Fatalf("failed to create renderer: %v", err)
	}

	return New(mgr, renderer, pricing.DefaultUSEast1Prices(), zerolog.Nop())
}

func TestListInventoriesAPI_Empty(t *testing.T) {
	h := newTestHandlers(t)

	req := httptest.NewRequest(http.MethodGet, "/api/inventories", http.NoBody)
	w := httptest.NewRecorder()

	h.ListInventoriesAPI(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want %d", w.Code, http.StatusOK)
	}

	var resp APIResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	if !resp.Success {
		t.Error("success = false, want true")
	}
}

func TestRegisterInventoryAPI(t *testing.T) {
	h := newTestHandlers(t)

	body := `{"id":"test","name":"Test Inventory","path":"/path/to/index"}`
	req := httptest.NewRequest(http.MethodPost, "/api/inventories", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	h.RegisterInventoryAPI(w, req)

	if w.Code != http.StatusCreated {
		t.Errorf("status = %d, want %d", w.Code, http.StatusCreated)
	}

	var resp APIResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	if !resp.Success {
		t.Errorf("success = false, want true; error = %s", resp.Error)
	}
}

func TestRegisterInventoryAPI_MissingFields(t *testing.T) {
	h := newTestHandlers(t)

	tests := []struct {
		name string
		body string
	}{
		{"missing id", `{"name":"Test","path":"/path"}`},
		{"missing name", `{"id":"test","path":"/path"}`},
		{"missing path", `{"id":"test","name":"Test"}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/api/inventories", strings.NewReader(tt.body))
			req.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()

			h.RegisterInventoryAPI(w, req)

			if w.Code != http.StatusBadRequest {
				t.Errorf("status = %d, want %d", w.Code, http.StatusBadRequest)
			}
		})
	}
}

func TestRegisterInventoryAPI_Duplicate(t *testing.T) {
	h := newTestHandlers(t)

	body := `{"id":"test","name":"Test Inventory","path":"/path/to/index"}`

	// First registration
	req := httptest.NewRequest(http.MethodPost, "/api/inventories", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.RegisterInventoryAPI(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("first registration status = %d, want %d", w.Code, http.StatusCreated)
	}

	// Second registration (duplicate)
	req = httptest.NewRequest(http.MethodPost, "/api/inventories", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	h.RegisterInventoryAPI(w, req)

	if w.Code != http.StatusConflict {
		t.Errorf("second registration status = %d, want %d", w.Code, http.StatusConflict)
	}
}

func TestGetInventoryAPI_NotFound(t *testing.T) {
	h := newTestHandlers(t)

	req := httptest.NewRequest(http.MethodGet, "/api/inventories/nonexistent", http.NoBody)
	w := httptest.NewRecorder()

	// Set up chi context with URL param
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("id", "nonexistent")
	ctx := context.WithValue(req.Context(), chi.RouteCtxKey, rctx)
	req = req.WithContext(ctx)

	h.GetInventoryAPI(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("status = %d, want %d", w.Code, http.StatusNotFound)
	}
}

func TestWantsJSON(t *testing.T) {
	tests := []struct {
		name   string
		accept string
		query  string
		want   bool
	}{
		{"accept json", "application/json", "", true},
		{"accept json with params", "application/json; charset=utf-8", "", true},
		{"format query param", "", "format=json", true},
		{"accept html", "text/html", "", false},
		{"no header", "", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			url := "/test"
			if tt.query != "" {
				url += "?" + tt.query
			}
			req := httptest.NewRequest(http.MethodGet, url, http.NoBody)
			if tt.accept != "" {
				req.Header.Set("Accept", tt.accept)
			}

			got := WantsJSON(req)
			if got != tt.want {
				t.Errorf("WantsJSON() = %v, want %v", got, tt.want)
			}
		})
	}
}
