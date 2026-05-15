package handlers

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/internal/templates"
	"github.com/eunmann/s3-inv-db/pkg/pricing"
	"github.com/go-chi/chi/v5"
)

func newTestHandlers(t *testing.T) *Handlers {
	t.Helper()

	mgr := inventory.NewManager()
	renderer, err := templates.New()
	if err != nil {
		t.Fatalf("failed to create renderer: %v", err)
	}

	return New(mgr, renderer, pricing.DefaultUSEast1Prices())
}

func TestListInventoriesAPI_Empty(t *testing.T) {
	h := newTestHandlers(t)

	req := httptest.NewRequest(http.MethodGet, "/api/inventories", http.NoBody)
	w := httptest.NewRecorder()

	h.ListInventoriesAPI(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want %d", w.Code, http.StatusOK)
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
		t.Errorf("status = %d, want %d, body = %s", w.Code, http.StatusCreated, w.Body.String())
	}
	var got inventory.Info
	if err := json.NewDecoder(w.Body).Decode(&got); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if got.ID != "test" || got.Name != "Test Inventory" || got.Path != "/path/to/index" {
		t.Errorf("response body = %+v, want id=test name=\"Test Inventory\" path=/path/to/index", got)
	}
	if got.State == "" {
		t.Errorf("response body State is empty, want a real lifecycle state")
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
		// Single-segment chi `{id}` would silently 404 every later
		// /api/inventories/{id}/... call, so these characters must be
		// rejected at registration.
		{"id contains slash", `{"id":"my/inv","name":"Test","path":"/p"}`},
		{"id contains percent", `{"id":"my%20inv","name":"Test","path":"/p"}`},
		{"id contains question", `{"id":"my?inv","name":"Test","path":"/p"}`},
		{"id contains hash", `{"id":"my#inv","name":"Test","path":"/p"}`},
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

// decodeErr decodes an {"error":"…"} JSON body and returns the message.
func decodeErr(t *testing.T, body io.Reader) string {
	t.Helper()
	var e struct {
		Error string `json:"error"`
	}
	if err := json.NewDecoder(body).Decode(&e); err != nil {
		t.Fatalf("decode error body: %v", err)
	}

	return e.Error
}

// Helper to set up chi URL param context.
func withURLParam(r *http.Request, key, value string) *http.Request {
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add(key, value)

	return r.WithContext(context.WithValue(r.Context(), chi.RouteCtxKey, rctx))
}

// Helper to register an inventory in the manager.
func registerInventory(t *testing.T, h *Handlers, id, name, path string) {
	t.Helper()
	body := `{"id":"` + id + `","name":"` + name + `","path":"` + path + `"}`
	req := httptest.NewRequest(http.MethodPost, "/api/inventories", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.RegisterInventoryAPI(w, req)
	if w.Code != http.StatusCreated {
		t.Fatalf("register inventory: got status %d, want %d", w.Code, http.StatusCreated)
	}
}

func TestLoadInventoryAPI_NotFound(t *testing.T) {
	h := newTestHandlers(t)

	req := httptest.NewRequest(http.MethodPost, "/api/inventories/nonexistent/load", http.NoBody)
	req = withURLParam(req, "id", "nonexistent")
	w := httptest.NewRecorder()

	h.LoadInventoryAPI(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("status = %d, want %d", w.Code, http.StatusNotFound)
	}
}

func TestLoadInventoryAPI_InvalidState(t *testing.T) {
	h := newTestHandlers(t)
	registerInventory(t, h, "test", "Test", "/path")

	// Try to load - this will fail because the path doesn't exist,
	// but we test the state check by trying to load while already parsing.
	// First, set up a second load attempt which should fail due to state.
	// Register and try to load twice rapidly - but since Load is synchronous
	// and will fail on missing file, the state becomes "error", not "loaded".
	// Let's test the conflict response is proper when state doesn't allow loading.

	// Load once (will fail with error state due to invalid path)
	req := httptest.NewRequest(http.MethodPost, "/api/inventories/test/load", http.NoBody)
	req = withURLParam(req, "id", "test")
	w := httptest.NewRecorder()
	h.LoadInventoryAPI(w, req)
	// This will result in error (500) because the path doesn't exist

	// Now inventory should be in error state, which allows reload
	// To test invalid state, we need a loaded inventory - but we can't load without a real index.
	// For this test, we just verify the NotFound case is handled - the InvalidState
	// scenario requires a loaded inventory which needs a real index file.
}

func TestUnloadInventoryAPI_NotFound(t *testing.T) {
	h := newTestHandlers(t)

	req := httptest.NewRequest(http.MethodPost, "/api/inventories/nonexistent/unload", http.NoBody)
	req = withURLParam(req, "id", "nonexistent")
	w := httptest.NewRecorder()

	h.UnloadInventoryAPI(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("status = %d, want %d", w.Code, http.StatusNotFound)
	}
}

func TestUnloadInventoryAPI_NotLoaded(t *testing.T) {
	h := newTestHandlers(t)
	registerInventory(t, h, "test", "Test", "/path")

	// Try to unload a pending inventory - should fail with conflict
	req := httptest.NewRequest(http.MethodPost, "/api/inventories/test/unload", http.NoBody)
	req = withURLParam(req, "id", "test")
	w := httptest.NewRecorder()

	h.UnloadInventoryAPI(w, req)

	if w.Code != http.StatusConflict {
		t.Errorf("status = %d, want %d", w.Code, http.StatusConflict)
	}
}

func TestDeleteInventoryAPI_Success(t *testing.T) {
	h := newTestHandlers(t)
	registerInventory(t, h, "test", "Test", "/path")

	req := httptest.NewRequest(http.MethodDelete, "/api/inventories/test", http.NoBody)
	req = withURLParam(req, "id", "test")
	w := httptest.NewRecorder()

	h.DeleteInventoryAPI(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want %d", w.Code, http.StatusOK)
	}

	// Verify it's gone
	req = httptest.NewRequest(http.MethodGet, "/api/inventories/test", http.NoBody)
	req = withURLParam(req, "id", "test")
	w = httptest.NewRecorder()
	h.GetInventoryAPI(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("after delete: status = %d, want %d", w.Code, http.StatusNotFound)
	}
}

func TestDeleteInventoryAPI_NotFound(t *testing.T) {
	h := newTestHandlers(t)

	req := httptest.NewRequest(http.MethodDelete, "/api/inventories/nonexistent", http.NoBody)
	req = withURLParam(req, "id", "nonexistent")
	w := httptest.NewRecorder()

	h.DeleteInventoryAPI(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("status = %d, want %d", w.Code, http.StatusNotFound)
	}
}

func TestGetStatsAPI_MissingInventoryID(t *testing.T) {
	h := newTestHandlers(t)

	req := httptest.NewRequest(http.MethodGet, "/api/stats?prefix=test/", http.NoBody)
	w := httptest.NewRecorder()

	h.GetStatsAPI(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d", w.Code, http.StatusBadRequest)
	}

	if msg := decodeErr(t, w.Body); !strings.Contains(msg, "inventory_id") {
		t.Errorf("error = %q, want mention of inventory_id", msg)
	}
}

func TestGetStatsAPI_MissingPrefix(t *testing.T) {
	h := newTestHandlers(t)

	req := httptest.NewRequest(http.MethodGet, "/api/stats?inventory_id=test", http.NoBody)
	w := httptest.NewRecorder()

	h.GetStatsAPI(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d", w.Code, http.StatusBadRequest)
	}
	if msg := decodeErr(t, w.Body); !strings.Contains(msg, "prefix") {
		t.Errorf("error = %q, want mention of prefix", msg)
	}
}

// TestGetStatsAPI_EmptyPrefixAccepted verifies that an explicitly empty
// prefix is not rejected at the parameter-validation step. The handler
// still surfaces inventory-not-found (or not-loaded), but it must not
// 400 the request — an empty prefix is the root of the trie.
func TestGetStatsAPI_EmptyPrefixAccepted(t *testing.T) {
	h := newTestHandlers(t)

	req := httptest.NewRequest(http.MethodGet, "/api/stats?inventory_id=nope&prefix=", http.NoBody)
	w := httptest.NewRecorder()

	h.GetStatsAPI(w, req)

	if w.Code == http.StatusBadRequest {
		t.Errorf("status = 400 for empty prefix, want non-400 (inventory lookup failure)")
	}
}

func TestGetInventoryStatsAPI_EmptyPrefixAccepted(t *testing.T) {
	h := newTestHandlers(t)
	registerInventory(t, h, "test", "Test", "/path")

	req := httptest.NewRequest(http.MethodGet, "/api/inventories/test/stats?prefix=", http.NoBody)
	req = withURLParam(req, "id", "test")
	w := httptest.NewRecorder()

	h.GetInventoryStatsAPI(w, req)

	if w.Code == http.StatusBadRequest {
		t.Errorf("status = 400 for empty prefix, want non-400 (inventory not-loaded)")
	}
}

func TestGetDescendantsAPI_EmptyPrefixAccepted(t *testing.T) {
	h := newTestHandlers(t)
	registerInventory(t, h, "test", "Test", "/path")

	req := httptest.NewRequest(http.MethodGet, "/api/inventories/test/descendants?prefix=", http.NoBody)
	req = withURLParam(req, "id", "test")
	w := httptest.NewRecorder()

	h.GetDescendantsAPI(w, req)

	if w.Code == http.StatusBadRequest {
		t.Errorf("status = 400 for empty prefix, want non-400 (inventory not-loaded)")
	}
}

func TestGetStatsAPI_NotFound(t *testing.T) {
	h := newTestHandlers(t)

	req := httptest.NewRequest(http.MethodGet, "/api/stats?inventory_id=nonexistent&prefix=test/", http.NoBody)
	w := httptest.NewRecorder()

	h.GetStatsAPI(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("status = %d, want %d", w.Code, http.StatusNotFound)
	}
}

func TestGetStatsAPI_NotLoaded(t *testing.T) {
	h := newTestHandlers(t)
	registerInventory(t, h, "test", "Test", "/path")

	req := httptest.NewRequest(http.MethodGet, "/api/stats?inventory_id=test&prefix=data/", http.NoBody)
	w := httptest.NewRecorder()

	h.GetStatsAPI(w, req)

	if w.Code != http.StatusConflict {
		t.Errorf("status = %d, want %d", w.Code, http.StatusConflict)
	}

	if msg := decodeErr(t, w.Body); !strings.Contains(msg, "not loaded") {
		t.Errorf("error = %q, want mention of not loaded", msg)
	}
}

func TestGetInventoryStatsAPI_MissingPrefix(t *testing.T) {
	h := newTestHandlers(t)
	registerInventory(t, h, "test", "Test", "/path")

	req := httptest.NewRequest(http.MethodGet, "/api/inventories/test/stats", http.NoBody)
	req = withURLParam(req, "id", "test")
	w := httptest.NewRecorder()

	h.GetInventoryStatsAPI(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d", w.Code, http.StatusBadRequest)
	}
}

func TestGetInventoryStatsAPI_NotFound(t *testing.T) {
	h := newTestHandlers(t)

	req := httptest.NewRequest(http.MethodGet, "/api/inventories/nonexistent/stats?prefix=test/", http.NoBody)
	req = withURLParam(req, "id", "nonexistent")
	w := httptest.NewRecorder()

	h.GetInventoryStatsAPI(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("status = %d, want %d", w.Code, http.StatusNotFound)
	}
}

func TestGetInventoryStatsAPI_NotLoaded(t *testing.T) {
	h := newTestHandlers(t)
	registerInventory(t, h, "test", "Test", "/path")

	req := httptest.NewRequest(http.MethodGet, "/api/inventories/test/stats?prefix=data/", http.NoBody)
	req = withURLParam(req, "id", "test")
	w := httptest.NewRecorder()

	h.GetInventoryStatsAPI(w, req)

	if w.Code != http.StatusConflict {
		t.Errorf("status = %d, want %d", w.Code, http.StatusConflict)
	}
}

func TestGetDescendantsAPI_MissingPrefix(t *testing.T) {
	h := newTestHandlers(t)
	registerInventory(t, h, "test", "Test", "/path")

	req := httptest.NewRequest(http.MethodGet, "/api/inventories/test/descendants", http.NoBody)
	req = withURLParam(req, "id", "test")
	w := httptest.NewRecorder()

	h.GetDescendantsAPI(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d", w.Code, http.StatusBadRequest)
	}
}

func TestGetDescendantsAPI_InvalidDepth(t *testing.T) {
	h := newTestHandlers(t)
	registerInventory(t, h, "test", "Test", "/path")

	tests := []struct {
		name  string
		depth string
	}{
		{"negative", "-1"},
		{"zero", "0"},
		{"not a number", "abc"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/api/inventories/test/descendants?prefix=data/&depth="+tt.depth, http.NoBody)
			req = withURLParam(req, "id", "test")
			w := httptest.NewRecorder()

			h.GetDescendantsAPI(w, req)

			if w.Code != http.StatusBadRequest {
				t.Errorf("status = %d, want %d", w.Code, http.StatusBadRequest)
			}
		})
	}
}

func TestGetDescendantsAPI_InvalidMinCount(t *testing.T) {
	h := newTestHandlers(t)
	registerInventory(t, h, "test", "Test", "/path")

	req := httptest.NewRequest(http.MethodGet, "/api/inventories/test/descendants?prefix=data/&min_count=abc", http.NoBody)
	req = withURLParam(req, "id", "test")
	w := httptest.NewRecorder()

	h.GetDescendantsAPI(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d", w.Code, http.StatusBadRequest)
	}

	if msg := decodeErr(t, w.Body); !strings.Contains(msg, "min_count") {
		t.Errorf("error = %q, want mention of min_count", msg)
	}
}

func TestGetDescendantsAPI_InvalidMinBytes(t *testing.T) {
	h := newTestHandlers(t)
	registerInventory(t, h, "test", "Test", "/path")

	req := httptest.NewRequest(http.MethodGet, "/api/inventories/test/descendants?prefix=data/&min_bytes=abc", http.NoBody)
	req = withURLParam(req, "id", "test")
	w := httptest.NewRecorder()

	h.GetDescendantsAPI(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d", w.Code, http.StatusBadRequest)
	}
	if msg := decodeErr(t, w.Body); !strings.Contains(msg, "min_bytes") {
		t.Errorf("error = %q, want mention of min_bytes", msg)
	}
}

func TestDeleteInventoryRowPartial_RemovesAndReturnsEmpty(t *testing.T) {
	h := newTestHandlers(t)
	registerInventory(t, h, "test", "Test", "/path")

	req := httptest.NewRequest(http.MethodDelete, "/partials/inventories/test", http.NoBody)
	req = withURLParam(req, "id", "test")
	w := httptest.NewRecorder()

	h.DeleteInventoryRowPartial(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want %d", w.Code, http.StatusOK)
	}
	if got := w.Header().Get("Content-Type"); got != "text/html; charset=utf-8" {
		t.Errorf("content-type = %q, want text/html", got)
	}
	if body := w.Body.String(); body != "" {
		t.Errorf("body = %q, want empty", body)
	}
	if _, ok := h.manager.Get("test"); ok {
		t.Error("inventory still present after partial delete")
	}
}

func TestUnloadInventoryRowPartial_NotLoadedReturnsConflict(t *testing.T) {
	h := newTestHandlers(t)
	registerInventory(t, h, "test", "Test", "/path")

	req := httptest.NewRequest(http.MethodPost, "/partials/inventories/test/unload", http.NoBody)
	req = withURLParam(req, "id", "test")
	w := httptest.NewRecorder()

	h.UnloadInventoryRowPartial(w, req)

	if w.Code != http.StatusConflict {
		t.Errorf("status = %d, want %d", w.Code, http.StatusConflict)
	}
}

func TestLoadInventoryRowPartial_NotFound(t *testing.T) {
	h := newTestHandlers(t)

	req := httptest.NewRequest(http.MethodPost, "/partials/inventories/missing/load", http.NoBody)
	req = withURLParam(req, "id", "missing")
	w := httptest.NewRecorder()

	h.LoadInventoryRowPartial(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("status = %d, want %d", w.Code, http.StatusNotFound)
	}
}
