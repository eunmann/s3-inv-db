package handlers_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/eunmann/s3-inv-db/internal/handlers"
)

func TestPostBatchStatsAPI_ReturnsRowsWithFoundFlag(t *testing.T) {
	h := buildLoadedTestHandlers(t)

	body := `{"prefixes":["", "does-not-exist/"]}`
	req := httptest.NewRequest(http.MethodPost, "/api/inventories/loaded/stats:batch", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req = withURLParam(req, "id", "loaded")
	w := httptest.NewRecorder()
	h.PostBatchStatsAPI(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", w.Code, w.Body.String())
	}

	var resp handlers.BatchStatsResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Rows) != 2 {
		t.Fatalf("rows = %d, want 2", len(resp.Rows))
	}
	if !resp.Rows[0].Found {
		t.Errorf("row[0] (root) not found")
	}
	if resp.Rows[1].Found {
		t.Errorf("row[1] (missing) reported found")
	}
}

func TestPostBatchStatsAPI_EmptyPrefixes_400(t *testing.T) {
	h := buildLoadedTestHandlers(t)

	req := httptest.NewRequest(http.MethodPost, "/api/inventories/loaded/stats:batch", strings.NewReader(`{"prefixes":[]}`))
	req.Header.Set("Content-Type", "application/json")
	req = withURLParam(req, "id", "loaded")
	w := httptest.NewRecorder()
	h.PostBatchStatsAPI(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", w.Code)
	}
}

func TestPostBatchStatsAPI_TooManyPrefixes_400(t *testing.T) {
	h := buildLoadedTestHandlers(t, handlers.WithQueryBatchMax(2))

	req := httptest.NewRequest(http.MethodPost, "/api/inventories/loaded/stats:batch",
		strings.NewReader(`{"prefixes":["a","b","c"]}`))
	req.Header.Set("Content-Type", "application/json")
	req = withURLParam(req, "id", "loaded")
	w := httptest.NewRecorder()
	h.PostBatchStatsAPI(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", w.Code)
	}
}
