package handlers_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/eunmann/s3-inv-db/internal/handlers"
)

func TestGetTopAPI_RanksByBytes(t *testing.T) {
	h := buildLoadedTestHandlers(t)

	req := httptest.NewRequest(http.MethodGet, "/api/inventories/loaded/top?prefix=&depth=1&limit=3&by=bytes", http.NoBody)
	req = withURLParam(req, "id", "loaded")
	w := httptest.NewRecorder()
	h.GetTopAPI(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", w.Code, w.Body.String())
	}

	var resp handlers.TopResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.By != "bytes" {
		t.Errorf("by = %q, want bytes", resp.By)
	}
	if len(resp.Results) == 0 {
		t.Fatalf("results empty")
	}
	for i := 1; i < len(resp.Results); i++ {
		if resp.Results[i-1].TotalBytes < resp.Results[i].TotalBytes {
			t.Errorf("results not sorted by bytes desc at index %d", i)
		}
	}
}

func TestGetTopAPI_LimitClampedAndBy(t *testing.T) {
	h := buildLoadedTestHandlers(t)

	req := httptest.NewRequest(http.MethodGet, "/api/inventories/loaded/top?prefix=&depth=1&limit=999999&by=count", http.NoBody)
	req = withURLParam(req, "id", "loaded")
	w := httptest.NewRecorder()
	h.GetTopAPI(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", w.Code, w.Body.String())
	}

	var resp handlers.TopResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.By != "count" {
		t.Errorf("by = %q, want count", resp.By)
	}
	for i := 1; i < len(resp.Results); i++ {
		if resp.Results[i-1].ObjectCount < resp.Results[i].ObjectCount {
			t.Errorf("results not sorted by count desc at index %d", i)
		}
	}
}

func TestGetTopAPI_BadBy(t *testing.T) {
	h := buildLoadedTestHandlers(t)

	req := httptest.NewRequest(http.MethodGet, "/api/inventories/loaded/top?by=garbage", http.NoBody)
	req = withURLParam(req, "id", "loaded")
	w := httptest.NewRecorder()
	h.GetTopAPI(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", w.Code)
	}
}
