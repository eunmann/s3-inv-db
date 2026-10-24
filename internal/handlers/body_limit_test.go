package handlers_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/eunmann/s3-inv-db/internal/handlers"
)

func TestBodyDecodingHandlers_RejectOversizedBodies(t *testing.T) {
	oversizedForm := "pinned=true&pad=" + strings.Repeat("a", 64*1024)
	oversizedJSON := `{"id":"x","name":"` + strings.Repeat("a", 64*1024) + `","path":"/p"}`
	cases := []struct {
		call        func(h *handlers.Handlers, w http.ResponseWriter, r *http.Request)
		name        string
		body        string
		contentType string
		params      []string
	}{
		{
			name: "register", body: oversizedJSON, contentType: "application/json",
			call: (*handlers.Handlers).RegisterInventoryAPI,
		},
		{
			name: "auto-load config", body: oversizedForm, contentType: "application/x-www-form-urlencoded",
			params: []string{"src", "buck", "name", "inv"},
			call:   (*handlers.Handlers).SetAutoLoadConfigAPI,
		},
		{
			name: "api pin", body: oversizedForm, contentType: "application/x-www-form-urlencoded",
			params: []string{"id", "buck"},
			call:   (*handlers.Handlers).SetPinAPI,
		},
		{
			name: "partial pin", body: oversizedForm, contentType: "application/x-www-form-urlencoded",
			params: []string{"src", "buck", "id", "inv", "run", "2026-05-13T03-00Z"},
			call:   (*handlers.Handlers).PinDiscoveredRowPartial,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			h := newDiscoveredHandlers(t, &fakeDiscoverer{}, &fakeBuilder{})
			req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/", strings.NewReader(c.body))
			req.Header.Set("Content-Type", c.contentType)
			req = chiCtxWithParams(req, c.params...)
			w := httptest.NewRecorder()
			c.call(h, w, req)
			if w.Code != http.StatusRequestEntityTooLarge {
				t.Errorf("status = %d, want 413; body=%s", w.Code, w.Body.String())
			}
		})
	}
}

func batchBody(t *testing.T, prefixes []string) string {
	t.Helper()
	b, err := json.Marshal(handlers.BatchStatsRequest{Prefixes: prefixes})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	return string(b)
}

func postBatch(t *testing.T, h *handlers.Handlers, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/api/inventories/loaded/stats:batch", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req = withURLParam(req, "id", "loaded")
	w := httptest.NewRecorder()
	h.PostBatchStatsAPI(w, req)

	return w
}

func TestPostBatchStatsAPI_OversizedBody_413(t *testing.T) {
	h := buildLoadedTestHandlers(t, handlers.WithQueryBatchMax(2))
	huge := strings.Repeat("k", 32*1024)
	w := postBatch(t, h, batchBody(t, []string{huge, huge, huge}))
	if w.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("status = %d, want 413; body=%s", w.Code, w.Body.String())
	}
}

func TestPostBatchStatsAPI_MaxLengthKeysAtCapFit(t *testing.T) {
	h := buildLoadedTestHandlers(t, handlers.WithQueryBatchMax(2))
	// S3 keys are at most 1024 bytes; control characters JSON-escape to
	// six bytes each, the worst case the body limit has to admit.
	key := strings.Repeat("\x01", 1024)
	w := postBatch(t, h, batchBody(t, []string{key, key}))
	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want 200; body=%s", w.Code, w.Body.String())
	}
}
