package server

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/pricing"
	"github.com/rs/zerolog"
)

func TestRefreshIndexes_NoSourceIs503(t *testing.T) {
	srv, err := New(t.Context(), Config{Addr: ":0", Logger: zerolog.Nop(), PriceTable: pricing.DefaultUSEast1Prices(), DB: testDB(t)})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "http://localhost/api/indexes/refresh", http.NoBody)
	w := httptest.NewRecorder()
	srv.Router().ServeHTTP(w, req)
	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503 with no s3_source or index_source", w.Code)
	}
}
