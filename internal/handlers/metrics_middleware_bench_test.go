package handlers_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/eunmann/s3-inv-db/internal/inventory"
)

// BenchmarkMetricsMiddlewareParallel times the per-request cost of the
// metrics middleware alone (the wrapped handler does nothing), from
// many goroutines at once: what every instrumented query pays on top
// of its own work.
func BenchmarkMetricsMiddlewareParallel(b *testing.B) {
	h, _ := newWiredHandlersWithConfigs(b, inventory.NewCatalog(nil))
	handler := h.MetricsMiddleware("stats")(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	req := httptest.NewRequest(http.MethodGet, "/api/stats", http.NoBody)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		w := httptest.NewRecorder()
		for pb.Next() {
			handler.ServeHTTP(w, req)
		}
	})
}
