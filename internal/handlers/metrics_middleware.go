package handlers

import (
	"net/http"
	"strconv"
	"time"

	"github.com/eunmann/s3-inv-db/internal/metrics"
)

// statusRecorder wraps http.ResponseWriter to capture the final HTTP
// status; required because Go's stdlib doesn't expose it after WriteHeader.
type statusRecorder struct {
	http.ResponseWriter

	status int
}

func (s *statusRecorder) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

// MetricsMiddleware records request count + duration into the handler's
// metrics registry. Series are labelled by route name and status code so
// dashboards can break out errors and slow endpoints separately.
//
// The route name comes from a per-request label; pass it via the route
// definition (e.g. r.With(h.MetricsMiddleware("stats")).Get(...)).
func (h *Handlers) MetricsMiddleware(route string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
			start := time.Now()
			next.ServeHTTP(rec, r)
			elapsed := time.Since(start).Seconds()
			h.reg.Counter(
				"s3invdb_http_requests_total",
				"Total HTTP requests served",
				metrics.Label("route", route),
				metrics.Label("status", strconv.Itoa(rec.status)),
			).Inc()
			h.reg.Histogram(
				"s3invdb_http_request_seconds",
				"HTTP request duration in seconds",
				latencyBuckets,
				metrics.Label("route", route),
			).Observe(elapsed)
		})
	}
}

// latencyBuckets covers the expected query latency range — mmap point
// lookup (<1ms) up through pathological multi-megabyte browse responses.
// Histogram buckets are immutable for a registered series, so making this
// a package var keeps every middleware invocation sharing the same
// definition and avoids re-allocating the slice per request.
//
//nolint:gochecknoglobals // immutable bucket bounds shared by every series
var latencyBuckets = []float64{0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5}
