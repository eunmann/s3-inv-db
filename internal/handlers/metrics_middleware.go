package handlers

import (
	"bufio"
	"errors"
	"net"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/eunmann/s3-inv-db/internal/metrics"
)

// statusRecorder wraps http.ResponseWriter to capture the final HTTP
// status; required because Go's stdlib doesn't expose it after WriteHeader.
//
// Flusher / Hijacker are forwarded so SSE and connection-upgrade
// handlers keep working if this middleware is ever applied to them.
type statusRecorder struct {
	http.ResponseWriter

	status int
}

func (s *statusRecorder) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

// Flush forwards to the underlying ResponseWriter when it implements
// http.Flusher (e.g. the SSE jobs stream). A no-op when not supported.
func (s *statusRecorder) Flush() {
	if f, ok := s.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// ErrHijackUnsupported is returned by Hijack when the wrapped
// ResponseWriter does not implement http.Hijacker.
var ErrHijackUnsupported = errors.New("statusRecorder: underlying ResponseWriter does not support hijacking")

// Hijack forwards to the underlying ResponseWriter when it implements
// http.Hijacker. Errors when not supported rather than panicking.
//
//nolint:wrapcheck // intentional passthrough — callers depend on the upstream error
func (s *statusRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if h, ok := s.ResponseWriter.(http.Hijacker); ok {
		return h.Hijack()
	}

	return nil, nil, ErrHijackUnsupported
}

// MetricsMiddleware records request count + duration into the handler's
// metrics registry. Series are labelled by route name and status code so
// dashboards can break out errors and slow endpoints separately.
//
// The route name comes from a per-request label; pass it via the route
// definition (e.g. r.With(h.MetricsMiddleware("stats")).Get(...)).
//
// The route's series are resolved here, once, not per request: the
// registry lookup takes its lock and builds a label key, which on a
// sub-microsecond query would cost more than the query and serialize
// every core. A request does four atomic adds (three for the histogram,
// one for the counter) and one atomic load.
func (h *Handlers) MetricsMiddleware(route string) func(http.Handler) http.Handler {
	duration := h.reg.Histogram(
		"s3invdb_http_request_seconds",
		"HTTP request duration in seconds",
		latencyBuckets,
		metrics.Label("route", route),
	)
	requests := &statusCounters{reg: h.reg, route: route}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
			start := time.Now()
			next.ServeHTTP(rec, r)
			duration.Observe(time.Since(start).Seconds())
			requests.counter(rec.status).Inc()
		})
	}
}

// statusCounters caches one route's request counter per status code,
// resolved from the registry on a code's first use.
type statusCounters struct {
	reg      *metrics.Registry
	route    string
	byStatus [maxHTTPStatus]atomic.Pointer[metrics.Counter]
}

// maxHTTPStatus bounds the cached status codes; anything past it (not
// a valid HTTP status) goes to the registry every time.
const maxHTTPStatus = 600

func (s *statusCounters) counter(status int) *metrics.Counter {
	if status >= 0 && status < maxHTTPStatus {
		if c := s.byStatus[status].Load(); c != nil {
			return c
		}
	}
	c := s.reg.Counter(
		"s3invdb_http_requests_total",
		"Total HTTP requests served",
		metrics.Label("route", s.route),
		metrics.Label("status", strconv.Itoa(status)),
	)
	if status >= 0 && status < maxHTTPStatus {
		// The registry returns the same counter for the same labels, so
		// racing first uses store the same pointer.
		s.byStatus[status].Store(c)
	}

	return c
}

// latencyBuckets covers the expected query latency range — mmap point
// lookup (<1ms) up through pathological multi-megabyte browse responses.
// Histogram buckets are immutable for a registered series, so making this
// a package var keeps every middleware invocation sharing the same
// definition and avoids re-allocating the slice per request.
//
//nolint:gochecknoglobals // immutable bucket bounds shared by every series
var latencyBuckets = []float64{0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5}
