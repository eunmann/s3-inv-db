package handlers_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestMetricsMiddleware_RecordsAndExposes drives a request through the
// middleware, hits /metrics, and checks the counter + histogram series
// for that route are present.
func TestMetricsMiddleware_RecordsAndExposes(t *testing.T) {
	h := buildLoadedTestHandlers(t)

	inner := h.MetricsMiddleware("smoke")(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTeapot)
	}))

	reqIn := httptest.NewRequest(http.MethodGet, "/x", http.NoBody)
	wIn := httptest.NewRecorder()
	inner.ServeHTTP(wIn, reqIn)

	if wIn.Code != http.StatusTeapot {
		t.Fatalf("inner status = %d, want 418", wIn.Code)
	}

	wM := httptest.NewRecorder()
	h.MetricsHandler(wM, httptest.NewRequest(http.MethodGet, "/metrics", http.NoBody))

	body := wM.Body.String()
	mustContain := []string{
		`s3invdb_http_requests_total{route="smoke",status="418"} 1`,
		`s3invdb_http_request_seconds_bucket{le="+Inf",route="smoke"}`,
		`s3invdb_http_request_seconds_count{route="smoke"} 1`,
		"# TYPE s3invdb_http_requests_total counter",
		"# TYPE s3invdb_http_request_seconds histogram",
	}
	for _, want := range mustContain {
		if !strings.Contains(body, want) {
			t.Errorf("metrics output missing %q\nfull output:\n%s", want, body)
		}
	}
}

// TestMetricsMiddleware_DefaultStatusIs200 confirms a handler that
// never calls WriteHeader still records status=200 (the stdlib
// implicit default).
func TestMetricsMiddleware_DefaultStatusIs200(t *testing.T) {
	h := buildLoadedTestHandlers(t)

	inner := h.MetricsMiddleware("implicit")(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("hi"))
	}))

	inner.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/x", http.NoBody))

	wM := httptest.NewRecorder()
	h.MetricsHandler(wM, httptest.NewRequest(http.MethodGet, "/metrics", http.NoBody))

	if !strings.Contains(wM.Body.String(), `s3invdb_http_requests_total{route="implicit",status="200"} 1`) {
		t.Errorf("expected status=200 counter; metrics:\n%s", wM.Body.String())
	}
}
