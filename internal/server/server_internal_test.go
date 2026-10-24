package server

import (
	"context"
	"database/sql"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/eunmann/s3-inv-db/internal/dbtest"
	"github.com/eunmann/s3-inv-db/pkg/pricing"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/rs/zerolog"
)

// localhostHost is the synthetic request Host used across the
// same-origin middleware tests. Pulled into a constant so goconst
// doesn't flag the repeated literal.
const localhostHost = "localhost"

// testDB returns an in-memory SQLite handle wired up the same way main
// opens the production one (foreign_keys on for cascade tests).
func testDB(t *testing.T) *sql.DB {
	t.Helper()

	return dbtest.OpenMemDB(t)
}

func TestNewServer(t *testing.T) {
	cfg := Config{
		Addr:       ":8080",
		Logger:     zerolog.Nop(),
		PriceTable: pricing.DefaultUSEast1Prices(),
		DB:         testDB(t),
	}

	srv, err := New(t.Context(), cfg)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if srv.Router() == nil {
		t.Error("Router() returned nil")
	}

	if srv.manager == nil {
		t.Error("manager field was nil after New")
	}
}

func TestServerAPIRoutes(t *testing.T) {
	cfg := Config{
		Addr:       ":8080",
		Logger:     zerolog.Nop(),
		PriceTable: pricing.DefaultUSEast1Prices(),
		DB:         testDB(t),
	}

	srv, err := New(t.Context(), cfg)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	// Test API routes return JSON
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/configurations", http.NoBody)
	w := httptest.NewRecorder()

	srv.Router().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want %d", w.Code, http.StatusOK)
	}

	contentType := w.Header().Get("Content-Type")
	if contentType != "application/json" {
		t.Errorf("Content-Type = %q, want %q", contentType, "application/json")
	}
}

// TestServerHTTPTimeouts pins the slow-loris hardening: every long-lived
// timeout knob must be set on the http.Server so a peer can't hold a
// connection or header read open indefinitely. Inspects the helper
// directly to avoid racing against Run's goroutine.
func TestServerHTTPTimeouts(t *testing.T) {
	cfg := Config{
		Addr:       "127.0.0.1:0",
		Logger:     zerolog.Nop(),
		PriceTable: pricing.DefaultUSEast1Prices(),
		DB:         testDB(t),
	}

	srv, err := New(t.Context(), cfg)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	httpSrv := srv.newHTTPServer(t.Context())

	if httpSrv.ReadHeaderTimeout <= 0 {
		t.Errorf("ReadHeaderTimeout = %v, want > 0", httpSrv.ReadHeaderTimeout)
	}
	if httpSrv.ReadTimeout <= 0 {
		t.Errorf("ReadTimeout = %v, want > 0", httpSrv.ReadTimeout)
	}
	if httpSrv.IdleTimeout <= 0 {
		t.Errorf("IdleTimeout = %v, want > 0", httpSrv.IdleTimeout)
	}
	if httpSrv.MaxHeaderBytes <= 0 {
		t.Errorf("MaxHeaderBytes = %d, want > 0", httpSrv.MaxHeaderBytes)
	}
}

// TestServerGracefulShutdown verifies Run returns nil when ctx is cancelled,
// so SIGINT/SIGTERM produce a clean exit, and that the inventory manager is
// closed (cleared) afterward.
func TestServerGracefulShutdown(t *testing.T) {
	lc := &net.ListenConfig{}
	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	ln.Close()

	cfg := Config{
		Addr:       addr,
		Logger:     zerolog.Nop(),
		PriceTable: pricing.DefaultUSEast1Prices(),
		DB:         testDB(t),
	}

	srv, err := New(t.Context(), cfg)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	// Register an inventory so Close has something to clear.
	if err := srv.manager.Register(t.Context(), "probe", "Probe", "/no/such/path"); err != nil {
		t.Fatalf("register: %v", err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	runErr := make(chan error, 1)
	go func() {
		runErr <- srv.Run(ctx)
	}()

	// Give the server a moment to start accepting.
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-runErr:
		if err != nil {
			t.Errorf("Run() returned %v on graceful shutdown, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run() did not return within 5s after context cancel")
	}

	if got := len(srv.manager.List()); got != 0 {
		t.Errorf("Manager.List() length = %d after Run, want 0 (manager not closed)", got)
	}
}

// TestServerGracefulShutdown_OpenSSEStream pins that an open job stream
// does not hold shutdown hostage: http.Server.Shutdown never cancels
// request contexts, so the SSE loop only exits if Run cancels the
// server's base context itself.
func TestServerGracefulShutdown_OpenSSEStream(t *testing.T) {
	lc := &net.ListenConfig{}
	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	ln.Close()

	srv, err := New(t.Context(), Config{
		Addr:       addr,
		Logger:     zerolog.Nop(),
		PriceTable: pricing.DefaultUSEast1Prices(),
		DB:         testDB(t),
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	runErr := make(chan error, 1)
	go func() {
		runErr <- srv.Run(ctx)
	}()

	resp := openSSEStream(t, "http://"+addr+"/api/jobs/stream")
	defer resp.Body.Close()

	start := time.Now()
	cancel()

	select {
	case err := <-runErr:
		if err != nil {
			t.Errorf("Run() returned %v with an open SSE stream, want nil", err)
		}
		if elapsed := time.Since(start); elapsed > shutdownDrainTimeout/2 {
			t.Errorf("shutdown took %v, want well under %v", elapsed, shutdownDrainTimeout)
		}
	case <-time.After(shutdownDrainTimeout / 2):
		t.Fatal("Run() did not return with an open SSE stream")
	}
}

// openSSEStream retries until the server accepts, then reads the
// ": connected" preamble so the handler is known to be in its loop.
func openSSEStream(t *testing.T, url string) *http.Response {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, http.NoBody)
		if err != nil {
			t.Fatalf("new request: %v", err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			buf := make([]byte, len(": connected"))
			if _, err := io.ReadFull(resp.Body, buf); err != nil {
				resp.Body.Close()
				t.Fatalf("read SSE preamble: %v", err)
			}

			return resp
		}
		if time.Now().After(deadline) {
			t.Fatalf("SSE stream never opened: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestServerRun_ListenError verifies that when ListenAndServe fails (port
// already in use), Run returns the error and still closes the inventory
// manager on the way out.
func TestServerRun_ListenError(t *testing.T) {
	// Occupy a port so the server can't bind to it.
	lc := &net.ListenConfig{}
	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	cfg := Config{
		Addr:       ln.Addr().String(),
		Logger:     zerolog.Nop(),
		PriceTable: pricing.DefaultUSEast1Prices(),
		DB:         testDB(t),
	}

	srv, err := New(t.Context(), cfg)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if err := srv.manager.Register(t.Context(), "probe", "Probe", "/no/such/path"); err != nil {
		t.Fatalf("register: %v", err)
	}

	runErr := make(chan error, 1)
	go func() {
		runErr <- srv.Run(context.Background())
	}()

	select {
	case err := <-runErr:
		if err == nil {
			t.Error("Run() returned nil on port conflict, want non-nil error")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run() did not return within 5s on port conflict")
	}

	if got := len(srv.manager.List()); got != 0 {
		t.Errorf("Manager.List() length = %d after ListenAndServe failure, want 0", got)
	}
}

func TestSameOriginMiddleware_RejectsCrossOriginMutation(t *testing.T) {
	cfg := Config{Addr: ":0", Logger: zerolog.Nop(), PriceTable: pricing.DefaultUSEast1Prices(), DB: testDB(t)}
	srv, err := New(t.Context(), cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// POST with cross-origin Referer must be rejected.
	req := httptest.NewRequestWithContext(t.Context(), http.MethodDelete, "http://localhost/api/inventories/foo", http.NoBody)
	req.Host = localhostHost
	req.Header.Set("Origin", "http://attacker.example")
	w := httptest.NewRecorder()
	srv.Router().ServeHTTP(w, req)
	if w.Code != http.StatusForbidden {
		t.Errorf("cross-origin DELETE status = %d, want 403", w.Code)
	}
}

func TestSameOriginMiddleware_AllowsSameOriginMutation(t *testing.T) {
	cfg := Config{Addr: ":0", Logger: zerolog.Nop(), PriceTable: pricing.DefaultUSEast1Prices(), DB: testDB(t)}
	srv, err := New(t.Context(), cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	req := httptest.NewRequestWithContext(t.Context(), http.MethodDelete, "http://localhost/api/inventories/foo", http.NoBody)
	req.Host = localhostHost
	req.Header.Set("Origin", "http://localhost")
	w := httptest.NewRecorder()
	srv.Router().ServeHTTP(w, req)
	// Will 404 because the inventory doesn't exist; we just care that
	// it got past the middleware (i.e., not 403).
	if w.Code == http.StatusForbidden {
		t.Errorf("same-origin DELETE was rejected as cross-origin")
	}
}

func TestSameOriginMiddleware_AllowsNoOrigin(t *testing.T) {
	cfg := Config{Addr: ":0", Logger: zerolog.Nop(), PriceTable: pricing.DefaultUSEast1Prices(), DB: testDB(t)}
	srv, err := New(t.Context(), cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// No Origin and no Referer — typical curl/script request, allowed.
	req := httptest.NewRequestWithContext(t.Context(), http.MethodDelete, "http://localhost/api/inventories/foo", http.NoBody)
	req.Host = localhostHost
	w := httptest.NewRecorder()
	srv.Router().ServeHTTP(w, req)
	if w.Code == http.StatusForbidden {
		t.Errorf("origin-less DELETE was rejected")
	}
}

func TestSameOriginMiddleware_DoesNotBlockReads(t *testing.T) {
	cfg := Config{Addr: ":0", Logger: zerolog.Nop(), PriceTable: pricing.DefaultUSEast1Prices(), DB: testDB(t)}
	srv, err := New(t.Context(), cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "http://localhost/api/configurations", http.NoBody)
	req.Host = localhostHost
	req.Header.Set("Origin", "http://attacker.example")
	w := httptest.NewRecorder()
	srv.Router().ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("cross-origin GET status = %d, want 200 (reads are public)", w.Code)
	}
}

func TestSameOrigin_RejectsSchemeless(t *testing.T) {
	// url.Parse("//localhost") yields Host=localhost with no Scheme.
	// Defense-in-depth: reject anything without a real http/https scheme.
	for _, raw := range []string{"//localhost", "localhost", "null", "about:blank"} {
		if sameOrigin(raw, "localhost") {
			t.Errorf("sameOrigin(%q, localhost) = true, want false", raw)
		}
	}
}

func TestSameOrigin_CaseInsensitiveHost(t *testing.T) {
	if !sameOrigin("http://LOCALHOST:8080", "localhost:8080") {
		t.Error("case differences in host should still match")
	}
}

func TestSameOrigin_PortMismatch(t *testing.T) {
	if sameOrigin("http://localhost:8080", "localhost:9090") {
		t.Error("port mismatch should not match")
	}
}

func TestSameOrigin_HostInPath(t *testing.T) {
	// An origin like "http://victim@evil.com" should resolve to evil.com.
	if sameOrigin("http://localhost@evil.com", "localhost") {
		t.Error("userinfo-encoded origin must not match victim host")
	}
}

func TestSameOriginMiddleware_ReadsBypassOriginCheck(t *testing.T) {
	cfg := Config{Addr: ":0", Logger: zerolog.Nop(), PriceTable: pricing.DefaultUSEast1Prices(), DB: testDB(t)}
	srv, err := New(t.Context(), cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	// HEAD/OPTIONS aren't in our middleware's isMutating set; this
	// pins the contract that only POST/PUT/PATCH/DELETE are blocked.
	for _, method := range []string{http.MethodGet, http.MethodHead, http.MethodOptions} {
		req := httptest.NewRequestWithContext(t.Context(), method, "http://localhost/healthz", http.NoBody)
		req.Host = localhostHost
		req.Header.Set("Origin", "http://attacker.example")
		w := httptest.NewRecorder()
		srv.Router().ServeHTTP(w, req)
		if w.Code == http.StatusForbidden {
			t.Errorf("method %s with cross-origin Origin was rejected", method)
		}
	}
}

func TestHlogChain_LogsAfterPanic(t *testing.T) {
	// hlog.AccessHandler captures the panic-induced 500 because chi's
	// Recoverer (registered earlier in the actual server chain) writes
	// the 500 status through the access handler's instrumented writer.
	var sink writeBuffer
	logger := zerolog.New(&sink)

	chain := hlogChain(logger)
	var handler http.Handler = http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
		panic("boom")
	})
	handler = middleware.Recoverer(handler)
	// Apply hlog chain in reverse order (innermost first).
	for _, mw := range slices.Backward(chain) {
		handler = mw(handler)
	}

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "http://localhost/", http.NoBody)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if !strings.Contains(sink.String(), `"status":500`) {
		t.Errorf("access log missing status=500 for panicking handler; got %q", sink.String())
	}
}

type writeBuffer struct{ b []byte }

func (w *writeBuffer) Write(p []byte) (int, error) {
	w.b = append(w.b, p...)
	return len(p), nil
}
func (w *writeBuffer) String() string { return string(w.b) }

func TestClientIPMiddleware(t *testing.T) {
	cases := []struct {
		name    string
		trusted []netip.Prefix
		remote  string
		xff     string
		want    string
	}{
		{"no_proxy_ignores_spoofed_xff", nil, "203.0.113.9:4000", "1.2.3.4", "203.0.113.9"},
		{"untrusted_peer_ignores_xff", []netip.Prefix{netip.MustParsePrefix("10.0.0.0/8")}, "203.0.113.9:4000", "1.2.3.4", "203.0.113.9"},
		{"mapped_v4_peer_is_trusted", []netip.Prefix{netip.MustParsePrefix("10.0.0.0/8")}, "[::ffff:10.0.0.5]:4000", "198.51.100.7", "198.51.100.7"},
		{"trusted_proxy_uses_xff", []netip.Prefix{netip.MustParsePrefix("10.0.0.0/8")}, "10.0.0.5:4000", "198.51.100.7", "198.51.100.7"},
		{"trusted_proxy_skips_trusted_hops", []netip.Prefix{netip.MustParsePrefix("10.0.0.0/8")}, "10.0.0.5:4000", "198.51.100.7, 10.0.0.6", "198.51.100.7"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var got string
			h := clientIPMiddleware(c.trusted)(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
				got = middleware.GetClientIP(r.Context())
			}))
			req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", http.NoBody)
			req.RemoteAddr = c.remote
			req.Header.Set("X-Forwarded-For", c.xff)
			h.ServeHTTP(httptest.NewRecorder(), req)
			if got != c.want {
				t.Errorf("client IP = %q, want %q", got, c.want)
			}
		})
	}
}
