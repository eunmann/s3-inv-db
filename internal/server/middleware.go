package server

import (
	"context"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/eunmann/s3-inv-db/internal/logctx"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/rs/zerolog"
)

// contextLoggerMiddleware attaches a request-scoped logger to ctx so
// downstream handlers and packages can pull it with logctx.FromContext.
// Each request gets a child logger tagged with the chi request_id so
// log lines and access logs correlate one-to-one.
func contextLoggerMiddleware(base zerolog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			reqID := middleware.GetReqID(r.Context())
			reqLogger := base.With().Str("request_id", reqID).Logger()
			ctx := logctx.WithLogger(r.Context(), reqLogger)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// accessLogMiddleware logs the completed request with status, duration,
// remote addr, etc. The log call runs in a defer so panicking handlers
// still produce an access log entry: Recoverer (registered earlier)
// catches the panic and writes a 500, and our deferred log sees that
// status because Recoverer wrote to the same wrapped writer.
func accessLogMiddleware() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			wrapped := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}
			panicked := true
			ctx := r.Context()
			defer func() { writeAccessLog(ctx, r, wrapped.statusCode, time.Since(start), panicked) }()
			next.ServeHTTP(wrapped, r)
			panicked = false
		})
	}
}

// writeAccessLog emits one access-log line. Extracted so the defer in
// accessLogMiddleware can hand ctx in directly (contextcheck), and so
// the panic-bypass status synthesis lives in one place.
func writeAccessLog(ctx context.Context, r *http.Request, status int, dur time.Duration, panicked bool) {
	if panicked {
		// Handler panicked: Recoverer (inner wrapper) writes 500 to the
		// original writer, bypassing our wrapped responseWriter, so
		// wrapped.statusCode never updates. Surface the real outcome.
		status = http.StatusInternalServerError
	}
	logger := logctx.FromContext(ctx)
	fields := func(ev *zerolog.Event) *zerolog.Event {
		return ev.
			Str("method", r.Method).
			Str("path", r.URL.Path).
			Int("status", status).
			Dur("duration", dur).
			Str("remote_addr", r.RemoteAddr)
	}
	switch {
	case status >= http.StatusInternalServerError:
		fields(logger.Error()).Msg("http request")
	case status >= http.StatusBadRequest:
		fields(logger.Warn()).Msg("http request")
	default:
		fields(logger.Info()).Msg("http request")
	}
}

// responseWriter wraps http.ResponseWriter to capture status code.
type responseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}

// isMutating reports whether a method writes state. Read methods bypass
// the auth + CSRF checks; writes are gated.
func isMutating(method string) bool {
	switch method {
	case http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
		return true
	}
	return false
}

// sameOriginMiddleware rejects mutating browser requests whose Origin (or,
// fallback, Referer) does not match the request Host. This is the standard
// CSRF defense: an attacker page making a cross-site POST would send its
// own origin, which won't match ours. Non-browser callers (curl, scripts)
// typically send no Origin/Referer and are allowed through.
//
// Strictness: we require a real http/https scheme in the header — without
// it, url.Parse accepts shapes like "//victim-host" that have a Host but
// no Scheme, narrowing what would otherwise be a usable bypass surface.
// Host comparison is case-insensitive per RFC 3986.
func sameOriginMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !isMutating(r.Method) {
			next.ServeHTTP(w, r)
			return
		}
		origin := r.Header.Get("Origin")
		if origin == "" {
			origin = r.Header.Get("Referer")
		}
		if origin == "" {
			next.ServeHTTP(w, r)
			return
		}
		if !sameOrigin(origin, r.Host) {
			logctx.FromContext(r.Context()).Warn().
				Str("origin", origin).
				Str("host", r.Host).
				Str("path", r.URL.Path).
				Msg("rejecting cross-origin mutating request")
			http.Error(w, "cross-origin request rejected", http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// Returns true when origin's host matches host. The origin must be a
// fully-qualified http(s) URL; anything else (including "null", scheme-
// less values, empty Host) is rejected.
func sameOrigin(origin, host string) bool {
	u, err := url.Parse(origin)
	if err != nil {
		return false
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return false
	}
	if u.Host == "" {
		return false
	}
	return strings.EqualFold(u.Host, host)
}
