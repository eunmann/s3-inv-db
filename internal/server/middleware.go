package server

import (
	"net/http"
	"net/url"
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
// route pattern, etc. Must run AFTER contextLoggerMiddleware so it sees
// the request_id-tagged logger.
func accessLogMiddleware() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			wrapped := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}
			next.ServeHTTP(wrapped, r)

			logger := logctx.FromContext(r.Context())
			fields := func(ev *zerolog.Event) *zerolog.Event {
				return ev.
					Str("method", r.Method).
					Str("path", r.URL.Path).
					Int("status", wrapped.statusCode).
					Dur("duration", time.Since(start)).
					Str("remote_addr", r.RemoteAddr)
			}
			switch {
			case wrapped.statusCode >= http.StatusInternalServerError:
				fields(logger.Error()).Msg("http request")
			case wrapped.statusCode >= http.StatusBadRequest:
				fields(logger.Warn()).Msg("http request")
			default:
				fields(logger.Info()).Msg("http request")
			}
		})
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

// jsonContentType sets Content-Type to application/json for API routes.
func jsonContentType(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		next.ServeHTTP(w, r)
	})
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
		if origin != "" {
			u, err := url.Parse(origin)
			if err != nil || u.Host != r.Host {
				logctx.FromContext(r.Context()).Warn().
					Str("origin", origin).
					Str("host", r.Host).
					Str("path", r.URL.Path).
					Msg("rejecting cross-origin mutating request")
				http.Error(w, "cross-origin request rejected", http.StatusForbidden)
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}
