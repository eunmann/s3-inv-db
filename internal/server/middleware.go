package server

import (
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/go-chi/chi/v5/middleware"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/hlog"
)

// Wires the rs/zerolog/hlog middleware stack into a slice the route
// setup can range over. The chain attaches the base logger to ctx,
// copies the chi-issued request_id into the per-request logger, and
// emits an access-log line for every request. Recoverer and RequestID
// must run earlier in the chain so request_id is in ctx already.
func hlogChain(base zerolog.Logger) []func(http.Handler) http.Handler {
	return []func(http.Handler) http.Handler{
		hlog.NewHandler(base),
		// Pull chi's RequestID into the log context so every log line
		// from a request carries the same request_id.
		func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if id := middleware.GetReqID(r.Context()); id != "" {
					hlog.FromRequest(r).UpdateContext(func(c zerolog.Context) zerolog.Context {
						return c.Str("request_id", id)
					})
				}
				next.ServeHTTP(w, r)
			})
		},
		hlog.AccessHandler(func(r *http.Request, status, size int, dur time.Duration) {
			logger := hlog.FromRequest(r)
			fields := func(ev *zerolog.Event) *zerolog.Event {
				return ev.
					Str("method", r.Method).
					Str("path", r.URL.Path).
					Int("status", status).
					Dur("duration", dur).
					Int("size", size).
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
		}),
	}
}

// isMutating reports whether a method writes state. Read methods bypass
// the CSRF check; writes are gated.
func isMutating(method string) bool {
	switch method {
	case http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
		return true
	}

	return false
}

// requireDiscoveryMiddleware short-circuits routes that need a configured
// discovery service. Returns 503 (text/plain) when discovery is disabled
// so all /partials/discovered/* endpoints respond uniformly without each
// handler duplicating the check.
func requireDiscoveryMiddleware(enabled func() bool) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !enabled() {
				http.Error(w, "discovery not configured (start the server with --s3-source)", http.StatusServiceUnavailable)

				return
			}
			next.ServeHTTP(w, r)
		})
	}
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
			hlog.FromRequest(r).Warn().
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
