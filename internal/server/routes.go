package server

import (
	"net/http"

	"github.com/eunmann/s3-inv-db/internal/templates"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

// setupRoutes configures all HTTP routes.
func (s *Server) setupRoutes() {
	r := s.router

	// Global middleware. RequestID must come first so the request_id is
	// in ctx before hlog reads it. hlog provides the request-scoped
	// logger + access log; Recoverer goes BEFORE the access log so a
	// recovered panic still results in an access-log line. sameOrigin
	// blocks cross-origin mutating browser requests (CSRF).
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Recoverer)
	for _, mw := range hlogChain(s.config.Logger) {
		r.Use(mw)
	}
	r.Use(sameOriginMiddleware)

	// Liveness probe — independent of S3 / discovery / inventory state so
	// container orchestrators can tell the process is up.
	r.Get("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	// Compiled Tailwind CSS embedded into the binary. ETag lets the
	// browser revalidate cheaply; the content is immutable for a given
	// build, so a 1-day max-age + revalidate is fine.
	r.Get("/static/tailwind.css", func(w http.ResponseWriter, r *http.Request) {
		if match := r.Header.Get("If-None-Match"); match == templates.TailwindCSSETag {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("Content-Type", "text/css; charset=utf-8")
		w.Header().Set("ETag", templates.TailwindCSSETag)
		w.Header().Set("Cache-Control", "public, max-age=86400, must-revalidate")
		_, _ = w.Write(templates.TailwindCSS())
	})

	// HTML pages
	r.Get("/", s.handlers.Dashboard)
	r.Get("/inventories", s.handlers.InventoriesPage)
	r.Get("/browse", s.handlers.BrowsePage)
	// /stats redirects to /browse so old bookmarks still resolve.
	r.Get("/stats", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/browse", http.StatusMovedPermanently)
	})

	// HTMX partials. Mutating partial routes return updated row HTML so
	// the page never needs a JSON round-trip + reload. The Browse page
	// itself content-negotiates via the HX-Request header rather than
	// living at a separate /partials/ URL.
	r.Get("/partials/inventory-row/{id}", s.handlers.InventoryRowPartial)
	r.Post("/partials/inventories/{id}/load", s.handlers.LoadInventoryRowPartial)
	r.Post("/partials/inventories/{id}/unload", s.handlers.UnloadInventoryRowPartial)
	r.Delete("/partials/inventories/{id}", s.handlers.DeleteInventoryRowPartial)
	// Discovery-dependent partials. The group-scoped middleware
	// short-circuits with 503 when --s3-source is not configured, so
	// every route in here can assume Enabled() == true.
	r.Group(func(r chi.Router) {
		r.Use(requireDiscoveryMiddleware(s.handlers.DiscoveryEnabled))
		r.Get("/partials/discovered/{src}/{id}", s.handlers.DiscoveredRowPartial)
		r.Post("/partials/discovered/{src}/{id}/load", s.handlers.LoadDiscoveredRowPartial)
		r.Post("/partials/discovered/{src}/{id}/unload", s.handlers.UnloadDiscoveredRowPartial)
		r.Delete("/partials/discovered/{src}/{id}", s.handlers.EvictDiscoveredRowPartial)
	})

	// API routes
	r.Route("/api", func(r chi.Router) {
		// Job stream — SSE, one event per job state change.
		r.Get("/jobs/stream", s.handlers.JobsStream)
		r.Post("/jobs/{id}/cancel", s.handlers.CancelJob)

		// Inventory state (loaded/unloaded view). The POST endpoint is
		// not surfaced in the UI; kept for tests and direct-path callers.
		r.Get("/inventories", s.handlers.ListInventoriesAPI)
		r.Post("/inventories", s.handlers.RegisterInventoryAPI)
		r.Get("/inventories/{id}", s.handlers.GetInventoryAPI)
		r.Post("/inventories/{id}/load", s.handlers.LoadInventoryAPI)
		r.Post("/inventories/{id}/unload", s.handlers.UnloadInventoryAPI)
		r.Delete("/inventories/{id}", s.handlers.DeleteInventoryAPI)

		// Discovery — read-only listing. Mutating operations live on
		// the /partials/discovered/* routes and return HTML directly.
		r.Get("/discovered", s.handlers.ListDiscoveredAPI)

		// Stats queries
		r.Get("/stats", s.handlers.GetStatsAPI)
		r.Get("/inventories/{id}/stats", s.handlers.GetInventoryStatsAPI)
		r.Get("/inventories/{id}/descendants", s.handlers.GetDescendantsAPI)
	})
}
