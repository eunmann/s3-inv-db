package server

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

// setupRoutes configures all HTTP routes.
func (s *Server) setupRoutes() {
	r := s.router

	// Global middleware
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(loggingMiddleware(s.config.Logger))
	r.Use(middleware.Recoverer)

	// HTML pages
	r.Get("/", s.handlers.Dashboard)
	r.Get("/inventories", s.handlers.InventoriesPage)
	r.Get("/browse", s.handlers.BrowsePage)
	// /stats redirects to /browse so old bookmarks still resolve.
	r.Get("/stats", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/browse", http.StatusMovedPermanently)
	})

	// HTMX partials
	r.Get("/partials/browse-level", s.handlers.BrowseLevelPartial)
	r.Get("/partials/inventory-row/{id}", s.handlers.InventoryRowPartial)

	// API routes
	r.Route("/api", func(r chi.Router) {
		r.Use(jsonContentType)

		// Inventory state (loaded/unloaded view). The POST endpoint is
		// not surfaced in the UI; kept for tests and direct-path callers.
		r.Get("/inventories", s.handlers.ListInventoriesAPI)
		r.Post("/inventories", s.handlers.RegisterInventoryAPI)
		r.Get("/inventories/{id}", s.handlers.GetInventoryAPI)
		r.Post("/inventories/{id}/load", s.handlers.LoadInventoryAPI)
		r.Post("/inventories/{id}/unload", s.handlers.UnloadInventoryAPI)
		r.Delete("/inventories/{id}", s.handlers.DeleteInventoryAPI)

		// Discovery — S3 is the source of truth for what can be loaded.
		r.Get("/discovered", s.handlers.ListDiscoveredAPI)
		r.Post("/discovered/{src}/{id}/load", s.handlers.LoadDiscoveredAPI)
		r.Post("/discovered/{src}/{id}/unload", s.handlers.UnloadDiscoveredAPI)
		r.Delete("/discovered/{src}/{id}", s.handlers.EvictDiscoveredAPI)

		// Stats queries
		r.Get("/stats", s.handlers.GetStatsAPI)
		r.Get("/inventories/{id}/stats", s.handlers.GetInventoryStatsAPI)
		r.Get("/inventories/{id}/descendants", s.handlers.GetDescendantsAPI)
	})
}
