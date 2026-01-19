package server

import (
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
	r.Get("/stats", s.handlers.StatsPage)

	// HTMX partials
	r.Get("/partials/stats-result", s.handlers.StatsResultPartial)
	r.Get("/partials/inventory-row/{id}", s.handlers.InventoryRowPartial)

	// API routes
	r.Route("/api", func(r chi.Router) {
		r.Use(jsonContentType)

		// Inventory management
		r.Get("/inventories", s.handlers.ListInventoriesAPI)
		r.Post("/inventories", s.handlers.RegisterInventoryAPI)
		r.Get("/inventories/{id}", s.handlers.GetInventoryAPI)
		r.Post("/inventories/{id}/load", s.handlers.LoadInventoryAPI)
		r.Post("/inventories/{id}/unload", s.handlers.UnloadInventoryAPI)
		r.Delete("/inventories/{id}", s.handlers.DeleteInventoryAPI)

		// Stats queries
		r.Get("/stats", s.handlers.GetStatsAPI)
		r.Get("/inventories/{id}/stats", s.handlers.GetInventoryStatsAPI)
		r.Get("/inventories/{id}/descendants", s.handlers.GetDescendantsAPI)
	})
}
