// Package server provides the HTTP server for the S3 inventory service.
package server

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/eunmann/s3-inv-db/internal/handlers"
	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/internal/templates"
	"github.com/eunmann/s3-inv-db/pkg/pricing"
	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog"
)

// Config holds server configuration.
type Config struct {
	Addr       string
	Logger     zerolog.Logger
	DevMode    bool
	PriceTable pricing.PriceTable
}

// Server is the HTTP server.
type Server struct {
	config   Config
	router   chi.Router
	manager  *inventory.Manager
	renderer *templates.Renderer
	handlers *handlers.Handlers
	server   *http.Server
}

// New creates a new server.
func New(cfg Config) (*Server, error) {
	mgr := inventory.NewManager()

	renderer, err := templates.New(cfg.DevMode)
	if err != nil {
		return nil, fmt.Errorf("create renderer: %w", err)
	}

	h := handlers.New(mgr, renderer, cfg.PriceTable, cfg.Logger)

	s := &Server{
		config:   cfg,
		router:   chi.NewRouter(),
		manager:  mgr,
		renderer: renderer,
		handlers: h,
	}

	s.setupRoutes()

	return s, nil
}

// Run starts the HTTP server and blocks until the context is cancelled.
func (s *Server) Run(ctx context.Context) error {
	s.server = &http.Server{
		Addr:              s.config.Addr,
		Handler:           s.router,
		ReadHeaderTimeout: 10 * time.Second,
	}

	errChan := make(chan error, 1)
	go func() {
		s.config.Logger.Info().Str("addr", s.config.Addr).Msg("starting HTTP server")
		if err := s.server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errChan <- err
		}
		close(errChan)
	}()

	select {
	case <-ctx.Done():
		s.config.Logger.Info().Msg("shutting down HTTP server")
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()

		if err := s.server.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("shutdown: %w", err)
		}

		// Close the inventory manager
		if err := s.manager.Close(); err != nil {
			s.config.Logger.Error().Err(err).Msg("failed to close inventory manager")
		}

		return fmt.Errorf("context cancelled: %w", ctx.Err())

	case err := <-errChan:
		return err
	}
}

// Router returns the chi router for testing.
func (s *Server) Router() chi.Router {
	return s.router
}

// Manager returns the inventory manager.
func (s *Server) Manager() *inventory.Manager {
	return s.manager
}
