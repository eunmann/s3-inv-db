// Package server provides the HTTP server for the S3 inventory service.
package server

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/eunmann/s3-inv-db/internal/handlers"
	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/internal/loader"
	"github.com/eunmann/s3-inv-db/internal/s3disco"
	"github.com/eunmann/s3-inv-db/internal/templates"
	"github.com/eunmann/s3-inv-db/pkg/pricing"
	"github.com/eunmann/s3-inv-db/pkg/s3fetch"
	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog"
)

// Config holds server configuration.
type Config struct {
	Addr       string
	Logger     zerolog.Logger
	DevMode    bool
	PriceTable pricing.PriceTable
	// S3Source is the s3:// URI under which inventories are discovered.
	// When empty, the discovery API returns an empty list and the
	// /api/discovered routes still work (just with nothing to show).
	S3Source string
	// CacheDir is the local directory where built indexes are written.
	// Required when S3Source is set.
	CacheDir string
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

	hcfg := handlers.Config{
		Manager:    mgr,
		Renderer:   renderer,
		PriceTable: cfg.PriceTable,
		Logger:     cfg.Logger,
	}
	if cfg.S3Source != "" {
		s3Client, err := s3fetch.NewClient(context.Background())
		if err != nil {
			return nil, fmt.Errorf("s3 client: %w", err)
		}
		disco, err := s3disco.NewFromS3URI(s3Client.Raw(), cfg.S3Source)
		if err != nil {
			return nil, fmt.Errorf("discovery from %q: %w", cfg.S3Source, err)
		}
		if cfg.CacheDir == "" {
			return nil, errEmptyCacheDir
		}
		if err := os.MkdirAll(cfg.CacheDir, 0o755); err != nil {
			return nil, fmt.Errorf("ensure cache dir %s: %w", cfg.CacheDir, err)
		}
		hcfg.Discoverer = disco
		hcfg.Loader = loader.New(cfg.CacheDir, s3Client)
		hcfg.S3SourceURI = cfg.S3Source
		cfg.Logger.Info().
			Str("s3_source", cfg.S3Source).
			Str("cache_dir", cfg.CacheDir).
			Msg("discovery configured")
	}

	h := handlers.NewWithConfig(hcfg)

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

var errEmptyCacheDir = errors.New("CacheDir is required when S3Source is set")

// Run starts the HTTP server and blocks until the context is cancelled.
func (s *Server) Run(ctx context.Context) error {
	s.server = &http.Server{
		Addr:              s.config.Addr,
		Handler:           s.router,
		ReadHeaderTimeout: 10 * time.Second,
	}

	// Close the inventory manager on every exit path so mmaps and file
	// handles never outlive Run, even if Shutdown or ListenAndServe errors.
	defer func() {
		if err := s.manager.Close(); err != nil {
			s.config.Logger.Error().Err(err).Msg("failed to close inventory manager")
		}
	}()

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
		return nil

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
