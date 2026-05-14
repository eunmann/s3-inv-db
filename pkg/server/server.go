// Package server provides the HTTP server for the S3 inventory service.
package server

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/eunmann/s3-inv-db/internal/handlers"
	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/internal/jobs"
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
	PriceTable pricing.PriceTable
	// S3Source is the s3:// URI under which inventories are discovered.
	// When empty, the discovery API returns an empty list and the
	// /api/discovered routes still work (just with nothing to show).
	S3Source string
	// CacheDir is the local directory where built indexes are written.
	// Required when S3Source is set.
	CacheDir string
	// DB is the shared SQLite handle for domain Stores. Each domain
	// (inventory, jobs, downloads) constructs its Store from this.
	DB *sql.DB
}

// Server is the HTTP server.
type Server struct {
	config   Config
	router   chi.Router
	manager  *inventory.Manager
	invStore *inventory.Store
	jobStore *jobs.Store
	jobBus   *jobs.Bus
	jobMgr   *jobs.Manager
	bldr     *loader.Loader
	renderer *templates.Renderer
	handlers *handlers.Handlers
	server   *http.Server
}

// New creates a new server.
func New(cfg Config) (*Server, error) {
	if cfg.DB == nil {
		return nil, errNoDB
	}
	invStore, err := inventory.NewStore(cfg.DB)
	if err != nil {
		return nil, fmt.Errorf("inventory store: %w", err)
	}
	jobStore, err := jobs.NewStore(cfg.DB)
	if err != nil {
		return nil, fmt.Errorf("jobs store: %w", err)
	}
	jobBus := jobs.NewBus(64)
	jobMgr := jobs.NewManager(jobStore, jobBus)
	jobMgr.SetLogger(cfg.Logger)
	mgr := inventory.NewManager()
	mgr.SetStore(invStore)

	renderer, err := templates.New()
	if err != nil {
		return nil, fmt.Errorf("create renderer: %w", err)
	}

	hcfg := handlers.Config{
		Manager:    mgr,
		Renderer:   renderer,
		PriceTable: cfg.PriceTable,
		JobMgr:     jobMgr,
		JobStore:   jobStore,
		JobBus:     jobBus,
	}
	var bldr *loader.Loader
	if cfg.S3Source != "" {
		disco, ldr, err := newDiscoveryWiring(cfg)
		if err != nil {
			return nil, err
		}
		hcfg.Discoverer = disco
		hcfg.Loader = ldr
		hcfg.S3SourceURI = cfg.S3Source
		bldr = ldr
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
		invStore: invStore,
		jobStore: jobStore,
		jobBus:   jobBus,
		jobMgr:   jobMgr,
		bldr:     bldr,
		renderer: renderer,
		handlers: h,
	}

	s.setupRoutes()
	s.recover(cfg.Logger)

	return s, nil
}

// recover rehydrates the in-memory inventory.Manager from invStore and
// marks any jobs left running/queued by the previous process as
// aborted. Inventories left in the parsing state — meaning a load was
// in flight when the previous process exited — get flipped to error so
// the UI shows a Retry. Best-effort: failures are logged, not returned.
func (s *Server) recover(logger zerolog.Logger) {
	n, err := s.jobStore.MarkAborted("server restart", jobs.StateQueued, jobs.StateRunning)
	if err != nil {
		logger.Error().Err(err).Msg("mark stale jobs aborted")
	} else if n > 0 {
		logger.Info().Int64("count", n).Msg("aborted stale jobs from previous run")
	}

	infos, err := s.invStore.List()
	if err != nil {
		logger.Error().Err(err).Msg("list inventories at startup")
		return
	}
	for i := range infos {
		info := &infos[i]
		// Stale parsing → error so the row shows Retry instead of a
		// spinner that will never resolve.
		if info.State == inventory.StateLoading {
			info.State = inventory.StateError
			info.Error = "interrupted by server restart"
		}
		indexDir := ""
		if info.State == inventory.StateLoaded && s.bldr != nil {
			// Older entries written before per-run cache layout had two
			// segments; treat those as un-hydratable so the user sees
			// the error and can rebuild.
			if src, inv, run, ok := info.ID.Split(); ok {
				indexDir = s.bldr.CacheDirFor(src, inv, run)
			}
		}
		if err := s.manager.Hydrate(*info, indexDir); err != nil {
			logger.Error().Err(err).Stringer("id", info.ID).Msg("hydrate inventory")
			continue
		}
		final, _ := s.manager.Get(info.ID)
		logger.Info().Stringer("id", final.ID).Str("state", string(final.State)).Msg("hydrated inventory")
		// Manager.Hydrate already mirrors to invStore (via SetStore),
		// so no explicit Upsert is required here — but for the
		// StateLoading→StateError flip we performed above the input,
		// Hydrate's mirror already wrote the corrected state.
	}
}

var (
	errEmptyCacheDir = errors.New("CacheDir is required when S3Source is set")
	errNoDB          = errors.New("server.Config.DB is required")
)

// s3StartupTimeout caps how long s3fetch.NewClient may spend doing
// region/STS probes during server startup. 30s is generous enough for
// a real network round-trip and tight enough that a misconfigured
// endpoint fails fast.
const s3StartupTimeout = 30 * time.Second

// newDiscoveryWiring constructs the S3 client, discoverer, and loader
// for a server configured with --s3-source. Extracted from New so the
// happy path stays readable and so the wiring is testable in isolation.
func newDiscoveryWiring(cfg Config) (*s3disco.Discoverer, *loader.Loader, error) {
	if cfg.CacheDir == "" {
		return nil, nil, errEmptyCacheDir
	}
	ctx, cancel := context.WithTimeout(context.Background(), s3StartupTimeout)
	defer cancel()
	s3Client, err := s3fetch.NewClient(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("s3 client: %w", err)
	}
	disco, err := s3disco.NewFromS3URI(s3Client.Raw(), cfg.S3Source)
	if err != nil {
		return nil, nil, fmt.Errorf("discovery from %q: %w", cfg.S3Source, err)
	}
	if err := os.MkdirAll(cfg.CacheDir, 0o755); err != nil {
		return nil, nil, fmt.Errorf("ensure cache dir %s: %w", cfg.CacheDir, err)
	}
	return disco, loader.New(cfg.CacheDir, s3Client), nil
}

// Run starts the HTTP server and blocks until the context is cancelled.
func (s *Server) Run(ctx context.Context) error {
	s.server = &http.Server{
		Addr:              s.config.Addr,
		Handler:           s.router,
		ReadHeaderTimeout: 10 * time.Second,
	}

	// On every exit path: cancel in-flight jobs (so goroutines don't
	// outlive the DB they write to), then close the inventory manager
	// so mmaps and file handles are released. The shutdown context is
	// detached from ctx — if ctx is already cancelled (the usual
	// shutdown trigger), we still need a few seconds to drain workers.
	defer s.shutdownResources() //nolint:contextcheck // fresh ctx by design

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

// shutdownResources cancels live jobs (waiting up to 5s) and closes
// the inventory manager. Called from Run's defer, deliberately using a
// fresh ctx because the parent is typically the one that's been
// cancelled to trigger this exit.
func (s *Server) shutdownResources() {
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.jobMgr.Shutdown(shutdownCtx); err != nil {
		s.config.Logger.Error().Err(err).Msg("shutdown job manager")
	}
	if err := s.manager.Close(); err != nil {
		s.config.Logger.Error().Err(err).Msg("close inventory manager")
	}
}

// Router returns the underlying chi router. Useful for mounting the
// server onto another router (e.g., behind a path prefix) or for writing
// tests that drive handlers via httptest without binding a port.
func (s *Server) Router() chi.Router {
	return s.router
}
