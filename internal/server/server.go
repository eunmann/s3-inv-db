package server

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/eunmann/s3-inv-db/internal/autoload"
	"github.com/eunmann/s3-inv-db/internal/budget"
	"github.com/eunmann/s3-inv-db/internal/handlers"
	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/internal/jobs"
	"github.com/eunmann/s3-inv-db/internal/loadcontrol"
	"github.com/eunmann/s3-inv-db/internal/loader"
	"github.com/eunmann/s3-inv-db/internal/s3disco"
	"github.com/eunmann/s3-inv-db/internal/templates"
	"github.com/eunmann/s3-inv-db/pkg/format"
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
	// (inventory, jobs) constructs its Store from this.
	DB *sql.DB

	// AutoLoad and friends govern the background poller and budget
	// gate. When AutoLoad is true, MaxIndexDisk must be > 0.
	AutoLoad                 bool
	PollInterval             time.Duration
	MaxIndexDisk             uint64
	IndexHeadroomBytes       uint64
	AutoLoadConcurrency      int
	AutoLoadRetentionDefault uint32
	IndexRatio               float64

	// DiscoveryRefreshInterval governs how often the background
	// discovery refresher rebuilds the cached snapshot the HTTP
	// handlers serve. Zero means use defaultDiscoveryRefreshInterval.
	DiscoveryRefreshInterval time.Duration
}

// defaultDiscoveryRefreshInterval is the fallback cadence for the
// discovery snapshot refresher. The dashboard reads the snapshot, so
// the value sets the page-load freshness ceiling.
const defaultDiscoveryRefreshInterval = time.Minute

// Server is the HTTP server.
type Server struct {
	config      Config
	router      *chi.Mux
	manager     *inventory.Manager
	invStore    *inventory.Store
	configStore *inventory.ConfigStore
	jobStore    *jobs.Store
	jobBus      *jobs.Bus
	jobMgr      *jobs.Manager
	bldr        *loader.Loader
	tracker     *budget.Tracker
	autoloader  *autoload.AutoLoader
	discovery   *inventory.DiscoveryService
	renderer    *templates.Renderer
	handlers    *handlers.Handlers
	server      *http.Server
}

// New creates a new server. The ctx bounds any startup-time I/O the
// discovery wiring needs (the S3 client probe in particular).
func New(ctx context.Context, cfg Config) (*Server, error) {
	if cfg.DB == nil {
		return nil, errNoDB
	}
	invStore, err := inventory.NewStore(cfg.DB)
	if err != nil {
		return nil, fmt.Errorf("inventory store: %w", err)
	}
	configStore := inventory.NewConfigStore(cfg.DB)
	jobStore := jobs.NewStore(cfg.DB)
	jobBus := jobs.NewBus(64)
	jobMgr := jobs.NewManager(jobStore, jobBus)
	jobMgr.SetLogger(cfg.Logger)
	mgr := inventory.NewManager()
	mgr.SetStore(invStore)

	renderer, err := templates.New()
	if err != nil {
		return nil, fmt.Errorf("create renderer: %w", err)
	}

	tracker := budget.New(cfg.MaxIndexDisk, cfg.IndexHeadroomBytes)
	retentionLookup := configRetention{store: configStore, fallback: cfg.AutoLoadRetentionDefault}
	planner := budget.NewPlanner(tracker, retentionLookup)
	gate := loadcontrol.New(mgr, tracker, planner)

	var discovery *inventory.DiscoveryService
	var bldr *loader.Loader
	var s3Client *s3fetch.Client

	if cfg.S3Source != "" {
		wiring, err := newDiscoveryWiring(ctx, cfg)
		if err != nil {
			return nil, err
		}
		bldr = wiring.Loader
		s3Client = wiring.Client
		discovery = inventory.NewDiscoveryService(mgr, wiring.Discoverer, wiring.Loader)
		sizer := loadcontrol.NewManifestSizer(s3Client)
		discovery.SetGate(gate, sizer, cfg.IndexRatio)
		cfg.Logger.Info().
			Str("s3_source", cfg.S3Source).
			Str("cache_dir", cfg.CacheDir).
			Uint64("max_index_disk_bytes", cfg.MaxIndexDisk).
			Msg("discovery + budget configured")
	} else {
		discovery = inventory.NewDiscoveryService(mgr, nil, nil)
	}

	hcfg := handlers.Config{
		Manager:     mgr,
		Renderer:    renderer,
		PriceTable:  cfg.PriceTable,
		JobMgr:      jobMgr,
		JobStore:    jobStore,
		JobBus:      jobBus,
		Discovery:   discovery,
		Loader:      bldr,
		ConfigStore: configStore,
		Tracker:     tracker,
	}
	if cfg.S3Source != "" {
		hcfg.S3SourceURI = cfg.S3Source
	}

	h := handlers.NewWithConfig(hcfg)

	var al *autoload.AutoLoader
	if cfg.AutoLoad && discovery != nil && discovery.Enabled() {
		al = autoload.New(autoload.Config{
			PollInterval:     cfg.PollInterval,
			MaxConcurrency:   cfg.AutoLoadConcurrency,
			DefaultRetention: cfg.AutoLoadRetentionDefault,
		}, discovery, func(c context.Context, d inventory.Inventory) error {
			return discovery.AutoLoadWith(c, d, nil)
		}, configStore, mgr, &cfg.Logger)
		cfg.Logger.Info().Msg("auto-loader configured")
	}

	s := &Server{
		config:      cfg,
		router:      chi.NewRouter(),
		manager:     mgr,
		invStore:    invStore,
		configStore: configStore,
		jobStore:    jobStore,
		jobBus:      jobBus,
		jobMgr:      jobMgr,
		bldr:        bldr,
		tracker:     tracker,
		autoloader:  al,
		discovery:   discovery,
		renderer:    renderer,
		handlers:    h,
	}

	s.setupRoutes()
	// Startup-time recovery/backfill should complete even if the caller's
	// ctx is later cancelled — context.WithoutCancel inherits any
	// values (logger, request id) but drops cancellation.
	startupCtx := context.WithoutCancel(ctx)
	s.recover(startupCtx, cfg.Logger)
	s.backfillTracker(startupCtx, cfg.Logger)

	return s, nil
}

// configRetention adapts ConfigStore to budget.Config so the Planner
// can look up per-configuration retention without depending on the
// inventory package's persistence types.
type configRetention struct {
	store    *inventory.ConfigStore
	fallback uint32
}

func (c configRetention) Retention(source, name string) uint32 {
	// The budget.Config interface intentionally has no ctx — eviction
	// planning is a quick local lookup. Use a bounded background ctx so
	// the SQL call still goes through ConfigStore's *Context variants.
	cfg, err := c.store.Get(context.Background(), source, name)
	if err == nil && cfg.RetentionCount > 0 {
		return cfg.RetentionCount
	}
	if c.fallback > 0 {
		return c.fallback
	}

	return 0
}

// backfillTracker walks every currently-loaded inventory and attributes
// its on-disk size to the budget tracker so post-restart eviction
// decisions see an accurate baseline.
func (s *Server) backfillTracker(ctx context.Context, logger zerolog.Logger) {
	if s.tracker == nil || s.bldr == nil {
		return
	}
	list := s.manager.List()
	for i := range list {
		info := &list[i]
		if info.State != inventory.StateLoaded {
			continue
		}
		bytes := info.IndexBytes
		if bytes == 0 {
			p := info.ID.Split()
			if !p.OK {
				continue
			}
			dir := s.bldr.CacheDirFor(p.Source, p.Inventory, p.Run)
			if measured, err := budget.MeasureDir(ctx, dir); err == nil {
				bytes = measured
				updated := *info
				updated.IndexBytes = bytes
				_ = s.invStore.Upsert(ctx, updated)
			} else {
				logger.Warn().Stringer("id", info.ID).Err(err).Msg("backfill index size")
			}
		}
		s.tracker.Add(bytes)
	}
}

// recover rehydrates the in-memory inventory.Manager from invStore and
// marks any jobs left running/queued by the previous process as
// aborted. Inventories left in the parsing state — meaning a load was
// in flight when the previous process exited — get flipped to error so
// the UI shows a Retry. Best-effort: failures are logged, not returned.
func (s *Server) recover(ctx context.Context, logger zerolog.Logger) {
	n, err := s.jobStore.MarkAborted(ctx, "server restart", jobs.StateQueued, jobs.StateRunning)
	if err != nil {
		logger.Error().Err(err).Msg("mark stale jobs aborted")
	} else if n > 0 {
		logger.Info().Int64("count", n).Msg("aborted stale jobs from previous run")
	}

	infos, err := s.invStore.List(ctx)
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
			if p := info.ID.Split(); p.OK {
				indexDir = s.bldr.CacheDirFor(p.Source, p.Inventory, p.Run)
			}
		}
		if err := s.manager.Hydrate(ctx, *info, indexDir); err != nil {
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

// format.DirPerm is the directory mode used when ensuring the on-disk
// inventory cache directory exists. 0o750 satisfies gosec G301.

// discoveryWiring bundles the three values newDiscoveryWiring builds so
// callers don't end up with a 4-arity return that invites `_, _, _, err`.
type discoveryWiring struct {
	Discoverer *s3disco.Discoverer
	Loader     *loader.Loader
	Client     *s3fetch.Client
}

// newDiscoveryWiring constructs the S3 client, discoverer, and loader
// for a server configured with --s3-source. Extracted from New so the
// happy path stays readable and so the wiring is testable in isolation.
func newDiscoveryWiring(ctx context.Context, cfg Config) (discoveryWiring, error) {
	if cfg.CacheDir == "" {
		return discoveryWiring{}, errEmptyCacheDir
	}
	probeCtx, cancel := context.WithTimeout(ctx, s3StartupTimeout)
	defer cancel()
	s3Client, err := s3fetch.NewClient(probeCtx)
	if err != nil {
		return discoveryWiring{}, fmt.Errorf("s3 client: %w", err)
	}
	disco, err := s3disco.NewFromS3URI(s3Client.Raw(), cfg.S3Source)
	if err != nil {
		return discoveryWiring{}, fmt.Errorf("discovery from %q: %w", cfg.S3Source, err)
	}
	if err := os.MkdirAll(cfg.CacheDir, format.DirPerm); err != nil {
		return discoveryWiring{}, fmt.Errorf("ensure cache dir %s: %w", cfg.CacheDir, err)
	}

	return discoveryWiring{
		Discoverer: disco,
		Loader:     loader.New(cfg.CacheDir, s3Client),
		Client:     s3Client,
	}, nil
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
	defer s.shutdownResources(ctx)

	if s.discovery != nil && s.discovery.Enabled() {
		s.discovery.Start(ctx, s.discoveryRefreshInterval(), &s.config.Logger)
	}
	if s.autoloader != nil {
		s.autoloader.Start(ctx)
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

		return nil

	case err := <-errChan:
		return err
	}
}

// shutdownResources cancels live jobs (waiting up to 5s) and closes
// the inventory manager. Called from Run's defer.
//
// Context handling: the parent ctx is typically the one whose
// cancellation triggered this exit. To drain workers even after the
// run ctx has been cancelled, the shutdown ctx is rooted at
// context.WithoutCancel(parent) so any values (logger, request id)
// the parent carries are inherited while cancellation is dropped.
func (s *Server) shutdownResources(ctx context.Context) {
	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if s.autoloader != nil {
		s.autoloader.Stop()
	}
	if s.discovery != nil {
		s.discovery.Stop()
	}
	if err := s.jobMgr.Shutdown(shutdownCtx); err != nil {
		s.config.Logger.Error().Err(err).Msg("shutdown job manager")
	}
	if err := s.manager.Close(); err != nil {
		s.config.Logger.Error().Err(err).Msg("close inventory manager")
	}
}

// Router returns the underlying chi router for mounting or for tests
// that drive handlers via httptest. Returns *chi.Mux (the concrete
// type) rather than chi.Router (the interface) so ireturn stays
// happy; callers can still treat it as a chi.Router/http.Handler.
func (s *Server) Router() *chi.Mux {
	return s.router
}

func (s *Server) discoveryRefreshInterval() time.Duration {
	if s.config.DiscoveryRefreshInterval > 0 {
		return s.config.DiscoveryRefreshInterval
	}

	return defaultDiscoveryRefreshInterval
}
