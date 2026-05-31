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
	MaxConcurrentJobs        int
	AutoLoadRetentionDefault uint32

	// DiscoveryRefreshInterval governs how often the background
	// discovery refresher rebuilds the cached snapshot the HTTP
	// handlers serve. Zero means use DefaultDiscoveryRefreshInterval.
	DiscoveryRefreshInterval time.Duration

	// QueryBatchMax caps the number of prefixes accepted in one batch
	// stats request. Zero means use the handler default.
	QueryBatchMax int
}

// DefaultDiscoveryRefreshInterval is the fallback cadence for the
// discovery snapshot refresher. The dashboard reads the snapshot,
// so this value sets the page-load freshness ceiling.
const DefaultDiscoveryRefreshInterval = time.Minute

// Server is the HTTP server.
type Server struct {
	config      Config
	router      *chi.Mux
	manager     *inventory.Catalog
	invStore    *inventory.Store
	configStore *inventory.ConfigStore
	jobStore    *jobs.Store
	jobMgr      *jobs.Scheduler
	bldr        *loader.Loader
	tracker     *budget.Tracker
	autoloader  *autoload.AutoLoader
	discovery   *inventory.Discovery
	handlers    *handlers.Handlers
	server      *http.Server
}

// New creates a new server. The ctx bounds any startup-time I/O the
// discovery wiring needs (the S3 client probe in particular).
func New(ctx context.Context, cfg Config) (*Server, error) {
	if cfg.DB == nil {
		return nil, errNoDB
	}
	stores, err := newStores(cfg)
	if err != nil {
		return nil, err
	}

	renderer, err := templates.New()
	if err != nil {
		return nil, fmt.Errorf("create renderer: %w", err)
	}

	tracker := budget.New(cfg.MaxIndexDisk, cfg.IndexHeadroomBytes)
	planner := budget.NewPlanner(tracker, retentionLookup(stores.config, cfg.AutoLoadRetentionDefault))
	gate := loadcontrol.New(stores.catalog, tracker, planner)

	discovery, bldr, err := newDiscovery(ctx, cfg, stores.catalog, gate)
	if err != nil {
		return nil, err
	}

	h := handlers.New(
		stores.catalog, renderer, cfg.PriceTable,
		handlers.Deps{
			JobMgr:      stores.jobs,
			JobStore:    stores.jobStore,
			JobBus:      stores.jobBus,
			ConfigStore: stores.config,
			Tracker:     tracker,
		},
		buildHandlerOptions(cfg, discovery, bldr)...,
	)

	s := &Server{
		config:      cfg,
		router:      chi.NewRouter(),
		manager:     stores.catalog,
		invStore:    stores.inv,
		configStore: stores.config,
		jobStore:    stores.jobStore,
		jobMgr:      stores.jobs,
		bldr:        bldr,
		tracker:     tracker,
		autoloader:  newAutoLoader(cfg, discovery, stores.config, stores.catalog, stores.jobs),
		discovery:   discovery,
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

// serverStores bundles the persistence-backed singletons New constructs
// from cfg.DB. Pulled out so New stays readable and the wiring is
// testable in isolation.
type serverStores struct {
	inv      *inventory.Store
	config   *inventory.ConfigStore
	jobStore *jobs.Store
	jobBus   *jobs.Bus
	jobs     *jobs.Scheduler
	catalog  *inventory.Catalog
}

func newStores(cfg Config) (serverStores, error) {
	invStore, err := inventory.NewStore(cfg.DB)
	if err != nil {
		return serverStores{}, fmt.Errorf("inventory store: %w", err)
	}
	jobStore := jobs.NewStore(cfg.DB)
	jobBus := jobs.NewBus(jobBusBuffer)

	return serverStores{
		inv:      invStore,
		config:   inventory.NewConfigStore(cfg.DB),
		jobStore: jobStore,
		jobBus:   jobBus,
		jobs:     jobs.NewScheduler(jobStore, jobBus, jobs.WithLogger(cfg.Logger), jobs.WithMaxConcurrency(cfg.MaxConcurrentJobs)),
		catalog:  inventory.NewCatalog(invStore),
	}, nil
}

// newDiscovery wires the S3-side of the server. Returns a disabled
// Discovery (and nil loader) when --s3-source is unset.
func newDiscovery(ctx context.Context, cfg Config, catalog *inventory.Catalog, gate *loadcontrol.Gate) (*inventory.Discovery, *loader.Loader, error) {
	if cfg.S3Source == "" {
		return inventory.NewDiscovery(catalog), nil, nil
	}
	wiring, err := newDiscoveryWiring(ctx, cfg)
	if err != nil {
		return nil, nil, err
	}
	sizer := loadcontrol.NewManifestSizer(wiring.Client)
	disc := inventory.NewDiscovery(catalog,
		inventory.WithBackend(wiring.Discoverer, wiring.Loader),
		inventory.WithGate(gate.Load, sizer.ManifestSize, inventory.DefaultIndexRatio),
	)
	cfg.Logger.Info().
		Str("s3_source", cfg.S3Source).
		Str("cache_dir", cfg.CacheDir).
		Uint64("max_index_disk_bytes", cfg.MaxIndexDisk).
		Msg("discovery + budget configured")

	return disc, wiring.Loader, nil
}

func buildHandlerOptions(cfg Config, discovery *inventory.Discovery, bldr *loader.Loader) []handlers.Option {
	refreshInterval := cfg.DiscoveryRefreshInterval
	if refreshInterval <= 0 {
		refreshInterval = DefaultDiscoveryRefreshInterval
	}
	hopts := []handlers.Option{
		handlers.WithDiscovery(discovery),
		handlers.WithDiscoveryRefreshInterval(refreshInterval),
	}
	addIf := func(cond bool, opts ...handlers.Option) {
		if cond {
			hopts = append(hopts, opts...)
		}
	}
	addIf(bldr != nil, handlers.WithLoader(bldr), handlers.WithCacheStore(bldr))
	addIf(cfg.S3Source != "", handlers.WithS3Source(cfg.S3Source))
	addIf(cfg.QueryBatchMax > 0, handlers.WithQueryBatchMax(cfg.QueryBatchMax))

	return hopts
}

func newAutoLoader(cfg Config, discovery *inventory.Discovery, configStore *inventory.ConfigStore, catalog *inventory.Catalog, submitter autoload.Submitter) *autoload.AutoLoader {
	if !cfg.AutoLoad || !discovery.Enabled() {
		return nil
	}
	loadFn := func(c context.Context, d inventory.Inventory, onProgress func(stage string, done, total int64)) error {
		return discovery.AutoLoadWith(c, d, onProgress)
	}
	al := autoload.New(autoload.Config{
		PollInterval:     cfg.PollInterval,
		DefaultRetention: cfg.AutoLoadRetentionDefault,
	}, autoload.Deps{
		Discovery:   discovery,
		Loader:      loadFn,
		Submitter:   submitter,
		ConfigStore: configStore,
		Manager:     catalog,
	}, &cfg.Logger)
	cfg.Logger.Info().Msg("auto-loader configured")

	return al
}

// jobBusBuffer is the capacity of the job-events bus the SSE handler
// drains. Sized for headroom between successive UI ticks.
const jobBusBuffer = 64

// retentionLookup returns a budget.RetentionFunc that resolves
// per-configuration retention from the ConfigStore, falling back to
// the server-wide default when the store has no row.
func retentionLookup(store *inventory.ConfigStore, fallback uint32) budget.RetentionFunc {
	return func(ctx context.Context, source, name string) uint32 {
		cfg, err := store.Get(ctx, source, name)
		if err == nil && cfg.RetentionCount > 0 {
			return cfg.RetentionCount
		}
		if fallback > 0 {
			return fallback
		}

		return 0
	}
}

// backfillTracker walks every currently-loaded inventory and attributes
// its on-disk size to the budget tracker so post-restart eviction
// decisions see an accurate baseline.
func (s *Server) backfillTracker(ctx context.Context, logger zerolog.Logger) {
	if s.bldr == nil {
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
			dir := s.bldr.CacheDirFor(inventory.CacheKey{
				SourceBucket: p.Source,
				InventoryID:  p.Inventory,
				Run:          p.Run,
			})
			if measured, err := inventory.MeasureDir(ctx, dir); err == nil {
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

// recover rehydrates the in-memory inventory.Catalog from invStore and
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
				indexDir = s.bldr.CacheDirFor(inventory.CacheKey{
					SourceBucket: p.Source,
					InventoryID:  p.Inventory,
					Run:          p.Run,
				})
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

// HTTP server timeouts. WriteTimeout is intentionally omitted: SSE
// endpoints (/api/jobs/stream) stream indefinitely. Slow-loris is
// blocked by ReadHeaderTimeout + ReadTimeout instead.
const (
	readHeaderTimeout    = 10 * time.Second
	readTimeout          = 30 * time.Second
	idleTimeout          = 60 * time.Second
	maxHeaderBytes       = 1 << 20 // 1 MiB
	shutdownDrainTimeout = 10 * time.Second
	resourceDrainTimeout = 5 * time.Second
)

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

// newHTTPServer builds the *http.Server with the project-wide timeout
// hardening. Extracted so tests can assert the timeout shape without
// racing against Run's goroutine assignment to s.server.
func (s *Server) newHTTPServer() *http.Server {
	return &http.Server{
		Addr:              s.config.Addr,
		Handler:           s.router,
		ReadHeaderTimeout: readHeaderTimeout,
		ReadTimeout:       readTimeout,
		IdleTimeout:       idleTimeout,
		MaxHeaderBytes:    maxHeaderBytes,
	}
}

// Run starts the HTTP server and blocks until the context is cancelled.
func (s *Server) Run(ctx context.Context) error {
	s.server = s.newHTTPServer()

	// On every exit path: cancel in-flight jobs (so goroutines don't
	// outlive the DB they write to), then close the inventory manager
	// so mmaps and file handles are released. The shutdown context is
	// detached from ctx — if ctx is already cancelled (the usual
	// shutdown trigger), we still need a few seconds to drain workers.
	defer s.shutdownResources(ctx)

	if s.discovery.Enabled() {
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
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownDrainTimeout)
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
	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), resourceDrainTimeout)
	defer cancel()
	if s.autoloader != nil {
		s.autoloader.Stop()
	}
	s.discovery.Stop()
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

	return DefaultDiscoveryRefreshInterval
}
