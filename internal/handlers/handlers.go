package handlers

import (
	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/internal/jobs"
	"github.com/eunmann/s3-inv-db/internal/templates"
	"github.com/eunmann/s3-inv-db/pkg/pricing"
)

// Handlers contains all HTTP handlers and their dependencies. No logger
// field — handlers retrieve the request-scoped logger via
// zerolog.Ctx(r.Context()), set by the server's contextLoggerMiddleware.
//
// Domain operations on discovered inventories (list+merge, load, evict)
// go through `discovery` so the use-case logic lives in the inventory
// package rather than at the HTTP boundary.
type Handlers struct {
	manager     *inventory.Manager
	discovery   *inventory.DiscoveryService
	renderer    *templates.Renderer
	priceTable  pricing.PriceTable
	s3SourceURI string // for display in templates
	jobMgr      *jobs.Manager
	jobStore    *jobs.Store
	jobBus      *jobs.Bus
}

// Config gathers all Handlers dependencies for NewWithConfig. Discoverer
// and Loader take the narrow inventory.Discoverer / inventory.IndexBuilder
// interfaces so tests can wire fakes without spinning up MinIO. Production
// passes the concrete *s3disco.Discoverer and *loader.Loader pointers.
type Config struct {
	Manager     *inventory.Manager
	Renderer    *templates.Renderer
	PriceTable  pricing.PriceTable
	Discoverer  inventory.Discoverer
	Loader      inventory.IndexBuilder
	S3SourceURI string
	JobMgr      *jobs.Manager
	JobStore    *jobs.Store
	JobBus      *jobs.Bus
}

// New creates a Handlers instance without discovery wiring. Tests use it.
func New(mgr *inventory.Manager, renderer *templates.Renderer, priceTable pricing.PriceTable) *Handlers {
	return NewWithConfig(Config{
		Manager:    mgr,
		Renderer:   renderer,
		PriceTable: priceTable,
	})
}

// DiscoveryEnabled reports whether the wired DiscoveryService is usable.
// Exposed so the server can gate discovery-dependent routes via middleware
// rather than each handler duplicating the check.
func (h *Handlers) DiscoveryEnabled() bool { return h.discovery.Enabled() }

// NewWithConfig creates a Handlers wired with optional S3 discovery + loader.
func NewWithConfig(cfg Config) *Handlers {
	return &Handlers{
		manager:     cfg.Manager,
		discovery:   inventory.NewDiscoveryService(cfg.Manager, cfg.Discoverer, cfg.Loader),
		renderer:    cfg.Renderer,
		priceTable:  cfg.PriceTable,
		s3SourceURI: cfg.S3SourceURI,
		jobMgr:      cfg.JobMgr,
		jobStore:    cfg.JobStore,
		jobBus:      cfg.JobBus,
	}
}
