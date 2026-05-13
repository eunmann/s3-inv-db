package handlers

import (
	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/internal/loader"
	"github.com/eunmann/s3-inv-db/internal/s3disco"
	"github.com/eunmann/s3-inv-db/internal/templates"
	"github.com/eunmann/s3-inv-db/pkg/pricing"
)

// Handlers contains all HTTP handlers and their dependencies. No logger
// field — handlers retrieve the request-scoped logger via
// logctx.FromContext(r.Context()), set by the server's contextLoggerMiddleware.
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
}

// Config gathers all Handlers dependencies for NewWithConfig.
type Config struct {
	Manager     *inventory.Manager
	Renderer    *templates.Renderer
	PriceTable  pricing.PriceTable
	Discoverer  *s3disco.Discoverer
	Loader      *loader.Loader
	S3SourceURI string
}

// New creates a Handlers instance without discovery wiring. Tests use it.
func New(mgr *inventory.Manager, renderer *templates.Renderer, priceTable pricing.PriceTable) *Handlers {
	return NewWithConfig(Config{
		Manager:    mgr,
		Renderer:   renderer,
		PriceTable: priceTable,
	})
}

// NewWithConfig creates a Handlers wired with optional S3 discovery + loader.
func NewWithConfig(cfg Config) *Handlers {
	// Convert typed-nil pointers to true nil interfaces so DiscoveryService.Enabled()
	// reads correctly. (A typed-nil concrete pointer assigned to an interface
	// parameter yields a non-nil interface, the classic Go pitfall.)
	var disc inventory.Discoverer
	if cfg.Discoverer != nil {
		disc = cfg.Discoverer
	}
	var bld inventory.IndexBuilder
	if cfg.Loader != nil {
		bld = cfg.Loader
	}
	return &Handlers{
		manager:     cfg.Manager,
		discovery:   inventory.NewDiscoveryService(cfg.Manager, disc, bld),
		renderer:    cfg.Renderer,
		priceTable:  cfg.PriceTable,
		s3SourceURI: cfg.S3SourceURI,
	}
}
