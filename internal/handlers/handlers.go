package handlers

import (
	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/internal/loader"
	"github.com/eunmann/s3-inv-db/internal/s3disco"
	"github.com/eunmann/s3-inv-db/internal/templates"
	"github.com/eunmann/s3-inv-db/pkg/pricing"
	"github.com/rs/zerolog"
)

// Handlers contains all HTTP handlers and their dependencies.
type Handlers struct {
	manager     *inventory.Manager
	renderer    *templates.Renderer
	priceTable  pricing.PriceTable
	logger      zerolog.Logger
	discoverer  *s3disco.Discoverer // nil when --s3-source is not set
	loader      *loader.Loader      // nil when --s3-source is not set
	s3SourceURI string              // for display in templates
}

// Config gathers all Handlers dependencies for NewWithConfig.
type Config struct {
	Manager     *inventory.Manager
	Renderer    *templates.Renderer
	PriceTable  pricing.PriceTable
	Logger      zerolog.Logger
	Discoverer  *s3disco.Discoverer
	Loader      *loader.Loader
	S3SourceURI string
}

// New creates a new Handlers instance without discovery wiring. Kept for
// the existing test fixtures.
func New(mgr *inventory.Manager, renderer *templates.Renderer, priceTable pricing.PriceTable, logger zerolog.Logger) *Handlers {
	return NewWithConfig(Config{
		Manager:    mgr,
		Renderer:   renderer,
		PriceTable: priceTable,
		Logger:     logger,
	})
}

// NewWithConfig creates a Handlers wired with optional S3 discovery + loader.
func NewWithConfig(cfg Config) *Handlers {
	return &Handlers{
		manager:     cfg.Manager,
		renderer:    cfg.Renderer,
		priceTable:  cfg.PriceTable,
		logger:      cfg.Logger,
		discoverer:  cfg.Discoverer,
		loader:      cfg.Loader,
		s3SourceURI: cfg.S3SourceURI,
	}
}
