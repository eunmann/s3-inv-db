package handlers

import (
	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/internal/templates"
	"github.com/eunmann/s3-inv-db/pkg/pricing"
	"github.com/rs/zerolog"
)

// Handlers contains all HTTP handlers and their dependencies.
type Handlers struct {
	manager    *inventory.Manager
	renderer   *templates.Renderer
	priceTable pricing.PriceTable
	logger     zerolog.Logger
}

// New creates a new Handlers instance.
func New(mgr *inventory.Manager, renderer *templates.Renderer, priceTable pricing.PriceTable, logger zerolog.Logger) *Handlers {
	return &Handlers{
		manager:    mgr,
		renderer:   renderer,
		priceTable: priceTable,
		logger:     logger,
	}
}
