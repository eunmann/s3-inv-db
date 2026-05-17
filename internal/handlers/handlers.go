package handlers

import (
	"net/http"
	"time"

	"github.com/eunmann/s3-inv-db/internal/budget"
	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/internal/jobs"
	"github.com/eunmann/s3-inv-db/internal/templates"
	"github.com/eunmann/s3-inv-db/pkg/pricing"
	"github.com/rs/zerolog"
)

// DefaultSSEHeartbeat is the production cadence used when Config.SSEHeartbeat
// is left zero. 15s is small enough to free a stalled SSE slot in well under
// Chrome's ~60s TCP idle window, large enough to be cheap.
const DefaultSSEHeartbeat = 15 * time.Second

// Handlers contains all HTTP handlers and their dependencies. No logger
// field — handlers retrieve the request-scoped logger via
// zerolog.Ctx(r.Context()), set by the server's contextLoggerMiddleware.
//
// Domain operations on discovered inventories (list+merge, load, evict)
// go through `discovery` so the use-case logic lives in the inventory
// package rather than at the HTTP boundary.
type Handlers struct {
	manager      *inventory.Manager
	discovery    *inventory.DiscoveryService
	loader       inventory.IndexBuilder
	configStore  *inventory.ConfigStore
	tracker      *budget.Tracker
	renderer     *templates.Renderer
	priceTable   pricing.PriceTable
	s3SourceURI  string // for display in templates
	jobMgr       *jobs.Manager
	jobStore     *jobs.Store
	jobBus       *jobs.Bus
	sseHeartbeat time.Duration
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

	// Discovery, when non-nil, replaces the in-NewWithConfig fallback
	// of constructing an empty DiscoveryService. The server wires the
	// gate + sizer onto the same instance so manual and auto loads
	// share the same orchestration.
	Discovery *inventory.DiscoveryService

	// ConfigStore + Tracker, when set, drive the auto-load and disk-
	// budget portions of the UI. Tests that don't exercise those flows
	// can leave them nil.
	ConfigStore *inventory.ConfigStore
	Tracker     *budget.Tracker

	// SSEHeartbeat is how often the /api/jobs/stream handler emits a
	// keep-alive comment to detect dead clients. Zero falls back to
	// DefaultSSEHeartbeat.
	SSEHeartbeat time.Duration
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

// contentTypeHTML is the Content-Type set on every HTML response.
const contentTypeHTML = "text/html; charset=utf-8"

// renderHTML writes a full template, logging and returning HTTP 500 on
// failure. LogMsg becomes the zerolog message for renderer errors.
func (h *Handlers) renderHTML(w http.ResponseWriter, r *http.Request, name, logMsg string, data any) {
	w.Header().Set("Content-Type", contentTypeHTML)
	if err := h.renderer.Render(w, name, data); err != nil {
		zerolog.Ctx(r.Context()).Error().Err(err).Msg(logMsg)
		http.Error(w, "failed to render page", http.StatusInternalServerError)
	}
}

// renderHTMLPartial is renderHTML for partial templates (htmx fragments).
func (h *Handlers) renderHTMLPartial(w http.ResponseWriter, r *http.Request, name, logMsg string, data any) {
	w.Header().Set("Content-Type", contentTypeHTML)
	if err := h.renderer.RenderPartial(w, name, data); err != nil {
		zerolog.Ctx(r.Context()).Error().Err(err).Msg(logMsg)
		http.Error(w, "failed to render partial", http.StatusInternalServerError)
	}
}

// NewWithConfig creates a Handlers wired with optional S3 discovery + loader.
func NewWithConfig(cfg Config) *Handlers {
	heartbeat := cfg.SSEHeartbeat
	if heartbeat <= 0 {
		heartbeat = DefaultSSEHeartbeat
	}
	discovery := cfg.Discovery
	if discovery == nil {
		discovery = inventory.NewDiscoveryService(cfg.Manager, cfg.Discoverer, cfg.Loader)
	}

	return &Handlers{
		manager:      cfg.Manager,
		discovery:    discovery,
		loader:       cfg.Loader,
		configStore:  cfg.ConfigStore,
		tracker:      cfg.Tracker,
		renderer:     cfg.Renderer,
		priceTable:   cfg.PriceTable,
		s3SourceURI:  cfg.S3SourceURI,
		jobMgr:       cfg.JobMgr,
		jobStore:     cfg.JobStore,
		jobBus:       cfg.JobBus,
		sseHeartbeat: heartbeat,
	}
}
