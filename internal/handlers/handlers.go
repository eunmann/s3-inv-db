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

// defaultSSEHeartbeat is the production cadence used when Config.SSEHeartbeat
// is left zero. 15s is small enough to free a stalled SSE slot in well under
// Chrome's ~60s TCP idle window, large enough to be cheap.
const defaultSSEHeartbeat = 15 * time.Second

// CacheStore is the loader subset handlers actually use: cache size
// for the dashboard + cache removal on unload. Narrower than
// inventory.IndexBuilder (which DiscoveryService needs for BuildWith)
// so the handlers don't see methods they don't call.
type CacheStore interface {
	RemoveCache(srcBucket, invID, run string) error
	CacheSizeBytes(srcBucket, invID, run string) (int64, error)
}

// Handlers contains all HTTP handlers and their dependencies. The
// request-scoped logger comes from zerolog.Ctx(r.Context()).
type Handlers struct {
	manager      *inventory.Manager
	discovery    *inventory.DiscoveryService
	loader       CacheStore
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

// Config gathers Handlers dependencies for NewWithConfig. Discoverer
// and Loader take narrow interfaces so tests can wire fakes; production
// passes *s3disco.Discoverer and *loader.Loader.
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

	// Discovery, when non-nil, replaces the empty-DiscoveryService
	// fallback so manual+auto loads share one gate+sizer wiring.
	Discovery *inventory.DiscoveryService

	// ConfigStore + Tracker drive the auto-load + disk-budget UI;
	// optional for tests.
	ConfigStore *inventory.ConfigStore
	Tracker     *budget.Tracker

	// SSEHeartbeat: /api/jobs/stream keep-alive cadence. Zero falls
	// back to defaultSSEHeartbeat.
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
		heartbeat = defaultSSEHeartbeat
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
