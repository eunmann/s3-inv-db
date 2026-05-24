package handlers

import (
	"bytes"
	"net/http"
	"time"

	"github.com/eunmann/s3-inv-db/internal/budget"
	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/internal/jobs"
	"github.com/eunmann/s3-inv-db/internal/templates"
	"github.com/eunmann/s3-inv-db/pkg/pricing"
	"github.com/rs/zerolog"
)

// defaultSSEHeartbeat is the cadence used when WithSSEHeartbeat is not
// passed. 15s is small enough to free a stalled SSE slot in well under
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
	loader                   CacheStore
	manager                  *inventory.Manager
	discovery                *inventory.DiscoveryService
	configStore              *inventory.ConfigStore
	tracker                  *budget.Tracker
	renderer                 *templates.Renderer
	jobMgr                   *jobs.Manager
	jobStore                 *jobs.Store
	jobBus                   *jobs.Bus
	s3SourceURI              string
	priceTable               pricing.PriceTable
	sseHeartbeat             time.Duration
	discoveryRefreshInterval time.Duration

	discoverer inventory.Discoverer
	indexBldr  inventory.IndexBuilder
}

// Option configures optional Handlers fields. See WithLoader,
// WithDiscovery, WithS3Source, WithSSEHeartbeat, WithDiscoveryRefreshInterval,
// WithDiscoverer.
type Option func(*Handlers)

// WithLoader installs the IndexBuilder used by DiscoveryService for
// builds. Pair with WithCacheStore for the cache-side wiring; the two
// concerns are kept separate so the handler-facing cache subset is
// type-checked at the call site rather than via a runtime assertion.
func WithLoader(l inventory.IndexBuilder) Option {
	return func(h *Handlers) { h.indexBldr = l }
}

// WithCacheStore installs the cache-aware loader used for unload cache
// removal and the dashboard's on-disk size readout. Leaving it unset
// puts the server in browse-only mode.
func WithCacheStore(cs CacheStore) Option {
	return func(h *Handlers) { h.loader = cs }
}

// WithDiscovery installs a pre-built DiscoveryService. When unset, a
// disabled DiscoveryService is constructed from the discoverer and
// builder passed via WithDiscoverer / WithLoader (or nil for both,
// yielding a service whose Enabled() reports false).
func WithDiscovery(d *inventory.DiscoveryService) Option {
	return func(h *Handlers) { h.discovery = d }
}

// WithDiscoverer installs the discoverer used by the DiscoveryService
// that NewHandlers constructs when WithDiscovery is not provided.
func WithDiscoverer(d inventory.Discoverer) Option {
	return func(h *Handlers) { h.discoverer = d }
}

// WithS3Source sets the s3:// URI displayed on the dashboard.
func WithS3Source(uri string) Option {
	return func(h *Handlers) { h.s3SourceURI = uri }
}

// WithSSEHeartbeat overrides the SSE keepalive cadence.
func WithSSEHeartbeat(d time.Duration) Option {
	return func(h *Handlers) {
		if d > 0 {
			h.sseHeartbeat = d
		}
	}
}

// WithDiscoveryRefreshInterval sets the cadence the dashboard reports
// for the discovery snapshot refresher.
func WithDiscoveryRefreshInterval(d time.Duration) Option {
	return func(h *Handlers) { h.discoveryRefreshInterval = d }
}

// DiscoveryEnabled reports whether the wired DiscoveryService is usable.
// Exposed so the server can gate discovery-dependent routes via middleware
// rather than each handler duplicating the check.
func (h *Handlers) DiscoveryEnabled() bool { return h.discovery.Enabled() }

// contentTypeHTML is the Content-Type set on every HTML response.
const contentTypeHTML = "text/html; charset=utf-8"

// renderHTML writes a full template, logging and returning HTTP 500 on
// failure. Renders into a buffer first so a mid-template error leaves
// the response uncommitted and http.Error can emit a clean 500.
func (h *Handlers) renderHTML(w http.ResponseWriter, r *http.Request, name, logMsg string, data any) {
	var buf bytes.Buffer
	if err := h.renderer.Render(&buf, name, data); err != nil {
		zerolog.Ctx(r.Context()).Error().Err(err).Msg(logMsg)
		http.Error(w, "failed to render page", http.StatusInternalServerError)

		return
	}
	w.Header().Set("Content-Type", contentTypeHTML)
	_, _ = buf.WriteTo(w)
}

// renderHTMLPartial is renderHTML for partial templates (htmx fragments).
func (h *Handlers) renderHTMLPartial(w http.ResponseWriter, r *http.Request, name, logMsg string, data any) {
	var buf bytes.Buffer
	if err := h.renderer.RenderPartial(&buf, name, data); err != nil {
		zerolog.Ctx(r.Context()).Error().Err(err).Msg(logMsg)
		http.Error(w, "failed to render partial", http.StatusInternalServerError)

		return
	}
	w.Header().Set("Content-Type", contentTypeHTML)
	_, _ = buf.WriteTo(w)
}

// New builds a Handlers. All positional parameters are required; the
// caller's contract is that none is nil. Optional dependencies (loader,
// discovery, S3 source URI, SSE heartbeat) are passed as Options. When
// neither WithDiscovery nor WithDiscoverer/WithLoader is supplied,
// a disabled DiscoveryService is wired so handlers can call its
// methods without nil-checking.
func New(
	mgr *inventory.Manager,
	renderer *templates.Renderer,
	priceTable pricing.PriceTable,
	jobMgr *jobs.Manager,
	jobStore *jobs.Store,
	jobBus *jobs.Bus,
	configStore *inventory.ConfigStore,
	tracker *budget.Tracker,
	opts ...Option,
) *Handlers {
	h := &Handlers{
		manager:      mgr,
		renderer:     renderer,
		priceTable:   priceTable,
		jobMgr:       jobMgr,
		jobStore:     jobStore,
		jobBus:       jobBus,
		configStore:  configStore,
		tracker:      tracker,
		sseHeartbeat: defaultSSEHeartbeat,
	}
	for _, opt := range opts {
		opt(h)
	}
	if h.discovery == nil {
		if h.discoverer != nil && h.indexBldr != nil {
			h.discovery = inventory.NewDiscoveryService(mgr, h.discoverer, h.indexBldr)
		} else {
			h.discovery = inventory.NewDisabledDiscoveryService(mgr)
		}
	}

	return h
}
