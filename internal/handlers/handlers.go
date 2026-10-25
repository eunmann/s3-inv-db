package handlers

import (
	"bytes"
	"net/http"
	"sync"
	"time"

	"github.com/eunmann/s3-inv-db/internal/budget"
	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/internal/jobs"
	"github.com/eunmann/s3-inv-db/internal/metrics"
	"github.com/eunmann/s3-inv-db/internal/templates"
	"github.com/eunmann/s3-inv-db/pkg/pricing"
	"github.com/rs/zerolog"
)

// sseHeartbeat is the SSE keepalive cadence. 15s is small enough to
// free a stalled SSE slot in well under Chrome's ~60s TCP idle window,
// large enough to be cheap. Package var (not const) so tests can shorten
// it via SetSSEHeartbeatForTest.
//
//nolint:gochecknoglobals // mutable only via SetSSEHeartbeatForTest in export_test.go
var sseHeartbeat = 15 * time.Second

// sseMaxConnsPerIP caps concurrent SSE subscribers from one remote
// address. Each subscriber holds a 64-buffered channel allocated by
// jobs.Bus.Subscribe, so the worst-case memory blast radius is bounded
// by (clients × max-per-ip × buffer × per-event size). Browsers rarely
// need more than one per tab; 8 keeps a few tabs working without being
// a DoS amplifier. Package var so tests can tighten it via
// SetSSEMaxConnsPerIPForTest.
//
//nolint:gochecknoglobals // mutable only via SetSSEMaxConnsPerIPForTest in export_test.go
var sseMaxConnsPerIP = 8

// CacheStore is the loader subset handlers actually use: cache size
// for the dashboard + cache removal on unload. Narrower than
// inventory.IndexBuilder (which Discovery needs for BuildWith)
// so the handlers don't see methods they don't call.
type CacheStore interface {
	RemoveCache(key inventory.CacheKey) error
	CacheSizeBytes(key inventory.CacheKey) (int64, error)
}

// Handlers contains all HTTP handlers and their dependencies. The
// request-scoped logger comes from zerolog.Ctx(r.Context()).
type Handlers struct {
	loader      CacheStore
	manager     *inventory.Catalog
	discovery   *inventory.Discovery
	configStore *inventory.ConfigStore
	tracker     *budget.Tracker
	renderer    *templates.Renderer
	jobMgr      *jobs.Scheduler
	jobStore    *jobs.Store
	jobBus      *jobs.Bus
	s3SourceURI string
	priceTable  pricing.PriceTable
	// prices is priceTable compiled per tier ID, for pricing every child
	// of a level when sorting by cost.
	prices *pricing.Compiled
	// levels keeps the order of large browse and compare levels across
	// page views; nil caches nothing. Its cost orders use prices.
	levels                   *inventory.LevelCache
	discoveryRefreshInterval time.Duration

	discoverer inventory.Discoverer
	indexBldr  inventory.IndexBuilder

	// sseConnsByIP counts open SSE connections per remote IP. An entry is
	// deleted when its count returns to zero so the map tracks only IPs
	// with live streams; the mutex makes that delete atomic with a
	// concurrent increment for the same IP.
	sseMu        sync.Mutex
	sseConnsByIP map[string]int64

	// queryBatchMax bounds the multi-prefix POST /api/stats request.
	// Zero means use defaultQueryBatchMax.
	queryBatchMax int

	// reg is the metrics registry. Always non-nil — defaulted to an
	// empty registry when not supplied so handlers can record
	// unconditionally.
	reg *metrics.Registry
}

// Option configures optional Handlers fields. See WithLoader,
// WithDiscovery, WithS3Source, WithDiscoveryRefreshInterval,
// WithDiscoverer.
type Option func(*Handlers)

// WithLoader installs the IndexBuilder used by Discovery for
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

// WithDiscovery installs a pre-built Discovery. When unset, a
// disabled Discovery is constructed from the discoverer and
// builder passed via WithDiscoverer / WithLoader (or nil for both,
// yielding a service whose Enabled() reports false).
func WithDiscovery(d *inventory.Discovery) Option {
	return func(h *Handlers) { h.discovery = d }
}

// WithDiscoverer installs the discoverer used by the Discovery
// that NewHandlers constructs when WithDiscovery is not provided.
func WithDiscoverer(d inventory.Discoverer) Option {
	return func(h *Handlers) { h.discoverer = d }
}

// WithS3Source sets the s3:// URI displayed on the dashboard.
func WithS3Source(uri string) Option {
	return func(h *Handlers) { h.s3SourceURI = uri }
}

// acquireSSESlot atomically increments the per-IP counter and returns
// the new count. Caller must invoke releaseSSESlot on the same ip when
// the connection ends — even if the count exceeds the cap.
func (h *Handlers) acquireSSESlot(ip string) int64 {
	h.sseMu.Lock()
	defer h.sseMu.Unlock()
	h.sseConnsByIP[ip]++

	return h.sseConnsByIP[ip]
}

// releaseSSESlot decrements the per-IP counter and drops the entry once
// no connection from that IP remains. A release without a matching
// acquire is a no-op.
func (h *Handlers) releaseSSESlot(ip string) {
	h.sseMu.Lock()
	defer h.sseMu.Unlock()
	n, ok := h.sseConnsByIP[ip]
	if !ok {
		return
	}
	if n <= 1 {
		delete(h.sseConnsByIP, ip)

		return
	}
	h.sseConnsByIP[ip] = n - 1
}

// WithDiscoveryRefreshInterval sets the cadence the dashboard reports
// for the discovery snapshot refresher.
func WithDiscoveryRefreshInterval(d time.Duration) Option {
	return func(h *Handlers) { h.discoveryRefreshInterval = d }
}

// DefaultLevelCacheBytes is the default budget of the level-order cache.
const DefaultLevelCacheBytes = 64 << 20

// WithLevelCacheBytes sets the level-order cache's byte budget (see
// inventory.LevelCache); zero or less turns the cache off.
func WithLevelCacheBytes(n int64) Option {
	return func(h *Handlers) { h.levels = inventory.NewLevelCache(n) }
}

// LevelCacheBytes returns the level-order cache's byte budget; zero
// when the cache is off.
func (h *Handlers) LevelCacheBytes() int64 { return h.levels.MaxBytes() }

// WithQueryBatchMax caps the number of prefixes accepted in a single
// batch stats POST. Values <= 0 are ignored.
func WithQueryBatchMax(n int) Option {
	return func(h *Handlers) {
		if n > 0 {
			h.queryBatchMax = n
		}
	}
}

// DiscoveryEnabled reports whether the wired Discovery is usable.
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
	h.renderHTMLPartialStatus(w, r, http.StatusOK, name, logMsg, data)
}

// renderHTMLPartialStatus renders to a buffer first so a template
// failure can surface as a clean 500 instead of corrupting an already-
// committed 202/etc. Callers that need a non-200 status must use this
// helper rather than calling WriteHeader before renderHTMLPartial.
func (h *Handlers) renderHTMLPartialStatus(w http.ResponseWriter, r *http.Request, status int, name, logMsg string, data any) {
	var buf bytes.Buffer
	if err := h.renderer.RenderPartial(&buf, name, data); err != nil {
		zerolog.Ctx(r.Context()).Error().Err(err).Msg(logMsg)
		http.Error(w, "failed to render partial", http.StatusInternalServerError)

		return
	}
	w.Header().Set("Content-Type", contentTypeHTML)
	if status != http.StatusOK {
		w.WriteHeader(status)
	}
	_, _ = buf.WriteTo(w)
}

// Deps groups the persistence + job-fan-out dependencies required by
// New. All fields must be non-nil; the spine args (mgr, renderer,
// priceTable) stay positional so swapping them is a compile error.
type Deps struct {
	JobMgr      *jobs.Scheduler
	JobStore    *jobs.Store
	JobBus      *jobs.Bus
	ConfigStore *inventory.ConfigStore
	Tracker     *budget.Tracker
}

// New builds a Handlers. Mgr/renderer/priceTable are the typed spine
// of the request path; deps groups the job + persistence sinks. Optional
// dependencies (loader, discovery, S3 source URI, SSE heartbeat) are
// passed as Options. When neither WithDiscovery nor WithDiscoverer/
// WithLoader is supplied, a disabled Discovery is wired so handlers
// can call its methods without nil-checking.
func New(
	mgr *inventory.Catalog,
	renderer *templates.Renderer,
	priceTable pricing.PriceTable,
	deps Deps,
	opts ...Option,
) *Handlers {
	h := &Handlers{
		manager:     mgr,
		renderer:    renderer,
		priceTable:  priceTable,
		prices:      pricing.Compile(priceTable),
		levels:      inventory.NewLevelCache(DefaultLevelCacheBytes),
		jobMgr:      deps.JobMgr,
		jobStore:    deps.JobStore,
		jobBus:      deps.JobBus,
		configStore: deps.ConfigStore,
		tracker:     deps.Tracker,
		reg:         metrics.New(),

		sseConnsByIP: make(map[string]int64),
	}
	for _, opt := range opts {
		opt(h)
	}
	if h.discovery == nil {
		if h.discoverer != nil && h.indexBldr != nil {
			h.discovery = inventory.NewDiscovery(mgr, inventory.WithBackend(h.discoverer, h.indexBldr))
		} else {
			h.discovery = inventory.NewDiscovery(mgr)
		}
	}

	return h
}
