package inventory

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/eunmann/s3-inv-db/pkg/extsort/events"
	"github.com/rs/zerolog"
	"golang.org/x/sync/singleflight"
)

// Discoverer is the subset of s3disco.Discoverer that Discovery
// uses. Defined here so the service can be unit-tested with a fake and
// so this package doesn't import s3disco (the dependency runs the
// other way: s3disco constructs Inventory values defined here).
type Discoverer interface {
	List(ctx context.Context) ([]Inventory, error)
	Find(ctx context.Context, srcBucket, invID, run string) (Inventory, error)
	Bucket() string
}

// CacheKey identifies one inventory run's on-disk cache. The three
// fields appear together in every cache-facing method; folding them
// into a struct removes the silent-swap hazard of three adjacent
// positional strings. Defined in the inventory package because
// IndexBuilder lives here and is implemented by the loader.
type CacheKey struct {
	SourceBucket string
	InventoryID  string
	Run          string
}

// Valid reports whether the key's three identity fields are non-empty.
func (k CacheKey) Valid() bool {
	return k.SourceBucket != "" && k.InventoryID != "" && k.Run != ""
}

// IndexBuilder is the subset of loader.Loader that Discovery uses.
type IndexBuilder interface {
	BuildWith(ctx context.Context, key CacheKey, manifestURI string, onProgress func(stage string, done, total int64), eventBus *events.Bus) (string, error)
	RemoveCache(key CacheKey) error
	CacheSizeBytes(key CacheKey) (int64, error)
}

// MergedInventory is one discovered inventory plus its live load state
// from the local Catalog. Handlers and templates consume this directly;
// the join logic doesn't belong at the HTTP layer.
type MergedInventory struct {
	Inventory

	State       State
	Error       string
	NodeCount   uint64
	HasTierData bool
}

// GatedLoadOptions mirrors the fields of internal/loadcontrol.Options.
// Duplicated here so Discovery doesn't import the gate package
// (the gate already imports this one).
type GatedLoadOptions struct {
	EstimateBytes uint64
	Force         bool
	Pin           bool
}

// GatedLoadFunc runs a build under the disk-budget planner. Optional —
// when set via SetGate, LoadWith routes through this func so loads
// honour the global byte cap.
type GatedLoadFunc func(ctx context.Context, id ID, build BuildFunc, opts GatedLoadOptions) error

// ManifestSizeFunc reports the total compressed size of an inventory
// manifest's data files. Used to estimate the post-build index size
// before downloading anything.
type ManifestSizeFunc func(ctx context.Context, bucket, key string) (uint64, error)

// Discovery orchestrates the inventory use cases that span the
// Catalog (in-memory state), the Discoverer (S3 listing of available
// inventories), and the IndexBuilder (on-disk index materialisation).
//
// When discovery is unconfigured (--s3-source unset) the service is
// constructed via NewDisabledDiscovery — Enabled() reports false
// and every method returns ErrDiscoveryDisabled via the disabled
// discoverer/builder shims rather than via per-call nil checks.
type Discovery struct {
	cacheAt       time.Time
	discoverer    Discoverer
	builder       IndexBuilder
	cacheLastErr  error
	gate          GatedLoadFunc
	sizer         ManifestSizeFunc
	manager       *Catalog
	bgStop        chan struct{}
	bgClock       func() time.Time
	cacheViews    []MergedInventory
	refreshSF     singleflight.Group
	bgWG          sync.WaitGroup
	indexRatio    float64
	cacheMu       sync.RWMutex
	bgMu          sync.Mutex
	cachePopulate bool
	enabled       bool
}

// disabledDiscoverer is the placeholder Discoverer wired by
// NewDisabledDiscovery. Every call returns ErrDiscoveryDisabled
// so handlers don't need per-call nil checks on the service.
type disabledDiscoverer struct{}

func (disabledDiscoverer) List(context.Context) ([]Inventory, error) {
	return nil, ErrDiscoveryDisabled
}

func (disabledDiscoverer) Find(context.Context, string, string, string) (Inventory, error) {
	return Inventory{}, ErrDiscoveryDisabled
}
func (disabledDiscoverer) Bucket() string { return "" }

type disabledBuilder struct{}

func (disabledBuilder) BuildWith(context.Context, CacheKey, string, func(string, int64, int64), *events.Bus) (string, error) {
	return "", ErrDiscoveryDisabled
}
func (disabledBuilder) RemoveCache(CacheKey) error             { return ErrDiscoveryDisabled }
func (disabledBuilder) CacheSizeBytes(CacheKey) (int64, error) { return 0, nil }

// refreshKey is the singleflight key for cold-start Refresh dedupe in
// Snapshot. Constant because Refresh is process-global.
const refreshKey = "discovery-refresh"

// DiscoveryOption configures a Discovery at construction.
type DiscoveryOption func(*Discovery)

// WithBackend enables the Discovery by wiring a Discoverer + IndexBuilder.
// Without it, the Discovery is constructed with disabled stand-ins and
// Enabled() reports false.
func WithBackend(discoverer Discoverer, builder IndexBuilder) DiscoveryOption {
	return func(d *Discovery) {
		d.discoverer = discoverer
		d.builder = builder
		d.enabled = true
	}
}

// WithGate attaches a GatedLoadFunc + ManifestSizeFunc. When both are
// set, LoadWith routes through the gate so disk-budget rules apply.
// IndexRatio == 0 keeps the package default.
func WithGate(gate GatedLoadFunc, sizer ManifestSizeFunc, indexRatio float64) DiscoveryOption {
	return func(d *Discovery) {
		d.gate = gate
		d.sizer = sizer
		if indexRatio > 0 {
			d.indexRatio = indexRatio
		}
	}
}

// NewDiscovery constructs a Discovery. Without WithBackend, the service
// is disabled (every method returns ErrDiscoveryDisabled) so callers
// can treat Discovery as a non-nil dependency.
func NewDiscovery(mgr *Catalog, opts ...DiscoveryOption) *Discovery {
	d := &Discovery{
		manager:    mgr,
		discoverer: disabledDiscoverer{},
		builder:    disabledBuilder{},
		indexRatio: DefaultIndexRatio,
		bgClock:    time.Now,
		enabled:    false,
	}
	for _, opt := range opts {
		opt(d)
	}

	return d
}

// NewDisabledDiscovery returns a disabled Discovery. Equivalent to
// NewDiscovery(mgr) — kept for migration; new callers should use
// NewDiscovery directly.
func NewDisabledDiscovery(mgr *Catalog) *Discovery {
	return NewDiscovery(mgr)
}

// DefaultIndexRatio is the conservative seed ratio used to estimate
// final index bytes from a manifest's compressed CSV total. The
// expected per-load reservation is `manifest_total * indexRatio`.
// Loaders should refine via benchmarks per pkg/extsort.
const DefaultIndexRatio = 0.30

// ErrDiscoveryDisabled is returned by methods that require S3 discovery
// to be configured.
var ErrDiscoveryDisabled = errors.New("discovery not configured")

// ErrNoRun is returned by Prepare/Load when the supplied Inventory has
// an empty Run — the operation needs a specific timestamped run to
// register or build. Callers wrap it with the config ID for context.
var ErrNoRun = errors.New("inventory has no run")

// Enabled reports whether discovery is configured.
func (d *Discovery) Enabled() bool { return d.enabled }

// List walks the configured S3 source and merges each discovered
// inventory with its current Catalog state. Returns nil and
// ErrDiscoveryDisabled when discovery is unconfigured.
func (d *Discovery) List(ctx context.Context) ([]MergedInventory, error) {
	discovered, err := d.discoverer.List(ctx)
	if err != nil {
		return nil, fmt.Errorf("discover: %w", err)
	}
	out := make([]MergedInventory, 0, len(discovered))
	for i := range discovered {
		inv := &discovered[i]
		m := MergedInventory{Inventory: *inv, State: StateNotLoaded}
		if info, ok := d.manager.Get(inv.CompositeID()); ok {
			m.State = info.State
			m.Error = info.Error
			m.NodeCount = info.NodeCount
			m.HasTierData = info.HasTierData
		}
		out = append(out, m)
	}

	return out, nil
}

// Snapshot returns the most recently cached merged-inventory view and
// the time at which it was captured. When no refresh has succeeded yet,
// Snapshot performs one inline so cold-start callers (e.g. the first
// dashboard page load after process start) don't see an empty result.
// Subsequent calls always read the cache, even if a later Refresh
// failed — the FetchedAt timestamp lets callers age out the result
// themselves if they need fresher data.
//
// The S3 listing (what runs exist) comes from the cache, but the
// per-run lifecycle state (State / Error / NodeCount / HasTierData)
// is overlaid from the live Catalog at call time so the page reflects
// what just happened without waiting for the next discovery refresh.
// A load that finishes between two refresh ticks would otherwise leave
// the page rendering "not loaded" and let the user submit a duplicate
// load that fails with ErrInvalidState.
func (d *Discovery) Snapshot(ctx context.Context) ([]MergedInventory, time.Time, error) {
	if !d.enabled {
		return nil, time.Time{}, ErrDiscoveryDisabled
	}
	d.cacheMu.RLock()
	populated := d.cachePopulate
	d.cacheMu.RUnlock()
	if !populated {
		_, err, _ := d.refreshSF.Do(refreshKey, func() (any, error) {
			return nil, d.Refresh(ctx)
		})
		if err != nil {
			return nil, time.Time{}, fmt.Errorf("cold refresh: %w", err)
		}
	}
	d.cacheMu.RLock()
	out := make([]MergedInventory, len(d.cacheViews))
	copy(out, d.cacheViews)
	at := d.cacheAt
	d.cacheMu.RUnlock()
	for i := range out {
		if info, ok := d.manager.Get(out[i].CompositeID()); ok {
			out[i].State = info.State
			out[i].Error = info.Error
			out[i].NodeCount = info.NodeCount
			out[i].HasTierData = info.HasTierData
		}
	}

	return out, at, nil
}

// Refresh runs a live List and stores the result in the snapshot. The
// previous snapshot is preserved on error so consumers continue to see
// the last-known-good views instead of falling back to empty.
// LastRefreshErr returns the most recent error, if any.
func (d *Discovery) Refresh(ctx context.Context) error {
	if !d.enabled {
		return ErrDiscoveryDisabled
	}
	views, err := d.List(ctx)
	now := d.now()
	d.cacheMu.Lock()
	defer d.cacheMu.Unlock()
	if err != nil {
		d.cacheLastErr = err
		// Keep prior cacheViews / cachePopulate as-is so readers still
		// get the last-known-good snapshot.
		return err
	}
	d.cacheViews = views
	d.cacheAt = now
	d.cachePopulate = true
	d.cacheLastErr = nil

	return nil
}

// LastRefreshErr returns the error from the most recent Refresh, or nil
// if the most recent Refresh succeeded (or none has run yet).
func (d *Discovery) LastRefreshErr() error {
	d.cacheMu.RLock()
	defer d.cacheMu.RUnlock()

	return d.cacheLastErr
}

// Start launches a background goroutine that calls Refresh every
// `interval`. The very first refresh runs inline so Snapshot returns
// fresh data immediately after Start returns. Subsequent refreshes run
// asynchronously; ticker drift is acceptable. Refresh errors are
// logged at warn level via the supplied logger (nil disables logging).
//
// Start is a no-op when discovery is disabled. Calling Start a second
// time without Stop in between is also a no-op.
func (d *Discovery) Start(ctx context.Context, interval time.Duration, logger *zerolog.Logger) {
	if !d.enabled || interval <= 0 {
		return
	}
	d.bgMu.Lock()
	if d.bgStop != nil {
		d.bgMu.Unlock()

		return
	}
	stop := make(chan struct{})
	d.bgStop = stop
	d.bgMu.Unlock()

	if err := d.Refresh(ctx); err != nil && logger != nil {
		logger.Warn().Err(err).Msg("discovery: initial refresh failed; serving empty snapshot until next tick")
	}
	d.bgWG.Add(1)
	go d.runRefresher(ctx, interval, stop, logger)
}

func (d *Discovery) runRefresher(ctx context.Context, interval time.Duration, stop <-chan struct{}, logger *zerolog.Logger) {
	defer d.bgWG.Done()
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-stop:
			return
		case <-t.C:
			if err := d.Refresh(ctx); err != nil && logger != nil {
				logger.Warn().Err(err).Msg("discovery: background refresh failed; serving last good snapshot")
			}
		}
	}
}

// Stop signals the background refresher to exit and waits for it. Safe
// to call without a matching Start.
func (d *Discovery) Stop() {
	d.bgMu.Lock()
	stop := d.bgStop
	d.bgStop = nil
	d.bgMu.Unlock()
	if stop == nil {
		return
	}
	close(stop)
	d.bgWG.Wait()
}

func (d *Discovery) now() time.Time {
	return d.bgClock()
}

// Find returns a single discovered inventory run by source bucket, ID,
// and run timestamp.
func (d *Discovery) Find(ctx context.Context, src, id, run string) (Inventory, error) {
	inv, err := d.discoverer.Find(ctx, src, id, run)
	if err != nil {
		return inv, fmt.Errorf("find: %w", err)
	}

	return inv, nil
}

// PrepareDiscovered registers (if needed) one inventory run in the
// Catalog without performing a build. Each run gets its own composite
// ID; multiple runs of the same configuration can coexist as independent
// entries.
func (d *Discovery) PrepareDiscovered(ctx context.Context, disc Inventory) error {
	if !d.Enabled() {
		return ErrDiscoveryDisabled
	}
	if disc.Run == "" {
		return fmt.Errorf("prepare inventory %s: %w", disc.ConfigID(), ErrNoRun)
	}
	composite := disc.CompositeID()
	manifestURI := fmt.Sprintf("s3://%s/%s", d.discoverer.Bucket(), disc.ManifestKey)
	displayName := fmt.Sprintf("%s/%s @ %s", disc.SourceBucket, disc.Name, disc.Run)
	if err := d.manager.Register(ctx, composite, displayName, manifestURI); err != nil &&
		!errors.Is(err, ErrAlreadyExists) {
		return fmt.Errorf("register: %w", err)
	}

	return nil
}

// Load is LoadWith with no progress callback or event bus.
func (d *Discovery) Load(ctx context.Context, disc Inventory) error {
	return d.LoadWith(ctx, disc, nil, nil)
}

// LoadWith registers (if not already) and triggers a build+open for a
// specific inventory run. Routes through the disk-budget gate when one
// has been attached via SetGate; otherwise calls the manager directly
// (which the gate's tests and dev-mode setups depend on). The
// onProgress callback, if non-nil, receives stage transitions and
// per-chunk quantitative progress. The ctx threads through to the
// builder — cancellation kills the build.
func (d *Discovery) LoadWith(ctx context.Context, disc Inventory, onProgress func(stage string, done, total int64), eventBus *events.Bus) error {
	return d.loadInternal(ctx, disc, onProgress, eventBus, false /* auto */)
}

// AutoLoadWith is LoadWith without the Pin flag — used by the
// auto-loader so the discovered run remains eligible for future
// eviction. Routes through the gate just like LoadWith.
func (d *Discovery) AutoLoadWith(ctx context.Context, disc Inventory, onProgress func(stage string, done, total int64), eventBus *events.Bus) error {
	return d.loadInternal(ctx, disc, onProgress, eventBus, true /* auto */)
}

func (d *Discovery) loadInternal(ctx context.Context, disc Inventory, onProgress func(stage string, done, total int64), eventBus *events.Bus, auto bool) error {
	if !d.Enabled() {
		return ErrDiscoveryDisabled
	}
	if disc.Run == "" {
		return fmt.Errorf("load inventory %s: %w", disc.ConfigID(), ErrNoRun)
	}
	composite := disc.CompositeID()
	manifestURI := fmt.Sprintf("s3://%s/%s", d.discoverer.Bucket(), disc.ManifestKey)
	displayName := fmt.Sprintf("%s/%s @ %s", disc.SourceBucket, disc.Name, disc.Run)
	if err := d.manager.Register(ctx, composite, displayName, manifestURI); err != nil &&
		!errors.Is(err, ErrAlreadyExists) {
		return fmt.Errorf("register: %w", err)
	}
	build := func(c context.Context, _ Info) (string, error) {
		return d.builder.BuildWith(c, CacheKey{
			SourceBucket: disc.SourceBucket,
			InventoryID:  disc.Name,
			Run:          disc.Run,
		}, manifestURI, onProgress, eventBus)
	}
	if d.gate == nil {
		// No budget — manager direct. Pin manual loads.
		if auto {
			return d.manager.AutoLoad(ctx, composite, build)
		}

		return d.manager.LoadWith(ctx, composite, build)
	}
	opts := GatedLoadOptions{Pin: !auto}
	if d.sizer != nil {
		size, err := d.sizer(ctx, d.discoverer.Bucket(), disc.ManifestKey)
		if err == nil {
			opts.EstimateBytes = uint64(float64(size) * d.indexRatio)
		}
		// On error we proceed with EstimateBytes=0, letting the planner
		// reserve nothing — the load might still refuse later but at
		// least we don't lose the manual-Load attempt to a transient
		// manifest fetch hiccup.
	}
	if err := d.gate(ctx, composite, build, opts); err != nil {
		return fmt.Errorf("gated load %s: %w", composite, err)
	}

	return nil
}
