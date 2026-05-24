package inventory

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"golang.org/x/sync/singleflight"
)

// Discoverer is the subset of s3disco.Discoverer that DiscoveryService
// uses. Defined here so the service can be unit-tested with a fake and
// so this package doesn't import s3disco (the dependency runs the
// other way: s3disco constructs Inventory values defined here).
type Discoverer interface {
	List(ctx context.Context) ([]Inventory, error)
	Find(ctx context.Context, srcBucket, invID, run string) (Inventory, error)
	Bucket() string
}

// IndexBuilder is the subset of loader.Loader that DiscoveryService uses.
type IndexBuilder interface {
	BuildWith(ctx context.Context, srcBucket, invID, run, manifestURI string, onProgress func(stage string, done, total int64)) (string, error)
	RemoveCache(srcBucket, invID, run string) error
	CacheSizeBytes(srcBucket, invID, run string) (int64, error)
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
// Duplicated here so DiscoveryService doesn't import the gate package
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

// DiscoveryService orchestrates the inventory use cases that span the
// Catalog (in-memory state), the Discoverer (S3 listing of available
// inventories), and the IndexBuilder (on-disk index materialisation).
//
// When discovery is unconfigured (--s3-source unset) the service is
// constructed via NewDisabledDiscoveryService — Enabled() reports false
// and every method returns ErrDiscoveryDisabled via the disabled
// discoverer/builder shims rather than via per-call nil checks.
type DiscoveryService struct {
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
// NewDisabledDiscoveryService. Every call returns ErrDiscoveryDisabled
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

func (disabledBuilder) BuildWith(context.Context, string, string, string, string, func(string, int64, int64)) (string, error) {
	return "", ErrDiscoveryDisabled
}
func (disabledBuilder) RemoveCache(string, string, string) error             { return ErrDiscoveryDisabled }
func (disabledBuilder) CacheSizeBytes(string, string, string) (int64, error) { return 0, nil }

// refreshKey is the singleflight key for cold-start Refresh dedupe in
// Snapshot. Constant because Refresh is process-global.
const refreshKey = "discovery-refresh"

// NewDiscoveryService constructs an enabled service. Both discoverer
// and builder are required and must be non-nil; for the unconfigured
// case use NewDisabledDiscoveryService.
func NewDiscoveryService(mgr *Catalog, discoverer Discoverer, builder IndexBuilder) *DiscoveryService {
	return &DiscoveryService{
		manager:    mgr,
		discoverer: discoverer,
		builder:    builder,
		indexRatio: DefaultIndexRatio,
		bgClock:    time.Now,
		enabled:    true,
	}
}

// NewDisabledDiscoveryService returns a service whose methods all
// short-circuit to ErrDiscoveryDisabled. Used when --s3-source is unset
// so the rest of the wiring can treat DiscoveryService as a non-nil
// dependency.
func NewDisabledDiscoveryService(mgr *Catalog) *DiscoveryService {
	return &DiscoveryService{
		manager:    mgr,
		discoverer: disabledDiscoverer{},
		builder:    disabledBuilder{},
		indexRatio: DefaultIndexRatio,
		bgClock:    time.Now,
		enabled:    false,
	}
}

// SetGate attaches a GatedLoadFunc + ManifestSizeFunc. When both are set,
// LoadWith routes through the gate so disk-budget rules apply.
func (s *DiscoveryService) SetGate(gate GatedLoadFunc, sizer ManifestSizeFunc, indexRatio float64) {
	s.gate = gate
	s.sizer = sizer
	if indexRatio > 0 {
		s.indexRatio = indexRatio
	}
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
func (s *DiscoveryService) Enabled() bool { return s.enabled }

// List walks the configured S3 source and merges each discovered
// inventory with its current Catalog state. Returns nil and
// ErrDiscoveryDisabled when discovery is unconfigured.
func (s *DiscoveryService) List(ctx context.Context) ([]MergedInventory, error) {
	discovered, err := s.discoverer.List(ctx)
	if err != nil {
		return nil, fmt.Errorf("discover: %w", err)
	}
	out := make([]MergedInventory, 0, len(discovered))
	for i := range discovered {
		d := &discovered[i]
		m := MergedInventory{Inventory: *d, State: StateNotLoaded}
		if info, ok := s.manager.Get(d.CompositeID()); ok {
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
func (s *DiscoveryService) Snapshot(ctx context.Context) ([]MergedInventory, time.Time, error) {
	if !s.enabled {
		return nil, time.Time{}, ErrDiscoveryDisabled
	}
	s.cacheMu.RLock()
	populated := s.cachePopulate
	s.cacheMu.RUnlock()
	if !populated {
		_, err, _ := s.refreshSF.Do(refreshKey, func() (any, error) {
			return nil, s.Refresh(ctx)
		})
		if err != nil {
			return nil, time.Time{}, fmt.Errorf("cold refresh: %w", err)
		}
	}
	s.cacheMu.RLock()
	out := make([]MergedInventory, len(s.cacheViews))
	copy(out, s.cacheViews)
	at := s.cacheAt
	s.cacheMu.RUnlock()
	for i := range out {
		if info, ok := s.manager.Get(out[i].CompositeID()); ok {
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
func (s *DiscoveryService) Refresh(ctx context.Context) error {
	if !s.enabled {
		return ErrDiscoveryDisabled
	}
	views, err := s.List(ctx)
	now := s.now()
	s.cacheMu.Lock()
	defer s.cacheMu.Unlock()
	if err != nil {
		s.cacheLastErr = err
		// Keep prior cacheViews / cachePopulate as-is so readers still
		// get the last-known-good snapshot.
		return err
	}
	s.cacheViews = views
	s.cacheAt = now
	s.cachePopulate = true
	s.cacheLastErr = nil

	return nil
}

// LastRefreshErr returns the error from the most recent Refresh, or nil
// if the most recent Refresh succeeded (or none has run yet).
func (s *DiscoveryService) LastRefreshErr() error {
	s.cacheMu.RLock()
	defer s.cacheMu.RUnlock()

	return s.cacheLastErr
}

// Start launches a background goroutine that calls Refresh every
// `interval`. The very first refresh runs inline so Snapshot returns
// fresh data immediately after Start returns. Subsequent refreshes run
// asynchronously; ticker drift is acceptable. Refresh errors are
// logged at warn level via the supplied logger (nil disables logging).
//
// Start is a no-op when discovery is disabled. Calling Start a second
// time without Stop in between is also a no-op.
func (s *DiscoveryService) Start(ctx context.Context, interval time.Duration, logger *zerolog.Logger) {
	if !s.enabled || interval <= 0 {
		return
	}
	s.bgMu.Lock()
	if s.bgStop != nil {
		s.bgMu.Unlock()

		return
	}
	stop := make(chan struct{})
	s.bgStop = stop
	s.bgMu.Unlock()

	if err := s.Refresh(ctx); err != nil && logger != nil {
		logger.Warn().Err(err).Msg("discovery: initial refresh failed; serving empty snapshot until next tick")
	}
	s.bgWG.Add(1)
	go s.runRefresher(ctx, interval, stop, logger)
}

func (s *DiscoveryService) runRefresher(ctx context.Context, interval time.Duration, stop <-chan struct{}, logger *zerolog.Logger) {
	defer s.bgWG.Done()
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-stop:
			return
		case <-t.C:
			if err := s.Refresh(ctx); err != nil && logger != nil {
				logger.Warn().Err(err).Msg("discovery: background refresh failed; serving last good snapshot")
			}
		}
	}
}

// Stop signals the background refresher to exit and waits for it. Safe
// to call without a matching Start.
func (s *DiscoveryService) Stop() {
	s.bgMu.Lock()
	stop := s.bgStop
	s.bgStop = nil
	s.bgMu.Unlock()
	if stop == nil {
		return
	}
	close(stop)
	s.bgWG.Wait()
}

func (s *DiscoveryService) now() time.Time {
	return s.bgClock()
}

// Find returns a single discovered inventory run by source bucket, ID,
// and run timestamp.
func (s *DiscoveryService) Find(ctx context.Context, src, id, run string) (Inventory, error) {
	inv, err := s.discoverer.Find(ctx, src, id, run)
	if err != nil {
		return inv, fmt.Errorf("find: %w", err)
	}

	return inv, nil
}

// PrepareDiscovered registers (if needed) one inventory run in the
// Catalog without performing a build. Each run gets its own composite
// ID; multiple runs of the same configuration can coexist as independent
// entries.
func (s *DiscoveryService) PrepareDiscovered(ctx context.Context, disc Inventory) error {
	if !s.Enabled() {
		return ErrDiscoveryDisabled
	}
	if disc.Run == "" {
		return fmt.Errorf("prepare inventory %s: %w", disc.ConfigID(), ErrNoRun)
	}
	composite := disc.CompositeID()
	manifestURI := fmt.Sprintf("s3://%s/%s", s.discoverer.Bucket(), disc.ManifestKey)
	displayName := fmt.Sprintf("%s/%s @ %s", disc.SourceBucket, disc.Name, disc.Run)
	if err := s.manager.Register(ctx, composite, displayName, manifestURI); err != nil &&
		!errors.Is(err, ErrAlreadyExists) {
		return fmt.Errorf("register: %w", err)
	}

	return nil
}

// Load is LoadWith with no progress callback.
func (s *DiscoveryService) Load(ctx context.Context, disc Inventory) error {
	return s.LoadWith(ctx, disc, nil)
}

// LoadWith registers (if not already) and triggers a build+open for a
// specific inventory run. Routes through the disk-budget gate when one
// has been attached via SetGate; otherwise calls the manager directly
// (which the gate's tests and dev-mode setups depend on). The
// onProgress callback, if non-nil, receives stage transitions and
// per-chunk quantitative progress. The ctx threads through to the
// builder — cancellation kills the build.
func (s *DiscoveryService) LoadWith(ctx context.Context, disc Inventory, onProgress func(stage string, done, total int64)) error {
	return s.loadInternal(ctx, disc, onProgress, false /* auto */)
}

// AutoLoadWith is LoadWith without the Pin flag — used by the
// auto-loader so the discovered run remains eligible for future
// eviction. Routes through the gate just like LoadWith.
func (s *DiscoveryService) AutoLoadWith(ctx context.Context, disc Inventory, onProgress func(stage string, done, total int64)) error {
	return s.loadInternal(ctx, disc, onProgress, true /* auto */)
}

func (s *DiscoveryService) loadInternal(ctx context.Context, disc Inventory, onProgress func(stage string, done, total int64), auto bool) error {
	if !s.Enabled() {
		return ErrDiscoveryDisabled
	}
	if disc.Run == "" {
		return fmt.Errorf("load inventory %s: %w", disc.ConfigID(), ErrNoRun)
	}
	composite := disc.CompositeID()
	manifestURI := fmt.Sprintf("s3://%s/%s", s.discoverer.Bucket(), disc.ManifestKey)
	displayName := fmt.Sprintf("%s/%s @ %s", disc.SourceBucket, disc.Name, disc.Run)
	if err := s.manager.Register(ctx, composite, displayName, manifestURI); err != nil &&
		!errors.Is(err, ErrAlreadyExists) {
		return fmt.Errorf("register: %w", err)
	}
	build := func(c context.Context, _ Info) (string, error) {
		return s.builder.BuildWith(c, disc.SourceBucket, disc.Name, disc.Run, manifestURI, onProgress)
	}
	if s.gate == nil {
		// No budget — manager direct. Pin manual loads.
		if auto {
			return s.manager.AutoLoad(ctx, composite, build)
		}

		return s.manager.LoadWith(ctx, composite, build)
	}
	opts := GatedLoadOptions{Pin: !auto}
	if s.sizer != nil {
		size, err := s.sizer(ctx, s.discoverer.Bucket(), disc.ManifestKey)
		if err == nil {
			opts.EstimateBytes = uint64(float64(size) * s.indexRatio)
		}
		// On error we proceed with EstimateBytes=0, letting the planner
		// reserve nothing — the load might still refuse later but at
		// least we don't lose the manual-Load attempt to a transient
		// manifest fetch hiccup.
	}
	if err := s.gate(ctx, composite, build, opts); err != nil {
		return fmt.Errorf("gated load %s: %w", composite, err)
	}

	return nil
}
