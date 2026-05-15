package inventory

import (
	"context"
	"errors"
	"fmt"
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
	Build(ctx context.Context, srcBucket, invID, run, manifestURI string) (string, error)
	BuildWith(ctx context.Context, srcBucket, invID, run, manifestURI string, onProgress func(stage string, done, total int64)) (string, error)
	RemoveCache(srcBucket, invID, run string) error
	CacheSizeBytes(srcBucket, invID, run string) (int64, error)
}

// MergedInventory is one discovered inventory plus its live load state
// from the local Manager. Handlers and templates consume this directly;
// the join logic doesn't belong at the HTTP layer.
type MergedInventory struct {
	Inventory

	State       State
	Error       string
	NodeCount   uint64
	HasTierData bool
}

// GatedLoader runs a build under the disk-budget planner. Optional —
// when set on DiscoveryService, LoadWith routes through the gate so
// loads honour the global byte cap. Defined as an interface to keep
// inventory free of an import on internal/loadgate.
type GatedLoader interface {
	Load(ctx context.Context, id ID, build BuildFunc, opts GatedLoadOptions) error
}

// GatedLoadOptions mirrors the fields of internal/loadgate.Options.
// Duplicated here so DiscoveryService doesn't import the gate package
// (the gate already imports this one).
type GatedLoadOptions struct {
	EstimateBytes uint64
	Force         bool
	Pin           bool
}

// ManifestSizer reports the total compressed size of an inventory
// manifest's data files. Used to estimate the post-build index size
// before downloading anything.
type ManifestSizer interface {
	ManifestSize(ctx context.Context, bucket, key string) (uint64, error)
}

// DiscoveryService orchestrates the inventory use cases that span the
// Manager (in-memory state), the Discoverer (S3 listing of available
// inventories), and the IndexBuilder (on-disk index materialisation).
//
// Discoverer and IndexBuilder are optional — when discovery is disabled
// (--s3-source unset) the service still exists; Enabled() reports that
// state and the methods either short-circuit or return ErrDiscoveryDisabled.
type DiscoveryService struct {
	manager    *Manager
	discoverer Discoverer
	builder    IndexBuilder
	gate       GatedLoader
	sizer      ManifestSizer
	indexRatio float64
}

// NewDiscoveryService constructs a service. The discoverer and builder
// arguments may be nil, in which case Enabled() returns false.
func NewDiscoveryService(mgr *Manager, discoverer Discoverer, builder IndexBuilder) *DiscoveryService {
	return &DiscoveryService{manager: mgr, discoverer: discoverer, builder: builder, indexRatio: DefaultIndexRatio}
}

// SetGate attaches a GatedLoader + ManifestSizer. When both are set,
// LoadWith routes through the gate so disk-budget rules apply.
func (s *DiscoveryService) SetGate(gate GatedLoader, sizer ManifestSizer, indexRatio float64) {
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

// Enabled reports whether discovery is configured. List, Find, and Load
// require Enabled() == true.
func (s *DiscoveryService) Enabled() bool {
	return s.discoverer != nil && s.builder != nil
}

// List walks the configured S3 source and merges each discovered
// inventory with its current Manager state. Returns nil and
// ErrDiscoveryDisabled when discovery is unconfigured.
func (s *DiscoveryService) List(ctx context.Context) ([]MergedInventory, error) {
	if s.discoverer == nil {
		return nil, ErrDiscoveryDisabled
	}
	discovered, err := s.discoverer.List(ctx)
	if err != nil {
		return nil, fmt.Errorf("discover: %w", err)
	}
	out := make([]MergedInventory, 0, len(discovered))
	for _, d := range discovered {
		m := MergedInventory{Inventory: d, State: StateNotLoaded}
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

// Find returns a single discovered inventory run by source bucket, ID,
// and run timestamp.
func (s *DiscoveryService) Find(ctx context.Context, src, id, run string) (Inventory, error) {
	if s.discoverer == nil {
		return Inventory{}, ErrDiscoveryDisabled
	}
	inv, err := s.discoverer.Find(ctx, src, id, run)
	if err != nil {
		return inv, fmt.Errorf("find: %w", err)
	}

	return inv, nil
}

// PrepareDiscovered registers (if needed) one inventory run in the
// Manager without performing a build. Each run gets its own composite
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
	displayName := fmt.Sprintf("%s/%s @ %s", disc.SourceBucket, disc.InventoryName, disc.Run)
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
	displayName := fmt.Sprintf("%s/%s @ %s", disc.SourceBucket, disc.InventoryName, disc.Run)
	if err := s.manager.Register(ctx, composite, displayName, manifestURI); err != nil &&
		!errors.Is(err, ErrAlreadyExists) {
		return fmt.Errorf("register: %w", err)
	}
	build := func(c context.Context, _ Info) (string, error) {
		return s.builder.BuildWith(c, disc.SourceBucket, disc.InventoryName, disc.Run, manifestURI, onProgress)
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
		size, err := s.sizer.ManifestSize(ctx, s.discoverer.Bucket(), disc.ManifestKey)
		if err == nil {
			opts.EstimateBytes = uint64(float64(size) * s.indexRatio)
		}
		// On error we proceed with EstimateBytes=0, letting the planner
		// reserve nothing — the load might still refuse later but at
		// least we don't lose the manual-Load attempt to a transient
		// manifest fetch hiccup.
	}
	if err := s.gate.Load(ctx, composite, build, opts); err != nil {
		return fmt.Errorf("gated load %s: %w", composite, err)
	}

	return nil
}
