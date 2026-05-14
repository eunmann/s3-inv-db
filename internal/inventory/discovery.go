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
}

// NewDiscoveryService constructs a service. The discoverer and builder
// arguments may be nil, in which case Enabled() returns false.
func NewDiscoveryService(mgr *Manager, discoverer Discoverer, builder IndexBuilder) *DiscoveryService {
	return &DiscoveryService{manager: mgr, discoverer: discoverer, builder: builder}
}

// ErrDiscoveryDisabled is returned by methods that require S3 discovery
// to be configured.
var ErrDiscoveryDisabled = errors.New("discovery not configured")

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
func (s *DiscoveryService) PrepareDiscovered(disc Inventory) error {
	if !s.Enabled() {
		return ErrDiscoveryDisabled
	}
	if disc.Run == "" {
		return fmt.Errorf("prepare: inventory %s has no run", disc.ConfigID())
	}
	composite := disc.CompositeID()
	manifestURI := fmt.Sprintf("s3://%s/%s", s.discoverer.Bucket(), disc.ManifestKey)
	displayName := fmt.Sprintf("%s/%s @ %s", disc.SourceBucket, disc.InventoryName, disc.Run)
	if err := s.manager.Register(composite, displayName, manifestURI); err != nil &&
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
// specific inventory run. The onProgress callback, if non-nil, receives
// stage transitions and per-chunk quantitative progress. The ctx
// threads through to the builder — cancellation kills the build.
func (s *DiscoveryService) LoadWith(ctx context.Context, disc Inventory, onProgress func(stage string, done, total int64)) error {
	if !s.Enabled() {
		return ErrDiscoveryDisabled
	}
	if disc.Run == "" {
		return fmt.Errorf("load: inventory %s has no run", disc.ConfigID())
	}
	composite := disc.CompositeID()
	manifestURI := fmt.Sprintf("s3://%s/%s", s.discoverer.Bucket(), disc.ManifestKey)
	displayName := fmt.Sprintf("%s/%s @ %s", disc.SourceBucket, disc.InventoryName, disc.Run)
	if err := s.manager.Register(composite, displayName, manifestURI); err != nil &&
		!errors.Is(err, ErrAlreadyExists) {
		return fmt.Errorf("register: %w", err)
	}
	err := s.manager.LoadWith(ctx, composite, func(c context.Context, _ Info) (string, error) {
		return s.builder.BuildWith(c, disc.SourceBucket, disc.InventoryName, disc.Run, manifestURI, onProgress)
	})
	if err != nil {
		return err
	}
	return nil
}
