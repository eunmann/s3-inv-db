// Package loadcontrol orchestrates one load lifecycle — plan, evict,
// reserve, build, release — for both manual and auto loads.
package loadcontrol

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/eunmann/s3-inv-db/internal/budget"
	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/pkg/format"
	"github.com/eunmann/s3-inv-db/pkg/indexstore"
	"github.com/eunmann/s3-inv-db/pkg/s3fetch"
	"github.com/rs/zerolog"
)

// tokenCounter is incremented per Load to guarantee unique reservation
// tokens even when two forced loads of the same id race within 1ns.
//
//nolint:gochecknoglobals // monotonic per-process counter; no shared state to leak across tests
var tokenCounter atomic.Uint64

// BudgetRefusedError carries the planner's verdict to the UI.
type BudgetRefusedError struct {
	Plan budget.Plan
}

func (e *BudgetRefusedError) Error() string {
	return "load refused by disk-budget planner: " + e.Plan.Refusal
}

// CacheRemover deletes an evicted run's on-disk index. Nil when the
// server has no cache (no --s3-source or --index-source).
type CacheRemover func(id inventory.ID) error

// Gate orchestrates one load lifecycle.
type Gate struct {
	manager     *inventory.Catalog
	tracker     *budget.Tracker
	planner     *budget.Planner
	removeCache CacheRemover
	// admit serializes plan, evict and reserve (not the build), so two
	// loads never plan against the same free space and evict for a
	// reservation one of them then can't get.
	admit sync.Mutex
	// retiring counts, per run, the in-flight loads that retire it once
	// they succeed. Guarded by admit.
	retiring map[inventory.ID]int
	// measured holds the built size of each run retracted for not
	// fitting, so loading it again reserves that, not the estimate that
	// fell short, and is refused before it builds. Guarded by admit.
	measured map[inventory.ID]uint64
}

// New constructs a Gate. The tracker's used bytes must come from the
// catalog (Catalog.LoadedBytes); the gate never adjusts them itself.
func New(manager *inventory.Catalog, tracker *budget.Tracker, planner *budget.Planner, removeCache CacheRemover) *Gate {
	return &Gate{
		manager:     manager,
		tracker:     tracker,
		planner:     planner,
		removeCache: removeCache,
		retiring:    make(map[inventory.ID]int),
		measured:    make(map[inventory.ID]uint64),
	}
}

// Load plans, evicts, reserves, builds, settles at the built size,
// then retires the runs past retention the load didn't need the disk
// of. Returns *BudgetRefusedError when the planner refuses, before the
// build or once the built index proves too big, and opts.Force is
// false.
func (g *Gate) Load(ctx context.Context, id inventory.ID, build inventory.BuildFunc, opts inventory.GatedLoadOptions) error {
	plan, token, err := g.admitLoad(ctx, id, opts)
	if err != nil {
		return err
	}
	defer g.tracker.Release(token)
	defer g.unmarkRetiring(plan.AfterLoad)

	var loadErr error
	if opts.Pin {
		loadErr = g.manager.LoadWith(ctx, id, build)
	} else {
		loadErr = g.manager.AutoLoad(ctx, id, build)
	}
	if loadErr != nil {
		return fmt.Errorf("load %s: %w", id, loadErr)
	}
	if err := g.settle(ctx, id, token, opts.Force); err != nil {
		return err
	}
	// Retention evictions wait for a successful load, so a failed build
	// never leaves the configuration a run short.
	for _, victim := range plan.AfterLoad {
		if err := g.evict(ctx, victim); err != nil {
			zerolog.Ctx(ctx).Warn().Err(err).Stringer("id", victim).Msg("retire run past retention")
		}
	}

	return nil
}

// admitLoad plans, evicts what the load needs the disk of, and
// reserves the estimate, all under one lock. It returns the plan and
// the reservation token to release after the build.
func (g *Gate) admitLoad(ctx context.Context, id inventory.ID, opts inventory.GatedLoadOptions) (budget.Plan, string, error) {
	g.admit.Lock()
	defer g.admit.Unlock()

	opts.EstimateBytes = max(opts.EstimateBytes, g.measured[id])
	plan, err := g.planner.Plan(ctx, budget.Input{
		Target:        id,
		EstimateBytes: opts.EstimateBytes,
		All:           g.manager.List(),
		Pinned:        opts.Pin,
		Retiring:      g.retiringLocked(),
	})
	if refused, ok := errors.AsType[*budget.PlannerRefusedError](err); ok {
		if !opts.Force {
			return plan, "", &BudgetRefusedError{Plan: refused.Plan}
		}
	} else if err != nil {
		return plan, "", fmt.Errorf("plan: %w", err)
	}

	for _, victim := range plan.Evict {
		if err := g.evict(ctx, victim); err != nil {
			return plan, "", err
		}
	}

	token := fmt.Sprintf("%s-%d", id, tokenCounter.Add(1))
	if err := g.tracker.Reserve(token, opts.EstimateBytes); err != nil {
		if errors.Is(err, budget.ErrOverBudget) && !opts.Force {
			return plan, "", &BudgetRefusedError{Plan: plan}
		}
		if !opts.Force {
			return plan, "", fmt.Errorf("reserve: %w", err)
		}
	}

	for _, victim := range plan.AfterLoad {
		g.retiring[victim]++
	}

	return plan, token, nil
}

// retiringLocked returns the runs in-flight loads retire. Caller holds
// admit.
func (g *Gate) retiringLocked() map[inventory.ID]bool {
	retiring := make(map[inventory.ID]bool, len(g.retiring))
	for victim := range g.retiring {
		retiring[victim] = true
	}

	return retiring
}

// settle swaps the load's reservation for its built index, which the
// catalog now counts at its measured size, and evicts for whatever an
// estimate that fell short leaves over the cap. An index that can't be
// made to fit is retracted and its cache removed, unless forced.
func (g *Gate) settle(ctx context.Context, id inventory.ID, token string, force bool) error {
	g.admit.Lock()
	defer g.admit.Unlock()
	g.tracker.Release(token)
	delete(g.measured, id)
	over := g.tracker.Over()
	if over == 0 {
		return nil
	}
	// Disk only, as for a pinned load: the run's retention was planned
	// at admission.
	plan, err := g.planner.Plan(ctx, budget.Input{
		Target:        id,
		EstimateBytes: over,
		All:           g.manager.List(),
		Pinned:        true,
		Retiring:      g.retiringLocked(),
	})
	if _, refused := errors.AsType[*budget.PlannerRefusedError](err); refused && !force {
		return g.retract(ctx, id, over, plan)
	} else if err != nil && !refused {
		return fmt.Errorf("plan: %w", err)
	}
	for _, victim := range plan.Evict {
		if err := g.evict(ctx, victim); err != nil {
			zerolog.Ctx(ctx).Warn().Err(err).Stringer("id", victim).Msg("evict for an index over its estimate")
		}
	}

	return nil
}

// retract unloads id, whose built index is over the cap by over bytes
// that evictions can't free, and removes its cache. Caller holds admit.
func (g *Gate) retract(ctx context.Context, id inventory.ID, over uint64, plan budget.Plan) error {
	info, _ := g.manager.Get(id)
	if err := g.manager.Retract(ctx, id); err != nil {
		return fmt.Errorf("retract %s: %w", id, err)
	}
	g.measured[id] = info.IndexBytes
	if g.removeCache != nil {
		if err := g.removeCache(id); err != nil {
			zerolog.Ctx(ctx).Warn().Err(err).Stringer("id", id).Msg("remove retracted run's cache")
		}
	}
	plan.Refusal = fmt.Sprintf("the built index is %d bytes, %d over the disk budget: %s", info.IndexBytes, over, plan.Refusal)

	return &BudgetRefusedError{Plan: plan}
}

// unmarkRetiring drops one in-flight load's claim on its AfterLoad runs.
func (g *Gate) unmarkRetiring(victims []inventory.ID) {
	g.admit.Lock()
	defer g.admit.Unlock()
	for _, victim := range victims {
		g.retiring[victim]--
		if g.retiring[victim] <= 0 {
			delete(g.retiring, victim)
		}
	}
}

// evict unloads victim and deletes its cache. A run already unloaded or
// removed by someone else, or pinned since planning, frees nothing and
// isn't an error: the reservation that follows refuses a load the space
// is then short for.
func (g *Gate) evict(ctx context.Context, victim inventory.ID) error {
	err := g.manager.EvictForBudget(ctx, victim)
	switch {
	case errors.Is(err, inventory.ErrInvalidState), errors.Is(err, inventory.ErrNotFound), errors.Is(err, inventory.ErrPinned):
		return nil
	case err != nil:
		return fmt.Errorf("evict %s: %w", victim, err)
	}
	// The eviction only frees disk once the cache is gone; leaving it
	// would let unloaded runs pile up outside the budget.
	if g.removeCache != nil {
		if err := g.removeCache(victim); err != nil {
			zerolog.Ctx(ctx).Warn().Err(err).Stringer("id", victim).Msg("remove evicted run's cache")
		}
	}

	return nil
}

// ManifestSizer implements inventory.IndexSizeFunc over an
// s3fetch.Client. Caches nothing — the manifest is small JSON and
// fetched once per load attempt.
type ManifestSizer struct {
	client *s3fetch.Client
}

// NewManifestSizer returns a ManifestSizer that fetches via client.
func NewManifestSizer(client *s3fetch.Client) *ManifestSizer {
	return &ManifestSizer{client: client}
}

// IndexSize returns the on-disk size of the published index whose
// manifest is at key: what loading it takes, exactly.
func (s *ManifestSizer) IndexSize(ctx context.Context, bucket, key string) (uint64, error) {
	prefix, ok := strings.CutSuffix(key, format.ManifestFile)
	if !ok {
		return 0, fmt.Errorf("%w: %q is not a manifest key", errNotManifestKey, key)
	}
	m, err := indexstore.New(s.client.Raw(), bucket).ReadManifest(ctx, prefix)
	if err != nil {
		return 0, fmt.Errorf("read index manifest: %w", err)
	}

	return m.TotalBytes(), nil
}

// errNotManifestKey is returned for a key that doesn't end in
// manifest.json.
var errNotManifestKey = errors.New("not a manifest key")

// EstimatedIndexSize returns the disk the index built from the
// inventory manifest at key is estimated to take, from its file format
// and data-file sizes.
func (s *ManifestSizer) EstimatedIndexSize(ctx context.Context, bucket, key string) (uint64, error) {
	m, err := s.client.FetchManifest(ctx, bucket, key)
	if err != nil {
		return 0, fmt.Errorf("fetch manifest: %w", err)
	}
	var total uint64
	for _, f := range m.Files {
		if f.Size > 0 {
			total += uint64(f.Size)
		}
	}

	return inventory.EstimateIndexBytes(m.FileFormat, total), nil
}
