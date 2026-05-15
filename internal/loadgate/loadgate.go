// Package loadgate sits between HTTP handlers / the auto-loader and
// the inventory Manager. Every load request goes through the gate so
// the on-disk budget cap is honored: reservations are made before
// downloads start, evictions happen before new bytes arrive, and the
// planner gets the final say on whether a load can proceed at all.
//
// The gate exists outside both internal/inventory and internal/budget
// to break the otherwise-cyclic dependency: the planner reads
// inventory.Info, the manager would need to call the planner.
package loadgate

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/eunmann/s3-inv-db/internal/budget"
	"github.com/eunmann/s3-inv-db/internal/inventory"
)

// Build is the contract the gate uses to materialise an index. Same
// shape as inventory.BuildFunc — wrapped here so callers don't import
// both packages just for the type.
type Build = inventory.BuildFunc

// Options configures one load through the gate. Alias of
// inventory.GatedLoadOptions so callers using either type are
// interoperable.
type Options = inventory.GatedLoadOptions

// BudgetRefusedError carries the planner's structured refusal up to
// the handler so the UI can surface the reason verbatim.
type BudgetRefusedError struct {
	Plan budget.Plan
}

func (e *BudgetRefusedError) Error() string {
	return "load refused by disk-budget planner: " + e.Plan.Refusal
}

// Gate orchestrates a single load lifecycle. Safe for concurrent
// callers; the underlying Manager and Tracker handle their own locking.
type Gate struct {
	manager *inventory.Manager
	tracker *budget.Tracker
	planner *budget.Planner
}

// New constructs a Gate. Manager and tracker must be non-nil. Planner
// is non-nil too: the gate always consults it (a no-cap tracker is
// handled by the planner returning a fit).
func New(manager *inventory.Manager, tracker *budget.Tracker, planner *budget.Planner) *Gate {
	return &Gate{manager: manager, tracker: tracker, planner: planner}
}

// Load runs build for the given id under the budget. Sequence:
//
//  1. Snapshot the manager state and ask the planner for an eviction plan.
//  2. If the plan refuses and opts.Force is false, return *BudgetRefusedError.
//  3. Reserve opts.EstimateBytes against the tracker. On reservation
//     failure with Force=false, return budget.ErrOverBudget.
//  4. Evict every id the plan listed (Manager.EvictForBudget — the
//     auto-loader is free to bring them back later if room reappears).
//  5. Call Manager.LoadWith (if opts.Pin) or Manager.AutoLoad.
//  6. On success, attribute the actual IndexBytes to the tracker.
//  7. Always release the reservation in a defer.
func (g *Gate) Load(ctx context.Context, id inventory.ID, build Build, opts Options) error {
	plan, err := g.planner.Plan(budget.Input{
		Target:        id,
		EstimateBytes: opts.EstimateBytes,
		All:           g.manager.List(),
	})
	if err != nil {
		return fmt.Errorf("plan: %w", err)
	}
	if !plan.Fits() && !opts.Force {
		return &BudgetRefusedError{Plan: plan}
	}

	// Evict first so the new reservation actually has the room the
	// planner promised. With concurrency > 1 the gate's callers should
	// already serialize loads of overlapping budget; the single-flight
	// guarantee is the auto-loader's job.
	for _, victim := range plan.Evict {
		info, ok := g.manager.Get(victim)
		if !ok {
			continue
		}
		prevBytes := info.IndexBytes
		if err := g.manager.EvictForBudget(victim); err != nil && !errors.Is(err, inventory.ErrInvalidState) {
			return fmt.Errorf("evict %s: %w", victim, err)
		}
		g.tracker.Remove(string(victim), prevBytes)
	}

	token := fmt.Sprintf("%s-%d", id, time.Now().UnixNano())
	if err := g.tracker.Reserve(token, opts.EstimateBytes); err != nil {
		if errors.Is(err, budget.ErrOverBudget) && !opts.Force {
			return &BudgetRefusedError{Plan: plan}
		}
		if !opts.Force {
			return fmt.Errorf("reserve: %w", err)
		}
	}
	defer g.tracker.Release(token)

	var loadErr error
	if opts.Pin {
		loadErr = g.manager.LoadWith(ctx, id, build)
	} else {
		loadErr = g.manager.AutoLoad(ctx, id, build)
	}
	if loadErr != nil {
		return fmt.Errorf("load %s: %w", id, loadErr)
	}

	if info, ok := g.manager.Get(id); ok && info.IndexBytes > 0 {
		g.tracker.Add(string(id), info.IndexBytes)
	}
	return nil
}
