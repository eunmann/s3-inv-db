// Package loadcontrol orchestrates one load lifecycle — plan, evict,
// reserve, build, release — for both manual and auto loads.
package loadcontrol

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/eunmann/s3-inv-db/internal/budget"
	"github.com/eunmann/s3-inv-db/internal/inventory"
)

// Build / Options are convenience re-exports so callers can
// reference loadgate types without a parallel inventory import.
type (
	Build   = inventory.BuildFunc
	Options = inventory.GatedLoadOptions
)

// BudgetRefusedError carries the planner's verdict to the UI.
type BudgetRefusedError struct {
	Plan budget.Plan
}

func (e *BudgetRefusedError) Error() string {
	return "load refused by disk-budget planner: " + e.Plan.Refusal
}

// Gate orchestrates one load lifecycle.
type Gate struct {
	manager *inventory.Manager
	tracker *budget.Tracker
	planner *budget.Planner
}

func New(manager *inventory.Manager, tracker *budget.Tracker, planner *budget.Planner) *Gate {
	return &Gate{manager: manager, tracker: tracker, planner: planner}
}

// Load plans, evicts, reserves, builds, releases. Returns
// *BudgetRefusedError when the planner refuses and opts.Force is false.
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

	for _, victim := range plan.Evict {
		info, ok := g.manager.Get(victim)
		if !ok {
			continue
		}
		prevBytes := info.IndexBytes
		if err := g.manager.EvictForBudget(ctx, victim); err != nil && !errors.Is(err, inventory.ErrInvalidState) {
			return fmt.Errorf("evict %s: %w", victim, err)
		}
		g.tracker.Remove(prevBytes)
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
		g.tracker.Add(info.IndexBytes)
	}

	return nil
}
