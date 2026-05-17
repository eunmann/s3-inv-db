package loadcontrol_test

import (
	"context"
	"errors"
	"testing"

	"github.com/eunmann/s3-inv-db/internal/budget"
	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/internal/loadcontrol"
)

var errBuildOnPurpose = errors.New("build err on purpose")

func newTestGate(capBytes, headroom uint64) (*loadcontrol.Gate, *inventory.Manager, *budget.Tracker) {
	mgr := inventory.NewManager()
	tracker := budget.New(capBytes, headroom)
	planner := budget.NewPlanner(tracker, nil)

	return loadcontrol.New(mgr, tracker, planner), mgr, tracker
}

func TestGate_Load_RefusesWhenEstimateOverBudget(t *testing.T) {
	gate, mgr, _ := newTestGate(100, 0)
	id := inventory.ID("src/inv/runA")
	if err := mgr.Register(t.Context(), id, "n", "p"); err != nil {
		t.Fatal(err)
	}
	build := func(_ context.Context, _ inventory.Info) (string, error) {
		t.Fatal("build must not run when budget refuses")

		return "", nil
	}
	err := gate.Load(context.Background(), id, build, loadcontrol.Options{EstimateBytes: 500, Pin: true})
	var refused *loadcontrol.BudgetRefusedError
	if !errors.As(err, &refused) {
		t.Errorf("expected BudgetRefusedError, got %v", err)
	}
}

func TestGate_Load_ForceBypassesRefusal(t *testing.T) {
	gate, mgr, _ := newTestGate(100, 0)
	id := inventory.ID("src/inv/runA")
	if err := mgr.Register(t.Context(), id, "n", "p"); err != nil {
		t.Fatal(err)
	}
	called := false
	build := func(_ context.Context, _ inventory.Info) (string, error) {
		called = true

		return "", errBuildOnPurpose
	}
	_ = gate.Load(context.Background(), id, build, loadcontrol.Options{EstimateBytes: 500, Force: true, Pin: true})
	if !called {
		t.Error("force should let the build attempt run despite budget refusal")
	}
}

// TestGate_Load_RefusalCarriesPlan ensures the budget refusal carries
// the planner's verdict so the UI can surface the reason. End-to-end
// eviction is exercised in the planner tests (real Loaded state with
// IndexBytes requires a real index, which is overkill for this layer).
func TestGate_Load_RefusalCarriesPlan(t *testing.T) {
	gate, mgr, _ := newTestGate(100, 0)
	id := inventory.ID("src/inv/runA")
	if err := mgr.Register(t.Context(), id, "n", "p"); err != nil {
		t.Fatal(err)
	}
	err := gate.Load(context.Background(), id, nil, loadcontrol.Options{EstimateBytes: 9999, Pin: false})
	var refused *loadcontrol.BudgetRefusedError
	if !errors.As(err, &refused) {
		t.Fatalf("expected BudgetRefusedError, got %v", err)
	}
	if refused.Plan.EstimateBytes != 9999 {
		t.Errorf("plan estimate = %d, want 9999", refused.Plan.EstimateBytes)
	}
	if refused.Plan.Fits() {
		t.Error("plan should not fit when estimate exceeds cap")
	}
}

// Avoid unused import errors.
