package loadcontrol_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/eunmann/s3-inv-db/internal/budget"
	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/internal/loadcontrol"
	"github.com/eunmann/s3-inv-db/pkg/extsort"
	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

var errBuildOnPurpose = errors.New("build err on purpose")

func newTestGate(capBytes, headroom uint64) (*loadcontrol.Gate, *inventory.Catalog, *budget.Tracker) {
	mgr := inventory.NewCatalog(nil)
	tracker := budget.New(capBytes, headroom, mgr.LoadedBytes)
	planner := budget.NewPlanner(tracker, nil)

	return loadcontrol.New(mgr, tracker, planner, nil), mgr, tracker
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
	err := gate.Load(t.Context(), id, build, inventory.GatedLoadOptions{EstimateBytes: 500, Pin: true})
	if _, ok := errors.AsType[*loadcontrol.BudgetRefusedError](err); !ok {
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
	_ = gate.Load(t.Context(), id, build, inventory.GatedLoadOptions{EstimateBytes: 500, Force: true, Pin: true})
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
	err := gate.Load(t.Context(), id, nil, inventory.GatedLoadOptions{EstimateBytes: 9999, Pin: false})
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

// buildIndexDir builds a tiny real index the catalog can open.
func buildIndexDir(t *testing.T) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "index")
	agg := extsort.NewAggregator(2, 0)
	agg.AddObject("a/x", 1, tiers.Standard)
	agg.AddObject("b/y", 1, tiers.Standard)
	rows := agg.Drain()
	extsort.SortPrefixRows(rows)
	b, err := extsort.NewIndexBuilder(dir, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := b.SetPresentTiers(agg.PresentTiers()); err != nil {
		t.Fatal(err)
	}
	for _, r := range rows {
		if err := b.Add(r); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.Finalize(); err != nil {
		t.Fatal(err)
	}

	return dir
}

// Retention eviction must free disk: the evicted run's cache is deleted
// (it used to stay on disk, outside the budget), and "used" follows the
// catalog without any bookkeeping in the gate.
func TestGate_Load_RetentionEvictionRemovesCache(t *testing.T) {
	ctx := t.Context()
	mgr := inventory.NewCatalog(nil)
	defer mgr.Close()
	tracker := budget.New(1<<30, 0, mgr.LoadedBytes)
	var removed []inventory.ID
	gate := loadcontrol.New(mgr, tracker, budget.NewPlanner(tracker, nil), func(id inventory.ID) error {
		removed = append(removed, id)

		return nil
	})
	dir := buildIndexDir(t)
	build := func(context.Context, inventory.Info) (string, error) { return dir, nil }
	runs := []inventory.ID{"src/inv/2026-01-01T00-00Z", "src/inv/2026-01-02T00-00Z", "src/inv/2026-01-03T00-00Z"}
	for _, id := range runs {
		if err := mgr.Register(ctx, id, "n", ""); err != nil {
			t.Fatal(err)
		}
		if err := gate.Load(ctx, id, build, inventory.GatedLoadOptions{EstimateBytes: 1}); err != nil {
			t.Fatalf("load %s: %v", id, err)
		}
	}

	if len(removed) != 1 || removed[0] != runs[0] {
		t.Fatalf("removed caches = %v, want only the oldest run %s", removed, runs[0])
	}
	if info, _ := mgr.Get(runs[0]); info.State != inventory.StateNotLoaded {
		t.Errorf("oldest run state = %s, want not_loaded", info.State)
	}
	info, _ := mgr.Get(runs[2])
	if got, want := tracker.Used(), 2*info.IndexBytes; got != want || want == 0 {
		t.Errorf("Used = %d, want %d (two loaded runs)", got, want)
	}
}
