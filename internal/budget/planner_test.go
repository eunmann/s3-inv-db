package budget_test

import (
	"testing"
	"time"

	"github.com/eunmann/s3-inv-db/internal/budget"
	"github.com/eunmann/s3-inv-db/internal/inventory"
)

type fakeConfig map[string]uint32

func (f fakeConfig) Retention(source, name string) uint32 {
	return f[source+"/"+name]
}

func loaded(id string, bytes uint64, loadedAt, accessedAt time.Time, pinned bool) inventory.Info {
	return inventory.Info{
		ID:             inventory.ID(id),
		State:          inventory.StateLoaded,
		IndexBytes:     bytes,
		LoadedAt:       loadedAt,
		LastAccessedAt: accessedAt,
		Pinned:         pinned,
	}
}

func TestPlanner_FitsWithoutEviction(t *testing.T) {
	tr := budget.New(1000, 0)
	tr.Add(100)
	p := budget.NewPlanner(tr, nil)
	in := budget.Input{
		Target:        "src/inv/runC",
		EstimateBytes: 200,
		All:           []inventory.Info{loaded("src/inv/runA", 100, time.Unix(1, 0), time.Unix(1, 0), false)},
	}
	plan, err := p.Plan(in)
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if !plan.Fits() {
		t.Errorf("expected fit, got refusal: %s", plan.Refusal)
	}
	if len(plan.Evict) != 0 {
		t.Errorf("expected no evictions, got %v", plan.Evict)
	}
}

func TestPlanner_EvictsWithinConfigToRespectRetention(t *testing.T) {
	tr := budget.New(1000, 0)
	tr.Add(100)
	tr.Add(100)
	cfg := fakeConfig{"src/inv": 2}
	p := budget.NewPlanner(tr, cfg)
	in := budget.Input{
		Target:        "src/inv/runC",
		EstimateBytes: 100,
		All: []inventory.Info{
			loaded("src/inv/runA", 100, time.Unix(1, 0), time.Unix(1, 0), false),
			loaded("src/inv/runB", 100, time.Unix(2, 0), time.Unix(2, 0), false),
		},
	}
	plan, err := p.Plan(in)
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if !plan.Fits() {
		t.Fatalf("expected fit, got %s", plan.Refusal)
	}
	if len(plan.Evict) != 1 || plan.Evict[0] != "src/inv/runA" {
		t.Errorf("expected to evict runA, got %v", plan.Evict)
	}
}

func TestPlanner_GlobalLRUWhenStillOverBudget(t *testing.T) {
	tr := budget.New(1000, 0)
	tr.Add(400)
	tr.Add(400)
	cfg := fakeConfig{"alpha/inv": 5, "beta/inv": 5} // retention won't force eviction
	p := budget.NewPlanner(tr, cfg)
	in := budget.Input{
		Target:        "gamma/inv/run1",
		EstimateBytes: 400,
		All: []inventory.Info{
			loaded("alpha/inv/run1", 400, time.Unix(1, 0), time.Unix(10, 0), false),
			loaded("beta/inv/run1", 400, time.Unix(2, 0), time.Unix(5, 0), false),
		},
	}
	plan, err := p.Plan(in)
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if !plan.Fits() {
		t.Fatalf("expected fit, got %s", plan.Refusal)
	}
	// beta has older LastAccessedAt -> evicted first.
	if len(plan.Evict) != 1 || plan.Evict[0] != "beta/inv/run1" {
		t.Errorf("expected to evict beta first (oldest accessed), got %v", plan.Evict)
	}
}

func TestPlanner_RefusesWhenAllPinned(t *testing.T) {
	tr := budget.New(500, 0)
	tr.Add(400)
	p := budget.NewPlanner(tr, nil)
	in := budget.Input{
		Target:        "src/inv/runNew",
		EstimateBytes: 200,
		All:           []inventory.Info{loaded("src/inv/runOld", 400, time.Unix(1, 0), time.Unix(1, 0), true)},
	}
	plan, err := p.Plan(in)
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if plan.Fits() {
		t.Errorf("expected refusal, got fit with evictions %v", plan.Evict)
	}
}

func TestPlanner_RefusesWhenEstimateExceedsCap(t *testing.T) {
	tr := budget.New(500, 0)
	p := budget.NewPlanner(tr, nil)
	in := budget.Input{
		Target:        "src/inv/run1",
		EstimateBytes: 600,
	}
	plan, err := p.Plan(in)
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if plan.Fits() {
		t.Error("estimate over cap must refuse")
	}
}

func TestPlanner_ZeroCapPassesThrough(t *testing.T) {
	// No --max-index-disk configured — the planner must not police
	// anything. Otherwise manual loads break for every deployment that
	// hasn't opted into the budget (the regression that prompted this
	// test).
	tr := budget.New(0, 0)
	p := budget.NewPlanner(tr, nil)
	plan, _ := p.Plan(budget.Input{Target: "a/b/c", EstimateBytes: 1})
	if !plan.Fits() {
		t.Errorf("zero-cap planner should pass through, got refusal: %s", plan.Refusal)
	}
}

func TestPlanner_SkipsRunsWithoutKnownSize(t *testing.T) {
	tr := budget.New(1000, 0)
	tr.Add(400) // accounted for elsewhere
	p := budget.NewPlanner(tr, nil)
	in := budget.Input{
		Target:        "src/inv/run2",
		EstimateBytes: 700,
		All: []inventory.Info{
			loaded("src/inv/run1", 0, time.Unix(1, 0), time.Unix(1, 0), false), // unknown bytes, skip
		},
	}
	plan, _ := p.Plan(in)
	if plan.Fits() {
		t.Error("unknown-size eligible run should not be evicted blindly; expected refusal")
	}
}
