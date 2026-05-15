package budget

import (
	"testing"
	"time"

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
	tr := New(1000, 0)
	tr.Add("x", 100)
	p := NewPlanner(tr, nil)
	in := Input{
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
	tr := New(1000, 0)
	tr.Add("a", 100)
	tr.Add("b", 100)
	cfg := fakeConfig{"src/inv": 2}
	p := NewPlanner(tr, cfg)
	in := Input{
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
	tr := New(1000, 0)
	tr.Add("a", 400)
	tr.Add("b", 400)
	cfg := fakeConfig{"alpha/inv": 5, "beta/inv": 5} // retention won't force eviction
	p := NewPlanner(tr, cfg)
	in := Input{
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
	tr := New(500, 0)
	tr.Add("p1", 400)
	p := NewPlanner(tr, nil)
	in := Input{
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
	tr := New(500, 0)
	p := NewPlanner(tr, nil)
	in := Input{
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

func TestPlanner_ZeroCapRefuses(t *testing.T) {
	tr := New(0, 0)
	p := NewPlanner(tr, nil)
	plan, _ := p.Plan(Input{Target: "a/b/c", EstimateBytes: 1})
	if plan.Fits() {
		t.Error("zero cap must refuse non-zero estimate")
	}
}

func TestPlanner_SkipsRunsWithoutKnownSize(t *testing.T) {
	tr := New(1000, 0)
	tr.Add("x", 400) // accounted for elsewhere
	p := NewPlanner(tr, nil)
	in := Input{
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
