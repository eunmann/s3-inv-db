package loadcontrol_test

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/eunmann/s3-inv-db/internal/budget"
	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/internal/loadcontrol"
	"github.com/eunmann/s3-inv-db/pkg/extsort"
	"github.com/eunmann/s3-inv-db/pkg/indexread"
	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

var errBuildOnPurpose = errors.New("build err on purpose")

var errBuildFailed = errors.New("build failed")

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

// A failed build leaves the run past retention loaded, with its cache:
// retention evictions wait for a successful load.
func TestGate_Load_FailedBuildKeepsRetentionVictim(t *testing.T) {
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
	ok := func(context.Context, inventory.Info) (string, error) { return dir, nil }
	fail := func(context.Context, inventory.Info) (string, error) { return "", errBuildFailed }
	runs := []inventory.ID{"src/inv/2026-01-01T00-00Z", "src/inv/2026-01-02T00-00Z", "src/inv/2026-01-03T00-00Z"}
	for _, id := range runs {
		if err := mgr.Register(ctx, id, "n", ""); err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range runs[:2] {
		if err := gate.Load(ctx, id, ok, inventory.GatedLoadOptions{EstimateBytes: 1}); err != nil {
			t.Fatalf("load %s: %v", id, err)
		}
	}
	if err := gate.Load(ctx, runs[2], fail, inventory.GatedLoadOptions{EstimateBytes: 1}); err == nil {
		t.Fatal("failing build reported success")
	}
	if len(removed) != 0 {
		t.Errorf("removed caches = %v, want none after a failed build", removed)
	}
	for _, id := range runs[:2] {
		if info, _ := mgr.Get(id); info.State != inventory.StateLoaded {
			t.Errorf("%s state = %s, want loaded", id, info.State)
		}
	}
}

// A pinned load of an older run retires nothing: pinned runs don't count
// toward retention, and the auto-loader would rebuild a retired run.
func TestGate_Load_PinnedLoadDoesNotRetire(t *testing.T) {
	ctx := t.Context()
	mgr := inventory.NewCatalog(nil)
	defer mgr.Close()
	tracker := budget.New(1<<30, 0, mgr.LoadedBytes)
	gate := loadcontrol.New(mgr, tracker, budget.NewPlanner(tracker, nil), func(inventory.ID) error { return nil })
	dir := buildIndexDir(t)
	build := func(context.Context, inventory.Info) (string, error) { return dir, nil }
	runs := []inventory.ID{"src/inv/2026-01-01T00-00Z", "src/inv/2026-01-02T00-00Z", "src/inv/2025-12-31T00-00Z"}
	for _, id := range runs {
		if err := mgr.Register(ctx, id, "n", ""); err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range runs[:2] {
		if err := gate.Load(ctx, id, build, inventory.GatedLoadOptions{EstimateBytes: 1}); err != nil {
			t.Fatalf("load %s: %v", id, err)
		}
	}
	if err := gate.Load(ctx, runs[2], build, inventory.GatedLoadOptions{EstimateBytes: 1, Pin: true}); err != nil {
		t.Fatalf("pinned load: %v", err)
	}
	for _, id := range runs {
		if info, _ := mgr.Get(id); info.State != inventory.StateLoaded {
			t.Errorf("%s state = %s, want loaded", id, info.State)
		}
	}
}

// A run pinned while a newer run builds is not retired after the build:
// pinned runs are never auto-evicted.
func TestGate_Load_PinnedDuringBuildSkipsRetirement(t *testing.T) {
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
	ok := func(context.Context, inventory.Info) (string, error) { return dir, nil }
	runs := []inventory.ID{"src/inv/2026-01-01T00-00Z", "src/inv/2026-01-02T00-00Z", "src/inv/2026-01-03T00-00Z"}
	for _, id := range runs {
		if err := mgr.Register(ctx, id, "n", ""); err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range runs[:2] {
		if err := gate.Load(ctx, id, ok, inventory.GatedLoadOptions{EstimateBytes: 1}); err != nil {
			t.Fatalf("load %s: %v", id, err)
		}
	}
	pinOldest := func(ctx context.Context, _ inventory.Info) (string, error) {
		if err := mgr.SetPinned(ctx, runs[0], true); err != nil {
			return "", fmt.Errorf("pin: %w", err)
		}

		return dir, nil
	}
	if err := gate.Load(ctx, runs[2], pinOldest, inventory.GatedLoadOptions{EstimateBytes: 1}); err != nil {
		t.Fatalf("load %s: %v", runs[2], err)
	}
	if len(removed) != 0 {
		t.Errorf("removed caches = %v, want none: the oldest run was pinned during the build", removed)
	}
	for _, id := range runs {
		if info, _ := mgr.Get(id); info.State != inventory.StateLoaded {
			t.Errorf("%s state = %s, want loaded", id, info.State)
		}
	}
}

// A load that needs disk while another load's build is in flight evicts
// the run that build retires once it finishes, not another
// configuration's run worth keeping.
func TestGate_Load_ConcurrentLoadEvictsRetiringRunFirst(t *testing.T) {
	ctx := t.Context()
	mgr := inventory.NewCatalog(nil)
	defer mgr.Close()
	dir := buildIndexDir(t)
	ok := func(context.Context, inventory.Info) (string, error) { return dir, nil }

	z1 := inventory.ID("z/inv/2026-01-01T00-00Z")
	x1 := inventory.ID("x/inv/2026-01-01T00-00Z")
	x2 := inventory.ID("x/inv/2026-01-02T00-00Z")
	x3 := inventory.ID("x/inv/2026-01-03T00-00Z")
	y1 := inventory.ID("y/inv/2026-01-01T00-00Z")
	for _, id := range []inventory.ID{z1, x1, x2, x3, y1} {
		if err := mgr.Register(ctx, id, "n", ""); err != nil {
			t.Fatal(err)
		}
	}
	// Load z1, x1 and x2 without a budget to learn the index size.
	setup := budget.New(0, 0, mgr.LoadedBytes)
	setupGate := loadcontrol.New(mgr, setup, budget.NewPlanner(setup, nil), nil)
	for _, id := range []inventory.ID{z1, x1, x2} {
		if err := setupGate.Load(ctx, id, ok, inventory.GatedLoadOptions{EstimateBytes: 1}); err != nil {
			t.Fatalf("load %s: %v", id, err)
		}
	}
	info, _ := mgr.Get(z1)
	size := info.IndexBytes

	// Room for one more index: x3 fits and retires x1 after its build;
	// y1, admitted while x3 builds, must evict one run.
	tracker := budget.New(4*size, 0, mgr.LoadedBytes)
	var removed []inventory.ID
	var removedMu sync.Mutex
	gate := loadcontrol.New(mgr, tracker, budget.NewPlanner(tracker, nil), func(id inventory.ID) error {
		removedMu.Lock()
		defer removedMu.Unlock()
		removed = append(removed, id)

		return nil
	})
	building := make(chan struct{})
	finish := make(chan struct{})
	slow := func(context.Context, inventory.Info) (string, error) {
		close(building)
		<-finish

		return dir, nil
	}
	x3Err := make(chan error, 1)
	go func() {
		x3Err <- gate.Load(ctx, x3, slow, inventory.GatedLoadOptions{EstimateBytes: size})
	}()
	<-building
	y1Err := gate.Load(ctx, y1, ok, inventory.GatedLoadOptions{EstimateBytes: size})
	close(finish)
	if err := <-x3Err; err != nil {
		t.Fatalf("load %s: %v", x3, err)
	}
	if y1Err != nil {
		t.Fatalf("load %s: %v", y1, y1Err)
	}

	if got, _ := mgr.Get(z1); got.State != inventory.StateLoaded {
		t.Errorf("%s state = %s, want loaded: it isn't past retention", z1, got.State)
	}
	if got, _ := mgr.Get(x1); got.State != inventory.StateNotLoaded {
		t.Errorf("%s state = %s, want not_loaded", x1, got.State)
	}
	if len(removed) != 1 || removed[0] != x1 {
		t.Errorf("removed caches = %v, want only %s", removed, x1)
	}
}

// EvictForBudget checks the pin in the same critical section as the
// unload: a pinned run is refused and stays loaded; an unpinned one goes.
func TestCatalog_EvictForBudgetRefusesPinned(t *testing.T) {
	ctx := t.Context()
	mgr := inventory.NewCatalog(nil)
	defer mgr.Close()
	tracker := budget.New(1<<30, 0, mgr.LoadedBytes)
	gate := loadcontrol.New(mgr, tracker, budget.NewPlanner(tracker, nil), nil)
	dir := buildIndexDir(t)
	const id inventory.ID = "src/inv/2026-01-01T00-00Z"
	if err := mgr.Register(ctx, id, "n", ""); err != nil {
		t.Fatal(err)
	}
	if err := gate.Load(ctx, id, func(context.Context, inventory.Info) (string, error) { return dir, nil },
		inventory.GatedLoadOptions{EstimateBytes: 1, Pin: true}); err != nil {
		t.Fatal(err)
	}
	if err := mgr.EvictForBudget(ctx, id); !errors.Is(err, inventory.ErrPinned) {
		t.Fatalf("EvictForBudget(pinned) = %v, want ErrPinned", err)
	}
	if info, _ := mgr.Get(id); info.State != inventory.StateLoaded {
		t.Fatalf("pinned run state = %s, want loaded", info.State)
	}
	if err := mgr.SetPinned(ctx, id, false); err != nil {
		t.Fatal(err)
	}
	if err := mgr.EvictForBudget(ctx, id); err != nil {
		t.Fatalf("EvictForBudget(unpinned) = %v", err)
	}
	if info, _ := mgr.Get(id); info.State != inventory.StateNotLoaded {
		t.Errorf("unpinned run state = %s, want not_loaded", info.State)
	}
}

// holdReaders holds a WithIndex reader on each of ids until the returned
// release is called, so unloading any of them blocks until then.
func holdReaders(t *testing.T, mgr *inventory.Catalog, ids ...inventory.ID) func() {
	t.Helper()
	done := make(chan struct{})
	var wg sync.WaitGroup
	for _, id := range ids {
		held := make(chan struct{})
		wg.Go(func() {
			err := mgr.WithIndex(id, func(*indexread.Index) error {
				close(held)
				<-done

				return nil
			})
			if err != nil {
				t.Errorf("hold reader on %s: %v", id, err)
				close(held)
			}
		})
		<-held
	}

	return func() {
		close(done)
		wg.Wait()
	}
}

// awaitFirstUnloaded returns which of ids leaves the loaded state first.
func awaitFirstUnloaded(t *testing.T, mgr *inventory.Catalog, ids ...inventory.ID) inventory.ID {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		for _, id := range ids {
			if info, _ := mgr.Get(id); info.State != inventory.StateLoaded {
				return id
			}
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("none of %v was unloaded", ids)

	return ""
}

// pinSecondVictim starts load, waits until its first eviction of ids is
// blocked on a reader, pins the other run, then lets the load finish. It
// returns the pinned run and the load's error.
func pinSecondVictim(t *testing.T, mgr *inventory.Catalog, load func() error, ids [2]inventory.ID) (inventory.ID, error) {
	t.Helper()
	release := holdReaders(t, mgr, ids[0], ids[1])
	loadErr := make(chan error, 1)
	go func() { loadErr <- load() }()
	pinned := ids[0]
	if awaitFirstUnloaded(t, mgr, ids[0], ids[1]) == ids[0] {
		pinned = ids[1]
	}
	if err := mgr.SetPinned(t.Context(), pinned, true); err != nil {
		t.Fatal(err)
	}
	release()

	return pinned, <-loadErr
}

// A run the user pins after the gate planned to evict it for disk stays
// loaded with its cache, and the load is refused for the space it then
// lacks.
func TestGate_Load_PinnedAfterPlanningSkipsDiskEviction(t *testing.T) {
	ctx := t.Context()
	mgr := inventory.NewCatalog(nil)
	defer mgr.Close()
	dir := buildIndexDir(t)
	ok := func(context.Context, inventory.Info) (string, error) { return dir, nil }
	a := inventory.ID("a/inv/2026-01-01T00-00Z")
	b := inventory.ID("b/inv/2026-01-01T00-00Z")
	c := inventory.ID("c/inv/2026-01-01T00-00Z")
	for _, id := range []inventory.ID{a, b, c} {
		if err := mgr.Register(ctx, id, "n", ""); err != nil {
			t.Fatal(err)
		}
	}
	setup := budget.New(0, 0, mgr.LoadedBytes)
	setupGate := loadcontrol.New(mgr, setup, budget.NewPlanner(setup, nil), nil)
	for _, id := range []inventory.ID{a, b} {
		if err := setupGate.Load(ctx, id, ok, inventory.GatedLoadOptions{EstimateBytes: 1}); err != nil {
			t.Fatalf("load %s: %v", id, err)
		}
	}
	info, _ := mgr.Get(a)
	size := info.IndexBytes

	// c needs the disk of both a and b.
	tracker := budget.New(2*size, 0, mgr.LoadedBytes)
	var removed []inventory.ID
	var removedMu sync.Mutex
	gate := loadcontrol.New(mgr, tracker, budget.NewPlanner(tracker, nil), func(id inventory.ID) error {
		removedMu.Lock()
		defer removedMu.Unlock()
		removed = append(removed, id)

		return nil
	})
	pinned, err := pinSecondVictim(t, mgr, func() error {
		return gate.Load(ctx, c, ok, inventory.GatedLoadOptions{EstimateBytes: 2 * size})
	}, [2]inventory.ID{a, b})

	if _, refused := errors.AsType[*loadcontrol.BudgetRefusedError](err); !refused {
		t.Errorf("load %s = %v, want BudgetRefusedError", c, err)
	}
	if got, _ := mgr.Get(pinned); got.State != inventory.StateLoaded {
		t.Errorf("pinned %s state = %s, want loaded", pinned, got.State)
	}
	for _, id := range removed {
		if id == pinned {
			t.Errorf("removed caches = %v, want not the pinned %s", removed, pinned)
		}
	}
}

// A run the user pins after the gate planned to retire it stays loaded
// with its cache once the load succeeds.
func TestGate_Load_PinnedAfterPlanningSkipsRetirement(t *testing.T) {
	ctx := t.Context()
	mgr := inventory.NewCatalog(nil)
	defer mgr.Close()
	dir := buildIndexDir(t)
	ok := func(context.Context, inventory.Info) (string, error) { return dir, nil }
	runs := []inventory.ID{"src/inv/2026-01-01T00-00Z", "src/inv/2026-01-02T00-00Z", "src/inv/2026-01-03T00-00Z"}
	for _, id := range runs {
		if err := mgr.Register(ctx, id, "n", ""); err != nil {
			t.Fatal(err)
		}
	}
	// Two runs loaded with no budget, so neither is retired yet.
	setup := budget.New(0, 0, mgr.LoadedBytes)
	setupGate := loadcontrol.New(mgr, setup, budget.NewPlanner(setup, nil), nil)
	for _, id := range runs[:2] {
		if err := setupGate.Load(ctx, id, ok, inventory.GatedLoadOptions{EstimateBytes: 1}); err != nil {
			t.Fatalf("load %s: %v", id, err)
		}
	}

	// Retention 1: loading runs[2] retires both older runs after its build.
	tracker := budget.New(1<<30, 0, mgr.LoadedBytes)
	retention := func(context.Context, string, string) uint32 { return 1 }
	var removed []inventory.ID
	gate := loadcontrol.New(mgr, tracker, budget.NewPlanner(tracker, retention), func(id inventory.ID) error {
		removed = append(removed, id)

		return nil
	})
	pinned, err := pinSecondVictim(t, mgr, func() error {
		return gate.Load(ctx, runs[2], ok, inventory.GatedLoadOptions{EstimateBytes: 1})
	}, [2]inventory.ID{runs[0], runs[1]})
	if err != nil {
		t.Fatalf("load %s: %v", runs[2], err)
	}
	if got, _ := mgr.Get(pinned); got.State != inventory.StateLoaded {
		t.Errorf("pinned %s state = %s, want loaded", pinned, got.State)
	}
	if len(removed) != 1 || removed[0] == pinned {
		t.Errorf("removed caches = %v, want only the unpinned run", removed)
	}
}
