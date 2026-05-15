package autoload

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/internal/migrate"
	_ "modernc.org/sqlite"
)

type fakeDiscovery struct {
	mu       sync.Mutex
	enabled  bool
	views    []inventory.MergedInventory
	listCall int
	listErr  error
}

func (f *fakeDiscovery) Enabled() bool {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.enabled
}

func (f *fakeDiscovery) List(_ context.Context) ([]inventory.MergedInventory, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.listCall++
	if f.listErr != nil {
		return nil, f.listErr
	}

	return f.views, nil
}

type fakeLoader struct {
	mu        sync.Mutex
	loaded    []inventory.ID
	failOnce  bool
	failedOn  inventory.ID
	loadErr   error
	autoLoadE error
}

func (f *fakeLoader) AutoLoad(_ context.Context, disc inventory.Inventory) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	id := disc.CompositeID()
	if f.failOnce && f.failedOn != id {
		f.failedOn = id

		return f.autoLoadE
	}
	if f.loadErr != nil {
		return f.loadErr
	}
	f.loaded = append(f.loaded, id)

	return nil
}

func newFakeStores(t *testing.T) (*inventory.ConfigStore, *inventory.Manager) {
	t.Helper()
	db := openTestDB(t)
	if err := migrate.Apply(db); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	cs := inventory.NewConfigStore(db)
	mgr := inventory.NewManager()
	store, _ := inventory.NewStore(db)
	mgr.SetStore(store)

	return cs, mgr
}

func TestAutoLoader_PicksNewestUnloadedRun(t *testing.T) {
	cs, mgr := newFakeStores(t)
	_ = cs.Upsert(inventory.Config{Source: "bkt", Name: "inv", AutoLoad: true, RetentionCount: 2})

	disc := &fakeDiscovery{
		enabled: true,
		views: []inventory.MergedInventory{
			{Inventory: inventory.Inventory{SourceBucket: "bkt", InventoryName: "inv", Run: "2026-01-01T00-00Z", ManifestKey: "k1"}, State: inventory.StateNotLoaded},
			{Inventory: inventory.Inventory{SourceBucket: "bkt", InventoryName: "inv", Run: "2026-01-02T00-00Z", ManifestKey: "k2"}, State: inventory.StateNotLoaded},
			{Inventory: inventory.Inventory{SourceBucket: "bkt", InventoryName: "inv", Run: "2026-01-03T00-00Z", ManifestKey: "k3"}, State: inventory.StateNotLoaded},
		},
	}
	ldr := &fakeLoader{}
	a := New(Config{MaxConcurrency: 1, MinBackoff: time.Millisecond, MaxBackoff: time.Millisecond}, disc, ldr, cs, mgr, nil)
	a.tick(context.Background())

	if len(ldr.loaded) != 1 {
		t.Fatalf("expected 1 load, got %d", len(ldr.loaded))
	}
	if got := ldr.loaded[0]; got != "bkt/inv/2026-01-03T00-00Z" {
		t.Errorf("loaded %q, want newest run", got)
	}
}

func TestAutoLoader_SkipsConfigsWithAutoLoadOff(t *testing.T) {
	cs, mgr := newFakeStores(t)
	_ = cs.Upsert(inventory.Config{Source: "bkt", Name: "inv", AutoLoad: false})

	disc := &fakeDiscovery{
		enabled: true,
		views: []inventory.MergedInventory{
			{Inventory: inventory.Inventory{SourceBucket: "bkt", InventoryName: "inv", Run: "2026-01-01T00-00Z", ManifestKey: "k1"}},
		},
	}
	ldr := &fakeLoader{}
	a := New(Config{MaxConcurrency: 1}, disc, ldr, cs, mgr, nil)
	a.tick(context.Background())
	if len(ldr.loaded) != 0 {
		t.Errorf("expected 0 loads (auto-load off), got %d", len(ldr.loaded))
	}
}

func TestAutoLoader_SkipsUserUnloadedRuns(t *testing.T) {
	cs, mgr := newFakeStores(t)
	_ = cs.Upsert(inventory.Config{Source: "bkt", Name: "inv", AutoLoad: true})

	// Hydrate an Info that's NotLoaded but carries a UserUnloadedAt
	// stamp — simulating "user manually unloaded earlier".
	id := inventory.ID("bkt/inv/2026-01-01T00-00Z")
	if err := mgr.Hydrate(inventory.Info{
		ID:             id,
		Name:           "x",
		Path:           "p",
		State:          inventory.StateNotLoaded,
		UserUnloadedAt: time.Now().Add(-time.Hour),
	}, ""); err != nil {
		t.Fatalf("hydrate: %v", err)
	}

	disc := &fakeDiscovery{
		enabled: true,
		views: []inventory.MergedInventory{
			{Inventory: inventory.Inventory{SourceBucket: "bkt", InventoryName: "inv", Run: "2026-01-01T00-00Z", ManifestKey: "k1"}, State: inventory.StateNotLoaded},
		},
	}
	ldr := &fakeLoader{}
	a := New(Config{MaxConcurrency: 1}, disc, ldr, cs, mgr, nil)
	a.tick(context.Background())
	if len(ldr.loaded) != 0 {
		t.Errorf("expected 0 loads (user-unloaded), got %d", len(ldr.loaded))
	}
}

func TestAutoLoader_PollFailureSetsBackoff(t *testing.T) {
	cs, mgr := newFakeStores(t)
	_ = cs.Upsert(inventory.Config{Source: "bkt", Name: "inv", AutoLoad: true})

	disc := &fakeDiscovery{enabled: true, listErr: errors.New("boom")}
	a := New(Config{MaxConcurrency: 1, MinBackoff: time.Minute, MaxBackoff: time.Hour}, disc, &fakeLoader{}, cs, mgr, nil)
	a.tick(context.Background())
	cfg, err := cs.Get("bkt", "inv")
	if err != nil {
		t.Fatalf("get cfg: %v", err)
	}
	if cfg.PollFailureCount != 1 {
		t.Errorf("PollFailureCount = %d, want 1", cfg.PollFailureCount)
	}
	if cfg.LastPollError != "boom" {
		t.Errorf("LastPollError = %q, want boom", cfg.LastPollError)
	}
	if cfg.PollBackoffUntil.IsZero() {
		t.Error("PollBackoffUntil should be set after a poll failure")
	}
}

func TestBackoffDelay(t *testing.T) {
	const minB = time.Minute
	const maxB = time.Hour
	cases := []struct {
		count uint32
		want  time.Duration
	}{
		{0, time.Minute},
		{1, 2 * time.Minute},
		{2, 4 * time.Minute},
		{5, 32 * time.Minute},
		{6, time.Hour},
		{32, time.Hour},
		{99, time.Hour},
	}
	for _, c := range cases {
		got := backoffDelay(minB, maxB, c.count)
		if got != c.want {
			t.Errorf("backoffDelay(%d) = %v, want %v", c.count, got, c.want)
		}
	}
}

func TestAutoLoader_BackoffSuppressesPolling(t *testing.T) {
	cs, mgr := newFakeStores(t)
	future := time.Now().Add(time.Hour)
	_ = cs.Upsert(inventory.Config{Source: "bkt", Name: "inv", AutoLoad: true, PollBackoffUntil: future})

	disc := &fakeDiscovery{enabled: true}
	a := New(Config{MaxConcurrency: 1}, disc, &fakeLoader{}, cs, mgr, nil)
	a.tick(context.Background())
	if disc.listCall != 0 {
		t.Errorf("Discovery.List should be skipped during backoff, got %d calls", disc.listCall)
	}
}
