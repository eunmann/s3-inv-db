package autoload_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/eunmann/s3-inv-db/internal/autoload"
	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/internal/jobs"
	"github.com/eunmann/s3-inv-db/pkg/extsort/events"
)

// inlineSubmitter stands in for the jobs scheduler: it records the
// submitted inventory IDs and, unless it returns an error, runs the
// work closure synchronously so tests can assert on the load outcome.
type inlineSubmitter struct {
	err       error
	submitted []inventory.ID
}

func (s *inlineSubmitter) Submit(ctx context.Context, invID inventory.ID, _ jobs.Kind, work jobs.Work) (jobs.Job, error) {
	s.submitted = append(s.submitted, invID)
	if s.err != nil {
		return jobs.Job{}, s.err
	}
	_ = work(ctx, func(jobs.Update) {})

	return jobs.Job{ID: "job", InventoryID: invID}, nil
}

// errBoom is the sentinel error used by tests that need to simulate a
// discovery failure.
var errBoom = errors.New("boom")

type fakeDiscovery struct {
	listErr  error
	views    []inventory.MergedInventory
	listCall int
	mu       sync.Mutex
	enabled  bool
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
	loadErr   error
	autoLoadE error
	failedOn  inventory.ID
	loaded    []inventory.ID
	mu        sync.Mutex
	failOnce  bool
}

func (f *fakeLoader) AutoLoad(_ context.Context, disc inventory.Inventory, _ func(stage string, done, total int64), _ *events.Bus) error {
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

func newFakeStores(t *testing.T) (*inventory.ConfigStore, *inventory.Catalog) {
	t.Helper()
	db := openTestDB(t)
	cs := inventory.NewConfigStore(db)
	store, _ := inventory.NewStore(db)
	mgr := inventory.NewCatalog(store)

	return cs, mgr
}

func TestAutoLoader_PicksNewestUnloadedRun(t *testing.T) {
	cs, mgr := newFakeStores(t)
	_ = cs.Upsert(t.Context(), inventory.Config{Source: "bkt", Name: "inv", AutoLoad: true, RetentionCount: 2})

	disc := &fakeDiscovery{
		enabled: true,
		views: []inventory.MergedInventory{
			{Inventory: inventory.Inventory{SourceBucket: "bkt", Name: "inv", Run: "2026-01-01T00-00Z", ManifestKey: "k1"}, State: inventory.StateNotLoaded},
			{Inventory: inventory.Inventory{SourceBucket: "bkt", Name: "inv", Run: "2026-01-02T00-00Z", ManifestKey: "k2"}, State: inventory.StateNotLoaded},
			{Inventory: inventory.Inventory{SourceBucket: "bkt", Name: "inv", Run: "2026-01-03T00-00Z", ManifestKey: "k3"}, State: inventory.StateNotLoaded},
		},
	}
	ldr := &fakeLoader{}
	sub := &inlineSubmitter{}
	a := autoload.New(autoload.Config{}, autoload.Deps{Discovery: disc, Loader: ldr.AutoLoad, Submitter: sub, ConfigStore: cs, Manager: mgr}, nil)
	a.Tick(t.Context())

	if len(ldr.loaded) != 1 {
		t.Fatalf("expected 1 load, got %d", len(ldr.loaded))
	}
	if got := ldr.loaded[0]; got != "bkt/inv/2026-01-03T00-00Z" {
		t.Errorf("loaded %q, want newest run", got)
	}
	// The load must have been routed through the scheduler, not called
	// directly — that's the whole point of the jobmgr integration.
	if len(sub.submitted) != 1 || sub.submitted[0] != "bkt/inv/2026-01-03T00-00Z" {
		t.Errorf("submitted = %v, want one job for the newest run", sub.submitted)
	}
}

func TestAutoLoader_SkipsConfigsWithAutoLoadOff(t *testing.T) {
	cs, mgr := newFakeStores(t)
	_ = cs.Upsert(t.Context(), inventory.Config{Source: "bkt", Name: "inv", AutoLoad: false})

	disc := &fakeDiscovery{
		enabled: true,
		views: []inventory.MergedInventory{
			{Inventory: inventory.Inventory{SourceBucket: "bkt", Name: "inv", Run: "2026-01-01T00-00Z", ManifestKey: "k1"}},
		},
	}
	ldr := &fakeLoader{}
	a := autoload.New(autoload.Config{}, autoload.Deps{Discovery: disc, Loader: ldr.AutoLoad, Submitter: &inlineSubmitter{}, ConfigStore: cs, Manager: mgr}, nil)
	a.Tick(t.Context())
	if len(ldr.loaded) != 0 {
		t.Errorf("expected 0 loads (auto-load off), got %d", len(ldr.loaded))
	}
}

func TestAutoLoader_SkipsUserUnloadedRuns(t *testing.T) {
	cs, mgr := newFakeStores(t)
	_ = cs.Upsert(t.Context(), inventory.Config{Source: "bkt", Name: "inv", AutoLoad: true})

	// Hydrate an Info that's NotLoaded but carries a UserUnloadedAt
	// stamp — simulating "user manually unloaded earlier".
	id := inventory.ID("bkt/inv/2026-01-01T00-00Z")
	if err := mgr.Hydrate(t.Context(), inventory.Info{
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
			{Inventory: inventory.Inventory{SourceBucket: "bkt", Name: "inv", Run: "2026-01-01T00-00Z", ManifestKey: "k1"}, State: inventory.StateNotLoaded},
		},
	}
	ldr := &fakeLoader{}
	a := autoload.New(autoload.Config{}, autoload.Deps{Discovery: disc, Loader: ldr.AutoLoad, Submitter: &inlineSubmitter{}, ConfigStore: cs, Manager: mgr}, nil)
	a.Tick(t.Context())
	if len(ldr.loaded) != 0 {
		t.Errorf("expected 0 loads (user-unloaded), got %d", len(ldr.loaded))
	}
}

func TestAutoLoader_PollFailureSetsBackoff(t *testing.T) {
	cs, mgr := newFakeStores(t)
	_ = cs.Upsert(t.Context(), inventory.Config{Source: "bkt", Name: "inv", AutoLoad: true})

	disc := &fakeDiscovery{enabled: true, listErr: errBoom}
	a := autoload.New(autoload.Config{}, autoload.Deps{Discovery: disc, Loader: (&fakeLoader{}).AutoLoad, Submitter: &inlineSubmitter{}, ConfigStore: cs, Manager: mgr}, nil)
	a.Tick(t.Context())
	cfg, err := cs.Get(t.Context(), "bkt", "inv")
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
		got := autoload.BackoffDelay(minB, maxB, c.count)
		if got != c.want {
			t.Errorf("backoffDelay(%d) = %v, want %v", c.count, got, c.want)
		}
	}
}

func TestAutoLoader_BackoffSuppressesPolling(t *testing.T) {
	cs, mgr := newFakeStores(t)
	future := time.Now().Add(time.Hour)
	_ = cs.Upsert(t.Context(), inventory.Config{Source: "bkt", Name: "inv", AutoLoad: true, PollBackoffUntil: future})

	disc := &fakeDiscovery{enabled: true}
	a := autoload.New(autoload.Config{}, autoload.Deps{Discovery: disc, Loader: (&fakeLoader{}).AutoLoad, Submitter: &inlineSubmitter{}, ConfigStore: cs, Manager: mgr}, nil)
	a.Tick(t.Context())
	if disc.listCall != 0 {
		t.Errorf("Discovery.List should be skipped during backoff, got %d calls", disc.listCall)
	}
}

func TestAutoLoader_SkipsDuplicateSubmission(t *testing.T) {
	cs, mgr := newFakeStores(t)
	_ = cs.Upsert(t.Context(), inventory.Config{Source: "bkt", Name: "inv", AutoLoad: true})

	disc := &fakeDiscovery{
		enabled: true,
		views: []inventory.MergedInventory{
			{Inventory: inventory.Inventory{SourceBucket: "bkt", Name: "inv", Run: "2026-01-01T00-00Z", ManifestKey: "k1"}, State: inventory.StateNotLoaded},
		},
	}
	ldr := &fakeLoader{}
	// Scheduler reports a live job already exists for this inventory.
	sub := &inlineSubmitter{err: jobs.ErrDuplicateInventory}
	a := autoload.New(autoload.Config{}, autoload.Deps{Discovery: disc, Loader: ldr.AutoLoad, Submitter: sub, ConfigStore: cs, Manager: mgr}, nil)
	a.Tick(t.Context())

	if len(sub.submitted) != 1 {
		t.Fatalf("expected 1 submit attempt, got %d", len(sub.submitted))
	}
	if len(ldr.loaded) != 0 {
		t.Errorf("loader must not run when the scheduler reports a duplicate, got %d", len(ldr.loaded))
	}
}

func TestAutoLoader_LoadFailureRecordsBackoff(t *testing.T) {
	cs, mgr := newFakeStores(t)
	_ = cs.Upsert(t.Context(), inventory.Config{Source: "bkt", Name: "inv", AutoLoad: true})
	id := inventory.ID("bkt/inv/2026-01-01T00-00Z")
	if err := mgr.Hydrate(t.Context(), inventory.Info{ID: id, Name: "x", Path: "p", State: inventory.StateNotLoaded}, ""); err != nil {
		t.Fatalf("hydrate: %v", err)
	}

	disc := &fakeDiscovery{
		enabled: true,
		views: []inventory.MergedInventory{
			{Inventory: inventory.Inventory{SourceBucket: "bkt", Name: "inv", Run: "2026-01-01T00-00Z", ManifestKey: "k1"}, State: inventory.StateNotLoaded},
		},
	}
	ldr := &fakeLoader{loadErr: errBoom}
	a := autoload.New(autoload.Config{}, autoload.Deps{Discovery: disc, Loader: ldr.AutoLoad, Submitter: &inlineSubmitter{}, ConfigStore: cs, Manager: mgr}, nil)
	a.Tick(t.Context())

	info, ok := mgr.Get(id)
	if !ok {
		t.Fatal("inventory missing after failed load")
	}
	if info.AutoLoadFailureCount != 1 {
		t.Errorf("AutoLoadFailureCount = %d, want 1", info.AutoLoadFailureCount)
	}
	if info.AutoLoadBackoffUntil.IsZero() {
		t.Error("AutoLoadBackoffUntil should be set after a load failure")
	}
}
