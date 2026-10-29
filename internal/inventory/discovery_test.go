package inventory_test

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/pkg/extsort/events"
)

// errFakeS3Throttled is the sentinel returned by tests that simulate
// a throttled S3 listing. The err113 linter forbids errors.New at call
// sites so the sentinel is package-private.
var errFakeS3Throttled = errors.New("s3: throttled")

// fakeDiscoverer is a minimal stub for the inventory.Discoverer interface that
// the Discovery consumes. Each method has a single response to
// keep tests simple; specialise per-test by setting fields directly.
type fakeDiscoverer struct {
	listErr   error
	findErr   error
	findResp  inventory.Inventory
	listGate  chan struct{}
	bucket    string
	listResp  []inventory.Inventory
	listCalls atomic.Int64
	findCalls atomic.Int64
}

func (f *fakeDiscoverer) List(context.Context) ([]inventory.Inventory, error) {
	f.listCalls.Add(1)
	if f.listGate != nil {
		<-f.listGate
	}

	return f.listResp, f.listErr
}

func (f *fakeDiscoverer) Find(_ context.Context, _, _, _ string) (inventory.Inventory, error) {
	f.findCalls.Add(1)

	return f.findResp, f.findErr
}
func (f *fakeDiscoverer) Bucket() string { return f.bucket }

// fakeBuilder satisfies the inventory.IndexBuilder interface.
type fakeBuilder struct {
	buildErr  error
	buildResp string
}

func (f *fakeBuilder) BuildWith(_ context.Context, _ inventory.CacheKey, _ string, _ events.ProgressFunc, _ *events.Bus) (string, error) {
	return f.buildResp, f.buildErr
}

func (f *fakeBuilder) RemoveCache(inventory.CacheKey) error             { return nil }
func (f *fakeBuilder) CacheSizeBytes(inventory.CacheKey) (int64, error) { return 0, nil }

func TestDiscoveryService_EnabledReflectsConstructor(t *testing.T) {
	mgr := inventory.NewCatalog(nil)
	t.Cleanup(func() { _ = mgr.Close() })

	if got := inventory.NewDiscovery(mgr).Enabled(); got {
		t.Errorf("NewDiscovery(mgr) Enabled() = true, want false")
	}
	if got := inventory.NewDiscovery(mgr, inventory.WithBackend(&fakeDiscoverer{}, &fakeBuilder{})).Enabled(); !got {
		t.Errorf("NewDiscovery(mgr, d, b) Enabled() = false, want true")
	}
}

func TestDiscoveryService_ListWhenDisabledReturnsErr(t *testing.T) {
	s := inventory.NewDiscovery(inventory.NewCatalog(nil))
	_, err := s.List(t.Context())
	if !errors.Is(err, inventory.ErrDiscoveryDisabled) {
		t.Errorf("List() error = %v, want inventory.ErrDiscoveryDisabled", err)
	}
}

func TestDiscoveryService_FindWhenDisabledReturnsErr(t *testing.T) {
	s := inventory.NewDiscovery(inventory.NewCatalog(nil))
	_, err := s.Find(t.Context(), "src", "id", "")
	if !errors.Is(err, inventory.ErrDiscoveryDisabled) {
		t.Errorf("Find() error = %v, want inventory.ErrDiscoveryDisabled", err)
	}
}

func TestDiscoveryService_LoadWhenDisabledReturnsErr(t *testing.T) {
	s := inventory.NewDiscovery(inventory.NewCatalog(nil))
	err := s.Load(t.Context(), inventory.Inventory{})
	if !errors.Is(err, inventory.ErrDiscoveryDisabled) {
		t.Errorf("Load() error = %v, want inventory.ErrDiscoveryDisabled", err)
	}
}

func TestDiscoveryService_ListMergesWithManagerState(t *testing.T) {
	mgr := inventory.NewCatalog(nil)
	t.Cleanup(func() { _ = mgr.Close() })

	// Pre-load the manager with state for one of the inventories we'll
	// surface via the discoverer. The merge should preserve that state
	// instead of returning the default inventory.StateNotLoaded for known IDs.
	if err := mgr.Register(t.Context(), "bucket-a/inv-1", "Pre-registered", "/path"); err != nil {
		t.Fatalf("Register: %v", err)
	}

	disc := &fakeDiscoverer{
		listResp: []inventory.Inventory{
			{SourceBucket: "bucket-a", Name: "inv-1"},
			{SourceBucket: "bucket-a", Name: "inv-2"},
		},
	}
	s := inventory.NewDiscovery(mgr, inventory.WithBackend(disc, &fakeBuilder{}))
	views, err := s.List(t.Context())
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(views) != 2 {
		t.Fatalf("len(views) = %d, want 2", len(views))
	}

	// inv-1 is registered → State should be inventory.StateNotLoaded (the registered default).
	if views[0].CompositeID() != "bucket-a/inv-1" {
		t.Errorf("views[0].CompositeID() = %q, want bucket-a/inv-1", views[0].CompositeID())
	}
	if views[0].State != inventory.StateNotLoaded {
		t.Errorf("views[0].State = %q, want %q", views[0].State, inventory.StateNotLoaded)
	}
	// inv-2 is not registered → also inventory.StateNotLoaded (Discovery's default).
	if views[1].State != inventory.StateNotLoaded {
		t.Errorf("views[1].State = %q, want %q", views[1].State, inventory.StateNotLoaded)
	}
}

func TestDiscoveryService_ListPropagatesDiscovererError(t *testing.T) {
	mgr := inventory.NewCatalog(nil)
	t.Cleanup(func() { _ = mgr.Close() })
	disc := &fakeDiscoverer{listErr: errFakeS3Throttled}
	s := inventory.NewDiscovery(mgr, inventory.WithBackend(disc, &fakeBuilder{}))
	_, err := s.List(t.Context())
	if !errors.Is(err, errFakeS3Throttled) {
		t.Errorf("List() error = %v, want %v wrapped", err, errFakeS3Throttled)
	}
}

func TestDiscoveryService_PrepareDiscovered_DisabledReturnsErr(t *testing.T) {
	s := inventory.NewDiscovery(inventory.NewCatalog(nil))
	disc := inventory.Inventory{SourceBucket: "b", Name: "i", Run: "r", ManifestKey: "k"}
	if err := s.PrepareDiscovered(t.Context(), disc); !errors.Is(err, inventory.ErrDiscoveryDisabled) {
		t.Errorf("PrepareDiscovered err = %v, want inventory.ErrDiscoveryDisabled", err)
	}
}

func TestDiscoveryService_PrepareDiscovered_NoRunRejects(t *testing.T) {
	mgr := inventory.NewCatalog(nil)
	t.Cleanup(func() { _ = mgr.Close() })
	s := inventory.NewDiscovery(mgr, inventory.WithBackend(&fakeDiscoverer{bucket: "dst"}, &fakeBuilder{}))
	disc := inventory.Inventory{SourceBucket: "b", Name: "i"}
	err := s.PrepareDiscovered(t.Context(), disc)
	if err == nil {
		t.Fatal("PrepareDiscovered with empty Run returned nil")
	}
	if errors.Is(err, inventory.ErrDiscoveryDisabled) {
		t.Errorf("err = %v, want a non-inventory.ErrDiscoveryDisabled error", err)
	}
}

func TestDiscoveryService_PrepareDiscovered_RegistersInManager(t *testing.T) {
	mgr := inventory.NewCatalog(nil)
	t.Cleanup(func() { _ = mgr.Close() })
	s := inventory.NewDiscovery(mgr, inventory.WithBackend(&fakeDiscoverer{bucket: "dst"}, &fakeBuilder{}))
	disc := inventory.Inventory{
		SourceBucket: "b", Name: "i", Run: "2026-05-13",
		ManifestKey: "k/manifest.json",
	}
	if err := s.PrepareDiscovered(t.Context(), disc); err != nil {
		t.Fatalf("PrepareDiscovered: %v", err)
	}
	got, ok := mgr.Get(disc.CompositeID())
	if !ok {
		t.Fatalf("inventory %s not registered in manager", disc.CompositeID())
	}
	if got.State != inventory.StateNotLoaded {
		t.Errorf("state = %s, want %s", got.State, inventory.StateNotLoaded)
	}
	if got.Path == "" || got.Path != "s3://dst/k/manifest.json" {
		t.Errorf("path = %q, want s3://dst/k/manifest.json", got.Path)
	}
}

func TestDiscoveryService_PrepareDiscovered_AlreadyExistsIsIdempotent(t *testing.T) {
	mgr := inventory.NewCatalog(nil)
	t.Cleanup(func() { _ = mgr.Close() })
	s := inventory.NewDiscovery(mgr, inventory.WithBackend(&fakeDiscoverer{bucket: "dst"}, &fakeBuilder{}))
	disc := inventory.Inventory{
		SourceBucket: "b", Name: "i", Run: "2026-05-13",
		ManifestKey: "k/manifest.json",
	}
	if err := s.PrepareDiscovered(t.Context(), disc); err != nil {
		t.Fatalf("first PrepareDiscovered: %v", err)
	}
	if err := s.PrepareDiscovered(t.Context(), disc); err != nil {
		t.Errorf("second PrepareDiscovered: %v, want nil", err)
	}
}

func TestDiscoveryService_Snapshot_DisabledReturnsErr(t *testing.T) {
	s := inventory.NewDiscovery(inventory.NewCatalog(nil))
	if _, _, err := s.Snapshot(t.Context()); !errors.Is(err, inventory.ErrDiscoveryDisabled) {
		t.Errorf("Snapshot err = %v, want ErrDiscoveryDisabled", err)
	}
}

func TestDiscoveryService_Snapshot_ColdStartLoadsLive(t *testing.T) {
	mgr := inventory.NewCatalog(nil)
	t.Cleanup(func() { _ = mgr.Close() })
	disc := &fakeDiscoverer{
		listResp: []inventory.Inventory{{SourceBucket: "b", Name: "i"}},
	}
	s := inventory.NewDiscovery(mgr, inventory.WithBackend(disc, &fakeBuilder{}))

	views, at, err := s.Snapshot(t.Context())
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if len(views) != 1 {
		t.Errorf("len(views) = %d, want 1", len(views))
	}
	if at.IsZero() {
		t.Error("Snapshot timestamp zero on cold start")
	}
	if got := disc.listCalls.Load(); got != 1 {
		t.Errorf("listCalls after first Snapshot = %d, want 1", got)
	}
}

// TestDiscoveryService_Snapshot_OverlaysLiveManagerState pins the
// regression: after a Load completes the next Snapshot must reflect the
// live Catalog.State even though the cached views captured an earlier
// "not loaded" state. Without the overlay the inventories page renders
// stale "not loaded" until the next discovery Refresh tick and a user
// click submits a no-op Load that fails with ErrInvalidState.
func TestDiscoveryService_Snapshot_OverlaysLiveManagerState(t *testing.T) {
	mgr := inventory.NewCatalog(nil)
	t.Cleanup(func() { _ = mgr.Close() })
	run := "2026-05-13T03-00Z"
	disc := &fakeDiscoverer{
		listResp: []inventory.Inventory{{
			SourceBucket: "b", Name: "i", Run: run, ManifestKey: "k/" + run + "/manifest.json",
		}},
	}
	s := inventory.NewDiscovery(mgr, inventory.WithBackend(disc, &fakeBuilder{}))

	// Prime the cache while the run is not loaded.
	if _, _, err := s.Snapshot(t.Context()); err != nil {
		t.Fatalf("seed Snapshot: %v", err)
	}

	// Flip the run to StateError in the live Catalog *after* the cache
	// was captured. StateError is convenient for the test because
	// Hydrate doesn't need an indexDir to reach it; any state ≠ the
	// cached StateNotLoaded proves the overlay.
	composite := inventory.ID("b/i/" + run)
	if err := mgr.Hydrate(t.Context(), inventory.Info{
		ID:    composite,
		Name:  "b/i @ " + run,
		Path:  "s3://b/k/" + run + "/manifest.json",
		State: inventory.StateError,
		Error: "synthetic test error",
	}, ""); err != nil {
		t.Fatalf("hydrate: %v", err)
	}

	views, _, err := s.Snapshot(t.Context())
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if len(views) != 1 {
		t.Fatalf("views = %d, want 1", len(views))
	}
	if views[0].State != inventory.StateError {
		t.Errorf("state = %q, want %q — cache must not mask live Catalog state", views[0].State, inventory.StateError)
	}
	if views[0].Error != "synthetic test error" {
		t.Errorf("error = %q, want %q", views[0].Error, "synthetic test error")
	}
	// Confirm we did not pay for a re-list to get this — the overlay
	// must come from Catalog.Get, not a fresh Discoverer call.
	if got := disc.listCalls.Load(); got != 1 {
		t.Errorf("listCalls = %d, want 1 (overlay must not re-fetch from S3)", got)
	}
}

func TestDiscoveryService_Snapshot_ServesFromCache(t *testing.T) {
	mgr := inventory.NewCatalog(nil)
	t.Cleanup(func() { _ = mgr.Close() })
	disc := &fakeDiscoverer{listResp: []inventory.Inventory{{SourceBucket: "b", Name: "i"}}}
	s := inventory.NewDiscovery(mgr, inventory.WithBackend(disc, &fakeBuilder{}))

	if _, _, err := s.Snapshot(t.Context()); err != nil {
		t.Fatalf("first Snapshot: %v", err)
	}
	if _, _, err := s.Snapshot(t.Context()); err != nil {
		t.Fatalf("second Snapshot: %v", err)
	}
	if got := disc.listCalls.Load(); got != 1 {
		t.Errorf("listCalls after two Snapshots = %d, want 1 (cache hit)", got)
	}
}

func TestDiscoveryService_Refresh_UpdatesSnapshot(t *testing.T) {
	mgr := inventory.NewCatalog(nil)
	t.Cleanup(func() { _ = mgr.Close() })
	disc := &fakeDiscoverer{listResp: []inventory.Inventory{{SourceBucket: "b", Name: "first"}}}
	s := inventory.NewDiscovery(mgr, inventory.WithBackend(disc, &fakeBuilder{}))

	if err := s.Refresh(t.Context()); err != nil {
		t.Fatalf("first Refresh: %v", err)
	}
	disc.listResp = []inventory.Inventory{
		{SourceBucket: "b", Name: "first"},
		{SourceBucket: "b", Name: "second"},
	}
	if err := s.Refresh(t.Context()); err != nil {
		t.Fatalf("second Refresh: %v", err)
	}
	views, _, err := s.Snapshot(t.Context())
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if len(views) != 2 {
		t.Errorf("len(views) = %d, want 2 (second Refresh should replace cache)", len(views))
	}
	if got := disc.listCalls.Load(); got != 2 {
		t.Errorf("listCalls = %d, want 2 (two explicit Refresh calls)", got)
	}
}

func TestDiscoveryService_Refresh_ErrorPreservesPriorSnapshot(t *testing.T) {
	mgr := inventory.NewCatalog(nil)
	t.Cleanup(func() { _ = mgr.Close() })
	disc := &fakeDiscoverer{listResp: []inventory.Inventory{{SourceBucket: "b", Name: "i"}}}
	s := inventory.NewDiscovery(mgr, inventory.WithBackend(disc, &fakeBuilder{}))

	if err := s.Refresh(t.Context()); err != nil {
		t.Fatalf("seed Refresh: %v", err)
	}
	// Subsequent Refresh fails — prior snapshot must survive.
	disc.listErr = errFakeS3Throttled
	if err := s.Refresh(t.Context()); err == nil {
		t.Fatal("Refresh with discoverer error returned nil")
	}
	if got := s.LastRefreshErr(); !errors.Is(got, errFakeS3Throttled) {
		t.Errorf("LastRefreshErr = %v, want errFakeS3Throttled", got)
	}
	views, _, err := s.Snapshot(t.Context())
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if len(views) != 1 || views[0].Name != "i" {
		t.Errorf("snapshot lost after failed Refresh: %+v", views)
	}
}

func TestDiscoveryService_Refresh_RecordsClockTimestamp(t *testing.T) {
	mgr := inventory.NewCatalog(nil)
	t.Cleanup(func() { _ = mgr.Close() })
	disc := &fakeDiscoverer{listResp: []inventory.Inventory{{SourceBucket: "b", Name: "i"}}}
	s := inventory.NewDiscovery(mgr, inventory.WithBackend(disc, &fakeBuilder{}))
	want := time.Date(2026, 5, 15, 12, 0, 0, 0, time.UTC)
	s.SetClockForTest(func() time.Time { return want })

	if err := s.Refresh(t.Context()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	_, got, err := s.Snapshot(t.Context())
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if !got.Equal(want) {
		t.Errorf("Snapshot timestamp = %v, want %v", got, want)
	}
}

func TestDiscoveryService_StartStop_NoopWhenDisabled(t *testing.T) {
	s := inventory.NewDiscovery(inventory.NewCatalog(nil))
	// Should not block, panic, or leak a goroutine.
	s.Start(t.Context(), time.Millisecond, nil)
	s.Stop()
}

func TestDiscoveryService_Start_PerformsInitialRefresh(t *testing.T) {
	mgr := inventory.NewCatalog(nil)
	t.Cleanup(func() { _ = mgr.Close() })
	disc := &fakeDiscoverer{listResp: []inventory.Inventory{{SourceBucket: "b", Name: "i"}}}
	s := inventory.NewDiscovery(mgr, inventory.WithBackend(disc, &fakeBuilder{}))

	// Long interval — the test only asserts that Start() warms the
	// cache at once, so background ticks shouldn't fire.
	s.Start(t.Context(), time.Hour, nil)
	t.Cleanup(s.Stop)

	waitForListCalls(t, disc, 1)
	views, _, err := s.Snapshot(t.Context())
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if len(views) != 1 {
		t.Errorf("snapshot len = %d, want 1", len(views))
	}
	if got := disc.listCalls.Load(); got != 1 {
		t.Errorf("listCalls = %d, want 1 (Snapshot reuses the initial refresh)", got)
	}
}

// waitForListCalls waits until the discoverer has been listed n times.
func waitForListCalls(t *testing.T, disc *fakeDiscoverer, n int64) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for disc.listCalls.Load() < n {
		if time.Now().After(deadline) {
			t.Fatalf("listCalls = %d, want %d", disc.listCalls.Load(), n)
		}
		time.Sleep(time.Millisecond)
	}
}

// TestDiscoveryService_Start_DoesNotWaitForS3: Start returns while the
// first walk is still running, so the server can listen meanwhile; a
// Snapshot taken during that walk waits for it rather than walking again.
func TestDiscoveryService_Start_DoesNotWaitForS3(t *testing.T) {
	mgr := inventory.NewCatalog(nil)
	t.Cleanup(func() { _ = mgr.Close() })
	gate := make(chan struct{})
	disc := &fakeDiscoverer{listGate: gate, listResp: []inventory.Inventory{{SourceBucket: "b", Name: "i"}}}
	s := inventory.NewDiscovery(mgr, inventory.WithBackend(disc, &fakeBuilder{}))

	started := make(chan struct{})
	go func() {
		s.Start(t.Context(), time.Hour, nil)
		close(started)
	}()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("Start blocked on the first discovery walk")
	}
	t.Cleanup(s.Stop)
	waitForListCalls(t, disc, 1)

	got := make(chan int, 1)
	go func() {
		views, _, err := s.Snapshot(t.Context())
		if err != nil {
			t.Error(err)
		}
		got <- len(views)
	}()
	time.Sleep(20 * time.Millisecond) // the Snapshot joins the walk in flight
	close(gate)
	if n := <-got; n != 1 {
		t.Fatalf("snapshot len = %d, want 1", n)
	}
	if calls := disc.listCalls.Load(); calls != 1 {
		t.Fatalf("listCalls = %d, want 1", calls)
	}
}

func TestDiscoveryService_Start_DoubleStartIsNoop(t *testing.T) {
	mgr := inventory.NewCatalog(nil)
	t.Cleanup(func() { _ = mgr.Close() })
	disc := &fakeDiscoverer{listResp: []inventory.Inventory{{SourceBucket: "b", Name: "i"}}}
	s := inventory.NewDiscovery(mgr, inventory.WithBackend(disc, &fakeBuilder{}))

	s.Start(t.Context(), time.Hour, nil)
	t.Cleanup(s.Stop)
	// Second Start with an active background loop must not launch a
	// second goroutine — listCalls should still be 1 after.
	s.Start(t.Context(), time.Hour, nil)
	waitForListCalls(t, disc, 1)
	time.Sleep(20 * time.Millisecond) // a second loop would list by now
	if got := disc.listCalls.Load(); got != 1 {
		t.Errorf("listCalls after double Start = %d, want 1", got)
	}
}

func TestDiscoveryService_Stop_WithoutStartIsNoop(_ *testing.T) {
	s := inventory.NewDiscovery(inventory.NewCatalog(nil), inventory.WithBackend(&fakeDiscoverer{}, &fakeBuilder{}))
	s.Stop() // must not block or panic
}

func TestDiscoveryService_Background_TickerFiresRefresh(t *testing.T) {
	mgr := inventory.NewCatalog(nil)
	t.Cleanup(func() { _ = mgr.Close() })
	disc := &fakeDiscoverer{listResp: []inventory.Inventory{{SourceBucket: "b", Name: "i"}}}
	s := inventory.NewDiscovery(mgr, inventory.WithBackend(disc, &fakeBuilder{}))

	// Very short interval — the ticker should fire at least once
	// after the initial inline refresh before we stop.
	s.Start(t.Context(), 5*time.Millisecond, nil)
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if disc.listCalls.Load() >= 2 {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	s.Stop()

	if got := disc.listCalls.Load(); got < 2 {
		t.Errorf("listCalls = %d, want >= 2 (initial + at least one tick)", got)
	}
}

// TestDiscoveryService_Snapshot_ColdStartDeduplicatesConcurrent pins
// that concurrent first-callers on an empty cache trigger exactly one
// Refresh — the singleflight dedupe. Without it both callers each fire
// an S3 List.
func TestDiscoveryService_Snapshot_ColdStartDeduplicatesConcurrent(t *testing.T) {
	mgr := inventory.NewCatalog(nil)
	t.Cleanup(func() { _ = mgr.Close() })
	gate := make(chan struct{})
	disc := &fakeDiscoverer{
		listResp: []inventory.Inventory{{SourceBucket: "b", Name: "i"}},
		listGate: gate,
	}
	s := inventory.NewDiscovery(mgr, inventory.WithBackend(disc, &fakeBuilder{}))

	const callers = 4
	var wg sync.WaitGroup
	wg.Add(callers)
	errs := make([]error, callers)
	for i := range callers {
		go func(idx int) {
			defer wg.Done()
			_, _, err := s.Snapshot(t.Context())
			errs[idx] = err
		}(i)
	}

	// Give every goroutine time to reach the !populated branch and
	// queue behind the singleflight key before releasing List.
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if disc.listCalls.Load() >= 1 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	// Hold the gate a touch longer so the other callers definitely
	// reach the singleflight wait before List returns.
	time.Sleep(20 * time.Millisecond)
	close(gate)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("caller %d: Snapshot err = %v", i, err)
		}
	}
	if got := disc.listCalls.Load(); got != 1 {
		t.Errorf("listCalls = %d, want 1 (concurrent cold-start callers must dedupe)", got)
	}
}

// Find serves a run from the snapshot only when the List read its
// manifest. Runs past the List's manifest-fetch cap, and runs whose
// manifest read failed, have no stats in the snapshot and must be
// looked up, or their rows never show a file count or size.
func TestDiscovery_FindServesOnlyDescribedRunsFromSnapshot(t *testing.T) {
	described := inventory.Inventory{SourceBucket: "b", Name: "i", Run: "2026-01-03T00-00Z", FileFormat: "CSV", FileCount: 3}
	undescribed := inventory.Inventory{SourceBucket: "b", Name: "i", Run: "2026-01-02T00-00Z"}
	failed := inventory.Inventory{SourceBucket: "b", Name: "i", Run: "2026-01-01T00-00Z", FileFormat: "unknown", Error: "failed to read manifest"}
	fake := &fakeDiscoverer{listResp: []inventory.Inventory{described, undescribed, failed}}
	mgr := inventory.NewCatalog(nil)
	t.Cleanup(func() { _ = mgr.Close() })
	disc := inventory.NewDiscovery(mgr, inventory.WithBackend(fake, &fakeBuilder{}))
	if err := disc.Refresh(t.Context()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}

	got, err := disc.Find(t.Context(), "b", "i", described.Run)
	if err != nil || got.FileCount != 3 || fake.findCalls.Load() != 0 {
		t.Fatalf("described run: got %+v err %v after %d lookups, want snapshot entry and no lookup", got, err, fake.findCalls.Load())
	}
	for _, run := range []inventory.Inventory{undescribed, failed} {
		fresh := run
		fresh.FileFormat, fresh.Error, fresh.FileCount = "CSV", "", 7
		fake.findResp = fresh
		before := fake.findCalls.Load()
		got, err := disc.Find(t.Context(), "b", "i", run.Run)
		if err != nil || got.FileCount != 7 || fake.findCalls.Load() != before+1 {
			t.Errorf("run %s: got %+v err %v, want one lookup returning its stats", run.Run, got, err)
		}
	}
}

// TestDiscovery_LoadSizesFromDiscoveredManifest: a run whose manifest
// discovery already read is sized from it, by its format; only a run
// without one asks the sizer, which fetches the manifest.
func TestDiscovery_LoadSizesFromDiscoveredManifest(t *testing.T) {
	mgr := inventory.NewCatalog(nil)
	t.Cleanup(func() { _ = mgr.Close() })
	var sizerCalls atomic.Int64
	var estimates []uint64
	gate := func(_ context.Context, _ inventory.ID, _ inventory.BuildFunc, opts inventory.GatedLoadOptions) error {
		estimates = append(estimates, opts.EstimateBytes)

		return nil
	}
	sizer := func(context.Context, string, string) (inventory.ManifestSize, error) {
		sizerCalls.Add(1)

		return inventory.ManifestSize{FileFormat: "Parquet", DataBytes: 1000}, nil
	}
	d := inventory.NewDiscovery(mgr,
		inventory.WithBackend(&fakeDiscoverer{bucket: "dst"}, &fakeBuilder{}),
		inventory.WithGate(gate, sizer))
	described := inventory.Inventory{SourceBucket: "b", Name: "i", Run: "2026-01-01T00-00Z", ManifestKey: "k1", FileFormat: "CSV", TotalBytes: 300}
	published := inventory.Inventory{SourceBucket: "b", Name: "i", Run: "2026-01-03T00-00Z", ManifestKey: "k3", FileFormat: inventory.IndexFileFormat, TotalBytes: 500}
	bare := inventory.Inventory{SourceBucket: "b", Name: "i", Run: "2026-01-02T00-00Z", ManifestKey: "k2"}
	for _, disc := range []inventory.Inventory{described, published, bare} {
		if err := d.Load(t.Context(), disc); err != nil {
			t.Fatal(err)
		}
	}
	if n := sizerCalls.Load(); n != 1 {
		t.Fatalf("sizer called %d times, want 1 (only the run without a read manifest)", n)
	}
	want := []uint64{inventory.EstimateIndexBytes("CSV", 300, inventory.IndexRatio{}), 500, inventory.EstimateIndexBytes("Parquet", 1000, inventory.IndexRatio{})}
	if !slices.Equal(estimates, want) {
		t.Fatalf("estimates %v, want %v", estimates, want)
	}
}

// EstimateIndexBytes is a published index's own size, and for an
// inventory its data total times the format's ratio: Parquet's lower,
// since its files are larger for the same objects, and an unknown
// format gets CSV's.
func TestEstimateIndexBytes(t *testing.T) {
	const data = 1000
	csv := inventory.EstimateIndexBytes("CSV", data, inventory.IndexRatio{})
	parquet := inventory.EstimateIndexBytes("Parquet", data, inventory.IndexRatio{})
	if got := inventory.EstimateIndexBytes(inventory.IndexFileFormat, data, inventory.IndexRatio{}); got != data {
		t.Errorf("published index = %d, want its size %d", got, data)
	}
	if csv < 2900 {
		t.Errorf("CSV = %d, want at least 2.9x: deep_pyramid indexes measure 2.99x their gzipped CSV", csv)
	}
	if parquet >= csv || 4*parquet < 3*csv {
		t.Errorf("Parquet = %d, want under CSV's %d by at most the 1.33x its files are larger", parquet, csv)
	}
	for _, format := range []string{"csv", "", "ORC"} {
		if got := inventory.EstimateIndexBytes(format, data, inventory.IndexRatio{}); got != csv {
			t.Errorf("format %q = %d, want CSV's %d", format, got, csv)
		}
	}
	if got := inventory.EstimateIndexBytes("parquet", data, inventory.IndexRatio{}); got != parquet {
		t.Errorf("lowercase parquet = %d, want %d", got, parquet)
	}
}
