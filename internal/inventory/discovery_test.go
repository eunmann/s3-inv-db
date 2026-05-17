package inventory_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eunmann/s3-inv-db/internal/inventory"
)

// errFakeS3Throttled is the sentinel returned by tests that simulate
// a throttled S3 listing. The err113 linter forbids errors.New at call
// sites so the sentinel is package-private.
var errFakeS3Throttled = errors.New("s3: throttled")

// fakeDiscoverer is a minimal stub for the inventory.Discoverer interface that
// the DiscoveryService consumes. Each method has a single response to
// keep tests simple; specialise per-test by setting fields directly.
type fakeDiscoverer struct {
	listResp  []inventory.Inventory
	listErr   error
	findResp  inventory.Inventory
	findErr   error
	bucket    string
	listCalls atomic.Int64
}

func (f *fakeDiscoverer) List(context.Context) ([]inventory.Inventory, error) {
	f.listCalls.Add(1)

	return f.listResp, f.listErr
}

func (f *fakeDiscoverer) Find(_ context.Context, _, _, _ string) (inventory.Inventory, error) {
	return f.findResp, f.findErr
}
func (f *fakeDiscoverer) Bucket() string { return f.bucket }

// fakeBuilder satisfies the inventory.IndexBuilder interface.
type fakeBuilder struct {
	buildResp string
	buildErr  error
}

func (f *fakeBuilder) BuildWith(_ context.Context, _, _, _, _ string, _ func(string, int64, int64)) (string, error) {
	return f.buildResp, f.buildErr
}

func (f *fakeBuilder) RemoveCache(_, _, _ string) error             { return nil }
func (f *fakeBuilder) CacheSizeBytes(_, _, _ string) (int64, error) { return 0, nil }

func TestDiscoveryService_DisabledWithoutDeps(t *testing.T) {
	mgr := inventory.NewManager()
	t.Cleanup(func() { _ = mgr.Close() })

	cases := []struct {
		name    string
		disc    inventory.Discoverer
		builder inventory.IndexBuilder
		enabled bool
	}{
		{"both nil", nil, nil, false},
		{"discoverer only", &fakeDiscoverer{}, nil, false},
		{"builder only", nil, &fakeBuilder{}, false},
		{"both set", &fakeDiscoverer{}, &fakeBuilder{}, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := inventory.NewDiscoveryService(mgr, c.disc, c.builder)
			if got := s.Enabled(); got != c.enabled {
				t.Errorf("Enabled() = %v, want %v", got, c.enabled)
			}
		})
	}
}

func TestDiscoveryService_ListWhenDisabledReturnsErr(t *testing.T) {
	s := inventory.NewDiscoveryService(inventory.NewManager(), nil, nil)
	_, err := s.List(context.Background())
	if !errors.Is(err, inventory.ErrDiscoveryDisabled) {
		t.Errorf("List() error = %v, want inventory.ErrDiscoveryDisabled", err)
	}
}

func TestDiscoveryService_FindWhenDisabledReturnsErr(t *testing.T) {
	s := inventory.NewDiscoveryService(inventory.NewManager(), nil, nil)
	_, err := s.Find(context.Background(), "src", "id", "")
	if !errors.Is(err, inventory.ErrDiscoveryDisabled) {
		t.Errorf("Find() error = %v, want inventory.ErrDiscoveryDisabled", err)
	}
}

func TestDiscoveryService_LoadWhenDisabledReturnsErr(t *testing.T) {
	s := inventory.NewDiscoveryService(inventory.NewManager(), &fakeDiscoverer{}, nil)
	err := s.Load(context.Background(), inventory.Inventory{})
	if !errors.Is(err, inventory.ErrDiscoveryDisabled) {
		t.Errorf("Load() error = %v, want inventory.ErrDiscoveryDisabled", err)
	}
}

func TestDiscoveryService_ListMergesWithManagerState(t *testing.T) {
	mgr := inventory.NewManager()
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
	s := inventory.NewDiscoveryService(mgr, disc, &fakeBuilder{})
	views, err := s.List(context.Background())
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
	// inv-2 is not registered → also inventory.StateNotLoaded (DiscoveryService's default).
	if views[1].State != inventory.StateNotLoaded {
		t.Errorf("views[1].State = %q, want %q", views[1].State, inventory.StateNotLoaded)
	}
}

func TestDiscoveryService_ListPropagatesDiscovererError(t *testing.T) {
	mgr := inventory.NewManager()
	t.Cleanup(func() { _ = mgr.Close() })
	disc := &fakeDiscoverer{listErr: errFakeS3Throttled}
	s := inventory.NewDiscoveryService(mgr, disc, &fakeBuilder{})
	_, err := s.List(context.Background())
	if !errors.Is(err, errFakeS3Throttled) {
		t.Errorf("List() error = %v, want %v wrapped", err, errFakeS3Throttled)
	}
}

func TestDiscoveryService_PrepareDiscovered_DisabledReturnsErr(t *testing.T) {
	s := inventory.NewDiscoveryService(inventory.NewManager(), nil, nil)
	disc := inventory.Inventory{SourceBucket: "b", Name: "i", Run: "r", ManifestKey: "k"}
	if err := s.PrepareDiscovered(t.Context(), disc); !errors.Is(err, inventory.ErrDiscoveryDisabled) {
		t.Errorf("PrepareDiscovered err = %v, want inventory.ErrDiscoveryDisabled", err)
	}
}

func TestDiscoveryService_PrepareDiscovered_NoRunRejects(t *testing.T) {
	mgr := inventory.NewManager()
	t.Cleanup(func() { _ = mgr.Close() })
	s := inventory.NewDiscoveryService(mgr, &fakeDiscoverer{bucket: "dst"}, &fakeBuilder{})
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
	mgr := inventory.NewManager()
	t.Cleanup(func() { _ = mgr.Close() })
	s := inventory.NewDiscoveryService(mgr, &fakeDiscoverer{bucket: "dst"}, &fakeBuilder{})
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
	mgr := inventory.NewManager()
	t.Cleanup(func() { _ = mgr.Close() })
	s := inventory.NewDiscoveryService(mgr, &fakeDiscoverer{bucket: "dst"}, &fakeBuilder{})
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
	s := inventory.NewDiscoveryService(inventory.NewManager(), nil, nil)
	if _, _, err := s.Snapshot(t.Context()); !errors.Is(err, inventory.ErrDiscoveryDisabled) {
		t.Errorf("Snapshot err = %v, want ErrDiscoveryDisabled", err)
	}
}

func TestDiscoveryService_Snapshot_ColdStartLoadsLive(t *testing.T) {
	mgr := inventory.NewManager()
	t.Cleanup(func() { _ = mgr.Close() })
	disc := &fakeDiscoverer{
		listResp: []inventory.Inventory{{SourceBucket: "b", Name: "i"}},
	}
	s := inventory.NewDiscoveryService(mgr, disc, &fakeBuilder{})

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

func TestDiscoveryService_Snapshot_ServesFromCache(t *testing.T) {
	mgr := inventory.NewManager()
	t.Cleanup(func() { _ = mgr.Close() })
	disc := &fakeDiscoverer{listResp: []inventory.Inventory{{SourceBucket: "b", Name: "i"}}}
	s := inventory.NewDiscoveryService(mgr, disc, &fakeBuilder{})

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
	mgr := inventory.NewManager()
	t.Cleanup(func() { _ = mgr.Close() })
	disc := &fakeDiscoverer{listResp: []inventory.Inventory{{SourceBucket: "b", Name: "first"}}}
	s := inventory.NewDiscoveryService(mgr, disc, &fakeBuilder{})

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
	mgr := inventory.NewManager()
	t.Cleanup(func() { _ = mgr.Close() })
	disc := &fakeDiscoverer{listResp: []inventory.Inventory{{SourceBucket: "b", Name: "i"}}}
	s := inventory.NewDiscoveryService(mgr, disc, &fakeBuilder{})

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
	mgr := inventory.NewManager()
	t.Cleanup(func() { _ = mgr.Close() })
	disc := &fakeDiscoverer{listResp: []inventory.Inventory{{SourceBucket: "b", Name: "i"}}}
	s := inventory.NewDiscoveryService(mgr, disc, &fakeBuilder{})
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
	s := inventory.NewDiscoveryService(inventory.NewManager(), nil, nil)
	// Should not block, panic, or leak a goroutine.
	s.Start(t.Context(), time.Millisecond, nil)
	s.Stop()
}

func TestDiscoveryService_Start_PerformsInitialRefresh(t *testing.T) {
	mgr := inventory.NewManager()
	t.Cleanup(func() { _ = mgr.Close() })
	disc := &fakeDiscoverer{listResp: []inventory.Inventory{{SourceBucket: "b", Name: "i"}}}
	s := inventory.NewDiscoveryService(mgr, disc, &fakeBuilder{})

	// Long interval — the test only asserts that Start() warms the
	// cache before returning, so background ticks shouldn't fire.
	s.Start(t.Context(), time.Hour, nil)
	t.Cleanup(s.Stop)

	if got := disc.listCalls.Load(); got != 1 {
		t.Errorf("listCalls after Start = %d, want 1 (initial refresh)", got)
	}
	views, _, err := s.Snapshot(t.Context())
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if len(views) != 1 {
		t.Errorf("snapshot len = %d, want 1", len(views))
	}
}

func TestDiscoveryService_Start_DoubleStartIsNoop(t *testing.T) {
	mgr := inventory.NewManager()
	t.Cleanup(func() { _ = mgr.Close() })
	disc := &fakeDiscoverer{listResp: []inventory.Inventory{{SourceBucket: "b", Name: "i"}}}
	s := inventory.NewDiscoveryService(mgr, disc, &fakeBuilder{})

	s.Start(t.Context(), time.Hour, nil)
	t.Cleanup(s.Stop)
	// Second Start with an active background loop must not launch a
	// second goroutine — listCalls should still be 1 after.
	s.Start(t.Context(), time.Hour, nil)
	if got := disc.listCalls.Load(); got != 1 {
		t.Errorf("listCalls after double Start = %d, want 1", got)
	}
}

func TestDiscoveryService_Stop_WithoutStartIsNoop(_ *testing.T) {
	s := inventory.NewDiscoveryService(inventory.NewManager(), &fakeDiscoverer{}, &fakeBuilder{})
	s.Stop() // must not block or panic
}

func TestDiscoveryService_Background_TickerFiresRefresh(t *testing.T) {
	mgr := inventory.NewManager()
	t.Cleanup(func() { _ = mgr.Close() })
	disc := &fakeDiscoverer{listResp: []inventory.Inventory{{SourceBucket: "b", Name: "i"}}}
	s := inventory.NewDiscoveryService(mgr, disc, &fakeBuilder{})

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
