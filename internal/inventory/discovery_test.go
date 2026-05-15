package inventory_test

import (
	"context"
	"errors"
	"testing"

	"github.com/eunmann/s3-inv-db/internal/inventory"
)

// errFakeS3Throttled is the sentinel returned by tests that simulate
// a throttled S3 listing. err113 forbids errors.New at call sites.
var errFakeS3Throttled = errors.New("s3: throttled")

// fakeDiscoverer is a minimal stub for the inventory.Discoverer interface that
// the DiscoveryService consumes. Each method has a single response to
// keep tests simple; specialise per-test by setting fields directly.
type fakeDiscoverer struct {
	listResp []inventory.Inventory
	listErr  error
	findResp inventory.Inventory
	findErr  error
	bucket   string
}

func (f *fakeDiscoverer) List(context.Context) ([]inventory.Inventory, error) {
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

func (f *fakeBuilder) Build(_ context.Context, _, _, _, _ string) (string, error) {
	return f.buildResp, f.buildErr
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
	if err := mgr.Register("bucket-a/inv-1", "Pre-registered", "/path"); err != nil {
		t.Fatalf("Register: %v", err)
	}

	disc := &fakeDiscoverer{
		listResp: []inventory.Inventory{
			{SourceBucket: "bucket-a", InventoryName: "inv-1"},
			{SourceBucket: "bucket-a", InventoryName: "inv-2"},
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
	disc := inventory.Inventory{SourceBucket: "b", InventoryName: "i", Run: "r", ManifestKey: "k"}
	if err := s.PrepareDiscovered(disc); !errors.Is(err, inventory.ErrDiscoveryDisabled) {
		t.Errorf("PrepareDiscovered err = %v, want inventory.ErrDiscoveryDisabled", err)
	}
}

func TestDiscoveryService_PrepareDiscovered_NoRunRejects(t *testing.T) {
	mgr := inventory.NewManager()
	t.Cleanup(func() { _ = mgr.Close() })
	s := inventory.NewDiscoveryService(mgr, &fakeDiscoverer{bucket: "dst"}, &fakeBuilder{})
	disc := inventory.Inventory{SourceBucket: "b", InventoryName: "i"}
	err := s.PrepareDiscovered(disc)
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
		SourceBucket: "b", InventoryName: "i", Run: "2026-05-13",
		ManifestKey: "k/manifest.json",
	}
	if err := s.PrepareDiscovered(disc); err != nil {
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
		SourceBucket: "b", InventoryName: "i", Run: "2026-05-13",
		ManifestKey: "k/manifest.json",
	}
	if err := s.PrepareDiscovered(disc); err != nil {
		t.Fatalf("first PrepareDiscovered: %v", err)
	}
	if err := s.PrepareDiscovered(disc); err != nil {
		t.Errorf("second PrepareDiscovered: %v, want nil", err)
	}
}
