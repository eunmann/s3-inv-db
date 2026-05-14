package inventory

import (
	"context"
	"errors"
	"testing"

	"github.com/eunmann/s3-inv-db/internal/s3disco"
)

// fakeDiscoverer is a minimal stub for the Discoverer interface that
// the DiscoveryService consumes. Each method has a single response to
// keep tests simple; specialise per-test by setting fields directly.
type fakeDiscoverer struct {
	listResp []s3disco.Inventory
	listErr  error
	findResp s3disco.Inventory
	findErr  error
	bucket   string
}

func (f *fakeDiscoverer) List(context.Context) ([]s3disco.Inventory, error) {
	return f.listResp, f.listErr
}
func (f *fakeDiscoverer) Find(_ context.Context, _, _ string) (s3disco.Inventory, error) {
	return f.findResp, f.findErr
}
func (f *fakeDiscoverer) Bucket() string { return f.bucket }

// fakeBuilder satisfies the IndexBuilder interface.
type fakeBuilder struct {
	buildResp string
	buildErr  error
}

func (f *fakeBuilder) Build(_ context.Context, _, _, _ string) (string, error) {
	return f.buildResp, f.buildErr
}

func (f *fakeBuilder) BuildWith(_ context.Context, _, _, _ string, _ func(string, int64, int64)) (string, error) {
	return f.buildResp, f.buildErr
}
func TestDiscoveryService_DisabledWithoutDeps(t *testing.T) {
	mgr := NewManager()
	t.Cleanup(func() { _ = mgr.Close() })

	cases := []struct {
		name    string
		disc    Discoverer
		builder IndexBuilder
		enabled bool
	}{
		{"both nil", nil, nil, false},
		{"discoverer only", &fakeDiscoverer{}, nil, false},
		{"builder only", nil, &fakeBuilder{}, false},
		{"both set", &fakeDiscoverer{}, &fakeBuilder{}, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := NewDiscoveryService(mgr, c.disc, c.builder)
			if got := s.Enabled(); got != c.enabled {
				t.Errorf("Enabled() = %v, want %v", got, c.enabled)
			}
		})
	}
}

func TestDiscoveryService_ListWhenDisabledReturnsErr(t *testing.T) {
	s := NewDiscoveryService(NewManager(), nil, nil)
	_, err := s.List(context.Background())
	if !errors.Is(err, ErrDiscoveryDisabled) {
		t.Errorf("List() error = %v, want ErrDiscoveryDisabled", err)
	}
}

func TestDiscoveryService_FindWhenDisabledReturnsErr(t *testing.T) {
	s := NewDiscoveryService(NewManager(), nil, nil)
	_, err := s.Find(context.Background(), "src", "id")
	if !errors.Is(err, ErrDiscoveryDisabled) {
		t.Errorf("Find() error = %v, want ErrDiscoveryDisabled", err)
	}
}

func TestDiscoveryService_LoadWhenDisabledReturnsErr(t *testing.T) {
	s := NewDiscoveryService(NewManager(), &fakeDiscoverer{}, nil)
	err := s.Load(context.Background(), s3disco.Inventory{})
	if !errors.Is(err, ErrDiscoveryDisabled) {
		t.Errorf("Load() error = %v, want ErrDiscoveryDisabled", err)
	}
}

func TestDiscoveryService_ListMergesWithManagerState(t *testing.T) {
	mgr := NewManager()
	t.Cleanup(func() { _ = mgr.Close() })

	// Pre-load the manager with state for one of the inventories we'll
	// surface via the discoverer. The merge should preserve that state
	// instead of returning the default StateNotLoaded for known IDs.
	if err := mgr.Register("bucket-a/inv-1", "Pre-registered", "/path"); err != nil {
		t.Fatalf("Register: %v", err)
	}

	disc := &fakeDiscoverer{
		listResp: []s3disco.Inventory{
			{SourceBucket: "bucket-a", InventoryID: "inv-1"},
			{SourceBucket: "bucket-a", InventoryID: "inv-2"},
		},
	}
	s := NewDiscoveryService(mgr, disc, &fakeBuilder{})
	views, err := s.List(context.Background())
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(views) != 2 {
		t.Fatalf("len(views) = %d, want 2", len(views))
	}

	// inv-1 is registered → State should be StateNotLoaded (the registered default).
	if views[0].CompositeID() != "bucket-a/inv-1" {
		t.Errorf("views[0].CompositeID() = %q, want bucket-a/inv-1", views[0].CompositeID())
	}
	if views[0].State != StateNotLoaded {
		t.Errorf("views[0].State = %q, want %q", views[0].State, StateNotLoaded)
	}
	// inv-2 is not registered → also StateNotLoaded (DiscoveryService's default).
	if views[1].State != StateNotLoaded {
		t.Errorf("views[1].State = %q, want %q", views[1].State, StateNotLoaded)
	}
}

func TestDiscoveryService_ListPropagatesDiscovererError(t *testing.T) {
	mgr := NewManager()
	t.Cleanup(func() { _ = mgr.Close() })
	want := errors.New("s3: throttled")
	disc := &fakeDiscoverer{listErr: want}
	s := NewDiscoveryService(mgr, disc, &fakeBuilder{})
	_, err := s.List(context.Background())
	if !errors.Is(err, want) {
		t.Errorf("List() error = %v, want %v wrapped", err, want)
	}
}
