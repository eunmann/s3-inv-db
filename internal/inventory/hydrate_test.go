package inventory_test

import (
	"testing"

	"github.com/eunmann/s3-inv-db/internal/inventory"
)

func TestHydrate_NonLoadedStateKeepsAsIs(t *testing.T) {
	mgr := inventory.NewManager()
	info := inventory.Info{ID: "id1", Name: "n", Path: "p", State: inventory.StateNotLoaded}
	if err := mgr.Hydrate(info, ""); err != nil {
		t.Fatalf("Hydrate: %v", err)
	}
	got, ok := mgr.Get("id1")
	if !ok {
		t.Fatal("inventory not registered")
	}
	if got.State != inventory.StateNotLoaded {
		t.Errorf("state = %s, want pending", got.State)
	}
}

func TestHydrate_LoadedStateOpenFailureBecomesError(t *testing.T) {
	mgr := inventory.NewManager()
	info := inventory.Info{ID: "id1", Name: "n", Path: "p", State: inventory.StateLoaded}
	// indexDir doesn't exist on disk → Open will fail.
	if err := mgr.Hydrate(info, "/nonexistent/index-dir"); err != nil {
		t.Fatalf("Hydrate must register even on open failure: %v", err)
	}
	got, ok := mgr.Get("id1")
	if !ok {
		t.Fatal("inventory not registered after open failure")
	}
	if got.State != inventory.StateError {
		t.Errorf("state after open failure = %s, want error", got.State)
	}
	if got.Error == "" {
		t.Error("error message not preserved")
	}
}

func TestHydrate_DuplicateID(t *testing.T) {
	mgr := inventory.NewManager()
	info := inventory.Info{ID: "id1", Name: "n", Path: "p", State: inventory.StateNotLoaded}
	if err := mgr.Hydrate(info, ""); err != nil {
		t.Fatal(err)
	}
	if err := mgr.Hydrate(info, ""); err == nil {
		t.Error("second Hydrate must error on duplicate id")
	}
}
