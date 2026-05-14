package inventory_test

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/internal/migrate"
	_ "modernc.org/sqlite"
)

func openManagerWithStore(t *testing.T) (*inventory.Manager, *inventory.Store) {
	t.Helper()
	db, err := sql.Open("sqlite", "file::memory:?cache=shared&_pragma=foreign_keys(1)")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := migrate.Apply(db); err != nil {
		t.Fatalf("migrate.Apply: %v", err)
	}
	store, err := inventory.NewStore(db)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	mgr := inventory.NewManager()
	mgr.SetStore(store)
	t.Cleanup(func() { _ = mgr.Close() })
	return mgr, store
}

// TestManager_RegisterMirrorsToStore pins the contract that Register
// writes through to the attached Store.
func TestManager_RegisterMirrorsToStore(t *testing.T) {
	mgr, store := openManagerWithStore(t)
	if err := mgr.Register("src/inv1", "src/inv1", "s3://src/manifest.json"); err != nil {
		t.Fatalf("Register: %v", err)
	}
	got, err := store.Get("src/inv1")
	if err != nil {
		t.Fatalf("store.Get: %v", err)
	}
	if got.State != inventory.StateNotLoaded {
		t.Errorf("persisted state = %s, want pending", got.State)
	}
}

// TestManager_RemoveDeletesFromStore pins that Remove cascades to the
// Store too.
func TestManager_RemoveDeletesFromStore(t *testing.T) {
	mgr, store := openManagerWithStore(t)
	if err := mgr.Register("src/inv1", "src/inv1", "u"); err != nil {
		t.Fatal(err)
	}
	if err := mgr.Remove("src/inv1"); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	_, err := store.Get("src/inv1")
	if !errors.Is(err, inventory.ErrStoreNotFound) {
		t.Errorf("store.Get after Remove err = %v, want ErrStoreNotFound", err)
	}
}

// TestManager_LoadWith_MirrorsErrorState verifies that a build failure
// is persisted to the store (so the UI shows error after restart).
func TestManager_LoadWith_MirrorsErrorState(t *testing.T) {
	mgr, store := openManagerWithStore(t)
	if err := mgr.Register("src/inv1", "src/inv1", "u"); err != nil {
		t.Fatal(err)
	}
	wantErr := errors.New("build broke")
	err := mgr.LoadWith(context.Background(), "src/inv1", func(context.Context, inventory.Info) (string, error) {
		return "", wantErr
	})
	if err == nil {
		t.Fatal("LoadWith returned nil, want build error")
	}

	persisted, err := store.Get("src/inv1")
	if err != nil {
		t.Fatalf("store.Get: %v", err)
	}
	if persisted.State != inventory.StateError {
		t.Errorf("persisted state = %s, want error", persisted.State)
	}
	if persisted.Error == "" {
		t.Error("error message not persisted")
	}
}

// TestManager_Hydrate_LoadedWithEmptyDir falls back to error rather
// than emitting a confusing "open : no such file" string.
func TestManager_Hydrate_LoadedWithEmptyDir(t *testing.T) {
	mgr, _ := openManagerWithStore(t)
	info := inventory.Info{ID: "src/inv1", Name: "n", Path: "p", State: inventory.StateLoaded}
	if err := mgr.Hydrate(info, ""); err != nil {
		t.Fatalf("Hydrate: %v", err)
	}
	got, ok := mgr.Get("src/inv1")
	if !ok {
		t.Fatal("Hydrate did not register the inventory")
	}
	if got.State != inventory.StateError {
		t.Errorf("state = %s, want error", got.State)
	}
	if got.Error == "" {
		t.Error("error message missing")
	}
}
