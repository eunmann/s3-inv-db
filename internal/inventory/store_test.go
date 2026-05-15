package inventory_test

import (
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/internal/migrate"
	_ "modernc.org/sqlite"
)

func openStore(t *testing.T) *inventory.Store {
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

	return store
}

func TestStore_UpsertRoundTrip(t *testing.T) {
	s := openStore(t)
	loaded := time.Unix(1_700_000_000, 0)

	want := inventory.Info{
		ID:          "src/inv1",
		Name:        "src/inv1",
		Path:        "s3://src/manifest.json",
		State:       inventory.StateLoaded,
		NodeCount:   42,
		MaxDepth:    7,
		HasTierData: true,
		LoadedAt:    loaded,
	}
	if err := s.Upsert(t.Context(), want); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	got, err := s.Get(t.Context(), "src/inv1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.ID != want.ID || got.State != want.State || got.NodeCount != 42 ||
		!got.HasTierData || !got.LoadedAt.Equal(loaded) {
		t.Errorf("got = %+v\nwant = %+v", got, want)
	}
}

func TestStore_UpsertReplacesRow(t *testing.T) {
	s := openStore(t)
	first := inventory.Info{ID: "id1", Name: "n", Path: "p", State: inventory.StateNotLoaded}
	if err := s.Upsert(t.Context(), first); err != nil {
		t.Fatalf("upsert first: %v", err)
	}
	first.State = inventory.StateLoaded
	first.NodeCount = 99
	if err := s.Upsert(t.Context(), first); err != nil {
		t.Fatalf("upsert second: %v", err)
	}

	got, err := s.Get(t.Context(), "id1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.State != inventory.StateLoaded || got.NodeCount != 99 {
		t.Errorf("upsert didn't update: %+v", got)
	}

	all, err := s.List(t.Context())
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(all) != 1 {
		t.Errorf("rows = %d, want 1", len(all))
	}
}

func TestStore_GetMissing(t *testing.T) {
	s := openStore(t)
	_, err := s.Get(t.Context(), "nope")
	if !errors.Is(err, inventory.ErrStoreNotFound) {
		t.Errorf("Get(missing) error = %v, want ErrStoreNotFound", err)
	}
}

func TestStore_ListInsertionOrder(t *testing.T) {
	s := openStore(t)
	for _, id := range []inventory.ID{"c", "a", "b"} {
		if err := s.Upsert(t.Context(), inventory.Info{ID: id, Name: string(id), Path: "p", State: inventory.StateNotLoaded}); err != nil {
			t.Fatalf("upsert %s: %v", id, err)
		}
	}
	all, err := s.List(t.Context())
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(all) != 3 || all[0].ID != "a" || all[1].ID != "b" || all[2].ID != "c" {
		t.Errorf("List not sorted: %+v", all)
	}
}

func TestStore_Delete(t *testing.T) {
	s := openStore(t)
	if err := s.Upsert(t.Context(), inventory.Info{ID: "id1", Name: "n", Path: "p", State: inventory.StateNotLoaded}); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	if err := s.Delete(t.Context(), "id1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	_, err := s.Get(t.Context(), "id1")
	if !errors.Is(err, inventory.ErrStoreNotFound) {
		t.Errorf("Get after Delete: %v, want ErrStoreNotFound", err)
	}
	if err := s.Delete(t.Context(), "id1"); !errors.Is(err, inventory.ErrStoreNotFound) {
		t.Errorf("Delete(missing) error = %v, want ErrStoreNotFound", err)
	}
}
