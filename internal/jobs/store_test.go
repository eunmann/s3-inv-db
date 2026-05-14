package jobs_test

import (
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/internal/jobs"
	_ "modernc.org/sqlite"
)

func storeWithInventory(t *testing.T, invID inventory.ID) (*jobs.Store, *sql.DB) {
	t.Helper()
	db, err := sql.Open("sqlite", "file::memory:?cache=shared&_pragma=foreign_keys(1)")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	invStore, err := inventory.NewStore(db)
	if err != nil {
		t.Fatalf("inventory.NewStore: %v", err)
	}
	if err := invStore.Upsert(inventory.Info{ID: invID, Name: "n", Path: "p", State: inventory.StateNotLoaded}); err != nil {
		t.Fatalf("seed inventory: %v", err)
	}
	store, err := jobs.NewStore(db)
	if err != nil {
		t.Fatalf("jobs.NewStore: %v", err)
	}
	return store, db
}

func TestStore_ListForInventory_EmptyReturnsNilNoError(t *testing.T) {
	store, _ := storeWithInventory(t, "src/inv1")
	got, err := store.ListForInventory("src/inv1")
	if err != nil {
		t.Fatalf("ListForInventory: %v", err)
	}
	if got != nil {
		t.Errorf("empty list = %v, want nil", got)
	}
}

func TestStore_ListForInventory_OrderedByUpdatedAtDesc(t *testing.T) {
	store, _ := storeWithInventory(t, "src/inv1")
	// updated_at has second resolution; sleep > 1s between writes so the
	// ORDER BY produces a stable order.
	for _, id := range []jobs.ID{"older", "middle", "newer"} {
		if err := store.Upsert(jobs.Job{ID: id, InventoryID: "src/inv1", Kind: jobs.KindBuild, State: jobs.StateSucceeded}); err != nil {
			t.Fatalf("Upsert %s: %v", id, err)
		}
		time.Sleep(1100 * time.Millisecond)
	}
	got, err := store.ListForInventory("src/inv1")
	if err != nil {
		t.Fatalf("ListForInventory: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("len = %d, want 3", len(got))
	}
	if got[0].ID != "newer" || got[1].ID != "middle" || got[2].ID != "older" {
		t.Errorf("order = %v %v %v, want newer middle older", got[0].ID, got[1].ID, got[2].ID)
	}
}

func TestStore_ListForInventory_ScopedByInventoryID(t *testing.T) {
	store, db := storeWithInventory(t, "src/inv1")
	invStore, err := inventory.NewStore(db)
	if err != nil {
		t.Fatalf("inventory.NewStore (re-open): %v", err)
	}
	if err := invStore.Upsert(inventory.Info{ID: "src/other", Name: "n", Path: "p", State: inventory.StateNotLoaded}); err != nil {
		t.Fatalf("seed other inventory: %v", err)
	}
	for _, j := range []jobs.Job{
		{ID: "j-mine", InventoryID: "src/inv1", Kind: jobs.KindBuild, State: jobs.StateSucceeded},
		{ID: "j-theirs", InventoryID: "src/other", Kind: jobs.KindBuild, State: jobs.StateSucceeded},
	} {
		if err := store.Upsert(j); err != nil {
			t.Fatalf("Upsert %s: %v", j.ID, err)
		}
	}
	mine, err := store.ListForInventory("src/inv1")
	if err != nil {
		t.Fatalf("List mine: %v", err)
	}
	if len(mine) != 1 || mine[0].ID != "j-mine" {
		t.Errorf("ListForInventory(src/inv1) = %v, want exactly j-mine", mine)
	}
}

func TestStore_LatestForInventory_PicksMostRecent(t *testing.T) {
	store, _ := storeWithInventory(t, "src/inv1")
	for _, id := range []jobs.ID{"old", "new"} {
		if err := store.Upsert(jobs.Job{ID: id, InventoryID: "src/inv1", Kind: jobs.KindBuild, State: jobs.StateSucceeded}); err != nil {
			t.Fatalf("Upsert %s: %v", id, err)
		}
		time.Sleep(1100 * time.Millisecond)
	}
	got, err := store.LatestForInventory("src/inv1")
	if err != nil {
		t.Fatalf("LatestForInventory: %v", err)
	}
	if got.ID != "new" {
		t.Errorf("LatestForInventory = %v, want id=new", got.ID)
	}
}

func TestStore_LatestForInventory_MissingReturnsErrStoreNotFound(t *testing.T) {
	store, _ := storeWithInventory(t, "src/inv1")
	_, err := store.LatestForInventory("src/inv1")
	if !errors.Is(err, jobs.ErrStoreNotFound) {
		t.Errorf("err = %v, want ErrStoreNotFound", err)
	}
}

func TestStore_GetRoundTripsAllFields(t *testing.T) {
	store, _ := storeWithInventory(t, "src/inv1")
	started := time.Now().Add(-time.Hour).Truncate(time.Second)
	finished := started.Add(30 * time.Minute)
	want := jobs.Job{
		ID: "j1", InventoryID: "src/inv1", Kind: jobs.KindUnload,
		State: jobs.StateSucceeded, Stage: "indexing", Progress: 100,
		BytesTotal: 12345, BytesDone: 12345,
		StartedAt: started, FinishedAt: finished, Error: "",
	}
	if err := store.Upsert(want); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	got, err := store.Get("j1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Stage != want.Stage || got.Progress != want.Progress ||
		got.BytesTotal != want.BytesTotal || got.BytesDone != want.BytesDone ||
		!got.StartedAt.Equal(started) || !got.FinishedAt.Equal(finished) {
		t.Errorf("round-trip mismatch:\n got=%+v\nwant=%+v", got, want)
	}
}
