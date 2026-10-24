package jobs_test

import (
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/eunmann/s3-inv-db/internal/dbtest"
	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/internal/jobs"
)

func storeWithInventory(t *testing.T, invID inventory.ID) (*jobs.Store, *sql.DB) {
	t.Helper()
	db := dbtest.OpenMemDB(t)
	invStore, err := inventory.NewStore(db)
	if err != nil {
		t.Fatalf("inventory.NewStore: %v", err)
	}
	if err := invStore.Upsert(t.Context(), inventory.Info{ID: invID, Name: "n", Path: "p", State: inventory.StateNotLoaded}); err != nil {
		t.Fatalf("seed inventory: %v", err)
	}
	store := jobs.NewStore(db)

	return store, db
}

func seedInventoryRows(t *testing.T, db *sql.DB, ids ...inventory.ID) {
	t.Helper()
	invStore, err := inventory.NewStore(db)
	if err != nil {
		t.Fatalf("inventory.NewStore: %v", err)
	}
	for _, id := range ids {
		if err := invStore.Upsert(t.Context(), inventory.Info{ID: id, Name: "n", Path: "p", State: inventory.StateNotLoaded}); err != nil {
			t.Fatalf("seed inventory %s: %v", id, err)
		}
	}
}

func TestStore_LatestSuccessfulBuildForConfig_ExcludesSelfAndPicksPrior(t *testing.T) {
	store, db := storeWithInventory(t, "src/inv1/run-a")
	seedInventoryRows(t, db, "src/inv1/run-b")

	t0 := time.Unix(1_000_000, 0)
	prior := jobs.Job{
		ID: "prior", InventoryID: "src/inv1/run-a", Kind: jobs.KindBuild,
		State: jobs.StateSucceeded, StartedAt: t0, FinishedAt: t0.Add(20 * time.Second),
	}
	current := jobs.Job{
		ID: "current", InventoryID: "src/inv1/run-b", Kind: jobs.KindBuild,
		State: jobs.StateSucceeded, StartedAt: t0.Add(time.Hour), FinishedAt: t0.Add(time.Hour + 99*time.Second),
	}
	for _, j := range []jobs.Job{prior, current} {
		if err := store.Upsert(t.Context(), j); err != nil {
			t.Fatalf("Upsert %s: %v", j.ID, err)
		}
	}

	// Excluding "current" must return "prior", not the current run itself
	// — even though current finished later.
	got, err := store.LatestSuccessfulBuildForConfig(t.Context(), "src/inv1", "current")
	if err != nil {
		t.Fatalf("LatestSuccessfulBuildForConfig: %v", err)
	}
	if got.ID != "prior" {
		t.Errorf("baseline = %q, want prior (current must be excluded)", got.ID)
	}

	// With only its own job present, excluding it yields not-found.
	_, err = store.LatestSuccessfulBuildForConfig(t.Context(), "src/inv1", "prior")
	if !errors.Is(err, jobs.ErrStoreNotFound) {
		// "current" is still present and not excluded here, so it should match.
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}
}

func TestStore_LatestSuccessfulBuildForConfig_LikeWildcardsAreLiteral(t *testing.T) {
	// A config ID with '_' must not match a different config where '_'
	// acts as a single-char wildcard.
	store, db := storeWithInventory(t, "src/daily_inv/run-1")
	seedInventoryRows(t, db, "src/dailyXinv/run-1")

	t0 := time.Unix(2_000_000, 0)
	decoy := jobs.Job{
		ID: "decoy", InventoryID: "src/dailyXinv/run-1", Kind: jobs.KindBuild,
		State: jobs.StateSucceeded, StartedAt: t0, FinishedAt: t0.Add(10 * time.Second),
	}
	if err := store.Upsert(t.Context(), decoy); err != nil {
		t.Fatalf("Upsert decoy: %v", err)
	}

	// Config "src/daily_inv" must NOT match the decoy under "src/dailyXinv".
	_, err := store.LatestSuccessfulBuildForConfig(t.Context(), "src/daily_inv", "none")
	if !errors.Is(err, jobs.ErrStoreNotFound) {
		t.Errorf("err = %v, want ErrStoreNotFound (underscore must be literal, not wildcard)", err)
	}
}

func TestStore_ListForInventory_EmptyReturnsNilNoError(t *testing.T) {
	store, _ := storeWithInventory(t, "src/inv1")
	got, err := store.ListForInventory(t.Context(), "src/inv1")
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
		if err := store.Upsert(t.Context(), jobs.Job{ID: id, InventoryID: "src/inv1", Kind: jobs.KindBuild, State: jobs.StateSucceeded}); err != nil {
			t.Fatalf("Upsert %s: %v", id, err)
		}
		time.Sleep(1100 * time.Millisecond)
	}
	got, err := store.ListForInventory(t.Context(), "src/inv1")
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
	if err := invStore.Upsert(t.Context(), inventory.Info{ID: "src/other", Name: "n", Path: "p", State: inventory.StateNotLoaded}); err != nil {
		t.Fatalf("seed other inventory: %v", err)
	}
	for _, j := range []jobs.Job{
		{ID: "j-mine", InventoryID: "src/inv1", Kind: jobs.KindBuild, State: jobs.StateSucceeded},
		{ID: "j-theirs", InventoryID: "src/other", Kind: jobs.KindBuild, State: jobs.StateSucceeded},
	} {
		if err := store.Upsert(t.Context(), j); err != nil {
			t.Fatalf("Upsert %s: %v", j.ID, err)
		}
	}
	mine, err := store.ListForInventory(t.Context(), "src/inv1")
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
		if err := store.Upsert(t.Context(), jobs.Job{ID: id, InventoryID: "src/inv1", Kind: jobs.KindBuild, State: jobs.StateSucceeded}); err != nil {
			t.Fatalf("Upsert %s: %v", id, err)
		}
		time.Sleep(1100 * time.Millisecond)
	}
	got, err := store.LatestForInventory(t.Context(), "src/inv1")
	if err != nil {
		t.Fatalf("LatestForInventory: %v", err)
	}
	if got.ID != "new" {
		t.Errorf("LatestForInventory = %v, want id=new", got.ID)
	}
}

func TestStore_LatestForInventory_MissingReturnsErrStoreNotFound(t *testing.T) {
	store, _ := storeWithInventory(t, "src/inv1")
	_, err := store.LatestForInventory(t.Context(), "src/inv1")
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
		StageTotal: 12345, StageDone: 12345,
		StartedAt: started, FinishedAt: finished, Error: "",
	}
	if err := store.Upsert(t.Context(), want); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	got, err := store.Get(t.Context(), "j1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Stage != want.Stage || got.Progress != want.Progress ||
		got.StageTotal != want.StageTotal || got.StageDone != want.StageDone ||
		!got.StartedAt.Equal(started) || !got.FinishedAt.Equal(finished) {
		t.Errorf("round-trip mismatch:\n got=%+v\nwant=%+v", got, want)
	}
}

func TestStore_PruneTerminal_KeepsRecentAndLiveJobs(t *testing.T) {
	store, db := storeWithInventory(t, "src/inv/a")
	seedInventoryRows(t, db, "src/inv/b")
	ctx := t.Context()
	now := time.Now()
	old := now.Add(-2 * jobs.RetentionWindow)

	// Inventory a: 25 old terminal jobs (cycling every terminal state)
	// plus one old running job and one recent failed job.
	terminal := []jobs.State{jobs.StateSucceeded, jobs.StateFailed, jobs.StateCancelled, jobs.StateAborted}
	type row struct {
		id    jobs.ID
		inv   inventory.ID
		state jobs.State
		at    time.Time
	}
	rows := make([]row, 0, 28)
	for i := range 25 {
		rows = append(rows, row{jobs.ID(fmt.Sprintf("a-old-%02d", i)), "src/inv/a", terminal[i%len(terminal)], old.Add(time.Duration(i) * time.Minute)})
	}
	rows = append(rows,
		row{"a-old-running", "src/inv/a", jobs.StateRunning, old.Add(-time.Hour)},
		row{"a-recent", "src/inv/a", jobs.StateFailed, now},
		row{"b-old", "src/inv/b", jobs.StateSucceeded, old},
	)
	for _, r := range rows {
		if err := store.Upsert(ctx, jobs.Job{ID: r.id, InventoryID: r.inv, Kind: jobs.KindBuild, State: r.state}); err != nil {
			t.Fatalf("Upsert %s: %v", r.id, err)
		}
		if _, err := db.ExecContext(ctx, `UPDATE jobs SET updated_at = ? WHERE id = ?`, r.at.Unix(), string(r.id)); err != nil {
			t.Fatalf("backdate %s: %v", r.id, err)
		}
	}

	const keep = 20
	n, err := store.PruneTerminal(ctx, now.Add(-jobs.RetentionWindow), keep)
	if err != nil {
		t.Fatalf("PruneTerminal: %v", err)
	}
	// Inventory a ranks a-recent first, then a-old-24..a-old-06 (19 more)
	// fill the keep window; a-old-00..a-old-05 are beyond it. The running
	// job is beyond it too but is never pruned.
	if n != 6 {
		t.Errorf("pruned = %d, want 6", n)
	}

	aJobs, err := store.ListForInventory(ctx, "src/inv/a")
	if err != nil {
		t.Fatalf("ListForInventory a: %v", err)
	}
	got := make(map[jobs.ID]bool, len(aJobs))
	for _, j := range aJobs {
		got[j.ID] = true
	}
	for i := range 6 {
		if id := jobs.ID(fmt.Sprintf("a-old-%02d", i)); got[id] {
			t.Errorf("%s survived, want pruned", id)
		}
	}
	for _, id := range []jobs.ID{"a-old-06", "a-old-24", "a-recent", "a-old-running"} {
		if !got[id] {
			t.Errorf("%s pruned, want kept", id)
		}
	}
	if _, err := store.Get(ctx, "b-old"); err != nil {
		t.Errorf("b-old (only job of its inventory) pruned: %v", err)
	}
}
