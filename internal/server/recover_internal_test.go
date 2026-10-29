package server

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/internal/jobs"
	"github.com/eunmann/s3-inv-db/pkg/pricing"
	"github.com/rs/zerolog"
)

// TestRecover_HydratesPersistedInventories pins the boot-time
// rehydration path: an inventory persisted to the store by a previous
// process must reappear in the in-memory Manager.
func TestRecover_HydratesPersistedInventories(t *testing.T) {
	db := testDB(t)

	// Seed the persistent store as if a previous server had registered
	// (then unloaded) an inventory.
	invStore, err := inventory.NewStore(db)
	if err != nil {
		t.Fatalf("inventory.NewStore: %v", err)
	}
	if err := invStore.Upsert(t.Context(), inventory.Info{
		ID:    "src/inv1",
		Name:  "src/inv1",
		Path:  "s3://src/inv1/manifest.json",
		State: inventory.StateNotLoaded,
	}); err != nil {
		t.Fatalf("seed inventory: %v", err)
	}

	srv, err := New(t.Context(), Config{
		Addr:       ":0",
		Logger:     zerolog.Nop(),
		PriceTable: pricing.DefaultUSEast1Prices(),
		DB:         db,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	got, ok := srv.manager.Get("src/inv1")
	if !ok {
		t.Fatal("inventory not hydrated into Manager")
	}
	if got.State != inventory.StateNotLoaded {
		t.Errorf("hydrated state = %s, want not_loaded", got.State)
	}
}

// TestRecover_FlipsStaleLoadingToError covers the most user-visible
// recovery behavior: an inventory caught mid-load when the previous
// process exited gets flipped to error so the UI shows Retry instead
// of a forever spinner.
func TestRecover_FlipsStaleLoadingToError(t *testing.T) {
	db := testDB(t)
	invStore, err := inventory.NewStore(db)
	if err != nil {
		t.Fatalf("inventory.NewStore: %v", err)
	}
	if err := invStore.Upsert(t.Context(), inventory.Info{
		ID:    "src/inv1",
		Name:  "src/inv1",
		Path:  "u",
		State: inventory.StateLoading,
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	srv, err := New(t.Context(), Config{
		Addr:       ":0",
		Logger:     zerolog.Nop(),
		PriceTable: pricing.DefaultUSEast1Prices(),
		DB:         db,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	got, _ := srv.manager.Get("src/inv1")
	if got.State != inventory.StateError {
		t.Errorf("post-recover state = %s, want error", got.State)
	}
	if got.Error == "" {
		t.Error("expected an explanatory error message after stale-loading flip")
	}

	persisted, err := invStore.Get(t.Context(), "src/inv1")
	if err != nil {
		t.Fatalf("re-read store: %v", err)
	}
	if persisted.State != inventory.StateError {
		t.Errorf("persisted state = %s, want error (flip not mirrored)", persisted.State)
	}
}

// TestRecover_MarksStaleJobsAborted verifies the jobs-side of recover:
// running/queued jobs from a previous process must be marked aborted
// at boot so the UI doesn't show a spinner for a phantom worker.
func TestRecover_MarksStaleJobsAborted(t *testing.T) {
	db := testDB(t)
	invStore, err := inventory.NewStore(db)
	if err != nil {
		t.Fatalf("inventory.NewStore: %v", err)
	}
	if err := invStore.Upsert(t.Context(), inventory.Info{ID: "src/inv1", Name: "n", Path: "p", State: inventory.StateLoading}); err != nil {
		t.Fatalf("seed inventory: %v", err)
	}
	jobStore := jobs.NewStore(db)
	for id, state := range map[jobs.ID]jobs.State{"running1": jobs.StateRunning, "queued1": jobs.StateQueued, "ok1": jobs.StateSucceeded} {
		if err := jobStore.Upsert(t.Context(), jobs.Job{ID: id, InventoryID: "src/inv1", Kind: jobs.KindBuild, State: state}); err != nil {
			t.Fatalf("seed job %s: %v", id, err)
		}
	}

	srv, err := New(t.Context(), Config{
		Addr:       ":0",
		Logger:     zerolog.Nop(),
		PriceTable: pricing.DefaultUSEast1Prices(),
		DB:         db,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	_ = srv // recover() is called inside New

	for _, id := range []jobs.ID{"running1", "queued1"} {
		j, err := jobStore.Get(t.Context(), id)
		if err != nil {
			t.Fatalf("Get %s: %v", id, err)
		}
		if j.State != jobs.StateAborted {
			t.Errorf("job %s state = %s, want aborted", id, j.State)
		}
	}
	survivor, err := jobStore.Get(t.Context(), "ok1")
	if err != nil {
		t.Fatalf("Get ok1: %v", err)
	}
	if survivor.State != jobs.StateSucceeded {
		t.Errorf("succeeded job got reaped: %s", survivor.State)
	}
}

var errLoadAbandoned = errors.New("load abandoned by the previous process")

// TestRecover_InterruptedManualLoadKeepsPreLoadPin: a manual load cut
// off by a restart leaves the run in error with the pin and user-unload
// mark it had before the load, not pinned by a load that never landed.
func TestRecover_InterruptedManualLoadKeepsPreLoadPin(t *testing.T) {
	db := testDB(t)
	invStore, err := inventory.NewStore(db)
	if err != nil {
		t.Fatalf("inventory.NewStore: %v", err)
	}
	const id inventory.ID = "src/inv1/2026-01-01T00-00Z"
	unloadedAt := time.Unix(1_700_000_000, 0)
	previous := inventory.NewCatalog(invStore)
	if err := previous.Hydrate(t.Context(), inventory.Info{
		ID: id, Name: "n", Path: "p", State: inventory.StateNotLoaded, UserUnloadedAt: unloadedAt,
	}, ""); err != nil {
		t.Fatal(err)
	}
	building, release := make(chan struct{}), make(chan struct{})
	loadDone := make(chan error, 1)
	go func() {
		loadDone <- previous.LoadWith(t.Context(), id, func(context.Context, inventory.Info) (string, error) {
			close(building)
			<-release

			return "", errLoadAbandoned
		})
	}()
	defer func() {
		close(release)
		<-loadDone
	}()
	<-building

	srv, err := New(t.Context(), Config{
		Addr:       ":0",
		Logger:     zerolog.Nop(),
		PriceTable: pricing.DefaultUSEast1Prices(),
		DB:         db,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	got, _ := srv.manager.Get(id)
	if got.State != inventory.StateError {
		t.Errorf("state = %s, want error", got.State)
	}
	if got.Pinned {
		t.Error("pinned by a load the restart interrupted")
	}
	if !got.UserUnloadedAt.Equal(unloadedAt) {
		t.Errorf("UserUnloadedAt = %v, want %v as before the load", got.UserUnloadedAt, unloadedAt)
	}
}

// TestRecover_NoFailureWhenStoreIsEmpty is the happy startup path on
// a clean install — no inventories, no jobs, no errors.
func TestRecover_NoFailureWhenStoreIsEmpty(t *testing.T) {
	srv, err := New(t.Context(), Config{
		Addr:       ":0",
		Logger:     zerolog.Nop(),
		PriceTable: pricing.DefaultUSEast1Prices(),
		DB:         testDB(t),
	})
	if err != nil {
		t.Fatalf("New on empty store: %v", err)
	}
	if got := srv.manager.List(); len(got) != 0 {
		t.Errorf("Manager.List() = %d entries on empty store, want 0", len(got))
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_ = ctx
}
