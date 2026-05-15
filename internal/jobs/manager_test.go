package jobs_test

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/internal/jobs"
	"github.com/eunmann/s3-inv-db/internal/migrate"
	_ "modernc.org/sqlite"
)

func openTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", "file::memory:?cache=shared&_pragma=foreign_keys(1)")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := migrate.Apply(db); err != nil {
		t.Fatalf("migrate.Apply: %v", err)
	}
	if _, err := inventory.NewStore(db); err != nil {
		t.Fatalf("inventory.NewStore: %v", err)
	}

	return db
}

func newManager(t *testing.T) (*jobs.Manager, *jobs.Store, *jobs.Bus) {
	t.Helper()
	db := openTestDB(t)
	invStore, err := inventory.NewStore(db)
	if err != nil {
		t.Fatalf("inventory.NewStore: %v", err)
	}
	if err := invStore.Upsert(inventory.Info{ID: "src/inv1", Name: "n", Path: "p", State: inventory.StateNotLoaded}); err != nil {
		t.Fatalf("seed inventory: %v", err)
	}
	store, err := jobs.NewStore(db)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	bus := jobs.NewBus(16)

	return jobs.NewManager(store, bus), store, bus
}

func waitForState(t *testing.T, store *jobs.Store, id jobs.ID, target jobs.State) jobs.Job {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		j, err := store.Get(id)
		if err == nil && j.State == target {
			return j
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("job %s never reached state %s", id, target)

	return jobs.Job{}
}

func TestManager_SubmitSucceeds(t *testing.T) {
	mgr, store, _ := newManager(t)
	job, err := mgr.Submit("src/inv1", jobs.KindBuild, func(_ context.Context, report func(jobs.Update)) error {
		report(jobs.Update{Stage: "fetch", Progress: 30})
		report(jobs.Update{Stage: "extsort", Progress: 70})

		return nil
	})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if job.State != jobs.StateQueued {
		t.Errorf("initial state = %s, want queued", job.State)
	}

	done := waitForState(t, store, job.ID, jobs.StateSucceeded)
	if done.Progress != 100 || done.FinishedAt.IsZero() {
		t.Errorf("succeeded job not finalized: %+v", done)
	}
}

func TestManager_FailingWork(t *testing.T) {
	mgr, store, _ := newManager(t)
	job, err := mgr.Submit("src/inv1", jobs.KindBuild, func(_ context.Context, _ func(jobs.Update)) error {
		return errors.New("boom")
	})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	final := waitForState(t, store, job.ID, jobs.StateFailed)
	if final.Error != "boom" {
		t.Errorf("error = %q, want boom", final.Error)
	}
}

func TestManager_Cancel(t *testing.T) {
	mgr, store, _ := newManager(t)

	started := make(chan struct{})
	job, err := mgr.Submit("src/inv1", jobs.KindBuild, func(ctx context.Context, _ func(jobs.Update)) error {
		close(started)
		<-ctx.Done()

		return ctx.Err()
	})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	<-started

	if err := mgr.Cancel(job.ID); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	final := waitForState(t, store, job.ID, jobs.StateCancelled)
	if final.FinishedAt.IsZero() {
		t.Errorf("cancelled job missing FinishedAt: %+v", final)
	}
}

func TestManager_CancelUnknown(t *testing.T) {
	mgr, _, _ := newManager(t)
	if err := mgr.Cancel("no-such-job"); !errors.Is(err, jobs.ErrNotFound) {
		t.Errorf("Cancel(unknown) error = %v, want ErrNotFound", err)
	}
}

func TestBus_FanOut(t *testing.T) {
	bus := jobs.NewBus(4)
	chA, cancelA := bus.Subscribe()
	chB, cancelB := bus.Subscribe()
	defer cancelA()
	defer cancelB()

	bus.Publish(jobs.Job{ID: "j1", State: jobs.StateQueued})

	for i, ch := range []<-chan jobs.Job{chA, chB} {
		select {
		case j := <-ch:
			if j.ID != "j1" {
				t.Errorf("sub %d got %+v", i, j)
			}
		case <-time.After(100 * time.Millisecond):
			t.Errorf("sub %d did not receive", i)
		}
	}
}

func TestStore_MarkAborted(t *testing.T) {
	db := openTestDB(t)
	invStore, err := inventory.NewStore(db)
	if err != nil {
		t.Fatalf("inventory.NewStore: %v", err)
	}
	if err := invStore.Upsert(inventory.Info{ID: "src/inv1", Name: "n", Path: "p", State: inventory.StateNotLoaded}); err != nil {
		t.Fatalf("seed inventory: %v", err)
	}
	store, err := jobs.NewStore(db)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	for _, st := range []jobs.State{jobs.StateRunning, jobs.StateQueued, jobs.StateSucceeded} {
		if err := store.Upsert(jobs.Job{ID: jobs.ID(st), InventoryID: "src/inv1", Kind: jobs.KindBuild, State: st}); err != nil {
			t.Fatalf("upsert: %v", err)
		}
	}
	n, err := store.MarkAborted("restart", jobs.StateRunning, jobs.StateQueued)
	if err != nil {
		t.Fatalf("MarkAborted: %v", err)
	}
	if n != 2 {
		t.Errorf("aborted = %d, want 2", n)
	}
	for _, id := range []jobs.ID{"running", "queued"} {
		j, err := store.Get(id)
		if err != nil {
			t.Fatalf("Get %s: %v", id, err)
		}
		if j.State != jobs.StateAborted || j.Error != "restart" {
			t.Errorf("job %s not aborted: %+v", id, j)
		}
	}
	survivor, err := store.Get("succeeded")
	if err != nil {
		t.Fatalf("Get succeeded: %v", err)
	}
	if survivor.State != jobs.StateSucceeded {
		t.Errorf("succeeded reaped: %+v", survivor)
	}
}

func TestManager_SubmitAfterShutdown(t *testing.T) {
	mgr, _, _ := newManager(t)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := mgr.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}

	_, err := mgr.Submit("src/inv1", jobs.KindBuild, func(_ context.Context, _ func(jobs.Update)) error {
		return nil
	})
	if !errors.Is(err, jobs.ErrShutdown) {
		t.Errorf("Submit after Shutdown error = %v, want ErrShutdown", err)
	}
}

func TestManager_ShutdownCancelsLiveJob(t *testing.T) {
	mgr, store, _ := newManager(t)

	started := make(chan struct{})
	job, err := mgr.Submit("src/inv1", jobs.KindBuild, func(ctx context.Context, _ func(jobs.Update)) error {
		close(started)
		<-ctx.Done()

		return ctx.Err()
	})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	<-started

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := mgr.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}

	final, err := store.Get(job.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if final.State != jobs.StateCancelled && final.State != jobs.StateFailed {
		t.Errorf("post-shutdown job state = %s, want cancelled or failed", final.State)
	}
}
