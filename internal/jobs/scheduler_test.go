package jobs_test

import (
	"context"
	"database/sql"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eunmann/s3-inv-db/internal/dbtest"
	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/internal/jobs"
)

var errBoom = errors.New("boom")

func openTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db := dbtest.OpenMemDB(t)
	if _, err := inventory.NewStore(db); err != nil {
		t.Fatalf("inventory.NewStore: %v", err)
	}

	return db
}

func newScheduler(t *testing.T) (*jobs.Scheduler, *jobs.Store, *jobs.Bus) {
	t.Helper()
	db := openTestDB(t)
	invStore, err := inventory.NewStore(db)
	if err != nil {
		t.Fatalf("inventory.NewStore: %v", err)
	}
	if err := invStore.Upsert(t.Context(), inventory.Info{ID: "src/inv1", Name: "n", Path: "p", State: inventory.StateNotLoaded}); err != nil {
		t.Fatalf("seed inventory: %v", err)
	}
	store := jobs.NewStore(db)
	bus := jobs.NewBus(16)

	// A short progress interval so a test reading running progress back
	// from the store doesn't wait out the one-second default.
	return jobs.NewSchedulerForTest(store, bus, 10*time.Millisecond), store, bus
}

func waitForState(t *testing.T, store *jobs.Store, id jobs.ID, target jobs.State) jobs.Job {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		j, err := store.Get(t.Context(), id)
		if err == nil && j.State == target {
			return j
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("job %s never reached state %s", id, target)

	return jobs.Job{}
}

func TestManager_SubmitSucceeds(t *testing.T) {
	sched, store, _ := newScheduler(t)
	job, err := sched.Submit(t.Context(), "src/inv1", jobs.KindBuild, func(_ context.Context, report func(jobs.Update)) error {
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
	sched, store, _ := newScheduler(t)
	job, err := sched.Submit(t.Context(), "src/inv1", jobs.KindBuild, func(_ context.Context, _ func(jobs.Update)) error {
		return errBoom
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
	sched, store, _ := newScheduler(t)

	started := make(chan struct{})
	job, err := sched.Submit(t.Context(), "src/inv1", jobs.KindBuild, func(ctx context.Context, _ func(jobs.Update)) error {
		close(started)
		<-ctx.Done()

		return ctx.Err()
	})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	<-started

	if err := sched.Cancel(job.ID); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	final := waitForState(t, store, job.ID, jobs.StateCancelled)
	if final.FinishedAt.IsZero() {
		t.Errorf("cancelled job missing FinishedAt: %+v", final)
	}
}

func TestManager_CancelUnknown(t *testing.T) {
	sched, _, _ := newScheduler(t)
	if err := sched.Cancel("no-such-job"); !errors.Is(err, jobs.ErrNotFound) {
		t.Errorf("Cancel(unknown) error = %v, want ErrNotFound", err)
	}
}

// TestManager_DedupsLiveInventory verifies one live job per inventory:
// a second Submit while the first is live returns ErrDuplicateInventory,
// and once the first finishes a fresh Submit is accepted again.
func TestManager_DedupsLiveInventory(t *testing.T) {
	sched, store, _ := newScheduler(t)

	started := make(chan struct{})
	release := make(chan struct{})
	first, err := sched.Submit(t.Context(), "src/inv1", jobs.KindBuild, func(_ context.Context, _ func(jobs.Update)) error {
		close(started)
		<-release

		return nil
	})
	if err != nil {
		t.Fatalf("first submit: %v", err)
	}
	<-started

	dup, err := sched.Submit(t.Context(), "src/inv1", jobs.KindBuild, func(_ context.Context, _ func(jobs.Update)) error {
		return nil
	})
	if !errors.Is(err, jobs.ErrDuplicateInventory) {
		t.Fatalf("duplicate submit error = %v, want ErrDuplicateInventory", err)
	}
	if dup.ID != first.ID {
		t.Errorf("duplicate submit returned job %s, want existing %s", dup.ID, first.ID)
	}

	close(release)
	waitForState(t, store, first.ID, jobs.StateSucceeded)

	// The inventory stops being live just after its terminal state is
	// written, so a fresh build is allowed once that settles.
	var third jobs.Job
	deadline := time.Now().Add(2 * time.Second)
	for {
		third, err = sched.Submit(t.Context(), "src/inv1", jobs.KindBuild, func(_ context.Context, _ func(jobs.Update)) error {
			return nil
		})
		if !errors.Is(err, jobs.ErrDuplicateInventory) || time.Now().After(deadline) {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if err != nil {
		t.Fatalf("submit after completion: %v", err)
	}
	waitForState(t, store, third.ID, jobs.StateSucceeded)
}

// TestManager_ConcurrencyLimitSerializes verifies WithMaxConcurrency(1)
// runs at most one job's work at a time; the rest stay queued.
func TestManager_ConcurrencyLimitSerializes(t *testing.T) {
	sched := jobs.NewScheduler(nil, jobs.NewBus(16), jobs.WithMaxConcurrency(1))

	var running, maxSeen atomic.Int32
	started := make(chan struct{}, 3)
	release := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(3)
	work := func(_ context.Context, _ func(jobs.Update)) error {
		n := running.Add(1)
		for {
			m := maxSeen.Load()
			if n <= m || maxSeen.CompareAndSwap(m, n) {
				break
			}
		}
		started <- struct{}{}
		<-release
		running.Add(-1)
		wg.Done()

		return nil
	}
	for i := range 3 {
		if _, err := sched.Submit(context.Background(), inventory.ID("src/inv"+strconv.Itoa(i)), jobs.KindBuild, work); err != nil {
			t.Fatalf("submit %d: %v", i, err)
		}
	}

	// Exactly one job can hold the single slot; the others block in run()
	// before incrementing running, so this is deterministic, not timing.
	<-started
	if got := running.Load(); got != 1 {
		t.Errorf("running = %d, want 1 under concurrency limit", got)
	}

	close(release)
	wg.Wait()
	if got := maxSeen.Load(); got != 1 {
		t.Errorf("max concurrent = %d, want 1", got)
	}
}

func TestBus_FanOut(t *testing.T) {
	bus := jobs.NewBus(4)
	subA := bus.Subscribe()
	subB := bus.Subscribe()
	defer subA.Close()
	defer subB.Close()

	bus.Publish(jobs.Job{ID: "j1", State: jobs.StateQueued})

	for i, sub := range []*jobs.Subscription{subA, subB} {
		select {
		case <-sub.Ready():
			evs, _ := sub.Drain()
			if len(evs) != 1 || evs[0].Job == nil || evs[0].Job.ID != "j1" {
				t.Errorf("sub %d got %+v", i, evs)
			}
		case <-time.After(100 * time.Millisecond):
			t.Errorf("sub %d did not receive", i)
		}
	}
}

// TestBus_CoalescesPerJobAndKeepsTerminal: a subscriber that does not
// read while a job races through progress ends up with the job's last
// snapshot, terminal state included, in the order jobs first changed.
func TestBus_CoalescesPerJobAndKeepsTerminal(t *testing.T) {
	bus := jobs.NewBus(4)
	sub := bus.Subscribe()
	defer sub.Close()

	bus.Publish(jobs.Job{ID: "a", State: jobs.StateRunning})
	bus.PublishInventory("src/inv/run")
	for i := range 1000 {
		bus.Publish(jobs.Job{ID: "b", State: jobs.StateRunning, StageDone: int64(i)})
	}
	bus.Publish(jobs.Job{ID: "b", State: jobs.StateSucceeded, StageDone: 1000})
	bus.Publish(jobs.Job{ID: "a", State: jobs.StateFailed})

	evs, overflowed := sub.Drain()
	if overflowed {
		t.Fatal("overflowed with 3 keys under a cap of 4")
	}
	if len(evs) != 3 {
		t.Fatalf("got %d events, want 3 (one per job, one per inventory)", len(evs))
	}
	if evs[0].Job == nil || evs[0].Job.ID != "a" || evs[0].Job.State != jobs.StateFailed {
		t.Errorf("first = %+v, want job a failed", evs[0])
	}
	if evs[1].Job != nil || evs[1].Inventory != "src/inv/run" {
		t.Errorf("second = %+v, want the inventory notice", evs[1])
	}
	if evs[2].Job == nil || evs[2].Job.ID != "b" || evs[2].Job.State != jobs.StateSucceeded {
		t.Errorf("third = %+v, want job b succeeded", evs[2])
	}
	if evs, _ := sub.Drain(); len(evs) != 0 {
		t.Errorf("second drain returned %d events, want 0", len(evs))
	}
}

// TestBus_OverflowFlagsResync: more distinct keys than the cap between
// reads drops the queue and reports overflow once, so the reader
// resynchronises instead of acting on a partial view.
func TestBus_OverflowFlagsResync(t *testing.T) {
	bus := jobs.NewBus(2)
	sub := bus.Subscribe()
	defer sub.Close()

	for i := range 5 {
		bus.Publish(jobs.Job{ID: jobs.ID(strconv.Itoa(i)), State: jobs.StateQueued})
	}
	if _, overflowed := sub.Drain(); !overflowed {
		t.Fatal("no overflow after 5 keys under a cap of 2")
	}
	bus.Publish(jobs.Job{ID: "next", State: jobs.StateQueued})
	evs, overflowed := sub.Drain()
	if overflowed || len(evs) != 1 {
		t.Fatalf("after resync: %d events, overflowed %v; want 1, false", len(evs), overflowed)
	}
}

// TestBus_ClosedSubscriptionIgnoresPublish: Close stops delivery and is
// safe to call twice.
func TestBus_ClosedSubscriptionIgnoresPublish(t *testing.T) {
	bus := jobs.NewBus(4)
	sub := bus.Subscribe()
	sub.Close()
	sub.Close()
	bus.Publish(jobs.Job{ID: "j1"})
	if evs, _ := sub.Drain(); len(evs) != 0 {
		t.Errorf("closed subscription got %d events", len(evs))
	}
}

// TestScheduler_LiveSnapshots: the scheduler reports a running job's
// newest progress from memory and forgets the job once it is terminal.
func TestScheduler_LiveSnapshots(t *testing.T) {
	sched := jobs.NewScheduler(nil, jobs.NewBus(16))
	t.Cleanup(func() { _ = sched.Shutdown(context.Background()) })

	reported := make(chan struct{})
	release := make(chan struct{})
	job, err := sched.Submit(t.Context(), "src/inv/run", jobs.KindBuild, func(_ context.Context, report func(jobs.Update)) error {
		report(jobs.Update{Stage: "downloading", StageDone: 7, StageTotal: 10})
		close(reported)
		<-release

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	<-reported
	got, ok := sched.LiveForInventory("src/inv/run")
	if !ok || got.ID != job.ID || got.StageDone != 7 || got.Stage != "downloading" {
		t.Fatalf("LiveForInventory = %+v, %v; want running job at 7/10", got, ok)
	}
	if live := sched.Live(); len(live) != 1 || live[0].ID != job.ID {
		t.Fatalf("Live = %+v, want the one job", live)
	}
	close(release)
	deadline := time.Now().Add(2 * time.Second)
	for len(sched.Live()) != 0 {
		if time.Now().After(deadline) {
			t.Fatal("finished job still live")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if _, ok := sched.LiveForInventory("src/inv/run"); ok {
		t.Error("LiveForInventory still reports the finished job")
	}
}

func TestStore_MarkAborted(t *testing.T) {
	db := openTestDB(t)
	invStore, err := inventory.NewStore(db)
	if err != nil {
		t.Fatalf("inventory.NewStore: %v", err)
	}
	if err := invStore.Upsert(t.Context(), inventory.Info{ID: "src/inv1", Name: "n", Path: "p", State: inventory.StateNotLoaded}); err != nil {
		t.Fatalf("seed inventory: %v", err)
	}
	store := jobs.NewStore(db)
	for _, st := range []jobs.State{jobs.StateRunning, jobs.StateQueued, jobs.StateSucceeded} {
		if err := store.Upsert(t.Context(), jobs.Job{ID: jobs.ID(st), InventoryID: "src/inv1", Kind: jobs.KindBuild, State: st}); err != nil {
			t.Fatalf("upsert: %v", err)
		}
	}
	n, err := store.MarkAborted(t.Context(), "restart", jobs.StateRunning, jobs.StateQueued)
	if err != nil {
		t.Fatalf("MarkAborted: %v", err)
	}
	if n != 2 {
		t.Errorf("aborted = %d, want 2", n)
	}
	for _, id := range []jobs.ID{"running", "queued"} {
		j, err := store.Get(t.Context(), id)
		if err != nil {
			t.Fatalf("Get %s: %v", id, err)
		}
		if j.State != jobs.StateAborted || j.Error != "restart" {
			t.Errorf("job %s not aborted: %+v", id, j)
		}
	}
	survivor, err := store.Get(t.Context(), "succeeded")
	if err != nil {
		t.Fatalf("Get succeeded: %v", err)
	}
	if survivor.State != jobs.StateSucceeded {
		t.Errorf("succeeded reaped: %+v", survivor)
	}
}

func TestManager_SubmitAfterShutdown(t *testing.T) {
	sched, _, _ := newScheduler(t)

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	if err := sched.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}

	_, err := sched.Submit(t.Context(), "src/inv1", jobs.KindBuild, func(_ context.Context, _ func(jobs.Update)) error {
		return nil
	})
	if !errors.Is(err, jobs.ErrShutdown) {
		t.Errorf("Submit after Shutdown error = %v, want ErrShutdown", err)
	}
}

func TestManager_NilStoreIsNoop(t *testing.T) {
	sched := jobs.NewScheduler(nil, jobs.NewBus(8))
	job, err := sched.Submit(t.Context(), "src/inv1", jobs.KindBuild, func(_ context.Context, _ func(jobs.Update)) error {
		return nil
	})
	if err != nil {
		t.Fatalf("Submit with nil store: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if err := sched.Cancel(job.ID); errors.Is(err, jobs.ErrNotFound) {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("job %s never finished", job.ID)
}

func TestManager_ShutdownCancelsLiveJob(t *testing.T) {
	sched, store, _ := newScheduler(t)

	started := make(chan struct{})
	job, err := sched.Submit(t.Context(), "src/inv1", jobs.KindBuild, func(ctx context.Context, _ func(jobs.Update)) error {
		close(started)
		<-ctx.Done()

		return ctx.Err()
	})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	<-started

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	if err := sched.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}

	final, err := store.Get(t.Context(), job.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if final.State != jobs.StateCancelled && final.State != jobs.StateFailed {
		t.Errorf("post-shutdown job state = %s, want cancelled or failed", final.State)
	}
}
