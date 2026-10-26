package jobs_test

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/eunmann/s3-inv-db/internal/jobs"
)

// recordingStore keeps every write the Scheduler makes.
type recordingStore struct {
	mu     sync.Mutex
	writes []jobs.Job
}

func (r *recordingStore) Upsert(_ context.Context, j jobs.Job) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.writes = append(r.writes, j)

	return nil
}

func (r *recordingStore) snapshot() []jobs.Job {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]jobs.Job(nil), r.writes...)
}

// TestScheduler_ProgressWritesAreCoalesced: thousands of progress
// reports become a handful of store writes; every transition is still
// written, the terminal state last, and the bus ends on the terminal state.
func TestScheduler_ProgressWritesAreCoalesced(t *testing.T) {
	store := &recordingStore{}
	bus := jobs.NewBus(1 << 14)
	sub := bus.Subscribe()
	defer sub.Close()
	sched := jobs.NewSchedulerForTest(store, bus, 20*time.Millisecond)
	t.Cleanup(func() { _ = sched.Shutdown(context.Background()) })

	const reports = 5000
	job, err := sched.Submit(t.Context(), "src/inv1", jobs.KindBuild, func(_ context.Context, report func(jobs.Update)) error {
		for i := range reports {
			report(jobs.Update{Stage: "build", StageDone: int64(i + 1), StageTotal: reports})
			if i%500 == 0 {
				time.Sleep(10 * time.Millisecond)
			}
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		w := store.snapshot()
		if len(w) > 0 && w[len(w)-1].State.IsTerminal() {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("job never reached a terminal state in the store")
		}
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(60 * time.Millisecond) // a stray progress write would land by now

	writes := store.snapshot()
	if last := writes[len(writes)-1]; last.ID != job.ID || last.State != jobs.StateSucceeded {
		t.Fatalf("last write %s %s, want %s succeeded", last.ID, last.State, job.ID)
	}
	var states []jobs.State
	progressWrites := 0
	for _, w := range writes {
		if len(states) == 0 || states[len(states)-1] != w.State {
			states = append(states, w.State)
		} else {
			progressWrites++
		}
	}
	if want := []jobs.State{jobs.StateQueued, jobs.StateRunning, jobs.StateSucceeded}; !slices.Equal(states, want) {
		t.Fatalf("written states %v, want %v", states, want)
	}
	if progressWrites > 20 {
		t.Fatalf("%d progress writes for %d reports, want them coalesced", progressWrites, reports)
	}

	// The bus coalesces per job: the subscriber, which never read while
	// the job ran, holds exactly one event for it, the terminal state.
	evs, overflowed := sub.Drain()
	if overflowed || len(evs) != 1 || evs[0].Job == nil {
		t.Fatalf("bus held %d events (overflowed %v), want the job's one latest", len(evs), overflowed)
	}
	if got := evs[0].Job; got.ID != job.ID || got.State != jobs.StateSucceeded || got.StageDone != reports {
		t.Fatalf("latest event %s %s done=%d, want %s succeeded done=%d", got.ID, got.State, got.StageDone, job.ID, reports)
	}
}
