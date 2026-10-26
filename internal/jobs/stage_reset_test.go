package jobs_test

import (
	"context"
	"testing"
	"time"

	"github.com/eunmann/s3-inv-db/internal/jobs"
)

// TestManager_ReportResetsProgressOnStageChange pins the contract that
// quantitative progress (Progress/BytesDone/BytesTotal) is cleared when
// the Stage transitions. Without this, the UI would briefly show
// "Building 10/10 · 0s remaining" (carried over from a just-finished
// Downloading stage) until the next chunk update lands.
func TestManager_ReportResetsProgressOnStageChange(t *testing.T) {
	mgr, store, _ := newScheduler(t)

	step := make(chan struct{})
	done := make(chan struct{})
	job, err := mgr.Submit(t.Context(), "src/inv1", jobs.KindBuild, func(_ context.Context, report func(jobs.Update)) error {
		// Stage 1: downloading, 5/10 done.
		report(jobs.Update{Stage: "downloading", StageDone: 5, StageTotal: 10})
		<-step
		// Stage 2: building, no quantitative progress yet. The previous
		// 5/10 must NOT persist into this stage.
		report(jobs.Update{Stage: "building"})
		close(done)

		return nil
	})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}

	// Snapshot after first stage so we don't race the second update.
	waitForStage(t, store, job.ID, "downloading")
	mid, err := store.Get(t.Context(), job.ID)
	if err != nil {
		t.Fatalf("Get mid: %v", err)
	}
	if mid.StageDone != 5 || mid.StageTotal != 10 {
		t.Errorf("downloading snapshot: BytesDone=%d BytesTotal=%d, want 5/10", mid.StageDone, mid.StageTotal)
	}

	close(step)
	<-done

	// Wait for the building snapshot.
	waitForStage(t, store, job.ID, "building")
	building, err := store.Get(t.Context(), job.ID)
	if err != nil {
		t.Fatalf("Get building: %v", err)
	}
	if building.StageDone != 0 || building.StageTotal != 0 {
		t.Errorf("building snapshot: BytesDone=%d BytesTotal=%d, want 0/0 (stale download progress leaked)",
			building.StageDone, building.StageTotal)
	}
}

func waitForStage(t *testing.T, store *jobs.Store, id jobs.ID, stage string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		j, err := store.Get(t.Context(), id)
		if err == nil && j.Stage == stage {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("job %s never reached stage %s", id, stage)
}

// TestManager_ReportResetsProgressOnStepChange pins that a new step
// within a stage resets the counter and restarts the clock its ETA
// extrapolates from, and that both are persisted for the UI.
func TestManager_ReportResetsProgressOnStepChange(t *testing.T) {
	mgr, store, _ := newScheduler(t)

	next := make(chan struct{})
	done := make(chan struct{})
	job, err := mgr.Submit(t.Context(), "src/inv1", jobs.KindBuild, func(_ context.Context, report func(jobs.Update)) error {
		report(jobs.Update{Stage: "finalizing", Step: "Writing hash slots", StageDone: 3, StageTotal: 10})
		<-next
		report(jobs.Update{Stage: "finalizing", Step: "Checksumming files"})
		close(done)
		<-next

		return nil
	})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}

	first := waitForStep(t, store, job.ID, "Writing hash slots")
	if first.StageDone != 3 || first.StageTotal != 10 || first.StepStartedAt.IsZero() {
		t.Errorf("first step: %d/%d started %v, want 3/10 and a start time", first.StageDone, first.StageTotal, first.StepStartedAt)
	}
	time.Sleep(1100 * time.Millisecond) // the store keeps whole seconds
	next <- struct{}{}
	<-done
	second := waitForStep(t, store, job.ID, "Checksumming files")
	if second.StageDone != 0 || second.StageTotal != 0 {
		t.Errorf("second step: %d/%d, want 0/0 (the previous step's counter leaked)", second.StageDone, second.StageTotal)
	}
	if !second.StepStartedAt.After(first.StepStartedAt) {
		t.Errorf("second step started %v, not after the first's %v", second.StepStartedAt, first.StepStartedAt)
	}
	close(next)
}

func waitForStep(t *testing.T, store *jobs.Store, id jobs.ID, step string) jobs.Job {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		j, err := store.Get(t.Context(), id)
		if err == nil && j.Step == step {
			return j
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("job %s never reached step %s", id, step)

	return jobs.Job{}
}
