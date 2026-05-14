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
	mgr, store, _ := newManager(t)

	step := make(chan struct{})
	done := make(chan struct{})
	job, err := mgr.Submit("src/inv1", jobs.KindBuild, func(_ context.Context, report func(jobs.Update)) error {
		// Stage 1: downloading, 5/10 done.
		report(jobs.Update{Stage: "downloading", BytesDone: 5, BytesTotal: 10})
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
	mid, err := store.Get(job.ID)
	if err != nil {
		t.Fatalf("Get mid: %v", err)
	}
	if mid.BytesDone != 5 || mid.BytesTotal != 10 {
		t.Errorf("downloading snapshot: BytesDone=%d BytesTotal=%d, want 5/10", mid.BytesDone, mid.BytesTotal)
	}

	close(step)
	<-done

	// Wait for the building snapshot.
	waitForStage(t, store, job.ID, "building")
	building, err := store.Get(job.ID)
	if err != nil {
		t.Fatalf("Get building: %v", err)
	}
	if building.BytesDone != 0 || building.BytesTotal != 0 {
		t.Errorf("building snapshot: BytesDone=%d BytesTotal=%d, want 0/0 (stale download progress leaked)",
			building.BytesDone, building.BytesTotal)
	}
}

func waitForStage(t *testing.T, store *jobs.Store, id jobs.ID, stage string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		j, err := store.Get(id)
		if err == nil && j.Stage == stage {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("job %s never reached stage %s", id, stage)
}
