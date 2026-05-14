package jobs_test

import (
	"context"
	"testing"

	"github.com/eunmann/s3-inv-db/internal/jobs"
	"github.com/rs/zerolog"
)

// TestManager_SetLogger covers the trivial setter. The interesting
// invariant is that the manager survives an explicit logger swap and
// can still drive a job to a terminal state via that logger path.
func TestManager_SetLogger(t *testing.T) {
	_, store, _ := newManager(t) // helper from manager_test.go
	mgr := jobs.NewManager(store, jobs.NewBus(8))
	mgr.SetLogger(zerolog.Nop())
	job, err := mgr.Submit("src/inv1", jobs.KindBuild, func(_ context.Context, _ func(jobs.Update)) error {
		return nil
	})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	_ = waitForState(t, store, job.ID, jobs.StateSucceeded)
}
