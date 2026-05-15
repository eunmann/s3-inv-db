package jobs_test

import (
	"context"
	"testing"

	"github.com/eunmann/s3-inv-db/internal/jobs"
	"github.com/rs/zerolog"
)

func TestManager_SetLogger(t *testing.T) {
	_, store, _ := newManager(t)
	mgr := jobs.NewManager(store, jobs.NewBus(8))
	mgr.SetLogger(zerolog.Nop())
	job, err := mgr.Submit(t.Context(), "src/inv1", jobs.KindBuild, func(_ context.Context, _ func(jobs.Update)) error {
		return nil
	})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	_ = waitForState(t, store, job.ID, jobs.StateSucceeded)
}
