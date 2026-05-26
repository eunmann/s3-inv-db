package jobs_test

import (
	"context"
	"testing"

	"github.com/eunmann/s3-inv-db/internal/jobs"
	"github.com/rs/zerolog"
)

func TestScheduler_WithLogger(t *testing.T) {
	_, store, _ := newScheduler(t)
	sched := jobs.NewScheduler(store, jobs.NewBus(8), jobs.WithLogger(zerolog.Nop()))
	job, err := sched.Submit(t.Context(), "src/inv1", jobs.KindBuild, func(_ context.Context, _ func(jobs.Update)) error {
		return nil
	})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	_ = waitForState(t, store, job.ID, jobs.StateSucceeded)
}
