package handlers_test

import (
	"testing"

	"github.com/eunmann/s3-inv-db/internal/budget"
	"github.com/eunmann/s3-inv-db/internal/handlers"
	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/internal/jobs"
	"github.com/eunmann/s3-inv-db/internal/templates"
	"github.com/eunmann/s3-inv-db/internal/testsupport/dbtest"
	"github.com/eunmann/s3-inv-db/pkg/pricing"
)

// newWiredHandlers builds a Handlers backed by an in-memory SQLite DB.
// Tests that need to inject their own inventory.Manager pass it via mgr;
// the rest of the required deps are constructed here.
func newWiredHandlers(t *testing.T, mgr *inventory.Manager, opts ...handlers.Option) *handlers.Handlers {
	t.Helper()
	if mgr == nil {
		mgr = inventory.NewManager()
		t.Cleanup(func() { _ = mgr.Close() })
	}
	db := dbtest.OpenMemDB(t)
	invStore, err := inventory.NewStore(db)
	if err != nil {
		t.Fatalf("inventory store: %v", err)
	}
	mgr.SetStore(invStore)
	renderer, err := templates.New()
	if err != nil {
		t.Fatalf("renderer: %v", err)
	}
	jobStore := jobs.NewStore(db)
	jobBus := jobs.NewBus(8)
	jobMgr := jobs.NewScheduler(jobStore, jobBus)
	configStore := inventory.NewConfigStore(db)
	tracker := budget.New(0, 0)

	return handlers.New(
		mgr, renderer, pricing.DefaultUSEast1Prices(),
		jobMgr, jobStore, jobBus, configStore, tracker,
		opts...,
	)
}
