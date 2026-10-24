package handlers_test

import (
	"testing"

	"github.com/eunmann/s3-inv-db/internal/budget"
	"github.com/eunmann/s3-inv-db/internal/dbtest"
	"github.com/eunmann/s3-inv-db/internal/handlers"
	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/internal/jobs"
	"github.com/eunmann/s3-inv-db/internal/templates"
	"github.com/eunmann/s3-inv-db/pkg/pricing"
)

// newWiredHandlers builds a Handlers backed by an in-memory SQLite DB.
// Tests that need to inject their own inventory.Catalog pass it via mgr;
// the rest of the required deps are constructed here.
func newWiredHandlers(t *testing.T, mgr *inventory.Catalog, opts ...handlers.Option) *handlers.Handlers {
	t.Helper()
	h, _ := newWiredHandlersWithConfigs(t, mgr, opts...)

	return h
}

// newWiredHandlersWithConfigs is newWiredHandlers that also returns the
// config store, for tests asserting on persisted configuration.
func newWiredHandlersWithConfigs(tb testing.TB, mgr *inventory.Catalog, opts ...handlers.Option) (*handlers.Handlers, *inventory.ConfigStore) {
	tb.Helper()
	db := dbtest.OpenMemDB(tb)
	invStore, err := inventory.NewStore(db)
	if err != nil {
		tb.Fatalf("inventory store: %v", err)
	}
	if mgr == nil {
		mgr = inventory.NewCatalog(invStore)
		tb.Cleanup(func() { _ = mgr.Close() })
	} else {
		mgr.AttachStoreForTest(invStore)
	}
	renderer, err := templates.New()
	if err != nil {
		tb.Fatalf("renderer: %v", err)
	}
	jobStore := jobs.NewStore(db)
	jobBus := jobs.NewBus(8)
	jobMgr := jobs.NewScheduler(jobStore, jobBus)
	configStore := inventory.NewConfigStore(db)
	tracker := budget.New(0, 0, mgr.LoadedBytes)

	return handlers.New(
		mgr, renderer, pricing.DefaultUSEast1Prices(),
		handlers.Deps{
			JobMgr:      jobMgr,
			JobStore:    jobStore,
			JobBus:      jobBus,
			ConfigStore: configStore,
			Tracker:     tracker,
		},
		opts...,
	), configStore
}
