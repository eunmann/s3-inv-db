package handlers_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/eunmann/s3-inv-db/internal/handlers"
	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/internal/jobs"
)

// BenchmarkInventoriesPage renders the inventories page over 200
// configurations of 10 runs each, every run with two past jobs: the
// per-run lookups dominate it.
func BenchmarkInventoriesPage(b *testing.B) {
	const configs, runs = 200, 10
	var list []inventory.Inventory
	for c := range configs {
		for r := range runs {
			run := time.Date(2026, 1, 1+r, 3, 0, 0, 0, time.UTC).Format("2006-01-02T15-04Z")
			list = append(list, inventory.Inventory{
				SourceBucket: "src", Name: fmt.Sprintf("inv%03d", c), Run: run,
				ManifestKey: fmt.Sprintf("k/inv%03d/%s/manifest.json", c, run),
			})
		}
	}
	mgr := inventory.NewCatalog(nil)
	b.Cleanup(func() { _ = mgr.Close() })
	h, _ := newWiredHandlersWithConfigs(b, mgr,
		handlers.WithDiscoverer(&fakeDiscoverer{listResp: list, bucket: "dst"}),
		handlers.WithLoader(&fakeBuilder{}))
	store := h.JobStoreForTest()
	for i, inv := range list {
		if err := mgr.Register(b.Context(), inv.CompositeID(), inv.Name, b.TempDir()); err != nil {
			b.Fatal(err)
		}
		for k := range 2 {
			j := jobs.Job{
				ID: jobs.ID(fmt.Sprintf("job-%d-%d", i, k)), InventoryID: inv.CompositeID(),
				Kind: jobs.KindBuild, State: jobs.StateSucceeded,
				StartedAt: time.Unix(int64(1000+k), 0), FinishedAt: time.Unix(int64(2000+k), 0),
			}
			if err := store.Upsert(b.Context(), j); err != nil {
				b.Fatal(err)
			}
		}
	}
	b.ReportAllocs()
	for b.Loop() {
		w := httptest.NewRecorder()
		h.InventoriesPage(w, httptest.NewRequestWithContext(b.Context(), http.MethodGet, "/inventories", http.NoBody))
		if w.Code != http.StatusOK {
			b.Fatalf("status %d", w.Code)
		}
	}
}
