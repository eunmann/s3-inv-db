package handlers_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/eunmann/s3-inv-db/internal/handlers"
	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/internal/jobs"
)

// TestInventoriesPage_RowsShowStateAndRetry: each run's row shows its
// state and offers Retry exactly when its latest job failed, aborted or
// was cancelled — the per-row values the handler precomputes for the
// template.
func TestInventoriesPage_RowsShowStateAndRetry(t *testing.T) {
	runs := map[string]jobs.State{
		"2026-01-01T03-00Z": jobs.StateFailed,
		"2026-01-02T03-00Z": jobs.StateSucceeded,
		"2026-01-03T03-00Z": jobs.StateCancelled,
	}
	list := make([]inventory.Inventory, 0, len(runs))
	for run := range runs {
		list = append(list, inventory.Inventory{SourceBucket: "src", Name: "inv", Run: run, ManifestKey: "k/" + run + "/manifest.json"})
	}
	mgr := inventory.NewCatalog(nil)
	t.Cleanup(func() { _ = mgr.Close() })
	h := newWiredHandlers(t, mgr,
		handlers.WithDiscoverer(&fakeDiscoverer{listResp: list, bucket: "dst"}),
		handlers.WithLoader(&fakeBuilder{}))
	for _, inv := range list {
		if err := mgr.Register(t.Context(), inv.CompositeID(), inv.Name, t.TempDir()); err != nil {
			t.Fatal(err)
		}
		j := jobs.Job{ID: jobs.ID("job-" + inv.Run), InventoryID: inv.CompositeID(), Kind: jobs.KindBuild, State: runs[inv.Run], FinishedAt: time.Unix(10, 0)}
		if err := h.JobStoreForTest().Upsert(t.Context(), j); err != nil {
			t.Fatal(err)
		}
	}
	w := httptest.NewRecorder()
	h.InventoriesPage(w, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/inventories", http.NoBody))
	if w.Code != http.StatusOK {
		t.Fatalf("status %d", w.Code)
	}
	doc := parseHTML(t, w.Body.String())
	for run, state := range runs {
		row := doc.Find("#discovered-row-src-inv-" + run)
		if row.Length() != 1 {
			t.Fatalf("row for %s not rendered", run)
		}
		html, _ := row.Html()
		text := row.Text()
		if !strings.Contains(text, "Not loaded") || !strings.Contains(html, "border-yellow-400") {
			t.Errorf("%s: row missing its not-loaded label or border", run)
		}
		wantRetry := state != jobs.StateSucceeded
		if got := strings.Contains(text, "Retry"); got != wantRetry {
			t.Errorf("%s (latest job %s): Retry shown %v, want %v", run, state, got, wantRetry)
		}
	}
}
