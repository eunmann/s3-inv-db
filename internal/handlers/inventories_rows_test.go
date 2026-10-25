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

// TestInventoriesPage_GroupsShowTheirConfig: each group's auto-load
// form and poll banner come from the config stored under its own
// (source, name); a group with no stored config shows the defaults, and
// a config sharing only the name doesn't leak into it.
func TestInventoriesPage_GroupsShowTheirConfig(t *testing.T) {
	list := []inventory.Inventory{
		{SourceBucket: "src", Name: "inv", Run: "2026-01-01T03-00Z", ManifestKey: "k/inv/manifest.json"},
		{SourceBucket: "src", Name: "plain", Run: "2026-01-01T03-00Z", ManifestKey: "k/plain/manifest.json"},
	}
	h, configs := newWiredHandlersWithConfigs(t, nil,
		handlers.WithDiscoverer(&fakeDiscoverer{listResp: list, bucket: "dst"}),
		handlers.WithLoader(&fakeBuilder{}))
	backoff := time.Date(2026, 1, 2, 4, 5, 6, 0, time.UTC)
	for _, cfg := range []inventory.Config{
		{
			Source: "src", Name: "inv", AutoLoad: true, RetentionCount: 5,
			LastPollError: "access denied", PollBackoffUntil: backoff, PollFailureCount: 1,
		},
		{Source: "other", Name: "plain", AutoLoad: true, RetentionCount: 7, LastPollError: "wrong group"},
	} {
		if err := configs.Upsert(t.Context(), cfg); err != nil {
			t.Fatal(err)
		}
	}

	w := httptest.NewRecorder()
	h.InventoriesPage(w, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/inventories", http.NoBody))
	if w.Code != http.StatusOK {
		t.Fatalf("status %d", w.Code)
	}
	doc := parseHTML(t, w.Body.String())

	configured := doc.Find(`section[aria-labelledby="grp-src-inv"]`)
	if configured.Length() != 1 {
		t.Fatal("group src/inv not rendered")
	}
	if _, checked := configured.Find(`input[type="checkbox"][name="auto_load"]`).Attr("checked"); !checked {
		t.Error("src/inv: auto-load box unchecked, want checked")
	}
	if got, _ := configured.Find(`input[name="retention"]`).Attr("value"); got != "5" {
		t.Errorf("src/inv: retention %q, want 5", got)
	}
	wantBanner := "Last poll failed: access denied (next attempt after " + backoff.Format(time.RFC3339) + ")"
	if text := strings.Join(strings.Fields(configured.Text()), " "); !strings.Contains(text, wantBanner) {
		t.Errorf("src/inv: missing %q in %q", wantBanner, text)
	}

	plain := doc.Find(`section[aria-labelledby="grp-src-plain"]`)
	if plain.Length() != 1 {
		t.Fatal("group src/plain not rendered")
	}
	if _, checked := plain.Find(`input[type="checkbox"][name="auto_load"]`).Attr("checked"); checked {
		t.Error("src/plain: auto-load box checked by another source's config")
	}
	if got, _ := plain.Find(`input[name="retention"]`).Attr("value"); got != "2" {
		t.Errorf("src/plain: retention %q, want the default 2", got)
	}
	if strings.Contains(plain.Text(), "Last poll failed") {
		t.Error("src/plain: shows another source's poll error")
	}
}
