package handlers_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/eunmann/s3-inv-db/internal/handlers"
	"github.com/eunmann/s3-inv-db/internal/jobs"
	"github.com/eunmann/s3-inv-db/pkg/humanfmt"
)

// drawerPage is an opened drawer, parsed and as sent.
type drawerPage struct {
	doc  *goquery.Document
	body string
}

func openDrawer(t *testing.T, h *handlers.Handlers, run string) drawerPage {
	t.Helper()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/partials/drawer/src/inv/"+run, http.NoBody)
	req = chiCtxWithParams(req, "src", "src", "id", "inv", "run", run)
	w := httptest.NewRecorder()
	h.RunDrawer(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status %d", w.Code)
	}

	return drawerPage{doc: parseHTMLFragment(t, w.Body.String()), body: w.Body.String()}
}

// TestRunDrawer_LeadsWithWhatTheRunHolds: a loaded run's drawer opens on
// its totals, cost and storage classes and the run's actions; where it
// came from follows; the build's details are folded away.
func TestRunDrawer_LeadsWithWhatTheRunHolds(t *testing.T) {
	h := newOverviewHandlers(t)
	page := openDrawer(t, h, overviewRuns[1])
	doc, body := page.doc, page.body

	holds := doc.Find(`section[aria-label="What this run holds"]`)
	text := strings.Join(strings.Fields(holds.Text()), " ")
	for _, want := range []string{"Objects 2", "Size " + humanfmt.BytesUint64(3*gib), "Est. cost per month $", "Prefixes", "Standard"} {
		if !strings.Contains(text, want) {
			t.Errorf("summary lacks %q: %q", want, text)
		}
	}
	if !strings.Contains(doc.Find("#run-drawer-title").Text(), "2026-10-26 03:00 UTC") {
		t.Errorf("title %q doesn't name the run", doc.Find("#run-drawer-title").Text())
	}
	actions := doc.Find(`section[aria-label="Actions"]`)
	if got := actions.Find(`a[href^="/browse"]`).AttrOr("href", ""); got != "/browse?inventory_id=src%2Finv%2F2026-10-26T03-00Z" {
		t.Errorf("Browse link %q", got)
	}
	if got := strings.TrimSpace(actions.Find(`a[href^="/compare"]`).Text()); got != "Compare with previous" {
		t.Errorf("compare link says %q", got)
	}
	if !strings.HasPrefix(actions.Find("#drawer-unload").AttrOr("hx-confirm", ""), "Unload this run?") {
		t.Error("drawer Unload has no confirmation")
	}
	source := strings.Join(strings.Fields(doc.Find(`section[aria-labelledby="drawer-source"]`).Text()), " ")
	if !strings.Contains(source, "src/inv/2026-10-26T03-00Z/manifest.json") || !strings.Contains(source, "CSV") || !strings.Contains(source, "80") {
		t.Errorf("source section %q lacks the manifest, format or file count", source)
	}
	if strings.Index(body, "What this run holds") > strings.Index(body, `id="drawer-source"`) {
		t.Error("the source comes before what the run holds")
	}
	if _, open := doc.Find("details#drawer-build").Attr("open"); open {
		t.Error("build details open for a finished build")
	}
	if doc.Find("#drawer-close").Length() != 1 {
		t.Error("close button has no stable id")
	}
}

// TestRunDrawer_FailedBuildSaysWhyOnce: a failed run's drawer says why
// in a sentence and keeps the raw error, once, under Technical details.
func TestRunDrawer_FailedBuildSaysWhyOnce(t *testing.T) {
	h := newOverviewHandlers(t)
	page := openDrawer(t, h, overviewRuns[3])
	doc, body := page.doc, page.body
	if !strings.Contains(doc.Text(), "An inventory data file is missing from S3: s3://dst/src/inv/data/chunk-0001.csv.gz.") {
		t.Error("drawer lacks the failure summary")
	}
	if n := strings.Count(body, "RD3RNTQ2KIO4OQZ6"); n != 1 {
		t.Errorf("raw error appears %d times, want once", n)
	}
	tech := doc.Find("details#drawer-technical")
	if !strings.Contains(tech.Find("summary").Text(), "Technical details") || !strings.Contains(tech.Text(), "RD3RNTQ2KIO4OQZ6") {
		t.Error("raw error isn't inside Technical details")
	}
	if retry := doc.Find("#drawer-retry"); retry.AttrOr("hx-post", "") != "/api/jobs/job-failed/retry" || retry.AttrOr("hx-disabled-elt", "") != "this" {
		t.Errorf("retry button %q", retry.AttrOr("hx-post", ""))
	}
}

// TestRunDrawer_LiveBuildShowsDetails: while a build runs its details
// are open and Cancel keeps one id across refreshes.
func TestRunDrawer_LiveBuildShowsDetails(t *testing.T) {
	h := newOverviewHandlers(t)
	run := overviewRuns[0]
	if err := h.ManagerForTest().Register(t.Context(), runID(run), "inv", "s3://dst/manifest.json"); err != nil {
		t.Fatal(err)
	}
	job := jobs.Job{ID: "job-live", InventoryID: runID(run), Kind: jobs.KindBuild, State: jobs.StateRunning, Stage: "downloading", StartedAt: time.Now()}
	if err := h.JobStoreForTest().Upsert(t.Context(), job); err != nil {
		t.Fatal(err)
	}
	doc := openDrawer(t, h, run).doc
	if _, open := doc.Find("details#drawer-build").Attr("open"); !open {
		t.Error("build details closed during a build")
	}
	if doc.Find("#drawer-cancel").AttrOr("hx-post", "") != "/api/jobs/job-live/cancel" {
		t.Error("live build has no Cancel with a stable id")
	}
}
