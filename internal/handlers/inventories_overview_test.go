package handlers_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/eunmann/s3-inv-db/internal/handlers"
	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/internal/jobs"
	"github.com/eunmann/s3-inv-db/pkg/humanfmt"
	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

const missingChunkErr = "gated load src/inv/2026-10-24T03-00Z: load src/inv/2026-10-24T03-00Z: build index: run pipeline: ingest phase: chunk worker: chunk 1: download object: " +
	"download s3://dst/src/inv/data/chunk-0001.csv.gz: operation error S3: GetObject, https response error StatusCode: 404, RequestID: RD3RNTQ2KIO4OQZ6, HostID: Cx6k=, NoSuchKey: The specified key does not exist."

// overviewRuns are one configuration's six runs, newest first: the
// latest not loaded, two loaded (1 GiB, then 3 GiB), one whose build
// failed and two older ones never loaded.
var overviewRuns = []string{
	"2026-10-27T03-00Z", // latest, not loaded
	"2026-10-26T03-00Z", // loaded, 3 GiB
	"2026-10-25T03-00Z", // not loaded
	"2026-10-24T03-00Z", // build failed
	"2026-10-23T03-00Z", // loaded, 1 GiB
	"2026-10-22T03-00Z", // not loaded
}

func runID(run string) inventory.ID { return inventory.ID("src/inv/" + run) }

// extraRun is a run of another configuration, loaded when it has
// objects.
type extraRun struct {
	inv  inventory.Inventory
	objs []costObj
}

// newOverviewHandlers wires handlers over overviewRuns with the two
// loaded runs' indexes built and the failed run's job recorded, plus
// any extra runs.
func newOverviewHandlers(t *testing.T, extra ...extraRun) *handlers.Handlers {
	t.Helper()
	list := make([]inventory.Inventory, 0, len(overviewRuns)+len(extra))
	for _, run := range overviewRuns {
		list = append(list, inventory.Inventory{
			SourceBucket: "src", Name: "inv", Run: run,
			ManifestKey: "src/inv/" + run + "/manifest.json", FileFormat: "CSV", FileCount: 80, TotalBytes: 4096,
		})
	}
	mgr := inventory.NewCatalog(nil)
	t.Cleanup(func() { _ = mgr.Close() })
	registerCostIndex(t, mgr, runID(overviewRuns[4]), []costObj{{"a/x", gib, tiers.Standard}})
	registerCostIndex(t, mgr, runID(overviewRuns[1]), []costObj{{"a/x", gib, tiers.Standard}, {"b/y", 2 * gib, tiers.Standard}})
	for i := range extra {
		e := &extra[i]
		list = append(list, e.inv)
		if len(e.objs) > 0 {
			registerCostIndex(t, mgr, e.inv.CompositeID(), e.objs)
		}
	}
	h := newWiredHandlers(t, mgr,
		handlers.WithDiscoverer(&fakeDiscoverer{listResp: list, bucket: "dst"}),
		handlers.WithLoader(&fakeBuilder{}))
	failed := runID(overviewRuns[3])
	if err := mgr.Register(t.Context(), failed, "inv", "s3://dst/manifest.json"); err != nil {
		t.Fatal(err)
	}
	job := jobs.Job{
		ID: "job-failed", InventoryID: failed, Kind: jobs.KindBuild, State: jobs.StateFailed, Error: missingChunkErr,
		StartedAt: time.Unix(5, 0), FinishedAt: time.Unix(10, 0),
		Stages: []jobs.StageRecord{{Name: "downloading", StartedAt: time.Unix(5, 0), Duration: 5 * time.Second, Err: missingChunkErr}},
	}
	if err := h.JobStoreForTest().Upsert(t.Context(), job); err != nil {
		t.Fatal(err)
	}

	return h
}

func rowSel(run string) string { return "#discovered-row-src-inv-" + run }

// assertTwoActionLines: a loaded run's actions are two lines, Browse
// and Compare on the first, Pin/Unpin and Unload on the second, styled
// alike.
func assertTwoActionLines(t *testing.T, scope *goquery.Selection, pinSel, unloadSel string) {
	t.Helper()
	browse := scope.Find(`a[href^="/browse"]`)
	pin, unload := scope.Find(pinSel), scope.Find(unloadSel)
	if browse.Length() != 1 || pin.Length() != 1 || unload.Length() != 1 {
		t.Fatalf("actions: %d Browse, %d pin, %d unload", browse.Length(), pin.Length(), unload.Length())
	}
	if !browse.Parent().IsSelection(scope.Find(`a[href^="/compare"]`).Parent()) {
		t.Error("Browse and Compare aren't on one line")
	}
	if !pin.Parent().IsSelection(unload.Parent()) || pin.Parent().IsSelection(browse.Parent()) {
		t.Error("Pin and Unload aren't together on their own line")
	}
	if pin.AttrOr("class", "") != unload.AttrOr("class", "") {
		t.Errorf("Pin and Unload styled differently: %q vs %q", pin.AttrOr("class", ""), unload.AttrOr("class", ""))
	}
}

// TestInventoriesPage_ShowsWhatEachRunHolds: a configuration's card
// leads with its newest loaded run's totals; its rows are the latest
// run and every loaded or failed one, the rest folded away; a loaded
// row says what it holds and offers Browse, Compare with the loaded run
// before it, Pin and a confirmed Unload; a failed row says why in a
// sentence. Build plumbing (manifest keys, chunk counts) stays out.
func TestInventoriesPage_ShowsWhatEachRunHolds(t *testing.T) {
	h := newOverviewHandlers(t)
	w := httptest.NewRecorder()
	h.InventoriesPage(w, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/inventories", http.NoBody))
	if w.Code != http.StatusOK {
		t.Fatalf("status %d", w.Code)
	}
	doc := parseHTML(t, w.Body.String())
	sec := doc.Find(`section[aria-labelledby="grp-src-inv"]`)

	shown := sec.Find("table").First().Find("tbody")
	var shownRuns []string
	shown.Each(func(_ int, s *goquery.Selection) { shownRuns = append(shownRuns, s.AttrOr("data-live-inv", "")) })
	want := []string{string(runID(overviewRuns[0])), string(runID(overviewRuns[1])), string(runID(overviewRuns[3])), string(runID(overviewRuns[4]))}
	if strings.Join(shownRuns, ",") != strings.Join(want, ",") {
		t.Errorf("shown rows %v, want %v", shownRuns, want)
	}
	older := sec.Find("details")
	if got := strings.TrimSpace(older.Find("summary").Text()); got != "Show 2 older runs" {
		t.Errorf("older runs summary %q", got)
	}
	if older.Find("tbody").Length() != 2 {
		t.Errorf("older runs = %d, want 2", older.Find("tbody").Length())
	}

	if n := sec.Find(`span:contains("Latest")`).Length(); n != 1 {
		t.Errorf("Latest marks = %d, want 1", n)
	}
	if !strings.Contains(doc.Find(rowSel(overviewRuns[0])).Text(), "Latest") {
		t.Error("the newest run isn't marked Latest")
	}

	newer := doc.Find(rowSel(overviewRuns[1]))
	newerLine := "2 objects · " + humanfmt.BytesUint64(3*gib) + " · est. $"
	if !strings.Contains(newer.Text(), newerLine) || !strings.Contains(newer.Text(), "/mo") {
		t.Errorf("loaded row lacks %q…/mo: %q", newerLine, strings.Join(strings.Fields(newer.Text()), " "))
	}
	summary := strings.Join(strings.Fields(sec.Find("#grp-sum-src_2finv").Text()), " ")
	if !strings.Contains(summary, newerLine) || !strings.Contains(summary, "2026-10-26 03:00 UTC") {
		t.Errorf("header summary %q doesn't name the newest loaded run", summary)
	}
	if got := doc.Find("[data-live-any][hx-select-oob]").AttrOr("hx-select-oob", ""); got != "#grp-sum-src_2finv" {
		t.Errorf("header refresher selects %q", got)
	}

	compare := newer.Find(`a[href^="/compare"]`)
	if got, want := compare.AttrOr("href", ""), "/compare?from=src%2Finv%2F2026-10-23T03-00Z&to=src%2Finv%2F2026-10-26T03-00Z"; got != want || strings.TrimSpace(compare.Text()) != "Compare with previous" {
		t.Errorf("newer loaded run's compare link %q %q, want %q Compare with previous", got, compare.Text(), want)
	}
	if got := strings.TrimSpace(doc.Find(rowSel(overviewRuns[4])).Find(`a[href^="/compare"]`).Text()); got != "Compare" {
		t.Errorf("oldest loaded run's compare link says %q, want Compare", got)
	}

	newerID := handlers.DomIDForTest(string(runID(overviewRuns[1])))
	assertTwoActionLines(t, newer, "#run-"+newerID+"-pin", "#run-"+newerID+"-unload")
	unload := newer.Find("#run-" + newerID + "-unload")
	if !strings.HasPrefix(unload.AttrOr("hx-confirm", ""), "Unload this run?") {
		t.Errorf("Unload has no confirmation: %q", unload.AttrOr("hx-confirm", ""))
	}
	for _, btn := range []*goquery.Selection{unload, newer.Find("#run-" + newerID + "-pin"), doc.Find(rowSel(overviewRuns[0])).Find("button")} {
		if btn.Length() != 1 || btn.AttrOr("hx-sync", "") != "closest tbody:replace" || btn.AttrOr("hx-disabled-elt", "") != "this" {
			t.Errorf("action button %s: hx-sync %q, hx-disabled-elt %q", btn.AttrOr("id", "?"), btn.AttrOr("hx-sync", ""), btn.AttrOr("hx-disabled-elt", ""))
		}
	}
	if got := newer.AttrOr("hx-sync", ""); got != "this:drop" {
		t.Errorf("row background refresh hx-sync %q, want this:drop", got)
	}
	// The global error toast skips background refreshes, never a click.
	if _, ok := newer.Attr("data-background-refresh"); !ok {
		t.Error("row's live refresh isn't marked a background refresh")
	}
	if doc.Find("button[data-background-refresh]").Length() != 0 {
		t.Error("a user-action button is marked a background refresh")
	}
	if doc.Find("[hx-select-oob][data-background-refresh]").Length() != 1 {
		t.Error("header refresher isn't marked a background refresh")
	}

	failed := doc.Find(rowSel(overviewRuns[3])).Text()
	if !strings.Contains(failed, "An inventory data file is missing from S3: s3://dst/src/inv/data/chunk-0001.csv.gz.") || !strings.Contains(failed, "Retry") {
		t.Errorf("failed row lacks its summary or Retry: %q", strings.Join(strings.Fields(failed), " "))
	}
	page := doc.Find("#inventories-live-root").Text()
	for _, gone := range []string{"RequestID", "manifest.json", "80 files", "chunks", "cache"} {
		if strings.Contains(page, gone) {
			t.Errorf("page shows %q", gone)
		}
	}
	if doc.Find("thead [title]").Length() != 0 || doc.Find("thead [aria-label]").Length() != 0 {
		t.Error("column headers still carry info icons")
	}
}

// TestDiscoveredRowPartial_KnowsItsPlace: a single row re-rendered by
// live.js keeps its Latest mark and its Compare with previous link.
func TestDiscoveredRowPartial_KnowsItsPlace(t *testing.T) {
	h := newOverviewHandlers(t)
	// The server's background refresher fills the discovery snapshot
	// rows are rendered from; here the page's first render does.
	h.InventoriesPage(httptest.NewRecorder(), httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/inventories", http.NoBody))
	render := func(run string) *goquery.Document {
		t.Helper()
		req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/partials/discovered/src/inv/"+run, http.NoBody)
		req = chiCtxWithParams(req, "src", "src", "id", "inv", "run", run)
		w := httptest.NewRecorder()
		h.DiscoveredRowPartial(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("status %d", w.Code)
		}

		return parseHTMLFragment(t, w.Body.String())
	}
	if !strings.Contains(render(overviewRuns[0]).Text(), "Latest") {
		t.Error("latest run's row lost its Latest mark")
	}
	newer := render(overviewRuns[1])
	if strings.Contains(newer.Text(), "Latest") {
		t.Error("an older run is marked Latest")
	}
	if got := newer.Find(`a[href^="/compare"]`).AttrOr("href", ""); !strings.Contains(got, "from=src%2Finv%2F2026-10-23T03-00Z") {
		t.Errorf("compare link %q doesn't diff against the previous loaded run", got)
	}
}
