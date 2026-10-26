package handlers_test

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/eunmann/s3-inv-db/internal/handlers"
	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/internal/jobs"
	"github.com/eunmann/s3-inv-db/internal/templates"
	"github.com/eunmann/s3-inv-db/pkg/humanfmt"
	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

var (
	hourlyRun = inventory.Inventory{SourceBucket: "logs", Name: "hourly", Run: "2026-10-19T03-00Z", ManifestKey: "logs/hourly/2026-10-19T03-00Z/manifest.json"}
	logsRun   = inventory.Inventory{SourceBucket: "logs", Name: "daily", Run: "2026-10-20T03-00Z", ManifestKey: "logs/daily/2026-10-20T03-00Z/manifest.json"}
	unusedRun = inventory.Inventory{SourceBucket: "new", Name: "weekly", Run: "2026-10-21T03-00Z", ManifestKey: "new/weekly/2026-10-21T03-00Z/manifest.json"}
)

func renderDashboard(t *testing.T, h *handlers.Handlers) *goquery.Document {
	t.Helper()
	w := httptest.NewRecorder()
	h.Dashboard(w, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", http.NoBody))
	if w.Code != http.StatusOK {
		t.Fatalf("status %d: %s", w.Code, w.Body.String())
	}

	return parseHTML(t, w.Body.String())
}

func squash(s string) string { return strings.Join(strings.Fields(s), " ") }

// TestDashboard_LeadsWithEachBucket: one entry per configuration, bucket
// first, with its newest loaded run's objects, size and cost, the change
// since the loaded run before it, Browse and Compare with previous; a
// newer run is offered for loading. The total adds each configuration's
// newest loaded run once, and the system's state (failed builds named
// and linked) comes after.
func TestDashboard_LeadsWithEachBucket(t *testing.T) {
	h := newOverviewHandlers(t,
		extraRun{inv: logsRun, objs: []costObj{{"z/q", 5 * gib, tiers.Standard}}},
		extraRun{inv: hourlyRun, objs: []costObj{{"h/q", gib, tiers.Standard}}},
		extraRun{inv: unusedRun})
	doc := renderDashboard(t, h)

	var buckets []string
	doc.Find("article h2").Each(func(_ int, s *goquery.Selection) { buckets = append(buckets, squash(s.Text())) })
	if got := strings.Join(buckets, "|"); got != "logs daily|logs hourly|new weekly|src inv" {
		t.Fatalf("configurations %q, want logs daily|logs hourly|new weekly|src inv", got)
	}

	src := doc.Find("article").Eq(3)
	text := squash(src.Text())
	for _, want := range []string{
		"Objects 2", "Size " + humanfmt.BytesUint64(3*gib), "Est. cost $",
		"+" + humanfmt.BytesUint64(2*gib) + " · +1 object · +$", "/mo since Oct 23",
		"A newer run from Oct 27 is available.",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("src card lacks %q: %q", want, text)
		}
	}
	// On a phone the three values sit two per row and never break
	// ("108.89" over "TiB").
	metrics := src.Find("dl")
	if class := " " + metrics.AttrOr("class", "") + " "; !strings.Contains(class, " grid-cols-2 ") || !strings.Contains(class, " sm:grid-cols-3 ") {
		t.Errorf("metrics grid %q", class)
	}
	metrics.Find("dd").Each(func(_ int, dd *goquery.Selection) {
		if !strings.Contains(" "+dd.AttrOr("class", "")+" ", " whitespace-nowrap ") {
			t.Errorf("metric %q can wrap", squash(dd.Text()))
		}
	})
	if got := src.Find(`a:contains("Browse")`).AttrOr("href", ""); got != "/browse?inventory_id=src%2Finv%2F2026-10-26T03-00Z" {
		t.Errorf("Browse href %q", got)
	}
	if got := src.Find(`a:contains("Compare with previous")`).AttrOr("href", ""); got != "/compare?from=src%2Finv%2F2026-10-23T03-00Z&to=src%2Finv%2F2026-10-26T03-00Z" {
		t.Errorf("Compare with previous href %q", got)
	}
	load := src.Find("button")
	if load.AttrOr("hx-post", "") != "/partials/discovered/src/inv/2026-10-27T03-00Z/load" || load.AttrOr("hx-disabled-elt", "") != "this" || load.AttrOr("hx-swap", "") != "none" {
		t.Errorf("newer-run Load button: %q", load.AttrOr("hx-post", ""))
	}

	unused := squash(doc.Find("article").Eq(2).Text())
	if !strings.Contains(unused, "Not loaded") || !strings.Contains(unused, "Load latest run") {
		t.Errorf("unloaded configuration %q lacks Not loaded / Load latest run", unused)
	}

	total := squash(doc.Find(`p:contains("Latest loaded runs of")`).Text())
	if want := "Latest loaded runs of 3 inventories in 2 buckets: 4 objects · " + humanfmt.BytesUint64(9*gib); !strings.HasPrefix(total, want) {
		t.Errorf("total %q, want %q… (each configuration's newest loaded run once)", total, want)
	}

	alert := doc.Find(`[role="alert"]`)
	link := alert.Find("a").First()
	if !strings.HasPrefix(link.AttrOr("href", ""), "/inventories#run=") || !strings.Contains(link.AttrOr("href", ""), "2026-10-24T03-00Z") {
		t.Errorf("failed build link %q", link.AttrOr("href", ""))
	}
	if !strings.Contains(alert.Text(), "An inventory data file is missing from S3") {
		t.Errorf("failed build alert lacks its summary: %q", squash(alert.Text()))
	}
	status := squash(doc.Find(`section[aria-labelledby="status-heading"]`).Text())
	for _, want := range []string{"Runs discovered 9", "Builds in progress 0", "Failed builds 1", "Auto-load Off"} {
		if !strings.Contains(status, want) {
			t.Errorf("status lacks %q: %q", want, status)
		}
	}
	if strings.Contains(doc.Text(), "Manifest data") {
		t.Error("dashboard still shows the manifest data card")
	}
	if doc.Find(`#dashboard-live a[href="/browse"]`).Length() != 1 {
		t.Error("no Open Browse with runs loaded")
	}

	live := doc.Find("#dashboard-live")
	if live.AttrOr("hx-disinherit", "") != "*" || live.AttrOr("hx-select", "") != "#dashboard-live" {
		t.Errorf("live block hx-disinherit %q", live.AttrOr("hx-disinherit", ""))
	}
	if _, ok := live.Attr("data-background-refresh"); !ok || doc.Find("button[data-background-refresh]").Length() != 0 {
		t.Error("only the live block is a background refresh")
	}
	doc.Find("dt, dd").Each(func(_ int, s *goquery.Selection) {
		if s.Closest("dl").Length() == 0 {
			t.Errorf("%s %q outside a dl", goquery.NodeName(s), squash(s.Text()))
		}
	})
}

// TestDashboard_StartHere: with nothing loaded the dashboard says what
// loading gives and how long it takes, offers each configuration's Load,
// and shows no Open Browse rather than a disabled one.
func TestDashboard_StartHere(t *testing.T) {
	h := newWiredHandlers(t, nil,
		handlers.WithDiscoverer(&fakeDiscoverer{listResp: []inventory.Inventory{unusedRun, logsRun}, bucket: "dst"}),
		handlers.WithLoader(&fakeBuilder{}))
	doc := renderDashboard(t, h)
	if !strings.Contains(squash(doc.Text()), "Start here Load a run to see its size, cost and prefix tree. Loading builds a local index; it takes seconds to minutes.") {
		t.Error("no Start here explanation")
	}
	if n := doc.Find(`button:contains("Load latest run")`).Length(); n != 2 {
		t.Errorf("Load latest run buttons = %d, want 2", n)
	}
	if doc.Find(`#dashboard-live a[href="/browse"]`).Length() != 0 || doc.Find(`#dashboard-live [aria-disabled]`).Length() != 0 {
		t.Error("Open Browse shown, or something disabled, with nothing loaded")
	}
	if doc.Find(`p:contains("Latest loaded runs of")`).Length() != 0 {
		t.Error("a total with nothing loaded")
	}
}

// TestDashboard_ShowsBuildProgress: a configuration whose run is
// building shows live.js's progress hooks for that run.
func TestDashboard_ShowsBuildProgress(t *testing.T) {
	mgr := inventory.NewCatalog(nil)
	t.Cleanup(func() { _ = mgr.Close() })
	h := newWiredHandlers(t, mgr,
		handlers.WithDiscoverer(&fakeDiscoverer{listResp: []inventory.Inventory{unusedRun}, bucket: "dst"}),
		handlers.WithLoader(&fakeBuilder{}))
	if err := mgr.Register(t.Context(), unusedRun.CompositeID(), "weekly", "s3://dst/m.json"); err != nil {
		t.Fatal(err)
	}
	job := jobs.Job{ID: "job-run", InventoryID: unusedRun.CompositeID(), Kind: jobs.KindBuild, State: jobs.StateRunning, Stage: "downloading", StartedAt: time.Now()}
	if err := h.JobStoreForTest().Upsert(t.Context(), job); err != nil {
		t.Fatal(err)
	}
	doc := renderDashboard(t, h)
	el := doc.Find(`[data-live-inv="new/weekly/2026-10-21T03-00Z"]`)
	if el.AttrOr("data-job-id", "") != "job-run" || el.AttrOr("data-job-state", "") != "running" || el.Find("[data-live-bar]").Length() != 1 || el.Find("[data-live-text]").Length() != 1 {
		t.Errorf("progress hooks missing: %q", squash(el.Text()))
	}
	if !strings.Contains(squash(doc.Find(`section[aria-labelledby="status-heading"]`).Text()), "Builds in progress 1") {
		t.Error("status doesn't count the build")
	}
}

// TestDashboard_ReadsTotalsWithoutTouchingRuns: reading each loaded
// run's root totals isn't a use of it; touching each would reorder the
// eviction LRU on every dashboard view.
func TestDashboard_ReadsTotalsWithoutTouchingRuns(t *testing.T) {
	h := newOverviewHandlers(t)
	id := runID(overviewRuns[1])
	before, _ := h.ManagerForTest().Get(id)
	renderDashboard(t, h)
	after, _ := h.ManagerForTest().Get(id)
	if !after.LastAccessedAt.Equal(before.LastAccessedAt) {
		t.Errorf("LastAccessedAt moved from %v to %v on a dashboard view", before.LastAccessedAt, after.LastAccessedAt)
	}
}

// TestDashboard_BudgetLine: the disk budget is one short line and a
// bar; the detail is in its title.
func TestDashboard_BudgetLine(t *testing.T) {
	r, err := templates.New()
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	data := handlers.DashboardData{Title: "Dashboard", HasDiscovery: true, Budget: &handlers.DashboardBudget{
		UsedH: "174 MiB", CapH: "4.66 GiB", Pct: 3, Detail: "174 MiB used + 0 B reserved for builds of 4.66 GiB",
	}}
	if err := r.Render(&buf, "dashboard.html", data); err != nil {
		t.Fatal(err)
	}
	doc := parseHTML(t, buf.String())
	line := doc.Find(`p:contains("Index disk:")`)
	if got := squash(line.Text()); got != "Index disk: 174 MiB of 4.66 GiB used" {
		t.Errorf("budget line %q", got)
	}
	if !strings.Contains(line.Parent().AttrOr("title", ""), "reserved") {
		t.Error("budget detail isn't in the title")
	}
	if doc.Find(`[role="progressbar"][aria-valuenow="3"]`).Length() != 1 {
		t.Error("budget bar missing")
	}
}

func TestRunDateLabelAndDelta(t *testing.T) {
	now := time.Date(2026, 10, 26, 12, 0, 0, 0, time.UTC)
	for in, want := range map[string]string{
		"2026-10-25T20-50Z":    "Oct 25",
		"2026-10-25T20-50-07Z": "Oct 25",
		"2025-12-31T23-00Z":    "Dec 31, 2025",
		"custom-run":           "custom-run",
	} {
		if got := handlers.RunDateLabelForTest(in, now); got != want {
			t.Errorf("runDateLabel(%q) = %q, want %q", in, got, want)
		}
	}
	d := handlers.RunDelta{BytesH: "+12.4 GiB", CostH: "+$3.10/mo", SinceH: "Oct 25"}
	if got, want := d.Line(), "+12.4 GiB · +$3.10/mo since Oct 25"; got != want {
		t.Errorf("delta Line = %q, want %q", got, want)
	}
	if got, want := (handlers.RunDelta{SinceH: "Oct 25"}).Line(), "No change since Oct 25"; got != want {
		t.Errorf("unchanged delta Line = %q, want %q", got, want)
	}
}

// TestNewRunDelta: the dashboard's change line names each total that
// moved, so a run whose object count changed while its bytes and cost
// stayed put never reads "No change".
func TestNewRunDelta(t *testing.T) {
	now := time.Date(2026, 10, 26, 12, 0, 0, 0, time.UTC)
	priced := func(objects, bytes, costMicro uint64) handlers.RunStats {
		return handlers.RunStats{Objects: objects, Bytes: bytes, CostMicro: costMicro, CostH: "priced"}
	}
	for _, tc := range []struct {
		name      string
		prev, cur handlers.RunStats
		want      string
	}{
		{"nothing moved", priced(10, 100, 5), priced(10, 100, 5), "No change since Oct 25"},
		{"only objects moved", priced(10, 100, 5), priced(12, 100, 5), "+2 objects since Oct 25"},
		{"one object fewer", priced(10, 100, 5), priced(9, 100, 5), "−1 object since Oct 25"},
		{"only bytes moved", priced(10, 100, 5), priced(10, 2148, 5), "+2.00 KiB since Oct 25"},
		{"everything moved", priced(12, 2148, 0), priced(10, 100, 2_000_000), "−2.00 KiB · −2 objects · +$2.00/mo since Oct 25"},
		{"cost without prices", handlers.RunStats{Objects: 1, Bytes: 1, CostMicro: 1}, handlers.RunStats{Objects: 1, Bytes: 1, CostMicro: 2}, "No change since Oct 25"},
	} {
		if got := handlers.NewRunDeltaForTest(tc.prev, tc.cur, "2026-10-25T20-50Z", now).Line(); got != tc.want {
			t.Errorf("%s: Line = %q, want %q", tc.name, got, tc.want)
		}
	}
}

func TestDashboardTotalLabel(t *testing.T) {
	for _, tc := range []struct {
		total handlers.DashboardTotal
		want  string
	}{
		{handlers.DashboardTotal{Configs: 4, Buckets: 2}, "Latest loaded runs of 4 inventories in 2 buckets"},
		{handlers.DashboardTotal{Configs: 2, Buckets: 1}, "Latest loaded runs of 2 inventories in 1 bucket"},
		{handlers.DashboardTotal{Configs: 1, Buckets: 1}, "Latest loaded runs of 1 inventory in 1 bucket"},
	} {
		if got := tc.total.Label(); got != tc.want {
			t.Errorf("Label() = %q, want %q", got, tc.want)
		}
	}
}
