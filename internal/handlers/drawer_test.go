package handlers_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/internal/jobs"
)

// seedInventoryRow registers a composite ID so the jobs FK constraint
// is satisfied for directly-inserted fixture jobs.
func seedInventoryRow(t *testing.T, mgr *inventory.Catalog, id inventory.ID) {
	t.Helper()
	if err := mgr.Register(t.Context(), id, "test", "s3://test/manifest.json"); err != nil {
		t.Fatalf("seed inventory %s: %v", id, err)
	}
}

func TestRunDrawer_NoInventoryRendersEmpty(t *testing.T) {
	h := newDiscoveredHandlers(t,
		&fakeDiscoverer{},
		&fakeBuilder{},
	)
	req := httptest.NewRequest(http.MethodGet, "/partials/drawer/missing/foo/r", http.NoBody)
	req = chiCtxWithParams(req, "src", "missing", "id", "foo", "run", "r")
	w := httptest.NewRecorder()
	h.RunDrawer(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	body := w.Body.String()
	if !strings.Contains(body, "No build activity recorded yet") {
		t.Errorf("body missing empty-state copy:\n%s", body)
	}
}

func TestRunDrawer_RendersLatestJobAndStageTimeline(t *testing.T) {
	disc := inventory.Inventory{SourceBucket: "b", Name: "i", Run: "r"}
	h := newDiscoveredHandlers(t,
		&fakeDiscoverer{findResp: disc, bucket: "dst"},
		&fakeBuilder{},
	)
	composite := disc.CompositeID()
	seedInventoryRow(t, h.ManagerForTest(), composite)

	now := time.Now().Truncate(time.Second)
	job := jobs.Job{
		ID: "j1", InventoryID: composite, Kind: jobs.KindBuild,
		State:     jobs.StateSucceeded,
		StartedAt: now.Add(-30 * time.Second), FinishedAt: now,
		AttemptCount: 1,
		Stages: []jobs.StageRecord{
			{Name: "preparing", StartedAt: now.Add(-30 * time.Second), EndedAt: now.Add(-29 * time.Second), Duration: time.Second},
			{Name: "downloading", StartedAt: now.Add(-29 * time.Second), EndedAt: now.Add(-15 * time.Second), Duration: 14 * time.Second, Rows: 1500, Bytes: 1024 * 1024},
			{Name: "building", StartedAt: now.Add(-15 * time.Second), EndedAt: now, Duration: 15 * time.Second},
		},
	}
	if err := h.JobStoreForTest().Upsert(t.Context(), job); err != nil {
		t.Fatalf("Upsert job: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/partials/drawer/b/i/r", http.NoBody)
	req = chiCtxWithParams(req, "src", "b", "id", "i", "run", "r")
	w := httptest.NewRecorder()
	h.RunDrawer(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	body := w.Body.String()
	for _, want := range []string{"Preparing", "Downloading", "Building"} {
		if !strings.Contains(body, want) {
			t.Errorf("body missing stage label %q:\n%s", want, body)
		}
	}
	if !strings.Contains(body, "took ") {
		t.Errorf("body missing 'took ' duration label:\n%s", body)
	}
	if strings.Contains(body, "Retry build") {
		t.Errorf("succeeded job should not show Retry button:\n%s", body)
	}
}

func TestRunDrawer_FailedJobShowsRetryAndError(t *testing.T) {
	disc := inventory.Inventory{SourceBucket: "b", Name: "i", Run: "r"}
	h := newDiscoveredHandlers(t,
		&fakeDiscoverer{findResp: disc, bucket: "dst"},
		&fakeBuilder{},
	)
	composite := disc.CompositeID()
	seedInventoryRow(t, h.ManagerForTest(), composite)

	now := time.Now().Truncate(time.Second)
	job := jobs.Job{
		ID: "j-fail", InventoryID: composite, Kind: jobs.KindBuild,
		State: jobs.StateFailed, Error: "network broken",
		StartedAt: now.Add(-3 * time.Second), FinishedAt: now,
		AttemptCount: 1,
		Stages: []jobs.StageRecord{
			{Name: "downloading", StartedAt: now.Add(-3 * time.Second), EndedAt: now, Duration: 3 * time.Second, Err: "network broken"},
		},
	}
	if err := h.JobStoreForTest().Upsert(t.Context(), job); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/partials/drawer/b/i/r", http.NoBody)
	req = chiCtxWithParams(req, "src", "b", "id", "i", "run", "r")
	w := httptest.NewRecorder()
	h.RunDrawer(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	body := w.Body.String()
	if !strings.Contains(body, "Retry build") {
		t.Errorf("failed job must show Retry button:\n%s", body)
	}
	if !strings.Contains(body, "/api/jobs/j-fail/retry") {
		t.Errorf("retry button must POST to /api/jobs/j-fail/retry:\n%s", body)
	}
	if !strings.Contains(body, "network broken") {
		t.Errorf("error message missing:\n%s", body)
	}
}

func TestRunDrawer_LiveJobShowsCancelButton(t *testing.T) {
	disc := inventory.Inventory{SourceBucket: "b", Name: "i", Run: "r"}
	h := newDiscoveredHandlers(t,
		&fakeDiscoverer{findResp: disc, bucket: "dst"},
		&fakeBuilder{},
	)
	composite := disc.CompositeID()
	seedInventoryRow(t, h.ManagerForTest(), composite)

	now := time.Now().Truncate(time.Second)
	job := jobs.Job{
		ID: "j-run", InventoryID: composite, Kind: jobs.KindBuild,
		State: jobs.StateRunning, Stage: "downloading",
		StartedAt:    now.Add(-5 * time.Second),
		AttemptCount: 1,
		Stages: []jobs.StageRecord{
			{Name: "downloading", StartedAt: now.Add(-5 * time.Second)},
		},
	}
	if err := h.JobStoreForTest().Upsert(t.Context(), job); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/partials/drawer/b/i/r", http.NoBody)
	req = chiCtxWithParams(req, "src", "b", "id", "i", "run", "r")
	w := httptest.NewRecorder()
	h.RunDrawer(w, req)

	body := w.Body.String()
	if !strings.Contains(body, "Cancel build") {
		t.Errorf("live job must show Cancel button:\n%s", body)
	}
	if !strings.Contains(body, "/api/jobs/j-run/cancel") {
		t.Errorf("cancel button must POST to /api/jobs/j-run/cancel:\n%s", body)
	}
}

func TestRunDrawer_OpenShellHasSSEWiringAndPushURL(t *testing.T) {
	disc := inventory.Inventory{SourceBucket: "b", Name: "i", Run: "r"}
	h := newDiscoveredHandlers(t,
		&fakeDiscoverer{findResp: disc, bucket: "dst"},
		&fakeBuilder{},
	)
	composite := disc.CompositeID()
	seedInventoryRow(t, h.ManagerForTest(), composite)

	req := httptest.NewRequest(http.MethodGet, "/partials/drawer/b/i/r", http.NoBody)
	req = chiCtxWithParams(req, "src", "b", "id", "i", "run", "r")
	w := httptest.NewRecorder()
	h.RunDrawer(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	body := w.Body.String()
	if !strings.Contains(body, `id="run-drawer"`) {
		t.Errorf("open shell missing id=run-drawer\n%s", body)
	}
	if !strings.Contains(body, `data-open`) {
		t.Errorf("open shell missing data-open marker\n%s", body)
	}
	if !strings.Contains(body, `hx-trigger="sse:row-b/i/r from:body"`) {
		t.Errorf("open shell missing SSE refresh trigger\n%s", body)
	}
	if !strings.Contains(body, `hx-get="/partials/drawer/b/i/r"`) {
		t.Errorf("open shell missing self-refresh hx-get\n%s", body)
	}
	if got := w.Header().Get("HX-Push-Url"); got != "/inventories#run=b/i/r" {
		t.Errorf("HX-Push-Url = %q, want /inventories#run=b/i/r", got)
	}
}

func TestRunDrawerClose_ReturnsClosedShellAndClearsHash(t *testing.T) {
	h := newDiscoveredHandlers(t,
		&fakeDiscoverer{bucket: "dst"},
		&fakeBuilder{},
	)
	req := httptest.NewRequest(http.MethodGet, "/partials/drawer-close", http.NoBody)
	w := httptest.NewRecorder()
	h.RunDrawerClose(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	body := w.Body.String()
	if !strings.Contains(body, `id="run-drawer"`) {
		t.Errorf("closed shell must keep id=run-drawer so the swap target matches\n%s", body)
	}
	if strings.Contains(body, `data-open`) {
		t.Errorf("closed shell must NOT carry data-open\n%s", body)
	}
	if strings.Contains(body, `sse:row-`) {
		t.Errorf("closed shell must NOT carry an SSE trigger\n%s", body)
	}
	if got := w.Header().Get("HX-Push-Url"); got != "/inventories" {
		t.Errorf("HX-Push-Url = %q, want /inventories", got)
	}
}

func TestRunDrawer_PrefersInfoLoadDurationForTook(t *testing.T) {
	disc := inventory.Inventory{SourceBucket: "b", Name: "i", Run: "r"}
	h := newDiscoveredHandlers(t,
		&fakeDiscoverer{findResp: disc, bucket: "dst"},
		&fakeBuilder{},
	)
	composite := disc.CompositeID()

	if err := h.ManagerForTest().Hydrate(t.Context(), inventory.Info{
		ID: composite, Name: "test", Path: "s3://test/manifest.json",
		State: inventory.StateLoaded, LoadDuration: 81 * time.Millisecond,
	}, t.TempDir()); err != nil {
		t.Fatalf("hydrate: %v", err)
	}

	now := time.Now().Truncate(time.Second)
	job := jobs.Job{
		ID: "j-loaded", InventoryID: composite, Kind: jobs.KindBuild,
		State:     jobs.StateSucceeded,
		StartedAt: now.Add(-time.Second), FinishedAt: now,
		AttemptCount: 1,
		Stages: []jobs.StageRecord{
			{Name: "preparing", Duration: 1 * time.Millisecond},
			{Name: "downloading", Duration: 32 * time.Millisecond},
			{Name: "building", Duration: 44 * time.Millisecond},
		},
	}
	if err := h.JobStoreForTest().Upsert(t.Context(), job); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/partials/drawer/b/i/r", http.NoBody)
	req = chiCtxWithParams(req, "src", "b", "id", "i", "run", "r")
	w := httptest.NewRecorder()
	h.RunDrawer(w, req)

	body := w.Body.String()
	if !strings.Contains(body, "81.0ms") {
		t.Errorf("drawer should show Info.LoadDuration (81.0ms), not stage sum (77ms):\n%s", body)
	}
}

func TestRunDrawer_RendersPrevAttemptsChain(t *testing.T) {
	disc := inventory.Inventory{SourceBucket: "b", Name: "i", Run: "r"}
	h := newDiscoveredHandlers(t,
		&fakeDiscoverer{findResp: disc, bucket: "dst"},
		&fakeBuilder{},
	)
	composite := disc.CompositeID()
	seedInventoryRow(t, h.ManagerForTest(), composite)

	t0 := time.Now().Add(-time.Hour).Truncate(time.Second)
	first := jobs.Job{
		ID: "att-1", InventoryID: composite, Kind: jobs.KindBuild,
		State: jobs.StateFailed, AttemptCount: 1, Error: "first boom",
		StartedAt: t0, FinishedAt: t0.Add(5 * time.Second),
	}
	second := jobs.Job{
		ID: "att-2", InventoryID: composite, Kind: jobs.KindBuild,
		State: jobs.StateFailed, AttemptCount: 2, Error: "second boom",
		PrevJobID: "att-1",
		StartedAt: t0.Add(10 * time.Second), FinishedAt: t0.Add(20 * time.Second),
	}
	third := jobs.Job{
		ID: "att-3", InventoryID: composite, Kind: jobs.KindBuild,
		State: jobs.StateRunning, AttemptCount: 3, Stage: "downloading",
		PrevJobID: "att-2",
		StartedAt: t0.Add(30 * time.Second),
	}
	for _, j := range []jobs.Job{first, second, third} {
		if err := h.JobStoreForTest().Upsert(t.Context(), j); err != nil {
			t.Fatalf("Upsert %s: %v", j.ID, err)
		}
		time.Sleep(1100 * time.Millisecond)
	}

	req := httptest.NewRequest(http.MethodGet, "/partials/drawer/b/i/r", http.NoBody)
	req = chiCtxWithParams(req, "src", "b", "id", "i", "run", "r")
	w := httptest.NewRecorder()
	h.RunDrawer(w, req)

	body := w.Body.String()
	if !strings.Contains(body, "Previous attempts") {
		t.Errorf("body missing 'Previous attempts' section:\n%s", body)
	}
	if !strings.Contains(body, "first boom") || !strings.Contains(body, "second boom") {
		t.Errorf("previous attempt errors missing in body:\n%s", body)
	}
	if !strings.Contains(body, "Attempt 3") {
		t.Errorf("current attempt number 3 missing in header:\n%s", body)
	}
}
