package handlers

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/eunmann/s3-inv-db/internal/inventory"
)

// TestListConfigurationsAPI_DiscoveryDisabled exercises the
// manager-only path: when no S3 source is configured the handler
// returns the manager's flat list grouped by parsing composite IDs.
func TestListConfigurationsAPI_DiscoveryDisabled(t *testing.T) {
	f := newTestFixture(t)
	// Two runs of one configuration + a legacy two-part entry.
	for _, id := range []inventory.ID{"src-a/inv-1/2026-05-13T03-00Z", "src-a/inv-1/2026-05-12T03-00Z", "src-a/inv-2/2026-05-13T03-00Z"} {
		if err := f.mgr.Register(id, string(id), "/p"); err != nil {
			t.Fatalf("register %s: %v", id, err)
		}
	}
	req := httptest.NewRequest(http.MethodGet, "/api/configurations", http.NoBody)
	w := httptest.NewRecorder()
	f.h.ListConfigurationsAPI(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", w.Code, w.Body.String())
	}
	if ct := w.Header().Get("Content-Type"); !strings.Contains(ct, "application/json") {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}
	var resp ConfigurationsResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.DiscoveryEnabled {
		t.Error("DiscoveryEnabled should be false with no s3 source")
	}
	if len(resp.Configurations) != 2 {
		t.Fatalf("configurations = %d, want 2 (src-a/inv-1, src-a/inv-2)", len(resp.Configurations))
	}
	for _, c := range resp.Configurations {
		if c.SourceBucket == "" || c.InventoryName == "" {
			t.Errorf("empty src/inv on group: %+v", c)
		}
		if len(c.Runs) == 0 {
			t.Errorf("group %s/%s has no runs", c.SourceBucket, c.InventoryName)
		}
	}
}

func TestBrowseLevelAPI_RequiresInventoryID(t *testing.T) {
	f := newTestFixture(t)
	req := httptest.NewRequest(http.MethodGet, "/api/browse", http.NoBody)
	w := httptest.NewRecorder()
	f.h.BrowseLevelAPI(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", w.Code)
	}
	if !strings.Contains(w.Body.String(), "inventory_id") {
		t.Errorf("body missing reason: %s", w.Body.String())
	}
}

func TestBrowseLevelAPI_NotFound(t *testing.T) {
	f := newTestFixture(t)
	req := httptest.NewRequest(http.MethodGet, "/api/browse?inventory_id=missing", http.NoBody)
	w := httptest.NewRecorder()
	f.h.BrowseLevelAPI(w, req)
	if w.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404; body=%s", w.Code, w.Body.String())
	}
}

func TestBrowseLevelAPI_NotLoaded(t *testing.T) {
	f := newTestFixture(t)
	if err := f.mgr.Register("inv1", "n", "/p"); err != nil {
		t.Fatalf("register: %v", err)
	}
	req := httptest.NewRequest(http.MethodGet, "/api/browse?inventory_id=inv1", http.NoBody)
	w := httptest.NewRecorder()
	f.h.BrowseLevelAPI(w, req)
	if w.Code != http.StatusConflict {
		t.Errorf("status = %d, want 409 (not loaded); body=%s", w.Code, w.Body.String())
	}
}

func TestDiffLevelAPI_RequiresFromAndTo(t *testing.T) {
	f := newTestFixture(t)
	req := httptest.NewRequest(http.MethodGet, "/api/diff", http.NoBody)
	w := httptest.NewRecorder()
	f.h.DiffLevelAPI(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", w.Code)
	}
}

func TestDiffLevelAPI_MismatchedConfigurations(t *testing.T) {
	f := newTestFixture(t)
	req := httptest.NewRequest(http.MethodGet, "/api/diff?from=a/b/c&to=x/y/z", http.NoBody)
	w := httptest.NewRecorder()
	f.h.DiffLevelAPI(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", w.Code)
	}
	if !strings.Contains(w.Body.String(), "same inventory configuration") {
		t.Errorf("body missing reason: %s", w.Body.String())
	}
}

func TestDiffLevelAPI_NotLoaded(t *testing.T) {
	f := newTestFixture(t)
	if err := f.mgr.Register("a/b/1", "n1", "/p1"); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := f.mgr.Register("a/b/2", "n2", "/p2"); err != nil {
		t.Fatalf("register: %v", err)
	}
	req := httptest.NewRequest(http.MethodGet, "/api/diff?from=a/b/1&to=a/b/2", http.NoBody)
	w := httptest.NewRecorder()
	f.h.DiffLevelAPI(w, req)
	if w.Code != http.StatusConflict {
		t.Errorf("status = %d, want 409 (not loaded); body=%s", w.Code, w.Body.String())
	}
}

func TestGroupManagerForAPI_GroupsAndFallsBack(t *testing.T) {
	in := []inventory.Info{
		{ID: "src/inv/2026-05-13T03-00Z", State: inventory.StateLoaded},
		{ID: "src/inv/2026-05-12T03-00Z", State: inventory.StateNotLoaded},
		{ID: "two-part-only", State: inventory.StateLoaded}, // legacy
	}
	got := groupManagerForAPI(in)
	if len(got) != 2 {
		t.Fatalf("groups = %d, want 2", len(got))
	}
	var srcGroup, fallback ConfigurationView
	for _, g := range got {
		if g.SourceBucket == "src" {
			srcGroup = g
		} else {
			fallback = g
		}
	}
	if len(srcGroup.Runs) != 2 {
		t.Errorf("src/inv runs = %d, want 2", len(srcGroup.Runs))
	}
	if fallback.SourceBucket != "_other_" {
		t.Errorf("legacy ID didn't fall back to _other_ group: %+v", fallback)
	}
}

func TestStatusRank(t *testing.T) {
	cases := map[string]int{"added": 1, "removed": 2, "changed": 3, "unchanged": 4, "garbage": 5}
	for s, want := range cases {
		if got := statusRank(s); got != want {
			t.Errorf("statusRank(%q) = %d, want %d", s, got, want)
		}
	}
}

func TestInventoryGroup_ConfigID(t *testing.T) {
	g := InventoryGroup{SourceBucket: "src-a", InventoryName: "inv-1"}
	if got := g.ConfigID(); got != "src-a/inv-1" {
		t.Errorf("ConfigID() = %q, want %q", got, "src-a/inv-1")
	}
}

func TestGroupDiscoveredForAPI_GroupsRunsByConfig(t *testing.T) {
	f := newTestFixture(t)
	// Two runs of one configuration + one run of another, all flowing
	// through the merge layer (Manager state defaults to StateNotLoaded
	// when the inventory isn't registered yet).
	views := []inventory.MergedInventory{
		{Inventory: inventory.Inventory{SourceBucket: "b1", InventoryName: "i1", Run: "2026-05-13T03-00Z", ManifestKey: "k1/2026-05-13T03-00Z/manifest.json"}, State: inventory.StateNotLoaded},
		{Inventory: inventory.Inventory{SourceBucket: "b1", InventoryName: "i1", Run: "2026-05-12T03-00Z", ManifestKey: "k1/2026-05-12T03-00Z/manifest.json"}, State: inventory.StateNotLoaded},
		{Inventory: inventory.Inventory{SourceBucket: "b1", InventoryName: "i2", Run: "2026-05-13T03-00Z", ManifestKey: "k2/2026-05-13T03-00Z/manifest.json"}, State: inventory.StateNotLoaded},
	}
	groups := f.h.groupDiscoveredForAPI(nil, views)
	if len(groups) != 2 {
		t.Fatalf("groups = %d, want 2 (i1, i2)", len(groups))
	}
	var i1, i2 ConfigurationView
	for _, g := range groups {
		switch g.InventoryName {
		case "i1":
			i1 = g
		case "i2":
			i2 = g
		}
	}
	if len(i1.Runs) != 2 {
		t.Errorf("i1.Runs = %d, want 2", len(i1.Runs))
	}
	if len(i2.Runs) != 1 {
		t.Errorf("i2.Runs = %d, want 1", len(i2.Runs))
	}
	// CompositeID round-trip: every run carries the composite key the
	// /partials/discovered/{src}/{id}/{run}/* endpoints expect.
	for _, run := range i1.Runs {
		if !strings.HasPrefix(string(run.ID), "b1/i1/") {
			t.Errorf("run.ID = %q, want b1/i1/* prefix", run.ID)
		}
	}
}
