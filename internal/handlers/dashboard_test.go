package handlers

import (
	"testing"

	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/rs/zerolog"
)

// TestAggregateDashboard_TalliesByState pins the SSR aggregation that
// drives the four stat cards on the dashboard. We craft a handful of
// MergedInventory values across every state and confirm the totals.
func TestAggregateDashboard_TalliesByState(t *testing.T) {
	h := newTestHandlers(t)
	views := []inventory.MergedInventory{
		// Two runs of "b/i1" — one loaded, one not_loaded.
		{Inventory: inventory.Inventory{SourceBucket: "b", InventoryName: "i1", Run: "2026-05-13"}, State: inventory.StateLoaded},
		{Inventory: inventory.Inventory{SourceBucket: "b", InventoryName: "i1", Run: "2026-05-12"}, State: inventory.StateNotLoaded},
		// One run of "b/i2" mid-load.
		{Inventory: inventory.Inventory{SourceBucket: "b", InventoryName: "i2", Run: "2026-05-13"}, State: inventory.StateLoading},
		// One run of "b/i3" in error.
		{Inventory: inventory.Inventory{SourceBucket: "b", InventoryName: "i3", Run: "2026-05-13"}, State: inventory.StateError},
	}
	data := &DashboardData{}
	logger := zerolog.Nop()
	confs, order, _ := h.aggregateDashboard(&logger, views, data)

	if data.TotalRuns != 4 {
		t.Errorf("TotalRuns = %d, want 4", data.TotalRuns)
	}
	if data.LoadedRuns != 1 {
		t.Errorf("LoadedRuns = %d, want 1", data.LoadedRuns)
	}
	if data.LoadingRuns != 1 {
		t.Errorf("LoadingRuns = %d, want 1", data.LoadingRuns)
	}
	if data.ErrorRuns != 1 {
		t.Errorf("ErrorRuns = %d, want 1", data.ErrorRuns)
	}
	if got := len(confs); got != 3 {
		t.Errorf("configurations = %d, want 3 (b/i1, b/i2, b/i3)", got)
	}
	if got := len(order); got != 3 {
		t.Errorf("order slice len = %d, want 3", got)
	}
	// b/i1 has two runs; its LoadedRuns should be 1.
	i1 := confs["b/i1"]
	if i1 == nil {
		t.Fatal("b/i1 missing from configs")
	}
	if i1.TotalRuns != 2 || i1.LoadedRuns != 1 {
		t.Errorf("b/i1 = {TotalRuns: %d, LoadedRuns: %d}, want {2, 1}", i1.TotalRuns, i1.LoadedRuns)
	}
}

// TestAggregateDashboard_LatestRunIsFirstSeen pins the contract that
// LatestRun is set from the FIRST encountered non-empty Run for a
// configuration — relying on the discoverer to return newest-first.
func TestAggregateDashboard_LatestRunIsFirstSeen(t *testing.T) {
	h := newTestHandlers(t)
	views := []inventory.MergedInventory{
		// Order matters: the first non-empty Run wins.
		{Inventory: inventory.Inventory{SourceBucket: "b", InventoryName: "i1", Run: "2026-05-13"}, State: inventory.StateLoaded},
		{Inventory: inventory.Inventory{SourceBucket: "b", InventoryName: "i1", Run: "2026-05-12"}, State: inventory.StateNotLoaded},
	}
	data := &DashboardData{}
	logger := zerolog.Nop()
	confs, _, _ := h.aggregateDashboard(&logger, views, data)
	if got := confs["b/i1"].LatestRun; got != "2026-05-13" {
		t.Errorf("LatestRun = %q, want 2026-05-13 (first-seen newest)", got)
	}
	if got := confs["b/i1"].LatestState; got != inventory.StateLoaded {
		t.Errorf("LatestState = %s, want loaded (state of first-seen run)", got)
	}
}

// TestAggregateDashboard_PlaceholderConfigCounts pins that a
// configuration discovered with no completed runs (Run == "") still
// shows up as a tracked configuration so the user sees it on the
// dashboard.
func TestAggregateDashboard_PlaceholderConfigCounts(t *testing.T) {
	h := newTestHandlers(t)
	views := []inventory.MergedInventory{
		{Inventory: inventory.Inventory{SourceBucket: "b", InventoryName: "no-runs"}, State: inventory.StateNotLoaded},
	}
	data := &DashboardData{}
	logger := zerolog.Nop()
	confs, _, _ := h.aggregateDashboard(&logger, views, data)
	if got := confs["b/no-runs"]; got == nil {
		t.Fatal("placeholder configuration missing")
	}
	if data.TotalRuns != 1 {
		t.Errorf("TotalRuns = %d, want 1 (placeholder still counted)", data.TotalRuns)
	}
}

// TestAddLoadedStats_TolerantOfUnloadedIndex pins that the helper logs
// (rather than panicking) when an inventory is reported as loaded but
// the manager has no open index for it — a race window that exists
// between merge and stats collection.
func TestAddLoadedStats_TolerantOfUnloadedIndex(t *testing.T) {
	h := newTestHandlers(t)
	v := inventory.MergedInventory{
		Inventory: inventory.Inventory{SourceBucket: "b", InventoryName: "i1", Run: "r"},
		State:     inventory.StateLoaded,
	}
	totals := dashTotals{}
	c := &dashConfAgg{Src: "b", ID: "i1"}
	logger := zerolog.Nop()
	// Manager has nothing registered — addLoadedStats logs and returns.
	// The asserting invariant is "does not panic".
	h.addLoadedStats(&logger, &v, c, &totals)
	if totals.objects != 0 || totals.bytes != 0 {
		t.Errorf("expected no stat accumulation when index isn't loaded; got %+v", totals)
	}
}
