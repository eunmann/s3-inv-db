package handlers_test

import (
	"testing"

	"github.com/eunmann/s3-inv-db/internal/handlers"
	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/rs/zerolog"
)

func TestAggregateDashboard_TalliesByState(t *testing.T) {
	h := newTestHandlers(t)
	views := []inventory.MergedInventory{
		{Inventory: inventory.Inventory{SourceBucket: "b", InventoryName: "i1", Run: "2026-05-13"}, State: inventory.StateLoaded},
		{Inventory: inventory.Inventory{SourceBucket: "b", InventoryName: "i1", Run: "2026-05-12"}, State: inventory.StateNotLoaded},
		{Inventory: inventory.Inventory{SourceBucket: "b", InventoryName: "i2", Run: "2026-05-13"}, State: inventory.StateLoading},
		{Inventory: inventory.Inventory{SourceBucket: "b", InventoryName: "i3", Run: "2026-05-13"}, State: inventory.StateError},
	}
	data := &handlers.DashboardData{}
	logger := zerolog.Nop()
	confs, order, _ := h.AggregateDashboardForTest(&logger, views, data)

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
		t.Errorf("configurations = %d, want 3", got)
	}
	if got := len(order); got != 3 {
		t.Errorf("order slice len = %d, want 3", got)
	}
	i1, ok := confs["b/i1"]
	if !ok {
		t.Fatal("b/i1 missing from configs")
	}
	if i1.TotalRuns != 2 || i1.LoadedRuns != 1 {
		t.Errorf("b/i1 = {TotalRuns: %d, LoadedRuns: %d}, want {2, 1}", i1.TotalRuns, i1.LoadedRuns)
	}
}

func TestAggregateDashboard_LatestRunIsFirstSeen(t *testing.T) {
	h := newTestHandlers(t)
	views := []inventory.MergedInventory{
		{Inventory: inventory.Inventory{SourceBucket: "b", InventoryName: "i1", Run: "2026-05-13"}, State: inventory.StateLoaded},
		{Inventory: inventory.Inventory{SourceBucket: "b", InventoryName: "i1", Run: "2026-05-12"}, State: inventory.StateNotLoaded},
	}
	data := &handlers.DashboardData{}
	logger := zerolog.Nop()
	confs, _, _ := h.AggregateDashboardForTest(&logger, views, data)
	if got := confs["b/i1"].LatestRun; got != "2026-05-13" {
		t.Errorf("LatestRun = %q, want 2026-05-13", got)
	}
	if got := confs["b/i1"].LatestState; got != inventory.StateLoaded {
		t.Errorf("LatestState = %s, want loaded", got)
	}
}

func TestAggregateDashboard_PlaceholderConfigCounts(t *testing.T) {
	h := newTestHandlers(t)
	views := []inventory.MergedInventory{
		{Inventory: inventory.Inventory{SourceBucket: "b", InventoryName: "no-runs"}, State: inventory.StateNotLoaded},
	}
	data := &handlers.DashboardData{}
	logger := zerolog.Nop()
	confs, _, _ := h.AggregateDashboardForTest(&logger, views, data)
	if _, ok := confs["b/no-runs"]; !ok {
		t.Fatal("placeholder configuration missing")
	}
	if data.TotalRuns != 1 {
		t.Errorf("TotalRuns = %d, want 1", data.TotalRuns)
	}
}

func TestAddLoadedStats_TolerantOfUnloadedIndex(t *testing.T) {
	h := newTestHandlers(t)
	v := inventory.MergedInventory{
		Inventory: inventory.Inventory{SourceBucket: "b", InventoryName: "i1", Run: "r"},
		State:     inventory.StateLoaded,
	}
	totals := handlers.DashTotalsForTest{}
	logger := zerolog.Nop()
	h.AddLoadedStatsForTest(&logger, &v, &totals)
	if totals.Objects != 0 || totals.Bytes != 0 {
		t.Errorf("totals after addLoadedStats with no index: %+v, want zero", totals)
	}
}
