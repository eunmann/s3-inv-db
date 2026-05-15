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
	agg := h.AggregateDashboardForTest(&logger, views, data)
	confs, order := agg.Confs, agg.Order

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
	confs := h.AggregateDashboardForTest(&logger, views, data).Confs
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
	confs := h.AggregateDashboardForTest(&logger, views, data).Confs
	if _, ok := confs["b/no-runs"]; !ok {
		t.Fatal("placeholder configuration missing")
	}
	if data.TotalRuns != 1 {
		t.Errorf("TotalRuns = %d, want 1", data.TotalRuns)
	}
}

func TestAggregateDashboard_SumsManifestStats(t *testing.T) {
	h := newTestHandlers(t)
	views := []inventory.MergedInventory{
		{Inventory: inventory.Inventory{
			SourceBucket: "b", InventoryName: "i1", Run: "2026-05-13",
			FileCount: 4, TotalBytes: 1000, FileFormat: "Parquet",
		}, State: inventory.StateLoaded},
		{Inventory: inventory.Inventory{
			SourceBucket: "b", InventoryName: "i1", Run: "2026-05-12",
			FileCount: 3, TotalBytes: 500,
		}, State: inventory.StateNotLoaded},
		{Inventory: inventory.Inventory{
			SourceBucket: "b", InventoryName: "i2", Run: "2026-05-13",
			FileCount: 7, TotalBytes: 2500, FileFormat: "CSV",
		}, State: inventory.StateLoaded},
	}
	data := &handlers.DashboardData{}
	logger := zerolog.Nop()
	agg := h.AggregateDashboardForTest(&logger, views, data)

	if agg.Totals.ManifestFiles != 14 {
		t.Errorf("Totals.ManifestFiles = %d, want 14", agg.Totals.ManifestFiles)
	}
	if agg.Totals.ManifestBytes != 4000 {
		t.Errorf("Totals.ManifestBytes = %d, want 4000", agg.Totals.ManifestBytes)
	}
	i1 := agg.Confs["b/i1"]
	if i1.LatestFiles != 4 || i1.LatestBytes != 1000 {
		t.Errorf("b/i1 latest manifest = {Files: %d, Bytes: %d}, want {4, 1000}", i1.LatestFiles, i1.LatestBytes)
	}
	if i1.LatestFormat != "Parquet" {
		t.Errorf("b/i1 LatestFormat = %q, want Parquet", i1.LatestFormat)
	}
	i2 := agg.Confs["b/i2"]
	if i2.LatestFormat != "CSV" {
		t.Errorf("b/i2 LatestFormat = %q, want CSV", i2.LatestFormat)
	}
}

func TestAggregateDashboard_OmitsManifestStatsWhenAbsent(t *testing.T) {
	h := newTestHandlers(t)
	views := []inventory.MergedInventory{
		{Inventory: inventory.Inventory{SourceBucket: "b", InventoryName: "i1", Run: "2026-05-13"}, State: inventory.StateNotLoaded},
	}
	data := &handlers.DashboardData{}
	logger := zerolog.Nop()
	agg := h.AggregateDashboardForTest(&logger, views, data)
	if agg.Totals.ManifestFiles != 0 || agg.Totals.ManifestBytes != 0 {
		t.Errorf("totals manifest = {%d, %d}, want zero when not fetched", agg.Totals.ManifestFiles, agg.Totals.ManifestBytes)
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
