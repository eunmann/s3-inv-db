package handlers_test

import (
	"testing"

	"github.com/eunmann/s3-inv-db/internal/handlers"
	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/pkg/tiers"
	"github.com/rs/zerolog"
)

// TestDashboardTotalsDoNotTouchAccessTime: reading every loaded run's
// root totals for the dashboard isn't a use of those runs; touching
// each would reorder the eviction LRU on every dashboard view.
func TestDashboardTotalsDoNotTouchAccessTime(t *testing.T) {
	const id = "src/inv/2026-01-01T00-00Z"
	mgr := loadCostIndex(t, id, []costObj{{"a/x", gib, tiers.Standard}})
	h := newWiredHandlers(t, mgr)
	before, _ := mgr.Get(id)

	logger := zerolog.Nop()
	var totals handlers.DashTotalsForTest
	v := &inventory.MergedInventory{SourceBucket: "src", Name: "inv", Run: "2026-01-01T00-00Z"}
	h.AddLoadedStatsForTest(&logger, v, &totals)

	if totals.Objects != 1 || totals.Bytes != gib {
		t.Fatalf("totals = %+v, want 1 object, %d bytes", totals, gib)
	}
	after, _ := mgr.Get(id)
	if !after.LastAccessedAt.Equal(before.LastAccessedAt) {
		t.Errorf("LastAccessedAt moved from %v to %v on a dashboard read", before.LastAccessedAt, after.LastAccessedAt)
	}
}
