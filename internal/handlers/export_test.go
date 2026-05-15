package handlers

import (
	"net/http"

	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/internal/jobs"
	"github.com/rs/zerolog"
)

// Test accessors. Defined in *_test.go so they are only compiled into
// the test binary — production callers cannot reach internal state via
// these. The external _test package (handlers_test) needs them to
// inspect the Manager / JobStore after exercising handlers.

// JobStoreForTest returns the JobStore wired into the handler.
func (h *Handlers) JobStoreForTest() *jobs.Store { return h.jobStore }

// ManagerForTest returns the inventory Manager wired into the handler.
func (h *Handlers) ManagerForTest() *inventory.Manager { return h.manager }

// BuildCompareSelfViewForTest exposes buildCompareSelfView so external
// tests can pin the formatted view shape without going through HTTP.
func (h *Handlers) BuildCompareSelfViewForTest(self inventory.CompareSelf) CompareSelfView {
	return h.buildCompareSelfView(self)
}

// BuildCompareChildViewForTest exposes buildCompareChildView for tests.
func (h *Handlers) BuildCompareChildViewForTest(c *inventory.CompareChild) CompareChildView {
	return h.buildCompareChildView(c)
}

// GroupDiscoveredForAPIForTest exposes groupDiscoveredForAPI for tests.
func (h *Handlers) GroupDiscoveredForAPIForTest(r *http.Request, views []inventory.MergedInventory) []ConfigurationView {
	return h.groupDiscoveredForAPI(r, views)
}

// AggregateDashboardForTest exposes aggregateDashboard for tests.
func (h *Handlers) AggregateDashboardForTest(logger *zerolog.Logger, views []inventory.MergedInventory, data *DashboardData) (map[string]DashConfAggForTest, []string, DashTotalsForTest) {
	confs, order, totals := h.aggregateDashboard(logger, views, data)
	out := make(map[string]DashConfAggForTest, len(confs))
	for k, v := range confs {
		out[k] = DashConfAggForTest{
			Src: v.Src, ID: v.ID,
			TotalRuns: v.TotalRuns, LoadedRuns: v.LoadedRuns,
			LatestRun: v.LatestRun, LatestState: v.LatestState,
			DiskBytes: v.DiskBytes,
		}
	}

	return out, order, DashTotalsForTest{Objects: totals.objects, Bytes: totals.bytes, Disk: totals.disk}
}

// AddLoadedStatsForTest exposes addLoadedStats for tests.
func (h *Handlers) AddLoadedStatsForTest(logger *zerolog.Logger, v *inventory.MergedInventory, totals *DashTotalsForTest) {
	c := &dashConfAgg{Src: v.SourceBucket, ID: v.InventoryName}
	t := dashTotals{objects: totals.Objects, bytes: totals.Bytes, disk: totals.Disk}
	h.addLoadedStats(logger, v, c, &t)
	totals.Objects = t.objects
	totals.Bytes = t.bytes
	totals.Disk = t.disk
}

// DashConfAggForTest mirrors dashConfAgg with exported fields for tests.
type DashConfAggForTest struct {
	Src, ID     string
	TotalRuns   int
	LoadedRuns  int
	LatestRun   string
	LatestState inventory.State
	DiskBytes   int64
}

// DashTotalsForTest mirrors dashTotals with exported fields for tests.
type DashTotalsForTest struct {
	Objects uint64
	Bytes   uint64
	Disk    int64
}

// Exported aliases for unexported helpers used by external tests.
var (
	NumericLabelForTest          = numericLabel
	FormatDeltaForTest           = formatDelta
	FormatCostDeltaForTest       = formatCostDelta
	PctChangeForTest             = pctChange
	AbsInt64ForTest              = absInt64
	DescribeRunForTest           = describeRun
	GroupManagerForAPIForTest    = groupManagerForAPI
	StatusRankForTest            = statusRank
	GroupLoadedInventoriesForTst = groupLoadedInventories
	BuildComparePickerForTest    = buildComparePicker
	SortCompareChildViewForTest  = sortCompareChildView
	SameConfigForTest            = sameConfig
)
