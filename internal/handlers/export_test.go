package handlers

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/internal/jobs"
	"github.com/rs/zerolog"
)

// SetSSEHeartbeatForTest swaps the package-level SSE heartbeat for the
// duration of one test, restoring it via t.Cleanup. Only the test
// binary sees this seam — production code uses the package default.
func SetSSEHeartbeatForTest(t *testing.T, d time.Duration) {
	t.Helper()
	prev := sseHeartbeat
	sseHeartbeat = d
	t.Cleanup(func() { sseHeartbeat = prev })
}

// SetSSEMaxConnsPerIPForTest swaps the SSE per-IP cap for one test.
func SetSSEMaxConnsPerIPForTest(t *testing.T, n int) {
	t.Helper()
	prev := sseMaxConnsPerIP
	sseMaxConnsPerIP = n
	t.Cleanup(func() { sseMaxConnsPerIP = prev })
}

// Test accessors. Defined in *_test.go so they are only compiled into
// the test binary — production callers cannot reach internal state via
// these. The external _test package (handlers_test) needs them to
// inspect the Manager / JobStore after exercising handlers.

// JobStoreForTest returns the JobStore wired into the handler.
func (h *Handlers) JobStoreForTest() *jobs.Store { return h.jobStore }

// RenderHTMLForTest exposes renderHTML so external tests can drive
// render failures and assert the buffered-render contract.
func (h *Handlers) RenderHTMLForTest(w http.ResponseWriter, r *http.Request, name, logMsg string, data any) {
	h.renderHTML(w, r, name, logMsg, data)
}

// RenderHTMLPartialForTest exposes renderHTMLPartial.
func (h *Handlers) RenderHTMLPartialForTest(w http.ResponseWriter, r *http.Request, name, logMsg string, data any) {
	h.renderHTMLPartial(w, r, name, logMsg, data)
}

// JobManagerForTest returns the JobManager wired into the handler.
func (h *Handlers) JobManagerForTest() *jobs.Scheduler { return h.jobMgr }

// ManagerForTest returns the inventory Manager wired into the handler.
func (h *Handlers) ManagerForTest() *inventory.Catalog { return h.manager }

// ConfigStoreForTest returns the ConfigStore wired into the handler.
func (h *Handlers) ConfigStoreForTest() *inventory.ConfigStore { return h.configStore }

// CompareViewOptionsForTest builds an opaque options struct for tests
// that need to drive ComputeCompareLevelForTest directly.
type CompareViewOptionsForTest struct {
	From, To, Prefix string
	SortBy, Dir      string
	Page, PageSize   int
	HideUnchanged    bool
}

// ComputeCompareLevelForTest exposes computeCompareLevel for tests.
func (h *Handlers) ComputeCompareLevelForTest(ctx context.Context, opts CompareViewOptionsForTest) (*CompareLevelView, error) {
	return h.computeCompareLevel(ctx, compareViewOptions{
		from:          inventory.ID(opts.From),
		to:            inventory.ID(opts.To),
		prefix:        opts.Prefix,
		sortBy:        opts.SortBy,
		dir:           opts.Dir,
		page:          opts.Page,
		pageSize:      opts.PageSize,
		hideUnchanged: opts.HideUnchanged,
	})
}

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

// AggregateForTest is the test-facing copy of dashAggregate using
// exported fields so handlers_test code can read them directly.
type AggregateForTest struct {
	Confs  map[string]DashConfAggForTest
	Order  []string
	Totals DashTotalsForTest
}

// AggregateDashboardForTest exposes aggregateDashboard for tests.
func (h *Handlers) AggregateDashboardForTest(logger *zerolog.Logger, views []inventory.MergedInventory, data *DashboardData) AggregateForTest {
	agg := h.aggregateDashboard(logger, views, data)
	out := make(map[string]DashConfAggForTest, len(agg.Confs))
	for k, v := range agg.Confs {
		out[k] = DashConfAggForTest{
			Src: v.Src, ID: v.ID,
			TotalRuns: v.TotalRuns, LoadedRuns: v.LoadedRuns,
			LatestRun: v.LatestRun, LatestState: v.LatestState,
			DiskBytes:    v.DiskBytes,
			LatestFiles:  v.LatestFiles,
			LatestBytes:  v.LatestBytes,
			LatestFormat: v.LatestFormat,
		}
	}

	return AggregateForTest{Confs: out, Order: agg.Order, Totals: DashTotalsForTest{
		Objects:       agg.Totals.objects,
		Bytes:         agg.Totals.bytes,
		Disk:          agg.Totals.disk,
		ManifestFiles: agg.Totals.manifestFiles,
		ManifestBytes: agg.Totals.manifestBytes,
	}}
}

// AddLoadedStatsForTest exposes addLoadedStats for tests.
func (h *Handlers) AddLoadedStatsForTest(logger *zerolog.Logger, v *inventory.MergedInventory, totals *DashTotalsForTest) {
	c := &dashConfAgg{Src: v.SourceBucket, ID: v.Name}
	t := dashTotals{objects: totals.Objects, bytes: totals.Bytes, disk: totals.Disk}
	h.addLoadedStats(logger, v, c, &t)
	totals.Objects = t.objects
	totals.Bytes = t.bytes
	totals.Disk = t.disk
}

// DashConfAggForTest mirrors dashConfAgg with exported fields for tests.
type DashConfAggForTest struct {
	Src          string
	ID           string
	LatestRun    string
	LatestState  inventory.State
	LatestFormat string
	TotalRuns    int
	LoadedRuns   int
	DiskBytes    int64
	LatestFiles  int
	LatestBytes  int64
}

// DashTotalsForTest mirrors dashTotals with exported fields for tests.
type DashTotalsForTest struct {
	Objects       uint64
	Bytes         uint64
	Disk          int64
	ManifestFiles int
	ManifestBytes int64
}

// Exported aliases for unexported helpers used by external tests.
var (
	NumericLabelForTest                = numericLabel
	FormatDeltaForTest                 = formatDelta
	FormatCostDeltaForTest             = formatCostDelta
	PctChangeForTest                   = pctChange
	AbsInt64ForTest                    = absInt64
	DescribeRunForTest                 = describeRun
	GroupManagerForAPIForTest          = groupManagerForAPI
	StatusRankForTest                  = statusRank
	GroupLoadedInventoriesForTst       = groupLoadedInventories
	BuildComparePickerForTest          = buildComparePicker
	SortCompareChildViewForTest        = sortCompareChildView
	SameConfigForTest                  = sameConfig
	FilterComparePickerByConfigForTest = filterComparePickerByConfig
	LoadDurationLabelForTest           = loadDurationLabel
	DiscoveredMetaLineForTest          = discoveredMetaLine
)
