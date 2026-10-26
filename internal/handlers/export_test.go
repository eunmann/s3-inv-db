package handlers

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/internal/jobs"
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

// SetSSEPaceForTest swaps the SSE batch pacing for one test.
func SetSSEPaceForTest(t *testing.T, d time.Duration) {
	t.Helper()
	prev := ssePace
	ssePace = d
	t.Cleanup(func() { ssePace = prev })
}

// JobBusForTest returns the job bus wired into the handler.
func (h *Handlers) JobBusForTest() *jobs.Bus { return h.jobBus }

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

// CompareSidesForTest mirrors compareSides with exported fields.
type CompareSidesForTest struct {
	From, To  inventory.ID
	Note      string
	SingleRun bool
}

// ResolveCompareSidesForTest exposes resolveCompareSides for tests.
func ResolveCompareSidesForTest(picker ComparePicker, from, to inventory.ID) CompareSidesForTest {
	s := resolveCompareSides(picker, from, to)

	return CompareSidesForTest{From: s.from, To: s.to, Note: s.note, SingleRun: s.singleRun}
}

// GroupDiscoveredForAPIForTest exposes groupDiscoveredForAPI for tests.
func (h *Handlers) GroupDiscoveredForAPIForTest(r *http.Request, views []inventory.MergedInventory) []ConfigurationView {
	return h.groupDiscoveredForAPI(r, views)
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
	GroupLoadedInventoriesForTst       = groupLoadedInventories
	ShareOfForTest                     = shareOf
	BuildComparePickerForTest          = buildComparePicker
	SameConfigForTest                  = sameConfig
	FilterComparePickerByConfigForTest = filterComparePickerByConfig
	SummarizeBuildErrorForTest         = summarizeBuildError
	DomIDForTest                       = domID
	RunDateLabelForTest                = runDateLabel
	NewRunDeltaForTest                 = newRunDelta
	SummarizePollErrorForTest          = summarizePollError
)

// AcquireSSESlotForTest exposes acquireSSESlot.
func (h *Handlers) AcquireSSESlotForTest(ip string) int64 { return h.acquireSSESlot(ip) }

// ReleaseSSESlotForTest exposes releaseSSESlot.
func (h *Handlers) ReleaseSSESlotForTest(ip string) { h.releaseSSESlot(ip) }

// SSETrackedIPsForTest reports how many remote IPs hold a counter entry.
func (h *Handlers) SSETrackedIPsForTest() int {
	h.sseMu.Lock()
	defer h.sseMu.Unlock()

	return len(h.sseConnsByIP)
}
