package handlers

import (
	"context"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/eunmann/s3-inv-db/internal/templates"
	"github.com/eunmann/s3-inv-db/pkg/humanfmt"
	"github.com/eunmann/s3-inv-db/pkg/pricing"
	"github.com/rs/zerolog"
)

// dashboardFailedShown caps the failed builds the dashboard names; the
// rest are a link away on the inventories page.
const dashboardFailedShown = 5

// DashboardData contains data for the dashboard page: what each
// inventory configuration holds first, the system's state after.
type DashboardData struct {
	// Total sums each configuration's latest loaded run; nil when none
	// is loaded.
	Total          *DashboardTotal
	Budget         *DashboardBudget
	Title          string
	DiscoveryError string
	SnapshotAge    string
	NextRefreshIn  string
	S3Source       string
	Configs        []DashboardConfig
	// FailedRuns are the first runs whose last build failed;
	// FailedCount counts them all.
	FailedRuns      []DiscoveredRowView
	FailedCount     int
	TotalRuns       int
	LoadedRuns      int
	BuildingRuns    int
	AutoLoadConfigs int
	AutoLoadIssues  int
	AutoLoadRunning bool
	HasDiscovery    bool
}

// MoreFailed counts the failed builds not named on the dashboard.
func (d DashboardData) MoreFailed() int { return d.FailedCount - len(d.FailedRuns) }

// DashboardConfig is one inventory configuration on the dashboard: its
// newest loaded run's totals, the change since the loaded run before
// it, and its newest run when that isn't the loaded one.
type DashboardConfig struct {
	// Loaded is the newest loaded run, with Stats; nil when none is.
	Loaded *DiscoveredRowView
	// Delta compares Loaded with the loaded run before it.
	Delta *RunDelta
	// Latest is the newest run; the same run as Loaded when that is
	// loaded.
	Latest       DiscoveredRowView
	SourceBucket string
	Name         string
	LoadedDateH  string
	LatestDateH  string
}

// DomID is the configuration's id made safe for HTML ids.
func (c DashboardConfig) DomID() string { return domID(c.SourceBucket + "/" + c.Name) }

// NewerRun: a run newer than the loaded one exists.
func (c DashboardConfig) NewerRun() bool {
	return c.Loaded != nil && c.Latest.Run != "" && c.Latest.Run != c.Loaded.Run
}

// DashboardTotal is the sum of each configuration's latest loaded run.
type DashboardTotal struct {
	ObjectsH string
	BytesH   string
	CostH    string // "" unless every summed run is priced
	Configs  int
}

// DashboardBudget is the index disk budget: a short line and bar, the
// detail in Detail.
type DashboardBudget struct {
	UsedH  string
	CapH   string
	Detail string
	Pct    int
}

// Dashboard renders the dashboard HTML page.
func (h *Handlers) Dashboard(w http.ResponseWriter, r *http.Request) {
	data := DashboardData{
		Title:           "Dashboard",
		S3Source:        h.s3SourceURI,
		HasDiscovery:    h.discovery.Enabled(),
		AutoLoadRunning: h.autoLoadRunning,
	}
	h.fillBudget(&data)
	if !data.HasDiscovery {
		h.renderHTML(w, r, "dashboard.html", "failed to render dashboard", data)

		return
	}

	ctx := r.Context()
	views, fetchedAt, err := h.discovery.Snapshot(ctx)
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msg("dashboard discovery failed")
		data.DiscoveryError = "Failed to list discovered inventories. See server logs for details."
	}
	if !fetchedAt.IsZero() {
		data.SnapshotAge = templates.Span(time.Since(fetchedAt))
		if h.discoveryRefreshInterval > 0 {
			remaining := time.Until(fetchedAt.Add(h.discoveryRefreshInterval))
			if remaining <= 0 {
				data.NextRefreshIn = "due now"
			} else {
				data.NextRefreshIn = templates.Span(remaining.Round(time.Second))
			}
		}
	}

	now := time.Now()
	for _, runs := range h.groupRuns(ctx, views) {
		tallyRuns(&data, runs)
		data.Configs = append(data.Configs, h.dashboardConfig(ctx, runs, now))
	}
	slices.SortFunc(data.Configs, func(a, b DashboardConfig) int {
		if c := strings.Compare(a.SourceBucket, b.SourceBucket); c != 0 {
			return c
		}

		return strings.Compare(a.Name, b.Name)
	})
	data.Total = sumLatestLoaded(data.Configs)
	h.fillAutoLoad(ctx, &data)

	h.renderHTML(w, r, "dashboard.html", "failed to render dashboard", data)
}

// tallyRuns counts one configuration's runs into the system status.
func tallyRuns(data *DashboardData, runs []DiscoveredRowView) {
	for i := range runs {
		r := &runs[i]
		if r.Run == "" {
			continue
		}
		data.TotalRuns++
		switch {
		case r.State.IsLoaded():
			data.LoadedRuns++
		case r.Busy():
			data.BuildingRuns++
		case r.Failed():
			data.FailedCount++
			if len(data.FailedRuns) < dashboardFailedShown {
				data.FailedRuns = append(data.FailedRuns, *r)
			}
		}
	}
}

// dashboardConfig summarizes one configuration's runs (newest first,
// linked): only its newest loaded run and the one before it are read.
func (h *Handlers) dashboardConfig(ctx context.Context, runs []DiscoveredRowView, now time.Time) DashboardConfig {
	c := DashboardConfig{SourceBucket: runs[0].SourceBucket, Name: runs[0].Name, Latest: runs[0]}
	c.LatestDateH = runDateLabel(c.Latest.Run, now)
	for i := range runs {
		if !runs[i].State.IsLoaded() || runs[i].Run == "" {
			continue
		}
		loaded := runs[i]
		h.addStats(ctx, &loaded)
		if loaded.Stats == nil {
			continue
		}
		c.Loaded = &loaded
		c.LoadedDateH = runDateLabel(loaded.Run, now)
		c.Delta = h.deltaFromPrevious(ctx, runs[i+1:], &loaded, now)

		break
	}

	return c
}

// deltaFromPrevious compares loaded with the loaded run before it among
// older, its configuration's older runs.
func (h *Handlers) deltaFromPrevious(ctx context.Context, older []DiscoveredRowView, loaded *DiscoveredRowView, now time.Time) *RunDelta {
	if loaded.PrevLoaded == "" {
		return nil
	}
	for i := range older {
		if older[i].CompositeID() != loaded.PrevLoaded {
			continue
		}
		prev := older[i]
		h.addStats(ctx, &prev)
		if prev.Stats == nil {
			return nil
		}
		d := newRunDelta(*prev.Stats, *loaded.Stats, prev.Run, now)

		return &d
	}

	return nil
}

// sumLatestLoaded totals each configuration's newest loaded run once:
// older loaded runs of a configuration are earlier views of the same
// bucket, not more data.
func sumLatestLoaded(configs []DashboardConfig) *DashboardTotal {
	var objects, bytes, cost uint64
	n, priced := 0, true
	for i := range configs {
		l := configs[i].Loaded
		if l == nil {
			continue
		}
		n++
		objects += l.Stats.Objects
		bytes += l.Stats.Bytes
		cost += l.Stats.CostMicro
		priced = priced && l.Stats.CostH != ""
	}
	if n == 0 {
		return nil
	}
	t := &DashboardTotal{Configs: n, ObjectsH: humanfmt.CountUint64(objects), BytesH: humanfmt.BytesUint64(bytes)}
	if priced {
		t.CostH = pricing.FormatCost(cost)
	}

	return t
}

func (h *Handlers) fillBudget(data *DashboardData) {
	capBytes := h.tracker.Cap()
	if capBytes == 0 {
		return
	}
	used, reserved := h.tracker.Used(), h.tracker.Reserved()
	data.Budget = &DashboardBudget{
		UsedH: humanfmt.BytesUint64(used),
		CapH:  humanfmt.BytesUint64(capBytes),
		Pct:   min(100, int(float64(used+reserved)/float64(capBytes)*100)),
		Detail: humanfmt.BytesUint64(used) + " used + " + humanfmt.BytesUint64(reserved) + " reserved for builds of " +
			humanfmt.BytesUint64(capBytes) + " (" + humanfmt.BytesUint64(h.tracker.Available()) + " available, " +
			humanfmt.BytesUint64(h.tracker.Headroom()) + " headroom)",
	}
}

func (h *Handlers) fillAutoLoad(ctx context.Context, data *DashboardData) {
	if configs, err := h.configStore.List(ctx); err == nil {
		for i := range configs {
			if configs[i].AutoLoad {
				data.AutoLoadConfigs++
			}
			if configs[i].LastPollError != "" {
				data.AutoLoadIssues++
			}
		}
	}
	infos := h.manager.List()
	for i := range infos {
		if infos[i].AutoLoadFailureCount > 0 {
			data.AutoLoadIssues++
		}
	}
}
