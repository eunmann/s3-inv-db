package handlers

import (
	"context"
	"net/http"
	"slices"
	"time"

	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/pkg/humanfmt"
	"github.com/eunmann/s3-inv-db/pkg/indexread"
	"github.com/rs/zerolog"
)

// DashboardData contains data for the dashboard page.
type DashboardData struct {
	BudgetAvailH      string
	DiskUsedH         string
	BudgetCapH        string
	DiscoveryError    string
	SnapshotAge       string
	BudgetHeadroomH   string
	ManifestBytesH    string
	Title             string
	BudgetReservedH   string
	BudgetUsedH       string
	TotalObjectsH     string
	TotalBytesH       string
	S3Source          string
	Configs           []DashboardConfig
	LoadedRuns        int
	AutoLoadConfigs   int
	ErrorRuns         int
	LoadingRuns       int
	TotalRuns         int
	Configurations    int
	BudgetUsedPct     int
	ManifestFiles     int
	AutoLoadFailures  int
	PendingPollErrors int
	BudgetActive      bool
	HasDiscovery      bool
}

// DashboardConfig is the per-configuration row shown on the dashboard.
type DashboardConfig struct {
	SourceBucket string
	Name         string
	LatestRun    string
	LatestState  inventory.State
	DiskBytesH   string
	LatestBytesH string
	LatestFormat string
	TotalRuns    int
	LoadedRuns   int
	LatestFiles  int
}

// Dashboard renders the dashboard HTML page.
func (h *Handlers) Dashboard(w http.ResponseWriter, r *http.Request) {
	data := DashboardData{
		Title:        "Dashboard",
		S3Source:     h.s3SourceURI,
		HasDiscovery: h.discovery.Enabled(),
	}

	logger := zerolog.Ctx(r.Context())
	if !h.discovery.Enabled() {
		h.renderHTML(w, r, "dashboard.html", "failed to render dashboard", data)

		return
	}

	views, fetchedAt, err := h.discovery.Snapshot(r.Context())
	if err != nil {
		logger.Error().Err(err).Msg("dashboard discovery failed")
		data.DiscoveryError = "Failed to list discovered inventories. See server logs for details."
	}
	if !fetchedAt.IsZero() {
		data.SnapshotAge = humanfmt.Duration(time.Since(fetchedAt))
	}

	agg := h.aggregateDashboard(logger, views, &data)
	data.Configurations = len(agg.Confs)
	if agg.Totals.objects > 0 {
		data.TotalObjectsH = humanfmt.CountUint64(agg.Totals.objects)
	}
	if agg.Totals.bytes > 0 {
		data.TotalBytesH = humanfmt.BytesUint64(agg.Totals.bytes)
	}
	if agg.Totals.disk > 0 {
		data.DiskUsedH = humanfmt.BytesUint64(uint64(agg.Totals.disk))
	}
	data.ManifestFiles = agg.Totals.manifestFiles
	if agg.Totals.manifestBytes > 0 {
		data.ManifestBytesH = humanfmt.BytesUint64(uint64(agg.Totals.manifestBytes))
	}
	h.fillBudgetCounters(&data)
	h.fillAutoLoadCounters(r.Context(), &data, views)

	// Stable, alphabetical order for the page rows.
	slices.Sort(agg.Order)
	for _, key := range agg.Order {
		c := agg.Confs[key]
		row := DashboardConfig{
			SourceBucket: c.Src,
			Name:         c.ID,
			TotalRuns:    c.TotalRuns,
			LoadedRuns:   c.LoadedRuns,
			LatestRun:    c.LatestRun,
			LatestState:  c.LatestState,
			LatestFiles:  c.LatestFiles,
			LatestFormat: c.LatestFormat,
		}
		if c.DiskBytes > 0 {
			row.DiskBytesH = humanfmt.BytesUint64(uint64(c.DiskBytes))
		}
		if c.LatestBytes > 0 {
			row.LatestBytesH = humanfmt.BytesUint64(uint64(c.LatestBytes))
		}
		data.Configs = append(data.Configs, row)
	}

	h.renderHTML(w, r, "dashboard.html", "failed to render dashboard", data)
}

func (h *Handlers) fillBudgetCounters(data *DashboardData) {
	if h.tracker == nil || h.tracker.Cap() == 0 {
		return
	}
	capBytes := h.tracker.Cap()
	used := h.tracker.Used()
	data.BudgetActive = true
	data.BudgetCapH = humanfmt.BytesUint64(capBytes)
	data.BudgetUsedH = humanfmt.BytesUint64(used)
	data.BudgetReservedH = humanfmt.BytesUint64(h.tracker.Reserved())
	data.BudgetAvailH = humanfmt.BytesUint64(h.tracker.Available())
	data.BudgetHeadroomH = humanfmt.BytesUint64(h.tracker.Headroom())
	data.BudgetUsedPct = int((float64(used+h.tracker.Reserved()) / float64(capBytes)) * 100)
}

func (h *Handlers) fillAutoLoadCounters(ctx context.Context, data *DashboardData, views []inventory.MergedInventory) {
	if h.configStore != nil {
		if configs, err := h.configStore.List(ctx); err == nil {
			for i := range configs {
				if configs[i].AutoLoad {
					data.AutoLoadConfigs++
				}
				if configs[i].LastPollError != "" {
					data.PendingPollErrors++
				}
			}
		}
	}
	for i := range views {
		id := views[i].CompositeID()
		if id == "" {
			continue
		}
		if info, ok := h.manager.Get(id); ok && info.AutoLoadFailureCount > 0 {
			data.AutoLoadFailures++
		}
	}
}

type dashConfAgg struct {
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

type dashTotals struct {
	objects       uint64
	bytes         uint64
	disk          int64
	manifestFiles int
	manifestBytes int64
}

// dashAggregate bundles aggregateDashboard's three outputs so the
// caller doesn't juggle a triple return.
type dashAggregate struct {
	Confs  map[string]*dashConfAgg
	Order  []string
	Totals dashTotals
}

func (h *Handlers) aggregateDashboard(logger *zerolog.Logger, views []inventory.MergedInventory, data *DashboardData) dashAggregate {
	agg := dashAggregate{Confs: map[string]*dashConfAgg{}}
	for i := range views {
		v := &views[i]
		key := v.ConfigID()
		data.TotalRuns++

		c, ok := agg.Confs[key]
		if !ok {
			c = &dashConfAgg{Src: v.SourceBucket, ID: v.Name}
			agg.Confs[key] = c
			agg.Order = append(agg.Order, key)
		}
		c.TotalRuns++
		if c.LatestRun == "" && v.Run != "" {
			c.LatestRun = v.Run
			c.LatestState = v.State
			c.LatestFiles = v.FileCount
			c.LatestBytes = v.TotalBytes
			c.LatestFormat = v.FileFormat
		}
		agg.Totals.manifestFiles += v.FileCount
		agg.Totals.manifestBytes += v.TotalBytes
		h.tallyView(logger, v, c, data, &agg.Totals)
	}

	return agg
}

func (h *Handlers) tallyView(logger *zerolog.Logger, v *inventory.MergedInventory, c *dashConfAgg, data *DashboardData, totals *dashTotals) {
	switch v.State {
	case inventory.StateLoaded:
		data.LoadedRuns++
		c.LoadedRuns++
		h.addLoadedStats(logger, v, c, totals)
	case inventory.StateLoading:
		data.LoadingRuns++
	case inventory.StateError:
		data.ErrorRuns++
	case inventory.StateNotLoaded:
		// Not counted; placeholder for a run that has been discovered
		// but not loaded into the manager yet.
	}
}

func (h *Handlers) addLoadedStats(logger *zerolog.Logger, v *inventory.MergedInventory, c *dashConfAgg, totals *dashTotals) {
	err := h.manager.WithIndex(v.CompositeID(), func(idx *indexread.Index) error {
		if pos, ok := idx.Lookup(""); ok {
			stats := idx.Stats(pos)
			totals.objects += stats.ObjectCount
			totals.bytes += stats.TotalBytes
		}

		return nil
	})
	if err != nil {
		logger.Warn().Err(err).Stringer("id", v.CompositeID()).Msg("dashboard root stats")
	}
	if h.loader == nil {
		return
	}
	size, err := h.loader.CacheSizeBytes(v.SourceBucket, v.Name, v.Run)
	if err != nil {
		return
	}
	totals.disk += size
	c.DiskBytes += size
}
