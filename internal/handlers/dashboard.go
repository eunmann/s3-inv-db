package handlers

import (
	"net/http"
	"sort"

	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/pkg/humanfmt"
	"github.com/eunmann/s3-inv-db/pkg/indexread"
	"github.com/rs/zerolog"
)

// DashboardData contains data for the dashboard page.
type DashboardData struct {
	Title          string
	S3Source       string
	HasDiscovery   bool
	DiscoveryError string

	// Top stats — one per card.
	Configurations int // distinct (src, inv) pairs
	TotalRuns      int // total Inventory entries from discovery
	LoadedRuns     int // how many are currently in memory
	LoadingRuns    int // how many have a build in flight
	ErrorRuns      int // how many ended in error
	TotalObjectsH  string
	TotalBytesH    string
	DiskUsedH      string

	BudgetCapH      string
	BudgetUsedH     string
	BudgetReservedH string
	BudgetAvailH    string
	BudgetHeadroomH string
	BudgetUsedPct   int
	BudgetActive    bool

	AutoLoadConfigs   int
	AutoLoadFailures  int
	PendingPollErrors int

	Configs []DashboardConfig
}

// DashboardConfig is the per-configuration row shown on the dashboard.
type DashboardConfig struct {
	SourceBucket  string
	InventoryName string
	TotalRuns     int
	LoadedRuns    int
	LatestRun     string // run timestamp of the newest entry
	LatestState   inventory.State
	DiskBytesH    string // size on disk across this configuration's loaded runs
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
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		if err := h.renderer.Render(w, "dashboard.html", data); err != nil {
			logger.Error().Err(err).Msg("failed to render dashboard")
			http.Error(w, "failed to render page", http.StatusInternalServerError)
		}
		return
	}

	views, err := h.discovery.List(r.Context())
	if err != nil {
		logger.Error().Err(err).Msg("dashboard discovery failed")
		data.DiscoveryError = "Failed to list discovered inventories. See server logs for details."
	}

	confs, order, totals := h.aggregateDashboard(logger, views, &data)
	data.Configurations = len(confs)
	if totals.objects > 0 {
		data.TotalObjectsH = humanfmt.CountUint64(totals.objects)
	}
	if totals.bytes > 0 {
		data.TotalBytesH = humanfmt.BytesUint64(totals.bytes)
	}
	if totals.disk > 0 {
		data.DiskUsedH = humanfmt.BytesUint64(uint64(totals.disk))
	}
	h.fillBudgetCounters(&data)
	h.fillAutoLoadCounters(&data, views)

	// Stable, alphabetical order for the page rows.
	sort.Strings(order)
	for _, key := range order {
		c := confs[key]
		row := DashboardConfig{
			SourceBucket:  c.Src,
			InventoryName: c.ID,
			TotalRuns:     c.TotalRuns,
			LoadedRuns:    c.LoadedRuns,
			LatestRun:     c.LatestRun,
			LatestState:   c.LatestState,
		}
		if c.DiskBytes > 0 {
			row.DiskBytesH = humanfmt.BytesUint64(uint64(c.DiskBytes))
		}
		data.Configs = append(data.Configs, row)
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := h.renderer.Render(w, "dashboard.html", data); err != nil {
		logger.Error().Err(err).Msg("failed to render dashboard")
		http.Error(w, "failed to render page", http.StatusInternalServerError)
	}
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

func (h *Handlers) fillAutoLoadCounters(data *DashboardData, views []inventory.MergedInventory) {
	if h.configStore != nil {
		if configs, err := h.configStore.List(); err == nil {
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
	Src, ID     string
	TotalRuns   int
	LoadedRuns  int
	LatestRun   string
	LatestState inventory.State
	DiskBytes   int64
}

type dashTotals struct {
	objects uint64
	bytes   uint64
	disk    int64
}

func (h *Handlers) aggregateDashboard(logger *zerolog.Logger, views []inventory.MergedInventory, data *DashboardData) (confs map[string]*dashConfAgg, order []string, totals dashTotals) {
	confs = map[string]*dashConfAgg{}
	for i := range views {
		v := &views[i]
		key := v.ConfigID()
		data.TotalRuns++

		c, ok := confs[key]
		if !ok {
			c = &dashConfAgg{Src: v.SourceBucket, ID: v.InventoryName}
			confs[key] = c
			order = append(order, key)
		}
		c.TotalRuns++
		if c.LatestRun == "" && v.Run != "" {
			c.LatestRun = v.Run
			c.LatestState = v.State
		}
		h.tallyView(logger, v, c, data, &totals)
	}
	return confs, order, totals
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
	size, err := h.loader.CacheSizeBytes(v.SourceBucket, v.InventoryName, v.Run)
	if err != nil {
		return
	}
	totals.disk += size
	c.DiskBytes += size
}
