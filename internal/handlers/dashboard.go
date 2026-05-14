package handlers

import (
	"net/http"
	"sort"

	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/pkg/humanfmt"
	"github.com/rs/zerolog"
)

// DashboardData contains data for the dashboard page.
type DashboardData struct {
	Title          string
	S3Source       string
	HasDiscovery   bool
	DiscoveryError string

	// Top stats — one per card.
	Configurations int    // distinct (src, inv) pairs
	TotalRuns      int    // total Inventory entries from discovery
	LoadedRuns     int    // how many are currently in memory
	LoadingRuns    int    // how many have a build in flight
	ErrorRuns      int    // how many ended in error
	TotalNodesH    string // sum of NodeCount across loaded runs
	DiskUsedH      string // sum of cache bytes across loaded runs

	// One summary row per configuration.
	Configs []DashboardConfig
}

// DashboardConfig is the per-configuration row shown on the dashboard.
type DashboardConfig struct {
	SourceBucket string
	InventoryID  string
	TotalRuns    int
	LoadedRuns   int
	LatestRun    string // run timestamp of the newest entry
	LatestState  inventory.State
	DiskBytesH   string // size on disk across this configuration's loaded runs
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

	// Aggregate per configuration in one pass.
	type confAgg struct {
		Src, ID     string
		TotalRuns   int
		LoadedRuns  int
		LatestRun   string
		LatestState inventory.State
		DiskBytes   int64
	}
	confs := map[string]*confAgg{}
	order := []string{}
	var totalNodes uint64
	var totalDisk int64

	for i := range views {
		v := &views[i]
		key := v.ConfigID()
		data.TotalRuns++

		c, ok := confs[key]
		if !ok {
			c = &confAgg{Src: v.SourceBucket, ID: v.InventoryID}
			confs[key] = c
			order = append(order, key)
		}
		c.TotalRuns++
		// Discovery returns runs newest-first within a config, so the
		// first one we see is the latest.
		if c.LatestRun == "" && v.Run != "" {
			c.LatestRun = v.Run
			c.LatestState = v.State
		}
		switch v.State {
		case inventory.StateLoaded:
			data.LoadedRuns++
			c.LoadedRuns++
			totalNodes += v.NodeCount
			if h.loader != nil {
				size, err := h.loader.CacheSizeBytes(v.SourceBucket, v.InventoryID, v.Run)
				if err == nil {
					totalDisk += size
					c.DiskBytes += size
				}
			}
		case inventory.StateLoading:
			data.LoadingRuns++
		case inventory.StateError:
			data.ErrorRuns++
		}
	}

	data.Configurations = len(confs)
	if totalNodes > 0 {
		data.TotalNodesH = humanfmt.CountUint64(totalNodes)
	}
	if totalDisk > 0 {
		data.DiskUsedH = humanfmt.BytesUint64(uint64(totalDisk))
	}

	// Stable, alphabetical order for the page rows.
	sort.Strings(order)
	for _, key := range order {
		c := confs[key]
		row := DashboardConfig{
			SourceBucket: c.Src,
			InventoryID:  c.ID,
			TotalRuns:    c.TotalRuns,
			LoadedRuns:   c.LoadedRuns,
			LatestRun:    c.LatestRun,
			LatestState:  c.LatestState,
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
