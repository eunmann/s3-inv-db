package handlers

import (
	"net/http"

	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/internal/logctx"
)

// DashboardData contains data for the dashboard page.
type DashboardData struct {
	Title          string
	TotalCount     int
	LoadedCount    int
	PendingCount   int
	ErrorCount     int
	Inventories    []inventory.Info
	TotalNodes     uint64
	TotalNodesH    string
	HasTierData    bool
	S3Source       string
	HasDiscovery   bool
	DiscoveryError string // non-empty when discovery is configured but failed
}

// Dashboard renders the dashboard HTML page. When discovery is configured
// the counts and rows reflect S3-discovered inventories merged with their
// current load state; otherwise they come from the Manager directly.
func (h *Handlers) Dashboard(w http.ResponseWriter, r *http.Request) {
	data := DashboardData{
		Title:        "Dashboard",
		S3Source:     h.s3SourceURI,
		HasDiscovery: h.discovery.Enabled(),
	}

	logger := logctx.FromContext(r.Context())
	var infos []inventory.Info
	if h.discovery.Enabled() {
		views, err := h.discovery.List(r.Context())
		if err != nil {
			logger.Error().Err(err).Msg("dashboard discovery failed")
			data.DiscoveryError = "Failed to list discovered inventories. See server logs for details."
		} else {
			infos = make([]inventory.Info, 0, len(views))
			for i := range views {
				v := &views[i]
				infos = append(infos, inventory.Info{
					ID:          v.CompositeID(),
					Name:        v.SourceBucket + " / " + v.InventoryID,
					Path:        v.ManifestKey,
					State:       v.State,
					NodeCount:   v.NodeCount,
					HasTierData: v.HasTierData,
				})
			}
		}
	} else {
		infos = h.manager.List()
	}

	data.Inventories = infos
	data.TotalCount = len(infos)
	for i := range infos {
		switch infos[i].State {
		case inventory.StateLoaded:
			data.LoadedCount++
			data.TotalNodes += infos[i].NodeCount
			if infos[i].HasTierData {
				data.HasTierData = true
			}
		case inventory.StatePending:
			data.PendingCount++
		case inventory.StateError:
			data.ErrorCount++
		}
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := h.renderer.Render(w, "dashboard.html", data); err != nil {
		logger.Error().Err(err).Msg("failed to render dashboard")
		http.Error(w, "failed to render page", http.StatusInternalServerError)
	}
}
