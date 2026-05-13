package handlers

import (
	"net/http"

	"github.com/eunmann/s3-inv-db/internal/inventory"
)

// DashboardData contains data for the dashboard page.
type DashboardData struct {
	Title        string
	TotalCount   int
	LoadedCount  int
	PendingCount int
	ErrorCount   int
	Inventories  []inventory.Info
	TotalNodes   uint64
	TotalNodesH  string
	HasTierData  bool
	S3Source     string
	HasDiscovery bool
}

// Dashboard renders the dashboard HTML page. When discovery is configured
// the counts and rows reflect S3-discovered inventories merged with their
// current load state; otherwise they come from the Manager directly.
func (h *Handlers) Dashboard(w http.ResponseWriter, r *http.Request) {
	data := DashboardData{
		Title:        "Dashboard",
		S3Source:     h.s3SourceURI,
		HasDiscovery: h.discoverer != nil,
	}

	var infos []inventory.Info
	if h.discoverer != nil {
		views, err := h.discoverAndMerge(r.Context())
		if err != nil {
			h.logger.Warn().Err(err).Msg("dashboard discovery failed; falling back to manager")
		} else {
			infos = make([]inventory.Info, 0, len(views))
			for i := range views {
				v := &views[i]
				infos = append(infos, inventory.Info{
					ID:          v.CompositeID,
					Name:        v.SourceBucket + " / " + v.InventoryID,
					Path:        v.ManifestKey,
					State:       v.State,
					NodeCount:   v.NodeCount,
					HasTierData: v.HasTierData,
				})
			}
		}
	}
	if infos == nil {
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
		h.logger.Error().Err(err).Msg("failed to render dashboard")
		http.Error(w, "failed to render page", http.StatusInternalServerError)
	}
}
