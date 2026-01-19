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
}

// Dashboard renders the dashboard HTML page.
func (h *Handlers) Dashboard(w http.ResponseWriter, _ *http.Request) {
	inventories := h.manager.List()

	data := DashboardData{
		Title:       "Dashboard",
		Inventories: inventories,
		TotalCount:  len(inventories),
	}

	for i := range inventories {
		switch inventories[i].State {
		case inventory.StateLoaded:
			data.LoadedCount++
			data.TotalNodes += inventories[i].NodeCount
			if inventories[i].HasTierData {
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
