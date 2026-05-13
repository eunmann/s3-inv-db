package handlers

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/pkg/humanfmt"
	"github.com/eunmann/s3-inv-db/pkg/indexread"
	"github.com/eunmann/s3-inv-db/pkg/pricing"
	"github.com/go-chi/chi/v5"
)

// StatsResponse is the response for stats queries.
type StatsResponse struct {
	Prefix        string        `json:"prefix"`
	ObjectCount   uint64        `json:"object_count"`
	ObjectCountH  string        `json:"object_count_human"`
	TotalBytes    uint64        `json:"total_bytes"`
	TotalBytesH   string        `json:"total_bytes_human"`
	TierBreakdown []TierStats   `json:"tier_breakdown,omitempty"`
	CostEstimate  *CostEstimate `json:"cost_estimate,omitempty"`
}

// TierStats contains per-tier statistics.
type TierStats struct {
	TierName     string `json:"tier_name"`
	ObjectCount  uint64 `json:"object_count"`
	ObjectCountH string `json:"object_count_human"`
	Bytes        uint64 `json:"bytes"`
	BytesH       string `json:"bytes_human"`
}

// CostEstimate contains cost estimation details.
type CostEstimate struct {
	TotalMicrodollars           uint64            `json:"total_microdollars"`
	TotalFormatted              string            `json:"total_formatted"`
	PerTierMicrodollars         map[string]uint64 `json:"per_tier_microdollars,omitempty"`
	PerTierFormatted            map[string]string `json:"per_tier_formatted,omitempty"`
	MonitoringMicrodollars      uint64            `json:"monitoring_microdollars,omitempty"`
	MinObjectSizeMicrodollars   uint64            `json:"min_object_size_microdollars,omitempty"`
	GlacierOverheadMicrodollars uint64            `json:"glacier_overhead_microdollars,omitempty"`
}

// DescendantInfo contains info about a descendant prefix.
type DescendantInfo struct {
	Prefix       string `json:"prefix"`
	ObjectCount  uint64 `json:"object_count"`
	ObjectCountH string `json:"object_count_human"`
	TotalBytes   uint64 `json:"total_bytes"`
	TotalBytesH  string `json:"total_bytes_human"`
	Depth        uint32 `json:"depth"`
}

// GetStatsAPI returns stats for a prefix.
func (h *Handlers) GetStatsAPI(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	inventoryID := q.Get("inventory_id")
	showTiers := q.Get("show_tiers") == "true"
	estimateCost := q.Get("estimate_cost") == "true"

	if inventoryID == "" {
		WriteJSONError(w, http.StatusBadRequest, "inventory_id is required")
		return
	}
	if !q.Has("prefix") {
		WriteJSONError(w, http.StatusBadRequest, "prefix is required")
		return
	}
	prefix := q.Get("prefix")

	idx, err := h.manager.GetIndex(inventoryID)
	if err != nil {
		if errors.Is(err, inventory.ErrNotFound) {
			WriteJSONError(w, http.StatusNotFound, "inventory not found")
			return
		}
		if errors.Is(err, inventory.ErrNotLoaded) {
			WriteJSONError(w, http.StatusConflict, "inventory not loaded")
			return
		}
		WriteJSONError(w, http.StatusInternalServerError, "failed to get index")
		return
	}

	resp, err := h.buildStatsResponse(idx, prefix, showTiers, estimateCost)
	if err != nil {
		WriteJSONError(w, http.StatusNotFound, err.Error())
		return
	}

	WriteJSON(w, http.StatusOK, resp)
}

// GetInventoryStatsAPI returns stats for a prefix within a specific inventory.
func (h *Handlers) GetInventoryStatsAPI(w http.ResponseWriter, r *http.Request) {
	inventoryID := chi.URLParam(r, "id")
	q := r.URL.Query()
	showTiers := q.Get("show_tiers") == "true"
	estimateCost := q.Get("estimate_cost") == "true"

	if !q.Has("prefix") {
		WriteJSONError(w, http.StatusBadRequest, "prefix query parameter is required")
		return
	}
	prefix := q.Get("prefix")

	idx, err := h.manager.GetIndex(inventoryID)
	if err != nil {
		if errors.Is(err, inventory.ErrNotFound) {
			WriteJSONError(w, http.StatusNotFound, "inventory not found")
			return
		}
		if errors.Is(err, inventory.ErrNotLoaded) {
			WriteJSONError(w, http.StatusConflict, "inventory not loaded")
			return
		}
		WriteJSONError(w, http.StatusInternalServerError, "failed to get index")
		return
	}

	resp, err := h.buildStatsResponse(idx, prefix, showTiers, estimateCost)
	if err != nil {
		WriteJSONError(w, http.StatusNotFound, err.Error())
		return
	}

	WriteJSON(w, http.StatusOK, resp)
}

// GetDescendantsAPI returns descendants at a specific depth.
func (h *Handlers) GetDescendantsAPI(w http.ResponseWriter, r *http.Request) {
	inventoryID := chi.URLParam(r, "id")
	q := r.URL.Query()
	depthStr := q.Get("depth")
	minCountStr := q.Get("min_count")
	minBytesStr := q.Get("min_bytes")

	if !q.Has("prefix") {
		WriteJSONError(w, http.StatusBadRequest, "prefix query parameter is required")
		return
	}
	prefix := q.Get("prefix")

	depth := 1
	if depthStr != "" {
		var err error
		depth, err = strconv.Atoi(depthStr)
		if err != nil || depth < 1 {
			WriteJSONError(w, http.StatusBadRequest, "invalid depth")
			return
		}
	}

	var filter indexread.Filter
	if minCountStr != "" {
		v, err := strconv.ParseUint(minCountStr, 10, 64)
		if err != nil {
			WriteJSONError(w, http.StatusBadRequest, "invalid min_count")
			return
		}
		filter.MinCount = v
	}
	if minBytesStr != "" {
		v, err := strconv.ParseUint(minBytesStr, 10, 64)
		if err != nil {
			WriteJSONError(w, http.StatusBadRequest, "invalid min_bytes")
			return
		}
		filter.MinBytes = v
	}

	idx, err := h.manager.GetIndex(inventoryID)
	if err != nil {
		if errors.Is(err, inventory.ErrNotFound) {
			WriteJSONError(w, http.StatusNotFound, "inventory not found")
			return
		}
		if errors.Is(err, inventory.ErrNotLoaded) {
			WriteJSONError(w, http.StatusConflict, "inventory not loaded")
			return
		}
		WriteJSONError(w, http.StatusInternalServerError, "failed to get index")
		return
	}

	pos, ok := idx.Lookup(prefix)
	if !ok {
		WriteJSONError(w, http.StatusNotFound, "prefix not found")
		return
	}

	positions, err := idx.DescendantsAtDepthFiltered(pos, depth, filter)
	if err != nil {
		h.logger.Error().Err(err).Msg("failed to get descendants")
		WriteJSONError(w, http.StatusInternalServerError, "failed to get descendants")
		return
	}

	descendants := make([]DescendantInfo, 0, len(positions))
	for _, p := range positions {
		prefixStr, err := idx.PrefixString(p)
		if err != nil {
			h.logger.Warn().Err(err).Uint64("pos", p).Msg("failed to get prefix string")
			continue
		}

		stats := idx.Stats(p)
		descendants = append(descendants, DescendantInfo{
			Prefix:       prefixStr,
			ObjectCount:  stats.ObjectCount,
			ObjectCountH: humanfmt.CountUint64(stats.ObjectCount),
			TotalBytes:   stats.TotalBytes,
			TotalBytesH:  humanfmt.BytesUint64(stats.TotalBytes),
			Depth:        idx.Depth(p),
		})
	}

	WriteJSON(w, http.StatusOK, descendants)
}

func (h *Handlers) buildStatsResponse(idx *indexread.Index, prefix string, showTiers, estimateCost bool) (*StatsResponse, error) {
	pos, ok := idx.Lookup(prefix)
	if !ok {
		return nil, errors.New("prefix not found")
	}

	stats := idx.Stats(pos)
	resp := &StatsResponse{
		Prefix:       prefix,
		ObjectCount:  stats.ObjectCount,
		ObjectCountH: humanfmt.CountUint64(stats.ObjectCount),
		TotalBytes:   stats.TotalBytes,
		TotalBytesH:  humanfmt.BytesUint64(stats.TotalBytes),
	}

	if showTiers && idx.HasTierData() {
		breakdown := idx.TierBreakdown(pos)
		resp.TierBreakdown = make([]TierStats, 0, len(breakdown))
		for _, tb := range breakdown {
			resp.TierBreakdown = append(resp.TierBreakdown, TierStats{
				TierName:     tb.TierName,
				ObjectCount:  tb.ObjectCount,
				ObjectCountH: humanfmt.CountUint64(tb.ObjectCount),
				Bytes:        tb.Bytes,
				BytesH:       humanfmt.BytesUint64(tb.Bytes),
			})
		}

		if estimateCost && len(breakdown) > 0 {
			cost := pricing.ComputeMonthlyCost(breakdown, h.priceTable)
			resp.CostEstimate = &CostEstimate{
				TotalMicrodollars:           cost.TotalMicrodollars,
				TotalFormatted:              pricing.FormatCost(cost.TotalMicrodollars),
				MonitoringMicrodollars:      cost.MonitoringMicrodollars,
				MinObjectSizeMicrodollars:   cost.MinObjectSizeMicrodollars,
				GlacierOverheadMicrodollars: cost.GlacierOverheadMicrodollars,
			}

			if len(cost.PerTierMicrodollars) > 0 {
				resp.CostEstimate.PerTierMicrodollars = cost.PerTierMicrodollars
				resp.CostEstimate.PerTierFormatted = make(map[string]string, len(cost.PerTierMicrodollars))
				for tier, microdollars := range cost.PerTierMicrodollars {
					resp.CostEstimate.PerTierFormatted[tier] = pricing.FormatCost(microdollars)
				}
			}
		}
	} else if estimateCost && !showTiers {
		// If cost estimation requested without tier breakdown
		breakdown := idx.TierBreakdown(pos)
		if len(breakdown) > 0 {
			cost := pricing.ComputeMonthlyCost(breakdown, h.priceTable)
			resp.CostEstimate = &CostEstimate{
				TotalMicrodollars: cost.TotalMicrodollars,
				TotalFormatted:    pricing.FormatCost(cost.TotalMicrodollars),
			}
		}
	}

	return resp, nil
}
