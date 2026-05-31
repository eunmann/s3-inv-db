package handlers

import (
	"fmt"
	"net/http"
	"net/url"

	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/pkg/format"
	"github.com/eunmann/s3-inv-db/pkg/humanfmt"
	"github.com/eunmann/s3-inv-db/pkg/indexread"
	"github.com/eunmann/s3-inv-db/pkg/pricing"
	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog"
)

// StatsResponse is the response for stats queries.
type StatsResponse struct {
	CostEstimate  *CostEstimate `json:"cost_estimate,omitempty"`
	Prefix        string        `json:"prefix"`
	ObjectCountH  string        `json:"object_count_human"`
	TotalBytesH   string        `json:"total_bytes_human"`
	TierBreakdown []TierStats   `json:"tier_breakdown,omitempty"`
	ObjectCount   uint64        `json:"object_count"`
	TotalBytes    uint64        `json:"total_bytes"`
}

// TierStats contains per-tier statistics.
type TierStats struct {
	TierName     string `json:"tier_name"`
	ObjectCountH string `json:"object_count_human"`
	BytesH       string `json:"bytes_human"`
	ObjectCount  uint64 `json:"object_count"`
	Bytes        uint64 `json:"bytes"`
}

// CostEstimate contains cost estimation details.
type CostEstimate struct {
	PerTierMicrodollars         map[string]uint64 `json:"per_tier_microdollars,omitempty"`
	PerTierFormatted            map[string]string `json:"per_tier_formatted,omitempty"`
	TotalFormatted              string            `json:"total_formatted"`
	TotalMicrodollars           uint64            `json:"total_microdollars"`
	MonitoringMicrodollars      uint64            `json:"monitoring_microdollars,omitempty"`
	MinObjectSizeMicrodollars   uint64            `json:"min_object_size_microdollars,omitempty"`
	GlacierOverheadMicrodollars uint64            `json:"glacier_overhead_microdollars,omitempty"`
}

// computeCostEstimate projects a tier breakdown through the price table
// and into the HTTP-shaped CostEstimate (with formatted strings).
// Returns nil when the breakdown is empty so callers can store the
// result unconditionally — omitempty on the field handles the JSON.
func (h *Handlers) computeCostEstimate(breakdown []format.TierBreakdown, includePerTier bool) *CostEstimate {
	if len(breakdown) == 0 {
		return nil
	}
	cost := pricing.ComputeMonthlyCost(breakdown, h.priceTable)
	est := &CostEstimate{
		TotalMicrodollars:           cost.TotalMicrodollars,
		TotalFormatted:              pricing.FormatCost(cost.TotalMicrodollars),
		MonitoringMicrodollars:      cost.MonitoringMicrodollars,
		MinObjectSizeMicrodollars:   cost.MinObjectSizeMicrodollars,
		GlacierOverheadMicrodollars: cost.GlacierOverheadMicrodollars,
	}
	if includePerTier && len(cost.PerTierMicrodollars) > 0 {
		est.PerTierMicrodollars = cost.PerTierMicrodollars
		est.PerTierFormatted = make(map[string]string, len(cost.PerTierMicrodollars))
		for tier, microdollars := range cost.PerTierMicrodollars {
			est.PerTierFormatted[tier] = pricing.FormatCost(microdollars)
		}
	}

	return est
}

// DescendantInfo contains info about a descendant prefix.
type DescendantInfo struct {
	Prefix       string `json:"prefix"`
	ObjectCountH string `json:"object_count_human"`
	TotalBytesH  string `json:"total_bytes_human"`
	ObjectCount  uint64 `json:"object_count"`
	TotalBytes   uint64 `json:"total_bytes"`
	Depth        uint32 `json:"depth"`
}

// GetStatsAPI returns stats for a prefix. Inventory_id comes from the
// query string (legacy / cross-inventory variant).
func (h *Handlers) GetStatsAPI(w http.ResponseWriter, r *http.Request) {
	id := inventory.ID(r.URL.Query().Get("inventory_id"))
	if id == "" {
		WriteJSONError(w, http.StatusBadRequest, "inventory_id is required")

		return
	}
	h.writeStatsForInventory(w, r, id)
}

// GetInventoryStatsAPI returns stats for a prefix within a specific
// inventory whose ID is in the URL path.
func (h *Handlers) GetInventoryStatsAPI(w http.ResponseWriter, r *http.Request) {
	h.writeStatsForInventory(w, r, inventory.ID(chi.URLParam(r, "id")))
}

// writeStatsForInventory backs both stats endpoints: GetStatsAPI (id
// from query) and GetInventoryStatsAPI (id from URL path).
func (h *Handlers) writeStatsForInventory(w http.ResponseWriter, r *http.Request, id inventory.ID) {
	q := r.URL.Query()
	if !q.Has("prefix") {
		WriteJSONError(w, http.StatusBadRequest, "prefix is required")

		return
	}
	prefix := q.Get("prefix")
	showTiers := q.Get("show_tiers") == trueLiteral
	estimateCost := q.Get("estimate_cost") == trueLiteral

	var resp *StatsResponse
	err := h.manager.WithIndex(id, func(idx *indexread.Index) error {
		var berr error
		resp, berr = h.buildStatsResponse(idx, prefix, showTiers, estimateCost)

		return berr
	})
	if err != nil {
		mr := managerErrorStatus(err)
		WriteJSONError(w, mr.Status, mr.Message)

		return
	}

	WriteJSON(w, http.StatusOK, resp)
}

// descendantsParams bundles the request knobs GetDescendantsAPI parses
// out of the query string.
type descendantsParams struct {
	prefix string
	depth  int
	filter indexread.Filter
}

// parseDescendantsParams reads the descendants-API query string and
// returns a populated params struct. On invalid input it writes a 400
// directly and returns ok=false; the caller should just return.
func parseDescendantsParams(w http.ResponseWriter, q url.Values) (descendantsParams, bool) {
	if !q.Has("prefix") {
		WriteJSONError(w, http.StatusBadRequest, "prefix query parameter is required")

		return descendantsParams{}, false
	}
	depth, err := parsePositiveInt(q, "depth", 1)
	if err != nil {
		WriteJSONError(w, http.StatusBadRequest, err.Error())

		return descendantsParams{}, false
	}
	filter, err := parseFilter(q)
	if err != nil {
		WriteJSONError(w, http.StatusBadRequest, err.Error())

		return descendantsParams{}, false
	}

	return descendantsParams{prefix: q.Get("prefix"), depth: depth, filter: filter}, true
}

// GetDescendantsAPI returns descendants at a specific depth.
func (h *Handlers) GetDescendantsAPI(w http.ResponseWriter, r *http.Request) {
	inventoryID := inventory.ID(chi.URLParam(r, "id"))
	params, ok := parseDescendantsParams(w, r.URL.Query())
	if !ok {
		return
	}
	prefix, depth, filter := params.prefix, params.depth, params.filter

	logger := zerolog.Ctx(r.Context())
	var descendants []DescendantInfo
	err := h.manager.WithIndex(inventoryID, func(idx *indexread.Index) error {
		pos, ok := idx.Lookup(prefix)
		if !ok {
			return errPrefixNotFound
		}
		positions, perr := idx.DescendantsAtDepthFiltered(pos, depth, filter)
		if perr != nil {
			logger.Error().Err(perr).Msg("failed to get descendants")

			return fmt.Errorf("descendants at depth: %w", perr)
		}
		descendants = make([]DescendantInfo, 0, len(positions))
		for _, p := range positions {
			prefixStr, pserr := idx.PrefixString(p)
			if pserr != nil {
				logger.Warn().Err(pserr).Uint64("pos", p).Msg("failed to get prefix string")

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

		return nil
	})
	if err != nil {
		resp := managerErrorStatus(err)
		WriteJSONError(w, resp.Status, resp.Message)

		return
	}

	WriteJSON(w, http.StatusOK, descendants)
}

func (h *Handlers) buildStatsResponse(idx *indexread.Index, prefix string, showTiers, estimateCost bool) (*StatsResponse, error) {
	pos, ok := idx.Lookup(prefix)
	if !ok {
		return nil, errPrefixNotFound
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
		if estimateCost {
			resp.CostEstimate = h.computeCostEstimate(breakdown, true)
		}
	} else if estimateCost {
		resp.CostEstimate = h.computeCostEstimate(idx.TierBreakdown(pos), false)
	}

	return resp, nil
}
