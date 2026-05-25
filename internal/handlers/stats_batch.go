package handlers

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/pkg/humanfmt"
	"github.com/eunmann/s3-inv-db/pkg/indexread"
	"github.com/go-chi/chi/v5"
)

// defaultBatchStatsMax bounds a single batch when no configured override
// is set. 1000 keeps a worst-case response within ~megabytes.
const defaultBatchStatsMax = 1000

// BatchStatsRequest is the POST body for /api/inventories/{id}/stats:batch.
type BatchStatsRequest struct {
	Prefixes     []string `json:"prefixes"`
	ShowTiers    bool     `json:"show_tiers,omitempty"`
	EstimateCost bool     `json:"estimate_cost,omitempty"`
}

// BatchStatsRow is one result in a batch response. Found=false means
// the prefix had no entry in this index.
type BatchStatsRow struct {
	Prefix        string        `json:"prefix"`
	Found         bool          `json:"found"`
	ObjectCount   uint64        `json:"object_count,omitempty"`
	TotalBytes    uint64        `json:"total_bytes,omitempty"`
	ObjectCountH  string        `json:"object_count_human,omitempty"`
	TotalBytesH   string        `json:"total_bytes_human,omitempty"`
	TierBreakdown []TierStats   `json:"tier_breakdown,omitempty"`
	CostEstimate  *CostEstimate `json:"cost_estimate,omitempty"`
}

// BatchStatsResponse wraps the per-prefix results.
type BatchStatsResponse struct {
	InventoryID string          `json:"inventory_id"`
	Rows        []BatchStatsRow `json:"rows"`
}

// PostBatchStatsAPI runs N prefix lookups in one round-trip.
//
// The id path param identifies the inventory; the body carries the
// prefix list and optional tier/cost flags. Empty or oversize lists
// produce 400. Per-row missing prefixes are returned with Found=false
// rather than aborting the whole batch — partial visibility is more
// useful than an all-or-nothing failure.
func (h *Handlers) PostBatchStatsAPI(w http.ResponseWriter, r *http.Request) {
	id := inventory.ID(chi.URLParam(r, "id"))
	if id == "" {
		id = inventory.ID(r.URL.Query().Get("inventory_id"))
	}
	if id == "" {
		WriteJSONError(w, http.StatusBadRequest, "inventory_id is required")

		return
	}

	var req BatchStatsRequest
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		WriteJSONError(w, http.StatusBadRequest, fmt.Sprintf("invalid JSON: %v", err))

		return
	}
	if len(req.Prefixes) == 0 {
		WriteJSONError(w, http.StatusBadRequest, "prefixes is required")

		return
	}
	maxBatch := h.queryBatchMax
	if maxBatch <= 0 {
		maxBatch = defaultBatchStatsMax
	}
	if len(req.Prefixes) > maxBatch {
		WriteJSONError(w, http.StatusBadRequest, fmt.Sprintf("too many prefixes: %d > %d", len(req.Prefixes), maxBatch))

		return
	}

	rows := make([]BatchStatsRow, 0, len(req.Prefixes))
	err := h.manager.WithIndex(id, func(idx *indexread.Index) error {
		for _, p := range req.Prefixes {
			rows = append(rows, h.lookupBatchRow(idx, p, req.ShowTiers, req.EstimateCost))
		}

		return nil
	})
	if err != nil {
		mr := managerErrorStatus(err)
		WriteJSONError(w, mr.Status, mr.Message)

		return
	}

	WriteJSON(w, http.StatusOK, BatchStatsResponse{InventoryID: string(id), Rows: rows})
}

func (h *Handlers) lookupBatchRow(idx *indexread.Index, prefix string, showTiers, estimateCost bool) BatchStatsRow {
	pos, ok := idx.Lookup(prefix)
	if !ok {
		return BatchStatsRow{Prefix: prefix, Found: false}
	}
	stats := idx.Stats(pos)
	row := BatchStatsRow{
		Prefix:       prefix,
		Found:        true,
		ObjectCount:  stats.ObjectCount,
		TotalBytes:   stats.TotalBytes,
		ObjectCountH: humanfmt.CountUint64(stats.ObjectCount),
		TotalBytesH:  humanfmt.BytesUint64(stats.TotalBytes),
	}
	if showTiers && idx.HasTierData() {
		breakdown := idx.TierBreakdown(pos)
		row.TierBreakdown = make([]TierStats, 0, len(breakdown))
		for _, tb := range breakdown {
			row.TierBreakdown = append(row.TierBreakdown, TierStats{
				TierName:     tb.TierName,
				ObjectCount:  tb.ObjectCount,
				ObjectCountH: humanfmt.CountUint64(tb.ObjectCount),
				Bytes:        tb.Bytes,
				BytesH:       humanfmt.BytesUint64(tb.Bytes),
			})
		}
		if estimateCost {
			row.CostEstimate = h.computeCostEstimate(breakdown, true)
		}
	} else if estimateCost {
		row.CostEstimate = h.computeCostEstimate(idx.TierBreakdown(pos), false)
	}

	return row
}
