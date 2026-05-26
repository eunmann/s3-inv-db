package handlers

import (
	"fmt"
	"net/http"
	"strconv"

	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/pkg/humanfmt"
	"github.com/eunmann/s3-inv-db/pkg/indexread"
	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog"
)

// TopResultRow is a single entry in a /api/top response.
type TopResultRow struct {
	Prefix       string `json:"prefix"`
	ObjectCountH string `json:"object_count_human"`
	TotalBytesH  string `json:"total_bytes_human"`
	ObjectCount  uint64 `json:"object_count"`
	TotalBytes   uint64 `json:"total_bytes"`
}

// TopResponse wraps the result set with the query that produced it.
type TopResponse struct {
	Parent        string         `json:"parent"`
	By            string         `json:"by"`
	RelativeDepth int            `json:"relative_depth"`
	Results       []TopResultRow `json:"results"`
}

// GetTopAPI returns the top-N descendants at a relative depth, ranked by
// object count or total bytes.
//
// Query params (all required unless defaulted):
//   - inventory_id (when path doesn't carry it)
//   - prefix       parent prefix (empty = root)
//   - depth        relative depth (default 1)
//   - limit        max rows (default 25, hard-capped to 1000)
//   - by           bytes|count (default bytes)
//   - min_count    optional filter
//   - min_bytes    optional filter
func (h *Handlers) GetTopAPI(w http.ResponseWriter, r *http.Request) {
	id := inventory.ID(chi.URLParam(r, "id"))
	if id == "" {
		id = inventory.ID(r.URL.Query().Get("inventory_id"))
	}
	if id == "" {
		WriteJSONError(w, http.StatusBadRequest, "inventory_id is required")

		return
	}
	q := r.URL.Query()
	prefix := q.Get("prefix")

	depth := 1
	if v := q.Get("depth"); v != "" {
		d, err := strconv.Atoi(v)
		if err != nil || d < 1 {
			WriteJSONError(w, http.StatusBadRequest, "invalid depth")

			return
		}
		depth = d
	}

	limit := defaultTopLimit
	if v := q.Get("limit"); v != "" {
		l, err := strconv.Atoi(v)
		if err != nil || l < 1 {
			WriteJSONError(w, http.StatusBadRequest, "invalid limit")

			return
		}
		if l > maxTopLimit {
			l = maxTopLimit
		}
		limit = l
	}

	by := q.Get("by")
	if by == "" {
		by = "bytes"
	}
	var metric indexread.TopMetric
	switch by {
	case "bytes":
		metric = indexread.TopByBytesMetric
	case "count":
		metric = indexread.TopByCountMetric
	default:
		WriteJSONError(w, http.StatusBadRequest, "by must be bytes or count")

		return
	}

	filter, ok := parseTopFilter(w, q)
	if !ok {
		return
	}

	logger := zerolog.Ctx(r.Context())
	var resp TopResponse
	err := h.manager.WithIndex(id, func(idx *indexread.Index) error {
		pos, found := idx.Lookup(prefix)
		if !found {
			return errPrefixNotFound
		}
		results, terr := idx.TopFiltered(pos, depth, limit, metric, filter)
		if terr != nil {
			logger.Error().Err(terr).Msg("top query failed")

			return fmt.Errorf("top: %w", terr)
		}
		rows := make([]TopResultRow, 0, len(results))
		for _, res := range results {
			name, pserr := idx.PrefixString(res.Pos)
			if pserr != nil {
				logger.Warn().Err(pserr).Uint64("pos", res.Pos).Msg("failed to get prefix string")

				continue
			}
			rows = append(rows, TopResultRow{
				Prefix:       name,
				ObjectCount:  res.Stats.ObjectCount,
				ObjectCountH: humanfmt.CountUint64(res.Stats.ObjectCount),
				TotalBytes:   res.Stats.TotalBytes,
				TotalBytesH:  humanfmt.BytesUint64(res.Stats.TotalBytes),
			})
		}
		resp = TopResponse{Parent: prefix, By: by, RelativeDepth: depth, Results: rows}

		return nil
	})
	if err != nil {
		mr := managerErrorStatus(err)
		WriteJSONError(w, mr.Status, mr.Message)

		return
	}

	WriteJSON(w, http.StatusOK, resp)
}

func parseTopFilter(w http.ResponseWriter, q map[string][]string) (indexread.Filter, bool) {
	var filter indexread.Filter
	if v := firstQ(q, "min_count"); v != "" {
		n, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			WriteJSONError(w, http.StatusBadRequest, "invalid min_count")

			return filter, false
		}
		filter.MinCount = n
	}
	if v := firstQ(q, "min_bytes"); v != "" {
		n, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			WriteJSONError(w, http.StatusBadRequest, "invalid min_bytes")

			return filter, false
		}
		filter.MinBytes = n
	}

	return filter, true
}

func firstQ(q map[string][]string, key string) string {
	v, ok := q[key]
	if !ok || len(v) == 0 {
		return ""
	}

	return v[0]
}

const (
	defaultTopLimit = 25
	maxTopLimit     = 1000
)
