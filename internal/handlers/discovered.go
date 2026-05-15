package handlers

import (
	"errors"
	"net/http"

	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/rs/zerolog"
)

// ListDiscoveredAPI lists inventories under the configured S3 source,
// merging in any current load state. Returns 503 when discovery is not
// configured (server started without --s3-source).
//
// The mutating /api/discovered routes (load/unload/evict) were removed
// in favour of the /partials/discovered/* HTML routes — there were no
// JSON-API callers and the duplication invited divergence between the
// two response shapes.
func (h *Handlers) ListDiscoveredAPI(w http.ResponseWriter, r *http.Request) {
	views, _, err := h.discovery.Snapshot(r.Context())
	if err != nil {
		if errors.Is(err, inventory.ErrDiscoveryDisabled) {
			WriteJSONError(w, http.StatusServiceUnavailable, "discovery not configured (start the server with --s3-source)")

			return
		}
		zerolog.Ctx(r.Context()).Error().Err(err).Msg("discover inventories")
		WriteJSONError(w, http.StatusBadGateway, "failed to discover inventories")

		return
	}
	WriteJSON(w, http.StatusOK, views)
}
