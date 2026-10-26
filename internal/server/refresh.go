package server

import (
	"errors"
	"net/http"

	"github.com/eunmann/s3-inv-db/internal/handlers"
	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/rs/zerolog"
)

// refreshResponse is POST /api/indexes/refresh's answer.
type refreshResponse struct {
	// AutoLoad reports whether an auto-load poll was started.
	AutoLoad bool `json:"auto_load"`
}

// refreshIndexes lists the inventory or index source now instead of at
// the next refresh interval, and starts an auto-load poll, so a build
// that just published an index (or an inventory that just landed) is
// picked up at once. Anything that can make an HTTP request can call
// it: the last step of a build job, a Lambda, an EventBridge API
// destination, curl.
func (s *Server) refreshIndexes(w http.ResponseWriter, r *http.Request) {
	if err := s.discovery.Refresh(r.Context()); err != nil {
		if errors.Is(err, inventory.ErrDiscoveryDisabled) {
			handlers.WriteJSONError(w, http.StatusServiceUnavailable, "no s3_source or index_source configured")

			return
		}
		zerolog.Ctx(r.Context()).Warn().Err(err).Msg("refresh discovery")
		handlers.WriteJSONError(w, http.StatusBadGateway, "listing the source failed; see the server log")

		return
	}
	resp := refreshResponse{}
	if s.autoloader != nil {
		s.autoloader.PollNow()
		resp.AutoLoad = true
	}
	// The inventories page's Refresh button: reload it to show the new
	// listing.
	if r.Header.Get("HX-Request") == "true" {
		w.Header().Set("HX-Refresh", "true")
	}
	handlers.WriteJSON(w, http.StatusOK, resp)
}
