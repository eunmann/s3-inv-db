package handlers

import (
	"errors"
	"net/http"

	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/internal/jobs"
	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog"
)

// RetryJob looks up a terminal-failure job and submits a fresh build
// for the same inventory. Returns 202 on success. Live or non-build
// jobs return 409; missing jobs return 404; non-3-part inventory IDs
// (registered placeholders) return 400.
func (h *Handlers) RetryJob(w http.ResponseWriter, r *http.Request) {
	id := jobs.ID(chi.URLParam(r, "id"))
	if h.jobStore == nil {
		http.Error(w, "job store not configured", http.StatusServiceUnavailable)

		return
	}
	prev, err := h.jobStore.Get(r.Context(), id)
	switch {
	case errors.Is(err, jobs.ErrStoreNotFound):
		http.Error(w, "job not found", http.StatusNotFound)

		return
	case err != nil:
		zerolog.Ctx(r.Context()).Warn().Err(err).Stringer("job", id).Msg("look up job for retry")
		http.Error(w, "look up job", http.StatusInternalServerError)

		return
	}
	if prev.Kind != jobs.KindBuild {
		http.Error(w, "only build jobs can be retried", http.StatusConflict)

		return
	}
	if !prev.State.IsTerminal() || prev.State == jobs.StateSucceeded {
		http.Error(w, "job is not in a retryable state", http.StatusConflict)

		return
	}

	parts := prev.InventoryID.Split()
	if !parts.OK {
		http.Error(w, "inventory id is not a 3-part composite", http.StatusBadRequest)

		return
	}
	disc, err := h.discovery.Find(r.Context(), parts.Source, parts.Inventory, parts.Run)
	if err != nil {
		zerolog.Ctx(r.Context()).Error().Err(err).Stringer("inv", prev.InventoryID).Msg("rediscover inventory for retry")
		http.Error(w, "rediscover inventory", http.StatusBadGateway)

		return
	}
	if disc.ManifestKey == "" || disc.Run == "" {
		http.Error(w, "no completed run for this inventory", http.StatusNotFound)

		return
	}
	if err := h.discovery.PrepareDiscovered(r.Context(), disc); err != nil {
		respondManagerError(w, r, err, "prepare discovered inventory")

		return
	}
	composite := disc.CompositeID()
	if info, ok := h.manager.Get(composite); ok && info.State == inventory.StateLoaded {
		w.WriteHeader(http.StatusAccepted)

		return
	}

	err = h.submitDiscoveredLoadJob(r.Context(), composite, disc)
	switch {
	case errors.Is(err, jobs.ErrDuplicateInventory):
		w.WriteHeader(http.StatusAccepted)

		return
	case err != nil:
		respondManagerError(w, r, err, "submit retry job")

		return
	}
	w.WriteHeader(http.StatusAccepted)
}
