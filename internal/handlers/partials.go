package handlers

import (
	"context"
	"errors"
	"net/http"

	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/internal/jobs"
	"github.com/eunmann/s3-inv-db/internal/s3disco"
	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog"
)

// The htmx-facing /partials/* routes mutate state via the inventory
// Manager or DiscoveryService and return the updated row's HTML directly
// so the client can swap it into the DOM without a full page reload.

// LoadInventoryRowPartial loads a (non-discovered) inventory and returns
// its updated row.
func (h *Handlers) LoadInventoryRowPartial(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if err := h.manager.Load(context.WithoutCancel(r.Context()), id); err != nil {
		respondManagerErrorHTML(w, r, err, "load inventory")
		return
	}
	h.renderInventoryRow(w, r, id)
}

// UnloadInventoryRowPartial unloads an inventory and returns its updated row.
func (h *Handlers) UnloadInventoryRowPartial(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if err := h.manager.Unload(id); err != nil {
		respondManagerErrorHTML(w, r, err, "unload inventory")
		return
	}
	h.renderInventoryRow(w, r, id)
}

// DeleteInventoryRowPartial removes an inventory and returns an empty body
// so htmx's outerHTML swap removes the row.
func (h *Handlers) DeleteInventoryRowPartial(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if err := h.manager.Remove(id); err != nil && !errors.Is(err, inventory.ErrNotFound) {
		respondManagerErrorHTML(w, r, err, "delete inventory")
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
}

// LoadDiscoveredRowPartial submits a background build job and returns
// the row in queued state with HTTP 202 immediately. The UI watches the
// SSE stream for state transitions and swaps the row when the job moves.
func (h *Handlers) LoadDiscoveredRowPartial(w http.ResponseWriter, r *http.Request) {
	src := chi.URLParam(r, "src")
	id := chi.URLParam(r, "id")
	logger := zerolog.Ctx(r.Context())

	disc, err := h.discovery.Find(r.Context(), src, id)
	if err != nil {
		logger.Error().Err(err).Str("src", src).Str("id", id).Msg("find discovered inventory")
		http.Error(w, "failed to find inventory", http.StatusBadGateway)
		return
	}
	if disc.ManifestKey == "" {
		http.Error(w, "no completed runs for this inventory", http.StatusNotFound)
		return
	}
	if h.jobMgr == nil {
		http.Error(w, "jobs not configured", http.StatusServiceUnavailable)
		return
	}

	if err := h.discovery.PrepareDiscovered(disc); err != nil {
		respondManagerErrorHTML(w, r, err, "prepare discovered inventory")
		return
	}
	composite := disc.CompositeID()

	// Reject double-submit: if a job is already queued or running for
	// this inventory, render the row with its current state instead of
	// spawning a duplicate that will fail with an InvalidState error.
	if existing, err := h.jobStore.LatestForInventory(composite); err == nil && existing.State.IsLive() {
		h.renderDiscoveredRowFrom(w, r, disc)
		return
	}

	// The job context is intentionally independent of the request — a
	// build outlives the HTTP request that started it.
	//nolint:contextcheck // job owns its own lifetime
	_, err = h.jobMgr.Submit(composite, jobs.KindBuild, func(ctx context.Context, report func(jobs.Update)) error {
		return h.discovery.LoadWith(ctx, disc, func(stage string, done, total int64) {
			report(jobs.Update{Stage: stage, BytesDone: done, BytesTotal: total})
		})
	})
	if err != nil {
		respondManagerErrorHTML(w, r, err, "submit load job")
		return
	}
	w.WriteHeader(http.StatusAccepted)
	h.renderDiscoveredRowFrom(w, r, disc)
}

// CancelJob cancels an in-flight job by ID. 404 if not currently live.
func (h *Handlers) CancelJob(w http.ResponseWriter, r *http.Request) {
	if h.jobMgr == nil {
		http.Error(w, "jobs not configured", http.StatusServiceUnavailable)
		return
	}
	id := chi.URLParam(r, "id")
	if err := h.jobMgr.Cancel(id); err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusAccepted)
}

// UnloadDiscoveredRowPartial unloads a discovered inventory and returns its row.
func (h *Handlers) UnloadDiscoveredRowPartial(w http.ResponseWriter, r *http.Request) {
	src := chi.URLParam(r, "src")
	id := chi.URLParam(r, "id")
	composite := src + "/" + id
	if err := h.manager.Unload(composite); err != nil {
		respondManagerErrorHTML(w, r, err, "unload inventory")
		return
	}
	h.renderDiscoveredRow(w, r, src, id)
}

// DiscoveredRowPartial returns the current state of one discovered row
// — used by htmx to refresh after an SSE notification.
func (h *Handlers) DiscoveredRowPartial(w http.ResponseWriter, r *http.Request) {
	src := chi.URLParam(r, "src")
	id := chi.URLParam(r, "id")
	h.renderDiscoveredRow(w, r, src, id)
}

// renderInventoryRow looks up the inventory in the manager and writes the
// inventory_row.html partial. Used by the htmx-facing partial handlers.
func (h *Handlers) renderInventoryRow(w http.ResponseWriter, r *http.Request, id string) {
	info, ok := h.manager.Get(id)
	if !ok {
		http.Error(w, "inventory not found", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := h.renderer.RenderPartial(w, "inventory_row.html", info); err != nil {
		zerolog.Ctx(r.Context()).Error().Err(err).Msg("render inventory row")
		http.Error(w, "failed to render row", http.StatusInternalServerError)
	}
}

// renderDiscoveredRow re-fetches the discovery entry, merges in current
// manager state, and renders the discovered_row.html partial.
func (h *Handlers) renderDiscoveredRow(w http.ResponseWriter, r *http.Request, src, id string) {
	logger := zerolog.Ctx(r.Context())
	disc, err := h.discovery.Find(r.Context(), src, id)
	if err != nil {
		logger.Error().Err(err).Str("src", src).Str("id", id).Msg("find discovered inventory for row render")
		http.Error(w, "failed to render row", http.StatusBadGateway)
		return
	}
	h.renderDiscoveredRowFrom(w, r, disc)
}

// DiscoveredRowView extends the merged inventory with the latest job
// for the same composite ID. The template surfaces progress and
// renders Retry/Cancel buttons based on the job's state.
type DiscoveredRowView struct {
	inventory.MergedInventory
	LatestJob *jobs.Job
}

// renderDiscoveredRowFrom renders a discovered_row using a pre-fetched
// disc value. Looks up the latest job (if jobs are configured) so the
// row can render progress / cancel / retry.
func (h *Handlers) renderDiscoveredRowFrom(w http.ResponseWriter, r *http.Request, disc s3disco.Inventory) {
	view := DiscoveredRowView{
		MergedInventory: inventory.MergedInventory{Inventory: disc, State: inventory.StateNotLoaded},
	}
	if info, ok := h.manager.Get(disc.CompositeID()); ok {
		view.State = info.State
		view.Error = info.Error
		view.NodeCount = info.NodeCount
		view.HasTierData = info.HasTierData
	}
	if h.jobStore != nil {
		j, err := h.jobStore.LatestForInventory(disc.CompositeID())
		switch {
		case err == nil:
			view.LatestJob = &j
		case errors.Is(err, jobs.ErrStoreNotFound):
			// No prior job — render the row without LatestJob.
		default:
			zerolog.Ctx(r.Context()).Warn().Err(err).
				Str("composite", disc.CompositeID()).
				Msg("look up latest job for row render")
		}
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := h.renderer.RenderPartial(w, "discovered_row.html", view); err != nil {
		zerolog.Ctx(r.Context()).Error().Err(err).Msg("render discovered row")
		http.Error(w, "failed to render row", http.StatusInternalServerError)
	}
}
