package handlers

import (
	"context"
	"errors"
	"net/http"

	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/internal/logctx"
	"github.com/eunmann/s3-inv-db/internal/s3disco"
	"github.com/go-chi/chi/v5"
)

// The htmx-facing /partials/* routes mutate state via the inventory
// Manager and return the updated row's HTML directly so the client can
// swap it into the DOM without a full page reload. This is the SSR side
// of the equivalent JSON /api/* routes.

// LoadInventoryRowPartial loads an inventory and returns its updated row.
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

// LoadDiscoveredRowPartial loads a discovered inventory and returns its row.
func (h *Handlers) LoadDiscoveredRowPartial(w http.ResponseWriter, r *http.Request) {
	if h.discoverer == nil || h.loader == nil {
		http.Error(w, "discovery not configured", http.StatusServiceUnavailable)
		return
	}
	src := chi.URLParam(r, "src")
	id := chi.URLParam(r, "id")

	logger := logctx.FromContext(r.Context())
	disc, err := h.discoverer.Find(r.Context(), src, id)
	if err != nil {
		logger.Error().Err(err).Str("src", src).Str("id", id).Msg("find discovered inventory")
		http.Error(w, "failed to find inventory", http.StatusBadGateway)
		return
	}
	if disc.ManifestKey == "" {
		http.Error(w, "no completed runs for this inventory", http.StatusNotFound)
		return
	}
	composite := disc.CompositeID()
	manifestURI := "s3://" + h.discoverer.Bucket() + "/" + disc.ManifestKey

	if err := h.manager.Register(composite, src+"/"+id, manifestURI); err != nil && !errors.Is(err, inventory.ErrAlreadyExists) {
		logger.Error().Err(err).Msg("register discovered inventory")
		http.Error(w, "failed to register inventory", http.StatusInternalServerError)
		return
	}

	loadCtx := context.WithoutCancel(r.Context())
	err = h.manager.LoadWith(loadCtx, composite, func(ctx context.Context, _ inventory.Info) (string, error) {
		return h.loader.Build(ctx, src, id, manifestURI)
	})
	if err != nil {
		respondManagerErrorHTML(w, r, err, "load discovered inventory")
		return
	}
	// Reuse the disc we already fetched at the top of this handler so a
	// transient post-load S3 hiccup doesn't surface a 502 that hides the
	// successful load.
	h.renderDiscoveredRowFrom(w, r, disc)
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

// EvictDiscoveredRowPartial evicts a discovered inventory (unload + remove +
// cache wipe) and returns an empty body so the row is removed via outerHTML.
func (h *Handlers) EvictDiscoveredRowPartial(w http.ResponseWriter, r *http.Request) {
	src := chi.URLParam(r, "src")
	id := chi.URLParam(r, "id")
	composite := src + "/" + id

	if err := h.manager.Unload(composite); err != nil &&
		!errors.Is(err, inventory.ErrInvalidState) &&
		!errors.Is(err, inventory.ErrNotFound) {
		respondManagerErrorHTML(w, r, err, "unload inventory")
		return
	}
	if err := h.manager.Remove(composite); err != nil && !errors.Is(err, inventory.ErrNotFound) {
		respondManagerErrorHTML(w, r, err, "remove inventory")
		return
	}
	if h.loader != nil {
		if err := h.loader.Evict(src, id); err != nil {
			logctx.FromContext(r.Context()).Warn().Err(err).Str("composite", composite).Msg("evict cache")
		}
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
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
		logctx.FromContext(r.Context()).Error().Err(err).Msg("render inventory row")
		http.Error(w, "failed to render row", http.StatusInternalServerError)
	}
}

// renderDiscoveredRow re-fetches the discovery entry, merges in current
// manager state, and renders the discovered_row.html partial.
func (h *Handlers) renderDiscoveredRow(w http.ResponseWriter, r *http.Request, src, id string) {
	logger := logctx.FromContext(r.Context())
	disc, err := h.discoverer.Find(r.Context(), src, id)
	if err != nil {
		logger.Error().Err(err).Str("src", src).Str("id", id).Msg("find discovered inventory for row render")
		http.Error(w, "failed to render row", http.StatusBadGateway)
		return
	}
	h.renderDiscoveredRowFrom(w, r, disc)
}

// renderDiscoveredRowFrom renders a discovered_row using a pre-fetched
// disc value. Splitting this out lets callers that already have a
// trusted disc (e.g. just before a successful load) skip the second
// S3 round-trip and avoid surfacing transient post-load errors.
func (h *Handlers) renderDiscoveredRowFrom(w http.ResponseWriter, r *http.Request, disc s3disco.Inventory) {
	view := DiscoveredView{
		Inventory:   disc,
		CompositeID: disc.CompositeID(),
		State:       inventory.StatePending,
	}
	if info, ok := h.manager.Get(disc.CompositeID()); ok {
		view.State = info.State
		view.Error = info.Error
		view.NodeCount = info.NodeCount
		view.HasTierData = info.HasTierData
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := h.renderer.RenderPartial(w, "discovered_row.html", view); err != nil {
		logctx.FromContext(r.Context()).Error().Err(err).Msg("render discovered row")
		http.Error(w, "failed to render row", http.StatusInternalServerError)
	}
}
