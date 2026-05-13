package handlers

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/internal/logctx"
	"github.com/eunmann/s3-inv-db/internal/s3disco"
	"github.com/go-chi/chi/v5"
)

// DiscoveredView is what the API returns for each discovered inventory.
// It merges the static S3 metadata with the live load state from the
// inventory Manager.
type DiscoveredView struct {
	s3disco.Inventory
	CompositeID string          `json:"composite_id"`
	State       inventory.State `json:"state"`
	Error       string          `json:"error,omitempty"`
	NodeCount   uint64          `json:"node_count,omitempty"`
	HasTierData bool            `json:"has_tier_data"`
}

// ListDiscoveredAPI lists inventories under the configured S3 source,
// merging in any current load state. Returns 503 when discovery is not
// configured (server started without --s3-source).
func (h *Handlers) ListDiscoveredAPI(w http.ResponseWriter, r *http.Request) {
	if h.discoverer == nil {
		WriteJSONError(w, http.StatusServiceUnavailable, "discovery not configured (start the server with --s3-source)")
		return
	}

	views, err := h.discoverAndMerge(r.Context())
	if err != nil {
		logctx.FromContext(r.Context()).Error().Err(err).Msg("discover inventories")
		WriteJSONError(w, http.StatusBadGateway, "failed to discover inventories")
		return
	}
	WriteJSON(w, http.StatusOK, views)
}

// LoadDiscoveredAPI registers a discovered inventory (if not yet known to
// the manager) and triggers a build into the local cache, ending in a
// loaded mmap-backed index. URL: /api/discovered/{src}/{id}/load.
func (h *Handlers) LoadDiscoveredAPI(w http.ResponseWriter, r *http.Request) {
	if h.discoverer == nil || h.loader == nil {
		WriteJSONError(w, http.StatusServiceUnavailable, "discovery not configured")
		return
	}
	src := chi.URLParam(r, "src")
	id := chi.URLParam(r, "id")
	if src == "" || id == "" {
		WriteJSONError(w, http.StatusBadRequest, "src and id are required")
		return
	}

	logger := logctx.FromContext(r.Context())
	disc, err := h.discoverer.Find(r.Context(), src, id)
	if err != nil {
		logger.Error().Err(err).Str("src", src).Str("id", id).Msg("find discovered inventory")
		WriteJSONError(w, http.StatusBadGateway, "failed to find inventory")
		return
	}
	if disc.ManifestKey == "" {
		WriteJSONError(w, http.StatusNotFound, "no completed runs for this inventory")
		return
	}

	composite := disc.CompositeID()
	manifestURI := fmt.Sprintf("s3://%s/%s", h.discoverer.Bucket(), disc.ManifestKey)

	// Idempotent register — ignore "already exists" so reloads work.
	if err := h.manager.Register(composite, src+"/"+id, manifestURI); err != nil && !errors.Is(err, inventory.ErrAlreadyExists) {
		logger.Error().Err(err).Msg("register discovered inventory")
		WriteJSONError(w, http.StatusInternalServerError, "failed to register inventory")
		return
	}

	// Decouple the build from the HTTP request lifetime: a user navigating
	// away or htmx cancelling on its end shouldn't poison the inventory
	// state with "context canceled". The build still terminates on server
	// shutdown because Manager.Close empties the inventory map and the
	// LoadWith post-build re-check then returns ErrNotFound.
	loadCtx := context.WithoutCancel(r.Context())
	err = h.manager.LoadWith(loadCtx, composite, func(ctx context.Context, _ inventory.Info) (string, error) {
		return h.loader.Build(ctx, src, id, manifestURI)
	})
	if err != nil {
		switch {
		case errors.Is(err, inventory.ErrInvalidState):
			WriteJSONError(w, http.StatusConflict, err.Error())
		case errors.Is(err, inventory.ErrNotFound):
			WriteJSONError(w, http.StatusNotFound, "inventory not found")
		default:
			logger.Error().Err(err).Str("composite", composite).Msg("load discovered inventory")
			WriteJSONError(w, http.StatusInternalServerError, "failed to load inventory")
		}
		return
	}

	info, _ := h.manager.Get(composite)
	WriteJSON(w, http.StatusOK, info)
}

// UnloadDiscoveredAPI unloads an inventory (closes mmap, keeps the cache
// dir for fast reload). URL: /api/discovered/{src}/{id}/unload.
func (h *Handlers) UnloadDiscoveredAPI(w http.ResponseWriter, r *http.Request) {
	src := chi.URLParam(r, "src")
	id := chi.URLParam(r, "id")
	composite := src + "/" + id
	if err := h.manager.Unload(composite); err != nil {
		writeManagerError(r.Context(), w, err, "unload")
		return
	}
	info, _ := h.manager.Get(composite)
	WriteJSON(w, http.StatusOK, info)
}

// EvictDiscoveredAPI unloads and removes the on-disk cache for an
// inventory. The S3 source is untouched; the user can re-load to rebuild.
// URL: DELETE /api/discovered/{src}/{id}.
func (h *Handlers) EvictDiscoveredAPI(w http.ResponseWriter, r *http.Request) {
	src := chi.URLParam(r, "src")
	id := chi.URLParam(r, "id")
	composite := src + "/" + id

	// Unload if loaded (ignore "not loaded" / "not found"; both mean
	// there's nothing to close).
	if err := h.manager.Unload(composite); err != nil &&
		!errors.Is(err, inventory.ErrInvalidState) &&
		!errors.Is(err, inventory.ErrNotFound) {
		writeManagerError(r.Context(), w, err, "unload")
		return
	}
	if err := h.manager.Remove(composite); err != nil && !errors.Is(err, inventory.ErrNotFound) {
		writeManagerError(r.Context(), w, err, "remove")
		return
	}
	if h.loader != nil {
		if err := h.loader.Evict(src, id); err != nil {
			logctx.FromContext(r.Context()).Warn().Err(err).Str("composite", composite).Msg("evict cache")
		}
	}
	WriteJSON(w, http.StatusOK, map[string]string{"status": "evicted"})
}

// discoverAndMerge fetches the current discovery list and overlays each
// entry with the inventory.Info state from the Manager.
func (h *Handlers) discoverAndMerge(ctx context.Context) ([]DiscoveredView, error) {
	discovered, err := h.discoverer.List(ctx)
	if err != nil {
		return nil, fmt.Errorf("discover: %w", err)
	}
	views := make([]DiscoveredView, 0, len(discovered))
	for _, d := range discovered {
		v := DiscoveredView{
			Inventory:   d,
			CompositeID: d.CompositeID(),
			State:       inventory.StatePending,
		}
		if info, ok := h.manager.Get(d.CompositeID()); ok {
			v.State = info.State
			v.Error = info.Error
			v.NodeCount = info.NodeCount
			v.HasTierData = info.HasTierData
		}
		views = append(views, v)
	}
	return views, nil
}

func writeManagerError(ctx context.Context, w http.ResponseWriter, err error, op string) {
	switch {
	case errors.Is(err, inventory.ErrNotFound):
		WriteJSONError(w, http.StatusNotFound, "inventory not found")
	case errors.Is(err, inventory.ErrInvalidState):
		WriteJSONError(w, http.StatusConflict, err.Error())
	default:
		logctx.FromContext(ctx).Error().Err(err).Str("op", op).Msg("manager error")
		WriteJSONError(w, http.StatusInternalServerError, "operation failed")
	}
}
