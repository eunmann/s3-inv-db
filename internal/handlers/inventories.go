package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/internal/jobs"
	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog"
)

// RegisterInventoryRequest is the request body for registering a new inventory.
type RegisterInventoryRequest struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	Path string `json:"path"`
}

// ListInventoriesAPI returns a JSON list of all inventories.
func (h *Handlers) ListInventoriesAPI(w http.ResponseWriter, _ *http.Request) {
	inventories := h.manager.List()
	WriteJSON(w, http.StatusOK, inventories)
}

// ConfigurationsResponse is the shape returned by ListConfigurationsAPI.
// Top-level wrapping object so callers can negotiate envelope additions
// (paging, filters) without a breaking change.
type ConfigurationsResponse struct {
	Configurations   []ConfigurationView `json:"configurations"`
	DiscoveryEnabled bool                `json:"discovery_enabled"`
}

// ConfigurationView is one (source bucket + inventory configuration)
// pair and its runs.
type ConfigurationView struct {
	SourceBucket  string          `json:"source_bucket"`
	InventoryName string          `json:"inventory_name"`
	Runs          []ConfigRunView `json:"runs"`
}

// ConfigRunView is one run's identity + lifecycle state. Bytes-on-disk
// is reported when the loader can measure it.
type ConfigRunView struct {
	ID          inventory.ID `json:"id"` // "<src>/<inv>/<run>"
	Run         string       `json:"run"`
	State       string       `json:"state"`
	Error       string       `json:"error,omitempty"`
	NodeCount   uint64       `json:"node_count,omitempty"`
	LoadedAt    string       `json:"loaded_at,omitempty"` // RFC3339
	CacheBytes  int64        `json:"cache_bytes,omitempty"`
	HasTierData bool         `json:"has_tier_data"`
	ManifestKey string       `json:"manifest_key,omitempty"`
}

// ListConfigurationsAPI returns inventory configurations grouped by
// (source bucket, inventory ID). When discovery is enabled the list
// covers every discovered run merged with its current Manager state;
// otherwise it returns whatever's in the Manager keyed by composite
// ID — useful for "list what's loadable" via one round trip.
func (h *Handlers) ListConfigurationsAPI(w http.ResponseWriter, r *http.Request) {
	resp := ConfigurationsResponse{DiscoveryEnabled: h.discovery.Enabled()}
	if h.discovery.Enabled() {
		views, err := h.discovery.List(r.Context())
		if err != nil {
			zerolog.Ctx(r.Context()).Error().Err(err).Msg("api configurations discovery")
			WriteJSONError(w, http.StatusBadGateway, "failed to list discovered inventories")
			return
		}
		resp.Configurations = h.groupDiscoveredForAPI(r, views)
	} else {
		resp.Configurations = groupManagerForAPI(h.manager.List())
	}
	WriteJSON(w, http.StatusOK, resp)
}

// groupDiscoveredForAPI maps MergedInventory views into the API shape.
// Same grouping the UI uses, but JSON-tagged + with cache-bytes when
// the loader can measure on-disk size.
func (h *Handlers) groupDiscoveredForAPI(_ *http.Request, views []inventory.MergedInventory) []ConfigurationView {
	groups := map[string]int{}
	out := []ConfigurationView{}
	for i := range views {
		v := &views[i]
		key := v.ConfigID()
		idx, ok := groups[key]
		if !ok {
			groups[key] = len(out)
			idx = len(out)
			out = append(out, ConfigurationView{
				SourceBucket:  v.SourceBucket,
				InventoryName: v.InventoryName,
			})
		}
		run := ConfigRunView{
			ID:          v.CompositeID(),
			Run:         v.Run,
			State:       string(v.State),
			Error:       v.Error,
			NodeCount:   v.NodeCount,
			HasTierData: v.HasTierData,
			ManifestKey: v.ManifestKey,
		}
		if info, ok := h.manager.Get(v.CompositeID()); ok && !info.LoadedAt.IsZero() {
			run.LoadedAt = info.LoadedAt.UTC().Format(time.RFC3339)
		}
		if h.loader != nil && v.Run != "" {
			if n, err := h.loader.CacheSizeBytes(v.SourceBucket, v.InventoryName, v.Run); err == nil {
				run.CacheBytes = n
			}
		}
		out[idx].Runs = append(out[idx].Runs, run)
	}
	return out
}

// groupManagerForAPI groups by parsing the composite ID. Used when
// discovery is disabled (no S3 source configured) — the Manager is
// the only source of truth.
func groupManagerForAPI(all []inventory.Info) []ConfigurationView {
	groups := map[string]int{}
	out := []ConfigurationView{}
	for i := range all {
		info := all[i]
		src, inv, run, ok := info.ID.Split()
		if !ok {
			// Fallback for legacy 2-part or hand-registered IDs: bucket
			// them into a single "_other_" group so they remain visible.
			src, inv = "_other_", string(info.ID)
		}
		key := src + "/" + inv
		idx, exists := groups[key]
		if !exists {
			groups[key] = len(out)
			idx = len(out)
			out = append(out, ConfigurationView{SourceBucket: src, InventoryName: inv})
		}
		view := ConfigRunView{
			ID:          info.ID,
			Run:         run,
			State:       string(info.State),
			Error:       info.Error,
			NodeCount:   info.NodeCount,
			HasTierData: info.HasTierData,
		}
		if !info.LoadedAt.IsZero() {
			view.LoadedAt = info.LoadedAt.UTC().Format(time.RFC3339)
		}
		out[idx].Runs = append(out[idx].Runs, view)
	}
	return out
}

// maxRegisterBodyBytes caps the registration request body. Three short
// strings (id, name, path) easily fit in 4 KiB; anything bigger is a
// client mistake or an attempted resource exhaustion.
const maxRegisterBodyBytes = 4 * 1024

// RegisterInventoryAPI registers a new inventory.
func (h *Handlers) RegisterInventoryAPI(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxRegisterBodyBytes)
	var req RegisterInventoryRequest
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		zerolog.Ctx(r.Context()).Debug().Err(err).Msg("invalid register body")
		WriteJSONError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}

	if req.ID == "" {
		WriteJSONError(w, http.StatusBadRequest, "id is required")
		return
	}
	if req.Name == "" {
		WriteJSONError(w, http.StatusBadRequest, "name is required")
		return
	}
	if req.Path == "" {
		WriteJSONError(w, http.StatusBadRequest, "path is required")
		return
	}

	id := inventory.ID(req.ID)
	if err := h.manager.Register(id, req.Name, req.Path); err != nil {
		if errors.Is(err, inventory.ErrAlreadyExists) {
			WriteJSONError(w, http.StatusConflict, "inventory already exists")
			return
		}
		zerolog.Ctx(r.Context()).Error().Err(err).Msg("failed to register inventory")
		WriteJSONError(w, http.StatusInternalServerError, "failed to register inventory")
		return
	}

	info, ok := h.manager.Get(id)
	if !ok {
		// Concurrent Remove between Register and Get.
		zerolog.Ctx(r.Context()).Warn().Stringer("id", id).Msg("inventory removed concurrently with create")
		WriteJSONError(w, http.StatusGone, "inventory was removed concurrently")
		return
	}
	WriteJSON(w, http.StatusCreated, info)
}

// GetInventoryAPI returns a single inventory by ID.
func (h *Handlers) GetInventoryAPI(w http.ResponseWriter, r *http.Request) {
	id := inventory.ID(chi.URLParam(r, "id"))

	info, exists := h.manager.Get(id)
	if !exists {
		WriteJSONError(w, http.StatusNotFound, "inventory not found")
		return
	}

	WriteJSON(w, http.StatusOK, info)
}

// LoadInventoryAPI loads an inventory index.
func (h *Handlers) LoadInventoryAPI(w http.ResponseWriter, r *http.Request) {
	id := inventory.ID(chi.URLParam(r, "id"))

	// Build runs under WithoutCancel so a navigate-away or htmx-side
	// cancellation doesn't poison the inventory state with "context
	// canceled". Server shutdown still terminates it via Manager.Close.
	loadCtx := context.WithoutCancel(r.Context())
	if err := h.manager.Load(loadCtx, id); err != nil {
		if errors.Is(err, inventory.ErrNotFound) {
			WriteJSONError(w, http.StatusNotFound, "inventory not found")
			return
		}
		if errors.Is(err, inventory.ErrInvalidState) {
			WriteJSONError(w, http.StatusConflict, err.Error())
			return
		}
		zerolog.Ctx(r.Context()).Error().Err(err).Stringer("id", id).Msg("failed to load inventory")
		WriteJSONError(w, http.StatusInternalServerError, "failed to load inventory")
		return
	}

	info, ok := h.manager.Get(id)
	if !ok {
		zerolog.Ctx(r.Context()).Warn().Stringer("id", id).Msg("inventory removed concurrently with load")
		WriteJSONError(w, http.StatusGone, "inventory was removed concurrently")
		return
	}
	WriteJSON(w, http.StatusOK, info)
}

// UnloadInventoryAPI unloads an inventory index.
func (h *Handlers) UnloadInventoryAPI(w http.ResponseWriter, r *http.Request) {
	id := inventory.ID(chi.URLParam(r, "id"))

	if err := h.manager.Unload(id); err != nil {
		if errors.Is(err, inventory.ErrNotFound) {
			WriteJSONError(w, http.StatusNotFound, "inventory not found")
			return
		}
		if errors.Is(err, inventory.ErrInvalidState) {
			WriteJSONError(w, http.StatusConflict, err.Error())
			return
		}
		zerolog.Ctx(r.Context()).Error().Err(err).Stringer("id", id).Msg("failed to unload inventory")
		WriteJSONError(w, http.StatusInternalServerError, "failed to unload inventory")
		return
	}

	info, ok := h.manager.Get(id)
	if !ok {
		zerolog.Ctx(r.Context()).Warn().Stringer("id", id).Msg("inventory removed concurrently with unload")
		WriteJSONError(w, http.StatusGone, "inventory was removed concurrently")
		return
	}
	WriteJSON(w, http.StatusOK, info)
}

// DeleteInventoryAPI removes an inventory from the manager.
func (h *Handlers) DeleteInventoryAPI(w http.ResponseWriter, r *http.Request) {
	id := inventory.ID(chi.URLParam(r, "id"))

	if err := h.manager.Remove(id); err != nil {
		if errors.Is(err, inventory.ErrNotFound) {
			WriteJSONError(w, http.StatusNotFound, "inventory not found")
			return
		}
		zerolog.Ctx(r.Context()).Error().Err(err).Stringer("id", id).Msg("failed to delete inventory")
		WriteJSONError(w, http.StatusInternalServerError, "failed to delete inventory")
		return
	}

	WriteJSON(w, http.StatusOK, map[string]string{"status": "deleted"})
}

// InventoriesData is the typed page data for inventories.html.
type InventoriesData struct {
	Title          string
	S3Source       string
	HasDiscovery   bool
	DiscoveryError string
	Groups         []InventoryGroup
}

// InventoryGroup pins one inventory configuration (SourceBucket +
// InventoryName) to all of its discovered runs. The template renders
// one section per group so users can compare runs side-by-side.
type InventoryGroup struct {
	SourceBucket  string
	InventoryName string
	Runs          []DiscoveredRowView
}

// ConfigID returns the "<src>/<inv>" identifier shared by every run in
// the group — handy as a stable HTML id / aria label.
func (g InventoryGroup) ConfigID() string {
	return g.SourceBucket + "/" + g.InventoryName
}

// InventoriesPage renders the inventories HTML page. The page is
// discovery-centric: when --s3-source is set, it lists S3-discovered
// inventories merged with their live load state. When discovery is
// disabled the page surfaces an empty state directing the operator to
// the Dashboard (which shows the Manager's local list directly).
func (h *Handlers) InventoriesPage(w http.ResponseWriter, r *http.Request) {
	data := InventoriesData{
		Title:        "Inventories",
		S3Source:     h.s3SourceURI,
		HasDiscovery: h.discovery.Enabled(),
	}
	if data.HasDiscovery {
		views, err := h.discovery.List(r.Context())
		if err != nil {
			zerolog.Ctx(r.Context()).Error().Err(err).Msg("discover for inventories page")
			data.DiscoveryError = "Failed to list discovered inventories. See server logs for details."
		}

		// Build group → run-list. Discovery returns runs newest-first
		// within a configuration; preserve that order. Use the order of
		// first appearance to keep configurations in a stable
		// (alphabetical-ish) sequence on the page.
		groupIdx := map[string]int{}
		for i := range views {
			row := DiscoveredRowView{MergedInventory: views[i]}
			if h.jobStore != nil {
				j, err := h.jobStore.LatestForInventory(views[i].CompositeID())
				switch {
				case err == nil:
					row.LatestJob = &j
				case errors.Is(err, jobs.ErrStoreNotFound):
					// no jobs yet — fine
				default:
					zerolog.Ctx(r.Context()).Warn().Err(err).
						Stringer("composite", views[i].CompositeID()).
						Msg("look up latest job for inventories page")
				}
			}
			row.CacheBytes, row.CacheBytesH = h.cacheSize(r, views[i].Inventory)
			key := views[i].ConfigID()
			if idx, ok := groupIdx[key]; ok {
				data.Groups[idx].Runs = append(data.Groups[idx].Runs, row)
				continue
			}
			groupIdx[key] = len(data.Groups)
			data.Groups = append(data.Groups, InventoryGroup{
				SourceBucket:  views[i].SourceBucket,
				InventoryName: views[i].InventoryName,
				Runs:          []DiscoveredRowView{row},
			})
		}
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := h.renderer.Render(w, "inventories.html", data); err != nil {
		zerolog.Ctx(r.Context()).Error().Err(err).Msg("failed to render inventories page")
		http.Error(w, "failed to render page", http.StatusInternalServerError)
	}
}

// InventoryRowPartial renders the row for one inventory ID.
func (h *Handlers) InventoryRowPartial(w http.ResponseWriter, r *http.Request) {
	h.renderInventoryRow(w, r, inventory.ID(chi.URLParam(r, "id")))
}
