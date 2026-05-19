package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
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
	SourceBucket string          `json:"source_bucket"`
	Name         string          `json:"inventory_name"`
	Runs         []ConfigRunView `json:"runs"`
}

// ConfigRunView is one run's identity + lifecycle state. Bytes-on-disk
// is reported when the loader can measure it.
type ConfigRunView struct {
	ID          inventory.ID `json:"id"`
	Run         string       `json:"run"`
	State       string       `json:"state"`
	Error       string       `json:"error,omitempty"`
	LoadedAt    string       `json:"loaded_at,omitempty"`
	ManifestKey string       `json:"manifest_key,omitempty"`
	NodeCount   uint64       `json:"node_count,omitempty"`
	CacheBytes  int64        `json:"cache_bytes,omitempty"`
	HasTierData bool         `json:"has_tier_data"`
}

// ListConfigurationsAPI returns inventory configurations grouped by
// (source bucket, inventory ID). When discovery is enabled the list
// covers every discovered run merged with its current Manager state;
// otherwise it returns whatever's in the Manager keyed by composite
// ID — useful for "list what's loadable" via one round trip.
func (h *Handlers) ListConfigurationsAPI(w http.ResponseWriter, r *http.Request) {
	resp := ConfigurationsResponse{DiscoveryEnabled: h.discovery.Enabled()}
	if h.discovery.Enabled() {
		views, _, err := h.discovery.Snapshot(r.Context())
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
				SourceBucket: v.SourceBucket,
				Name:         v.Name,
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
			if n, err := h.loader.CacheSizeBytes(v.SourceBucket, v.Name, v.Run); err == nil {
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
		p := info.ID.Split()
		src, inv, run := p.Source, p.Inventory, p.Run
		if !p.OK {
			// Fallback for legacy 2-part or hand-registered IDs: bucket
			// them into a single "_other_" group so they remain visible.
			src, inv = "_other_", string(info.ID)
		}
		key := src + "/" + inv
		idx, exists := groups[key]
		if !exists {
			groups[key] = len(out)
			idx = len(out)
			out = append(out, ConfigurationView{SourceBucket: src, Name: inv})
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
	if strings.ContainsAny(req.ID, "/?#%") {
		// chi's {id} URL param is single-segment; an ID with slashes
		// or other URL-meaningful characters would silently 404 every
		// subsequent /api/inventories/{id}/... call. Composite IDs
		// (src/name/run) flow through /partials/discovered/* instead.
		WriteJSONError(w, http.StatusBadRequest, "id must not contain '/', '?', '#', or '%'")

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
	if err := h.manager.Register(r.Context(), id, req.Name, req.Path); err != nil {
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

// LoadInventoryAPI loads an inventory index. Build runs under
// WithoutCancel so a navigate-away or htmx-side cancellation doesn't
// poison the state with "context canceled".
func (h *Handlers) LoadInventoryAPI(w http.ResponseWriter, r *http.Request) {
	id := inventory.ID(chi.URLParam(r, "id"))
	loadCtx := context.WithoutCancel(r.Context())
	h.runInventoryOp(w, r, id, "load", func(_ context.Context) error {
		return h.manager.Load(loadCtx, id)
	})
}

// UnloadInventoryAPI unloads an inventory index.
func (h *Handlers) UnloadInventoryAPI(w http.ResponseWriter, r *http.Request) {
	id := inventory.ID(chi.URLParam(r, "id"))
	h.runInventoryOp(w, r, id, "unload", func(ctx context.Context) error {
		return h.manager.Unload(ctx, id)
	})
}

// runInventoryOp runs op against the manager, maps the error space to
// the JSON-API responses Load/Unload share, then returns the post-op
// Info so the client can refresh its view.
func (h *Handlers) runInventoryOp(w http.ResponseWriter, r *http.Request, id inventory.ID, opName string, op func(context.Context) error) {
	if err := op(r.Context()); err != nil {
		switch {
		case errors.Is(err, inventory.ErrNotFound):
			WriteJSONError(w, http.StatusNotFound, "inventory not found")
		case errors.Is(err, inventory.ErrInvalidState):
			WriteJSONError(w, http.StatusConflict, err.Error())
		default:
			zerolog.Ctx(r.Context()).Error().Err(err).Stringer("id", id).Msgf("failed to %s inventory", opName)
			WriteJSONError(w, http.StatusInternalServerError, "failed to "+opName+" inventory")
		}

		return
	}
	info, ok := h.manager.Get(id)
	if !ok {
		zerolog.Ctx(r.Context()).Warn().Stringer("id", id).Msgf("inventory removed concurrently with %s", opName)
		WriteJSONError(w, http.StatusGone, "inventory was removed concurrently")

		return
	}
	WriteJSON(w, http.StatusOK, info)
}

// DeleteInventoryAPI removes an inventory from the manager.
func (h *Handlers) DeleteInventoryAPI(w http.ResponseWriter, r *http.Request) {
	id := inventory.ID(chi.URLParam(r, "id"))

	if err := h.manager.Remove(r.Context(), id); err != nil {
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
	DiscoveryError string
	Groups         []InventoryGroup
	HasDiscovery   bool
}

// InventoryGroup pins one inventory configuration (SourceBucket +
// Name) to all of its discovered runs.
type InventoryGroup struct {
	SourceBucket     string
	Name             string
	LastPollAt       string
	LastPollError    string
	PollBackoffUntil string
	Runs             []DiscoveredRowView
	VisibleRuns      int
	Retention        uint32
	AutoLoad         bool
}

// DefaultVisibleRuns caps how many runs in a configuration render in
// the inventories page above the fold. Older ones collapse into a
// <details> block.
const DefaultVisibleRuns = 5

// ConfigID returns the "<src>/<inv>" identifier shared by every run in
// the group — handy as a stable HTML id / aria label.
func (g InventoryGroup) ConfigID() string {
	return g.SourceBucket + "/" + g.Name
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
		views, _, err := h.discovery.Snapshot(r.Context())
		if err != nil {
			zerolog.Ctx(r.Context()).Error().Err(err).Msg("discover for inventories page")
			data.DiscoveryError = "Failed to list discovered inventories. See server logs for details."
		}
		data.Groups = h.buildInventoryGroups(r, views)
	}

	h.renderHTML(w, r, "inventories.html", "failed to render inventories page", data)
}

// InventoryRowPartial renders the row for one inventory ID.
func (h *Handlers) InventoryRowPartial(w http.ResponseWriter, r *http.Request) {
	h.renderInventoryRow(w, r, inventory.ID(chi.URLParam(r, "id")))
}

// buildInventoryGroups composes the per-configuration row groups for
// InventoriesPage. Extracted so the page handler stays under the
// cognitive-complexity budget; the same shape was inlined before.
func (h *Handlers) buildInventoryGroups(r *http.Request, views []inventory.MergedInventory) []InventoryGroup {
	groupIdx := map[string]int{}
	groups := make([]InventoryGroup, 0, len(views))
	for i := range views {
		row := h.buildDiscoveredRow(r, &views[i])
		key := views[i].ConfigID()
		if idx, ok := groupIdx[key]; ok {
			groups[idx].Runs = append(groups[idx].Runs, row)

			continue
		}
		groupIdx[key] = len(groups)
		groups = append(groups, InventoryGroup{
			SourceBucket: views[i].SourceBucket,
			Name:         views[i].Name,
			Runs:         []DiscoveredRowView{row},
		})
	}
	for i := range groups {
		groups[i].VisibleRuns = min(DefaultVisibleRuns, len(groups[i].Runs))
	}
	h.annotateGroupsFromConfig(r.Context(), groups)

	return groups
}

func (h *Handlers) buildDiscoveredRow(r *http.Request, v *inventory.MergedInventory) DiscoveredRowView {
	row := DiscoveredRowView{MergedInventory: *v}
	if info, ok := h.manager.Get(v.CompositeID()); ok {
		row.Pinned = info.Pinned
		row.UserUnloaded = !info.UserUnloadedAt.IsZero()
		row.AutoLoadFailureCount = info.AutoLoadFailureCount
		row.LoadDuration = info.LoadDuration
		if !info.AutoLoadBackoffUntil.IsZero() {
			row.AutoLoadBackoffUntil = info.AutoLoadBackoffUntil.UTC().Format("15:04:05")
		}
	}
	if h.jobStore != nil {
		j, err := h.jobStore.LatestForInventory(r.Context(), v.CompositeID())
		switch {
		case err == nil:
			row.LatestJob = &j
		case errors.Is(err, jobs.ErrStoreNotFound):
			// no jobs yet — fine
		default:
			zerolog.Ctx(r.Context()).Warn().Err(err).
				Stringer("composite", v.CompositeID()).
				Msg("look up latest job for inventories page")
		}
	}
	cs := h.measureCacheSize(r, v.Inventory)
	row.CacheBytes, row.CacheBytesH = cs.Bytes, cs.Human
	row.LoadDurationH = loadDurationLabel(row.LoadDuration, row.LatestJob)
	populateRowDerived(&row)

	return row
}

func (h *Handlers) annotateGroupsFromConfig(ctx context.Context, groups []InventoryGroup) {
	if h.configStore == nil {
		return
	}
	for i := range groups {
		g := &groups[i]
		cfg, err := h.configStore.Get(ctx, g.SourceBucket, g.Name)
		if err != nil {
			continue
		}
		g.AutoLoad = cfg.AutoLoad
		g.Retention = cfg.RetentionCount
		if !cfg.LastPolledAt.IsZero() {
			g.LastPollAt = cfg.LastPolledAt.UTC().Format(time.RFC3339)
		}
		g.LastPollError = cfg.LastPollError
		if !cfg.PollBackoffUntil.IsZero() {
			g.PollBackoffUntil = cfg.PollBackoffUntil.UTC().Format(time.RFC3339)
		}
	}
}

// ListDiscoveredAPI lists inventories under the configured S3 source,
// merging in any current load state. Returns 503 when discovery is not
// configured (server started without --s3-source).
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
