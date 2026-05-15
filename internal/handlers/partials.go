package handlers

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/internal/jobs"
	"github.com/eunmann/s3-inv-db/pkg/humanfmt"
	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog"
)

// The htmx-facing /partials/* routes mutate state via the inventory
// Manager or DiscoveryService and return the updated row's HTML directly
// so the client can swap it into the DOM without a full page reload.

// LoadInventoryRowPartial loads a (non-discovered) inventory and returns
// its updated row.
func (h *Handlers) LoadInventoryRowPartial(w http.ResponseWriter, r *http.Request) {
	id := inventory.ID(chi.URLParam(r, "id"))
	if err := h.manager.Load(context.WithoutCancel(r.Context()), id); err != nil {
		respondManagerErrorHTML(w, r, err, "load inventory")

		return
	}
	h.renderInventoryRow(w, r, id)
}

// UnloadInventoryRowPartial unloads an inventory and returns its updated row.
func (h *Handlers) UnloadInventoryRowPartial(w http.ResponseWriter, r *http.Request) {
	id := inventory.ID(chi.URLParam(r, "id"))
	if err := h.manager.Unload(r.Context(), id); err != nil {
		respondManagerErrorHTML(w, r, err, "unload inventory")

		return
	}
	h.renderInventoryRow(w, r, id)
}

// DeleteInventoryRowPartial removes an inventory and returns an empty body
// so htmx's outerHTML swap removes the row.
func (h *Handlers) DeleteInventoryRowPartial(w http.ResponseWriter, r *http.Request) {
	id := inventory.ID(chi.URLParam(r, "id"))
	if err := h.manager.Remove(r.Context(), id); err != nil && !errors.Is(err, inventory.ErrNotFound) {
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
	run := chi.URLParam(r, "run")
	logger := zerolog.Ctx(r.Context())

	disc, err := h.discovery.Find(r.Context(), src, id, run)
	if err != nil {
		logger.Error().Err(err).Str("src", src).Str("id", id).Str("run", run).Msg("find discovered inventory")
		http.Error(w, "failed to find inventory", http.StatusBadGateway)

		return
	}
	if disc.ManifestKey == "" || disc.Run == "" {
		http.Error(w, "no completed run for this inventory", http.StatusNotFound)

		return
	}
	if h.jobMgr == nil {
		http.Error(w, "jobs not configured", http.StatusServiceUnavailable)

		return
	}

	if err := h.discovery.PrepareDiscovered(r.Context(), disc); err != nil {
		respondManagerErrorHTML(w, r, err, "prepare discovered inventory")

		return
	}
	composite := disc.CompositeID()

	// Reject double-submit: if a job is already queued or running for
	// this inventory, render the row with its current state instead of
	// spawning a duplicate that will fail with an InvalidState error.
	if existing, err := h.jobStore.LatestForInventory(r.Context(), composite); err == nil && existing.State.IsLive() {
		h.renderDiscoveredRowFrom(w, r, disc)

		return
	}

	// The job context is intentionally independent of the request — a
	// build outlives the HTTP request that started it. Submitting via a
	// non-handler helper keeps contextcheck out of the trace from
	// r.Context() to the job ctx.
	_, err = h.submitDiscoveredLoadJob(r.Context(), composite, disc)
	if err != nil {
		respondManagerErrorHTML(w, r, err, "submit load job")

		return
	}
	// Headers must commit BEFORE WriteHeader, otherwise the Set on
	// Content-Type inside renderDiscoveredRowFrom is a no-op and the
	// browser falls back to Go's body sniffing.
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusAccepted)
	h.renderDiscoveredRowFrom(w, r, disc)
}

// CancelJob cancels an in-flight job by ID. 404 if not currently live.
func (h *Handlers) CancelJob(w http.ResponseWriter, r *http.Request) {
	if h.jobMgr == nil {
		http.Error(w, "jobs not configured", http.StatusServiceUnavailable)

		return
	}
	id := jobs.ID(chi.URLParam(r, "id"))
	if err := h.jobMgr.Cancel(id); err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)

		return
	}
	w.WriteHeader(http.StatusAccepted)
}

// UnloadDiscoveredRowPartial releases the in-memory index, removes the
// on-disk cache, and returns the row (now in StateNotLoaded).
func (h *Handlers) UnloadDiscoveredRowPartial(w http.ResponseWriter, r *http.Request) {
	src := chi.URLParam(r, "src")
	name := chi.URLParam(r, "id")
	run := chi.URLParam(r, "run")
	composite := inventory.ID(src + "/" + name + "/" + run)
	logger := zerolog.Ctx(r.Context())
	if err := h.manager.Unload(r.Context(), composite); err != nil {
		respondManagerErrorHTML(w, r, err, "unload inventory")

		return
	}
	if h.loader != nil {
		if err := h.loader.RemoveCache(src, name, run); err != nil {
			// Don't fail the request — the memory side is already
			// released; surface the disk error in logs so the operator
			// can clean up.
			logger.Warn().Err(err).
				Str("src", src).Str("id", name).Str("run", run).
				Msg("remove cache dir after unload")
		}
	}
	h.renderDiscoveredRow(w, r, src, name, run)
}

// PinDiscoveredRowPartial toggles the run's pin state and returns the
// refreshed row. Form body: pinned=true|false (anything else flips).
func (h *Handlers) PinDiscoveredRowPartial(w http.ResponseWriter, r *http.Request) {
	src := chi.URLParam(r, "src")
	name := chi.URLParam(r, "id")
	run := chi.URLParam(r, "run")
	composite := inventory.ID(src + "/" + name + "/" + run)
	if err := r.ParseForm(); err != nil {
		http.Error(w, "invalid form body", http.StatusBadRequest)

		return
	}
	pinned := parseBoolToggle(r.FormValue("pinned"))
	if err := h.manager.SetPinned(r.Context(), composite, pinned); err != nil {
		respondManagerErrorHTML(w, r, err, "set pin")

		return
	}
	h.renderDiscoveredRow(w, r, src, name, run)
}

// DiscoveredRowPartial returns the current state of one discovered row
// — used by htmx to refresh after an SSE notification.
func (h *Handlers) DiscoveredRowPartial(w http.ResponseWriter, r *http.Request) {
	src := chi.URLParam(r, "src")
	id := chi.URLParam(r, "id")
	run := chi.URLParam(r, "run")
	h.renderDiscoveredRow(w, r, src, id, run)
}

// renderInventoryRow looks up the inventory in the manager and writes the
// inventory_row.html partial. Used by the htmx-facing partial handlers.
func (h *Handlers) renderInventoryRow(w http.ResponseWriter, r *http.Request, id inventory.ID) {
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
func (h *Handlers) renderDiscoveredRow(w http.ResponseWriter, r *http.Request, src, id, run string) {
	logger := zerolog.Ctx(r.Context())
	disc, err := h.discovery.Find(r.Context(), src, id, run)
	if err != nil {
		logger.Error().Err(err).Str("src", src).Str("id", id).Str("run", run).Msg("find discovered inventory for row render")
		http.Error(w, "failed to render row", http.StatusBadGateway)

		return
	}
	h.renderDiscoveredRowFrom(w, r, disc)
}

// DiscoveredRowView extends the merged inventory with the latest job
// for the same composite ID and the on-disk size of its cache. The
// template surfaces progress and renders Retry/Cancel buttons based on
// the job's state.
type DiscoveredRowView struct {
	inventory.MergedInventory

	LatestJob   *jobs.Job
	CacheBytes  int64  // 0 when no on-disk cache exists
	CacheBytesH string // humanfmt.Bytes(CacheBytes); empty when zero

	// Pin / auto-load surfaces fed in from inventory.Info so the row
	// template can render the 📌 badge, the "auto-load suspended"
	// label, and the "user-unloaded" sticky hint without re-querying
	// the Manager.
	Pinned                  bool
	UserUnloaded            bool
	AutoLoadFailureCount    uint32
	AutoLoadBackoffUntil    string
	AutoLoadLastErrorString string
}

// renderDiscoveredRowFrom renders a discovered_row using a pre-fetched
// disc value. Looks up the latest job (if jobs are configured) so the
// row can render progress / cancel / retry.
func (h *Handlers) renderDiscoveredRowFrom(w http.ResponseWriter, r *http.Request, disc inventory.Inventory) {
	view := DiscoveredRowView{
		MergedInventory: inventory.MergedInventory{Inventory: disc, State: inventory.StateNotLoaded},
	}
	if info, ok := h.manager.Get(disc.CompositeID()); ok {
		view.State = info.State
		view.Error = info.Error
		view.NodeCount = info.NodeCount
		view.HasTierData = info.HasTierData
		view.Pinned = info.Pinned
		view.UserUnloaded = !info.UserUnloadedAt.IsZero()
		view.AutoLoadFailureCount = info.AutoLoadFailureCount
		if !info.AutoLoadBackoffUntil.IsZero() {
			view.AutoLoadBackoffUntil = info.AutoLoadBackoffUntil.UTC().Format("15:04:05")
		}
	}
	if h.jobStore != nil {
		j, err := h.jobStore.LatestForInventory(r.Context(), disc.CompositeID())
		switch {
		case err == nil:
			view.LatestJob = &j
		case errors.Is(err, jobs.ErrStoreNotFound):
			// No prior job — render the row without LatestJob.
		default:
			zerolog.Ctx(r.Context()).Warn().Err(err).
				Stringer("composite", disc.CompositeID()).
				Msg("look up latest job for row render")
		}
	}
	cs := h.cacheSize(r, disc)
	view.CacheBytes, view.CacheBytesH = cs.Bytes, cs.Human
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := h.renderer.RenderPartial(w, "discovered_row.html", view); err != nil {
		zerolog.Ctx(r.Context()).Error().Err(err).Msg("render discovered row")
		http.Error(w, "failed to render row", http.StatusInternalServerError)
	}
}

// CacheSize is the raw-bytes / human-formatted pair returned by
// cacheSize. Both are zero / empty when there's no loader wired, the
// dir is missing, or the measurement failed.
type CacheSize struct {
	Bytes int64
	Human string
}

// cacheSize measures the on-disk cache footprint of a single run.
func (h *Handlers) cacheSize(r *http.Request, disc inventory.Inventory) CacheSize {
	if h.loader == nil || disc.Run == "" {
		return CacheSize{}
	}
	n, err := h.loader.CacheSizeBytes(disc.SourceBucket, disc.InventoryName, disc.Run)
	if err != nil {
		zerolog.Ctx(r.Context()).Warn().Err(err).
			Str("src", disc.SourceBucket).
			Str("name", disc.InventoryName).
			Str("run", disc.Run).
			Msg("measure cache size")

		return CacheSize{}
	}
	if n <= 0 {
		return CacheSize{}
	}

	return CacheSize{Bytes: n, Human: humanfmt.BytesUint64(uint64(n))}
}

// submitDiscoveredLoadJob registers a background build job for one
// discovered run. The job context is owned by jobs.Manager and is
// intentionally independent of any request context — a build outlives
// the HTTP request that started it. The parent ctx only plumbs through
// logger/values via context.WithoutCancel inside jobs.Manager.Submit.
func (h *Handlers) submitDiscoveredLoadJob(parent context.Context, composite inventory.ID, disc inventory.Inventory) (jobs.Job, error) {
	job, err := h.jobMgr.Submit(parent, composite, jobs.KindBuild, func(ctx context.Context, report func(jobs.Update)) error {
		return h.discovery.LoadWith(ctx, disc, func(stage string, done, total int64) {
			report(jobs.Update{Stage: stage, BytesDone: done, BytesTotal: total})
		})
	})
	if err != nil {
		return job, fmt.Errorf("submit build job: %w", err)
	}

	return job, nil
}
