package handlers

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/internal/jobs"
	"github.com/eunmann/s3-inv-db/pkg/humanfmt"
	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog"
)

// The htmx-facing /partials/* routes mutate state via the inventory
// Manager or Discovery and return the updated row's HTML directly
// so the client can swap it into the DOM without a full page reload.

// LoadDiscoveredRowPartial submits a background build job and returns
// the row in queued state with HTTP 202 immediately. The UI watches the
// SSE stream for state transitions and swaps the row when the job moves.
func (h *Handlers) LoadDiscoveredRowPartial(w http.ResponseWriter, r *http.Request) {
	src := chi.URLParam(r, "src")
	id := chi.URLParam(r, "id")
	run := chi.URLParam(r, "run")

	disc, err := h.discovery.Find(r.Context(), src, id, run)
	if err != nil {
		respondFindError(w, r, err, src+"/"+id+"/"+run, "failed to find inventory")

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

	// Already loaded: the user clicked Load on a run the Manager already
	// holds open (typical cause: their cached page reflected a stale
	// discovery snapshot). Render the live row and skip the job so the
	// inventory doesn't end up with a no-op build job whose terminal
	// state may race the SSE subscriber and leave the row stuck.
	if info, ok := h.manager.Get(composite); ok && info.State == inventory.StateLoaded {
		h.renderDiscoveredRowFrom(w, r, disc)

		return
	}

	// The job context is intentionally independent of the request — a
	// build outlives the HTTP request that started it. Submitting via a
	// non-handler helper keeps contextcheck out of the trace from
	// r.Context() to the job ctx. The scheduler dedups by inventory, so a
	// build already queued/running comes back as ErrDuplicateInventory —
	// render the row's current state instead of spawning a duplicate.
	err = h.submitDiscoveredLoadJob(r.Context(), composite, disc)
	switch {
	case errors.Is(err, jobs.ErrDuplicateInventory):
		h.renderDiscoveredRowFrom(w, r, disc)

		return
	case err != nil:
		respondManagerError(w, r, err, "submit load job")

		return
	}
	// Render to a buffer first via the *Status variant — a template
	// failure must surface as a clean 500, not a 202 with an error body.
	h.renderDiscoveredRowFromStatus(w, r, http.StatusAccepted, disc)
}

// CancelJob cancels an in-flight job by ID. 404 if not currently live.
func (h *Handlers) CancelJob(w http.ResponseWriter, r *http.Request) {
	id := jobs.ID(chi.URLParam(r, "id"))
	if err := h.jobMgr.Cancel(id); err != nil {
		http.Error(w, "this job is no longer running", http.StatusNotFound)

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
	if err := h.manager.Unload(r.Context(), composite); err != nil {
		respondManagerError(w, r, err, "unload inventory")

		return
	}
	h.freeRunCache(r.Context(), composite)
	h.renderDiscoveredRow(w, r, src, name, run)
}

// PinDiscoveredRowPartial toggles the run's pin state and returns the
// refreshed row. Form body: pinned=true|false (anything else flips).
func (h *Handlers) PinDiscoveredRowPartial(w http.ResponseWriter, r *http.Request) {
	src := chi.URLParam(r, "src")
	name := chi.URLParam(r, "id")
	run := chi.URLParam(r, "run")
	composite := inventory.ID(src + "/" + name + "/" + run)
	r.Body = http.MaxBytesReader(w, r.Body, maxFormBodyBytes)
	if err := r.ParseForm(); err != nil {
		if bodyTooLarge(err) {
			http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)

			return
		}
		http.Error(w, "invalid form body", http.StatusBadRequest)

		return
	}
	pinned := parseBoolToggle(r.FormValue("pinned"))
	if err := h.manager.SetPinned(r.Context(), composite, pinned); err != nil {
		respondManagerError(w, r, err, "set pin")

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

// renderDiscoveredRow re-fetches the discovery entry, merges in current
// manager state, and renders the discovered_row.html partial.
func (h *Handlers) renderDiscoveredRow(w http.ResponseWriter, r *http.Request, src, id, run string) {
	disc, err := h.discovery.Find(r.Context(), src, id, run)
	if err != nil {
		respondFindError(w, r, err, src+"/"+id+"/"+run, "failed to render row")

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

	LatestJob            *jobs.Job
	CacheBytesH          string
	AutoLoadBackoffUntil string
	LoadDurationH        string
	RunLabel             string
	MetaLine             string
	StateName            string // State as the template's state helpers take it
	LoadDuration         time.Duration
	CacheBytes           int64
	AutoLoadFailureCount uint32
	Pinned               bool
	UserUnloaded         bool
	// CanRetry: the latest job ended failed, aborted or cancelled.
	CanRetry bool
}

// populateRowDerived fills the view fields that depend on the
// already-populated raw data: a humanised run timestamp, and a one-line
// "format · N files, X bytes · cache Y · loaded in Z" summary the
// template renders under the run timestamp.
func populateRowDerived(view *DiscoveredRowView) {
	view.RunLabel = humanfmt.RunTimestamp(view.Run)
	view.MetaLine = discoveredMetaLine(view)
	view.StateName = string(view.State)
	if j := view.LatestJob; j != nil {
		switch j.State {
		case jobs.StateFailed, jobs.StateAborted, jobs.StateCancelled:
			view.CanRetry = true
		case jobs.StateQueued, jobs.StateRunning, jobs.StateSucceeded:
		}
	}
}

// discoveredMetaLine joins the secondary facts about one run into a
// single bullet-separated string. Empty pieces are omitted so the
// result has no dangling separators. The manifest segment names what
// each number is ("manifest 1.40 GiB / 24 chunks") so the compressed
// total isn't mistaken for an individual chunk's size.
//
// The cache and "loaded in" segments are only emitted when the run is
// currently in StateLoaded — they describe the *current* on-disk index
// and the most recent successful build, neither of which is meaningful
// while the run is loading, unloaded, or in an error state. Without
// this guard, Info.LoadDuration / CacheBytesH from a prior successful
// load would leak into a busy or unloaded row's display.
func discoveredMetaLine(view *DiscoveredRowView) string {
	parts := make([]string, 0, 4)
	if view.FileFormat != "" {
		parts = append(parts, view.FileFormat)
	}
	if seg := manifestMetaSegment(view.FileCount, view.TotalBytes); seg != "" {
		parts = append(parts, seg)
	}
	if view.State == inventory.StateLoaded {
		if view.CacheBytesH != "" {
			parts = append(parts, "cache "+view.CacheBytesH)
		}
		if view.LoadDurationH != "" {
			parts = append(parts, "loaded in "+view.LoadDurationH)
		}
	}

	return strings.Join(parts, " · ")
}

// manifestMetaSegment renders the "manifest <size> / <N> chunks" piece
// of the meta line. Falls back gracefully when only one of the two
// numbers is available; returns "" when neither is.
func manifestMetaSegment(fileCount int, totalBytes int64) string {
	chunks := ""
	if fileCount > 0 {
		chunks = fmt.Sprintf("%d chunk", fileCount)
		if fileCount != 1 {
			chunks += "s"
		}
	}
	size := ""
	if totalBytes > 0 {
		size = humanfmt.Bytes(totalBytes)
	}
	switch {
	case size != "" && chunks != "":
		return "manifest " + size + " / " + chunks
	case size != "":
		return "manifest " + size
	case chunks != "":
		return "manifest " + chunks
	}

	return ""
}

// renderDiscoveredRowFrom renders a discovered_row using a pre-fetched
// disc value with HTTP 200. Looks up the latest job (if jobs are
// configured) so the row can render progress / cancel / retry.
func (h *Handlers) renderDiscoveredRowFrom(w http.ResponseWriter, r *http.Request, disc inventory.Inventory) {
	h.renderDiscoveredRowFromStatus(w, r, http.StatusOK, disc)
}

// renderDiscoveredRowFromStatus is renderDiscoveredRowFrom with an
// explicit status code. The status only commits after the template
// renders successfully — a render failure surfaces as 500, not a 5xx
// body attached to a 2xx response.
func (h *Handlers) renderDiscoveredRowFromStatus(w http.ResponseWriter, r *http.Request, status int, disc inventory.Inventory) {
	view := DiscoveredRowView{
		Inventory: disc, State: inventory.StateNotLoaded,
	}
	if info, ok := h.manager.Get(disc.CompositeID()); ok {
		view.State = info.State
		view.Error = info.Error
		view.NodeCount = info.NodeCount
		view.HasTierData = info.HasTierData
		view.Pinned = info.Pinned
		view.UserUnloaded = !info.UserUnloadedAt.IsZero()
		view.AutoLoadFailureCount = info.AutoLoadFailureCount
		view.LoadDuration = info.LoadDuration
		if !info.AutoLoadBackoffUntil.IsZero() {
			view.AutoLoadBackoffUntil = info.AutoLoadBackoffUntil.UTC().Format("15:04:05")
		}
	}
	j, err := h.latestJob(r.Context(), disc.CompositeID())
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
	cs := h.measureCacheSize(r, disc)
	view.CacheBytes, view.CacheBytesH = cs.Bytes, cs.Human
	view.LoadDurationH = loadDurationLabel(view.LoadDuration, view.LatestJob)
	populateRowDerived(&view)
	h.renderHTMLPartialStatus(w, r, status, "discovered_row.html", "render discovered row", view)
}

// latestJob returns inv's newest job: the scheduler's in-memory snapshot
// while one is queued or running, since the store's copy trails live
// progress by up to a second, else the stored one.
func (h *Handlers) latestJob(ctx context.Context, inv inventory.ID) (jobs.Job, error) {
	if j, ok := h.jobMgr.LiveForInventory(inv); ok {
		return j, nil
	}
	j, err := h.jobStore.LatestForInventory(ctx, inv)
	if err != nil {
		return jobs.Job{}, fmt.Errorf("latest job for %s: %w", inv, err)
	}

	return j, nil
}

// latestJobs is latestJob for every inventory at once: one store query,
// overlaid with the live snapshots.
func (h *Handlers) latestJobs(ctx context.Context) (map[inventory.ID]jobs.Job, error) {
	latest, err := h.jobStore.LatestByInventory(ctx)
	if latest == nil {
		latest = make(map[inventory.ID]jobs.Job)
	}
	live := h.jobMgr.Live()
	for i := range live {
		latest[live[i].InventoryID] = live[i]
	}
	if err != nil {
		return latest, fmt.Errorf("latest jobs: %w", err)
	}

	return latest, nil
}

// loadDurationLabel renders the wall-clock load time of the most recent
// successful load. Prefers Manager.Info.LoadDuration (populated for both
// user-driven and auto-driven loads) and falls back to the LatestJob's
// StartedAt/FinishedAt span — useful when Info.LoadDuration was reset
// by a server restart but the JobStore still has the build's timestamps.
// Returns "" when neither source has a usable duration.
func loadDurationLabel(infoDuration time.Duration, j *jobs.Job) string {
	if infoDuration > 0 {
		return humanfmt.Duration(infoDuration)
	}
	if j == nil || j.Kind != jobs.KindBuild {
		return ""
	}
	if j.StartedAt.IsZero() || j.FinishedAt.IsZero() {
		return ""
	}
	d := j.FinishedAt.Sub(j.StartedAt)
	if d <= 0 {
		return ""
	}

	return humanfmt.Duration(d)
}

// cacheSize is the raw-bytes / human-formatted pair returned by
// measureCacheSize. Zero / empty when there's no loader wired, the
// dir is missing, or the measurement failed.
type cacheSize struct {
	Human string
	Bytes int64
}

// measureCacheSize returns the on-disk cache footprint of a single run.
// Prefers the cached Info.IndexBytes (no filesystem walk) and falls
// back to an on-the-fly CacheSizeBytes walk only when the manager
// has no record of the run.
func (h *Handlers) measureCacheSize(r *http.Request, disc inventory.Inventory) cacheSize {
	if disc.Run == "" {
		return cacheSize{}
	}
	if info, ok := h.manager.Get(disc.CompositeID()); ok && info.IndexBytes > 0 {
		bytes := int64(info.IndexBytes)

		return cacheSize{Bytes: bytes, Human: humanfmt.Bytes(bytes)}
	}
	if h.loader == nil {
		return cacheSize{}
	}
	n, err := h.loader.CacheSizeBytes(inventory.CacheKey{
		SourceBucket: disc.SourceBucket,
		InventoryID:  disc.Name,
		Run:          disc.Run,
	})
	if err != nil {
		zerolog.Ctx(r.Context()).Warn().Err(err).
			Str("src", disc.SourceBucket).
			Str("name", disc.Name).
			Str("run", disc.Run).
			Msg("measure cache size")

		return cacheSize{}
	}
	if n <= 0 {
		return cacheSize{}
	}

	return cacheSize{Bytes: n, Human: humanfmt.BytesUint64(uint64(n))}
}

// submitDiscoveredLoadJob registers a background build job for one
// discovered run. The job context is owned by jobs.Scheduler and is
// intentionally independent of any request context — a build outlives
// the HTTP request that started it. The parent ctx only plumbs through
// logger/values via context.WithoutCancel inside jobs.Scheduler.Submit.
func (h *Handlers) submitDiscoveredLoadJob(parent context.Context, composite inventory.ID, disc inventory.Inventory, opts ...jobs.SubmitOption) error {
	_, err := h.jobMgr.Submit(parent, composite, jobs.KindBuild, func(ctx context.Context, report func(jobs.Update)) error {
		// Recorder bridges extsort pipeline events to Update.Stages so
		// the drawer's per-stage timeline populates in real time. Close
		// drains the subscription before the work returns so the
		// scheduler's terminal report can't race the drainer.
		recorder := jobs.NewRecorder(report)
		defer recorder.Close()

		return h.discovery.LoadWith(ctx, disc, recorder.OnProgress, recorder.Bus())
	}, opts...)
	if err != nil {
		return fmt.Errorf("submit build job: %w", err)
	}

	return nil
}

// freeRunCache deletes the on-disk cache of a discovered run after it
// was unloaded or removed, so an unloaded run never holds disk outside
// the budget. Only composite source/inventory/run IDs have a cache, and
// it lives under the loader's cache root: an inventory registered from
// a local path keeps its directory. A failure is logged, not returned:
// the memory side is already released.
func (h *Handlers) freeRunCache(ctx context.Context, id inventory.ID) {
	p := id.Split()
	if h.loader == nil || !p.OK {
		return
	}
	if err := h.loader.RemoveCache(inventory.CacheKey{SourceBucket: p.Source, InventoryID: p.Inventory, Run: p.Run}); err != nil {
		zerolog.Ctx(ctx).Warn().Err(err).Stringer("id", id).Msg("remove cache dir after unload")
	}
}
