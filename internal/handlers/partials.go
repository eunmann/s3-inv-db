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

// DiscoveredRowView is one run's row: its discovery entry merged with
// the catalog's state, its latest job, and for a loaded run what it
// holds. The handlers set Stats, IsLatest and PrevLoaded from the run's
// place among its configuration's runs.
type DiscoveredRowView struct {
	inventory.MergedInventory

	LatestJob            *jobs.Job
	Stats                *RunStats
	AutoLoadBackoffUntil string
	// PrevLoaded is the loaded run of the same configuration just
	// before this one: what "Compare with previous" diffs against.
	PrevLoaded           inventory.ID
	AutoLoadFailureCount uint32
	Pinned               bool
	UserUnloaded         bool
	IsLatest             bool
}

// RunLabel is the run's timestamp as people read it.
func (v DiscoveredRowView) RunLabel() string { return humanfmt.RunTimestamp(v.Run) }

// StateName is State as the template's state helpers take it.
func (v DiscoveredRowView) StateName() string { return string(v.State) }

// DomID is the run's id made safe for HTML ids, so each action button
// keeps one id across swaps and focus survives them.
func (v DiscoveredRowView) DomID() string { return domID(string(v.CompositeID())) }

// Busy: a build is queued or running. A live job counts before the
// catalog flips the run to loading.
func (v DiscoveredRowView) Busy() bool {
	return v.State.IsLoading() || (v.LatestJob != nil && v.LatestJob.State.IsLive())
}

// CanRetry: the latest job ended failed, aborted or cancelled.
func (v DiscoveredRowView) CanRetry() bool {
	if v.LatestJob == nil {
		return false
	}
	switch v.LatestJob.State {
	case jobs.StateFailed, jobs.StateAborted, jobs.StateCancelled:
		return true
	case jobs.StateQueued, jobs.StateRunning, jobs.StateSucceeded:
	}

	return false
}

// ErrorText is why the run's last load failed, raw: the catalog's error,
// else the latest job's when it failed or was interrupted.
func (v DiscoveredRowView) ErrorText() string {
	if v.Busy() || v.State.IsLoaded() {
		return ""
	}
	if v.Error != "" {
		return v.Error
	}
	if j := v.LatestJob; j != nil && (j.State == jobs.StateFailed || j.State == jobs.StateAborted) {
		return j.Error
	}

	return ""
}

// ErrorSummary is ErrorText as one sentence.
func (v DiscoveredRowView) ErrorSummary() string { return summarizeBuildError(v.ErrorText()) }

// Failed: the run isn't loaded or building because its last load failed.
func (v DiscoveredRowView) Failed() bool {
	if v.Busy() || v.State.IsLoaded() {
		return false
	}

	return v.State.IsError() || (v.LatestJob != nil && (v.LatestJob.State == jobs.StateFailed || v.LatestJob.State == jobs.StateAborted))
}

// prominent rows stay visible on the inventories page; the rest of a
// configuration's runs collapse under "Show N older runs".
func (v DiscoveredRowView) prominent() bool {
	return v.Run == "" || v.IsLatest || v.State.IsLoaded() || v.Busy() || v.Failed()
}

// BrowseHref opens the run in Browse.
func (v DiscoveredRowView) BrowseHref() string { return browseHref(v.CompositeID()) }

// CompareHref compares the run with the loaded run before it, or opens
// Compare with this run chosen when there is none.
func (v DiscoveredRowView) CompareHref() string {
	if v.PrevLoaded != "" {
		return compareHref(v.PrevLoaded, v.CompositeID())
	}

	return compareHref(v.CompositeID(), "")
}

// rowView builds one run's row from its discovery entry, the catalog's
// live state and its latest job (nil when it never had one).
func (h *Handlers) rowView(v inventory.MergedInventory, job *jobs.Job) DiscoveredRowView {
	row := DiscoveredRowView{MergedInventory: v, LatestJob: job}
	if row.State == "" {
		row.State = inventory.StateNotLoaded
	}
	info, ok := h.manager.Get(v.CompositeID())
	if !ok {
		return row
	}
	row.State = info.State
	row.Error = info.Error
	row.NodeCount = info.NodeCount
	row.HasTierData = info.HasTierData
	row.Pinned = info.Pinned
	row.UserUnloaded = !info.UserUnloadedAt.IsZero()
	row.AutoLoadFailureCount = info.AutoLoadFailureCount
	if !info.AutoLoadBackoffUntil.IsZero() {
		row.AutoLoadBackoffUntil = info.AutoLoadBackoffUntil.UTC().Format("15:04:05")
	}

	return row
}

// addStats reads a loaded row's root totals. A run evicted since its
// state was read shows none.
func (h *Handlers) addStats(ctx context.Context, row *DiscoveredRowView) {
	if row.Run == "" || !row.State.IsLoaded() {
		return
	}
	s, err := h.runStats(row.CompositeID(), false)
	if err != nil {
		if !errors.Is(err, inventory.ErrNotLoaded) && !errors.Is(err, inventory.ErrNotFound) {
			zerolog.Ctx(ctx).Warn().Err(err).Stringer("id", row.CompositeID()).Msg("read run totals")
		}

		return
	}
	row.Stats = &s
}

// placeRow sets what the row's place among its configuration's runs
// decides (the Latest mark, Compare with previous), from the discovery
// snapshot.
func (h *Handlers) placeRow(ctx context.Context, row *DiscoveredRowView) {
	views, _, err := h.discovery.Snapshot(ctx)
	if err != nil {
		zerolog.Ctx(ctx).Warn().Err(err).Msg("snapshot for row placement")

		return
	}
	cfg := row.ConfigID()
	runs := make([]DiscoveredRowView, 0, 8)
	runs = append(runs, *row)
	for i := range views {
		if views[i].Run != row.Run && views[i].ConfigID() == cfg {
			runs = append(runs, DiscoveredRowView{MergedInventory: views[i]})
		}
	}
	sortRunsNewestFirst(runs)
	linkRuns(runs)
	for i := range runs {
		if runs[i].Run == row.Run {
			row.IsLatest, row.PrevLoaded = runs[i].IsLatest, runs[i].PrevLoaded

			return
		}
	}
}

// renderDiscoveredRowFrom renders a discovered_row using a pre-fetched
// disc value with HTTP 200.
func (h *Handlers) renderDiscoveredRowFrom(w http.ResponseWriter, r *http.Request, disc inventory.Inventory) {
	h.renderDiscoveredRowFromStatus(w, r, http.StatusOK, disc)
}

// renderDiscoveredRowFromStatus is renderDiscoveredRowFrom with an
// explicit status code. The status only commits after the template
// renders successfully — a render failure surfaces as 500, not a 5xx
// body attached to a 2xx response.
func (h *Handlers) renderDiscoveredRowFromStatus(w http.ResponseWriter, r *http.Request, status int, disc inventory.Inventory) {
	view := h.rowView(inventory.MergedInventory{Inventory: disc}, h.latestJobOrNil(r.Context(), disc.CompositeID()))
	h.placeRow(r.Context(), &view)
	h.addStats(r.Context(), &view)
	h.renderHTMLPartialStatus(w, r, status, "discovered_row.html", "render discovered row", view)
}

// latestJobOrNil is latestJob with no job, or a failed lookup (logged),
// as nil.
func (h *Handlers) latestJobOrNil(ctx context.Context, inv inventory.ID) *jobs.Job {
	j, err := h.latestJob(ctx, inv)
	switch {
	case err == nil:
		return &j
	case errors.Is(err, jobs.ErrStoreNotFound):
	default:
		zerolog.Ctx(ctx).Warn().Err(err).Stringer("composite", inv).Msg("look up latest job")
	}

	return nil
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
