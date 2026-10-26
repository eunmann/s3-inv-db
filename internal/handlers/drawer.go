package handlers

import (
	"net/http"
	"time"

	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/internal/jobs"
	"github.com/eunmann/s3-inv-db/internal/templates"
	"github.com/eunmann/s3-inv-db/pkg/humanfmt"
	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog"
)

// Caps the PrevJobID chain walk so a circular link can't run forever.
const drawerMaxAttempts = 10

// RunDrawerView is the run drawer: what the run is and holds first,
// then where it came from, then how its builds went.
type RunDrawerView struct {
	// Run is the run as its inventories row sees it: state, place among
	// its configuration's runs, failure and the actions it offers.
	Run DiscoveredRowView
	// Stats, for a loaded run, adds prefixes and the per-tier breakdown
	// and costs Browse shows at the root.
	Stats        *RunStats
	LatestJob    *jobs.Job
	InventoryID  inventory.ID
	IndexBytesH  string
	ManifestH    string
	PrevAttempts []DrawerAttempt
	OverallETA   string
	// OverallDeadline is when OverallETA runs out, for the countdown.
	OverallDeadline time.Time
	BaselineH       string
	LatestTookH     string
	BaselineMs      int64
	Stages          []DrawerStage
	Live            bool
}

func (v RunDrawerView) CompositePath() string { return string(v.InventoryID) }

type DrawerStage struct {
	StartedAt   time.Time
	Name        string
	Label       string
	Description string
	DurationH   string
	Duration    time.Duration
	Bytes       uint64
	Rows        uint64
	InProgress  bool
	Final       bool
	Failed      bool
}

type DrawerAttempt struct {
	JobID      jobs.ID
	State      jobs.State
	Stage      string
	DurationH  string
	Summary    string
	AttemptNum int
	FinishedAt string
	WasRetried bool
}

func (h *Handlers) RunDrawer(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	src := chi.URLParam(r, "src")
	name := chi.URLParam(r, "id")
	run := chi.URLParam(r, "run")
	composite := inventory.ID(src + "/" + name + "/" + run)
	view := RunDrawerView{InventoryID: composite}

	// Only opening the drawer from a row adds a history entry: a refresh
	// of the open drawer, or reopening it from the URL's #run= on a
	// reload, Back or Forward, asks with refresh=1.
	if r.URL.Query().Get("refresh") == "" {
		w.Header().Set("HX-Push-Url", "/inventories#run="+string(composite))
	}

	view.LatestJob = h.latestJobOrNil(ctx, composite)
	views, _, err := h.discovery.Snapshot(ctx)
	if err != nil {
		zerolog.Ctx(ctx).Warn().Err(err).Msg("snapshot for drawer")
	}
	merged := inventory.MergedInventory{SourceBucket: src, Name: name, Run: run}
	for i := range views {
		if views[i].CompositeID() == composite {
			merged = views[i]

			break
		}
	}
	view.Run = h.rowView(merged, view.LatestJob)
	placeRowAmong(views, &view.Run)
	if merged.TotalBytes > 0 {
		view.ManifestH = humanfmt.Bytes(merged.TotalBytes)
	}

	// Info.LoadDuration is the canonical "took N" — keeps drawer + row in sync (per-stage sum excludes manager bookkeeping).
	var infoLoadDuration time.Duration
	if info, ok := h.manager.Get(composite); ok {
		infoLoadDuration = info.LoadDuration
		if info.State == inventory.StateLoaded && info.IndexBytes > 0 {
			view.IndexBytesH = humanfmt.BytesUint64(info.IndexBytes)
		}
	}
	if view.Run.State.IsLoaded() {
		if s, err := h.runStats(composite, true); err == nil {
			view.Stats = &s
		}
	}

	if latest := view.LatestJob; latest != nil {
		view.Live = latest.State.IsLive()
		view.Stages = drawerStagesFromJob(*latest)
		view.PrevAttempts = h.collectPrevAttempts(r, *latest)
		est := h.computeBaseline(r, *latest)
		view.BaselineMs = est.BaselineMs
		view.BaselineH = est.HumanBaseline
		view.OverallETA = est.OverallETA
		view.OverallDeadline = est.Deadline
		view.LatestTookH = pickTookDuration(infoLoadDuration, *latest)
	} else if infoLoadDuration > 0 {
		view.LatestTookH = humanfmt.Duration(infoLoadDuration)
	}

	h.renderHTMLPartial(w, r, "run_drawer.html", "render drawer", view)
}

// RunDrawerClose replaces the drawer's history entry rather than adding
// one, so opening and closing it doesn't pile up entries for Back.
func (h *Handlers) RunDrawerClose(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("HX-Replace-Url", "/inventories")
	h.renderHTMLPartial(w, r, "run_drawer_closed.html", "render drawer closed", nil)
}

func pickTookDuration(infoLoad time.Duration, j jobs.Job) string {
	if infoLoad > 0 {
		return humanfmt.Duration(infoLoad)
	}
	if d := totalDuration(j); d > 0 {
		return humanfmt.Duration(d)
	}

	return ""
}

// Prefer stage-duration sum; persisted StartedAt/FinishedAt round to whole seconds so sub-second builds come back as 0.
func totalDuration(j jobs.Job) time.Duration {
	var sum time.Duration
	for _, s := range j.Stages {
		if s.Duration > 0 {
			sum += s.Duration
		}
	}
	if sum > 0 {
		return sum
	}
	if !j.StartedAt.IsZero() && !j.FinishedAt.IsZero() {
		if d := j.FinishedAt.Sub(j.StartedAt); d > 0 {
			return d
		}
	}

	return 0
}

// "done" is a terminal sentinel, not a phase — filter it out so its sub-millisecond duration doesn't clutter the timeline.
func drawerStagesFromJob(j jobs.Job) []DrawerStage {
	visible := make([]jobs.StageRecord, 0, len(j.Stages))
	for _, s := range j.Stages {
		if s.Name == "done" {
			continue
		}
		visible = append(visible, s)
	}
	if len(visible) == 0 {
		return nil
	}
	out := make([]DrawerStage, len(visible))
	last := len(visible) - 1
	for i, s := range visible {
		out[i] = DrawerStage{
			StartedAt:   s.StartedAt,
			Name:        s.Name,
			Label:       templates.StageLabel(s.Name),
			Description: templates.StageDescription(s.Name),
			DurationH:   templates.DurationFromNs(s.Duration.Nanoseconds()),
			Duration:    s.Duration,
			Failed:      s.Err != "",
			Bytes:       s.Bytes,
			Rows:        s.Rows,
			InProgress:  s.InProgress(),
			Final:       i == last,
		}
		if out[i].InProgress && !s.StartedAt.IsZero() {
			elapsed := time.Since(s.StartedAt)
			if elapsed > 0 {
				// Whole seconds, as the drawer's ticking clock renders it.
				out[i].DurationH = templates.Clock(time.Now(), s.StartedAt)
				out[i].Duration = elapsed
			}
		}
	}

	return out
}

func (h *Handlers) collectPrevAttempts(r *http.Request, latest jobs.Job) []DrawerAttempt {
	if latest.PrevJobID == "" || h.jobStore == nil {
		return nil
	}
	out := make([]DrawerAttempt, 0, drawerMaxAttempts)
	cur := latest.PrevJobID
	for range drawerMaxAttempts {
		if cur == "" {
			break
		}
		prev, err := h.jobStore.Get(r.Context(), cur)
		if err != nil {
			break
		}
		dur := ""
		if !prev.StartedAt.IsZero() && !prev.FinishedAt.IsZero() {
			d := prev.FinishedAt.Sub(prev.StartedAt)
			if d > 0 {
				dur = humanfmt.Duration(d)
			}
		}
		finished := ""
		if !prev.FinishedAt.IsZero() {
			finished = prev.FinishedAt.UTC().Format("Jan 2 15:04:05")
		}
		out = append(out, DrawerAttempt{
			JobID:      prev.ID,
			AttemptNum: prev.AttemptCount,
			State:      prev.State,
			Stage:      prev.Stage,
			DurationH:  dur,
			Summary:    summarizeBuildError(prev.Error),
			FinishedAt: finished,
			WasRetried: prev.State == jobs.StateFailed || prev.State == jobs.StateCancelled || prev.State == jobs.StateAborted,
		})
		cur = prev.PrevJobID
	}

	return out
}

type baselineEstimate struct {
	HumanBaseline string
	OverallETA    string
	Deadline      time.Time
	BaselineMs    int64
}

func (h *Handlers) computeBaseline(r *http.Request, latest jobs.Job) baselineEstimate {
	if h.jobStore == nil {
		return baselineEstimate{}
	}
	configID := latest.InventoryID.ConfigID()
	prev, err := h.jobStore.LatestSuccessfulBuildForConfig(r.Context(), configID, latest.ID)
	if err != nil {
		return baselineEstimate{}
	}
	if prev.StartedAt.IsZero() || prev.FinishedAt.IsZero() {
		return baselineEstimate{}
	}
	baseline := prev.FinishedAt.Sub(prev.StartedAt)
	if baseline <= 0 {
		return baselineEstimate{}
	}
	est := baselineEstimate{
		BaselineMs:    baseline.Milliseconds(),
		HumanBaseline: humanfmt.Duration(baseline),
	}
	if latest.State != jobs.StateRunning || latest.StartedAt.IsZero() {
		return est
	}
	elapsed := time.Since(latest.StartedAt)
	remaining := baseline - elapsed
	// remaining <= 0 means we're already past the baseline; the build
	// is overrunning. Leave OverallETA empty so the drawer doesn't
	// render "~0ns left" — elapsed alone is more honest.
	if remaining > 0 {
		est.OverallETA = humanfmt.Duration(remaining)
		est.Deadline = time.Now().Add(remaining)
	}

	return est
}
