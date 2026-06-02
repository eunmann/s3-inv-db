package handlers

import (
	"errors"
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

type RunDrawerView struct {
	InventoryID  inventory.ID
	State        inventory.State
	StateError   string
	LatestJob    *jobs.Job
	PrevAttempts []DrawerAttempt
	OverallETA   string
	BaselineH    string
	LatestTookH  string
	BaselineMs   int64
	Stages       []DrawerStage
	Live         bool
	HasInventory bool
}

func (v RunDrawerView) CompositePath() string { return string(v.InventoryID) }

type DrawerStage struct {
	Name        string
	Label       string
	Description string
	DurationH   string
	Err         string
	Duration    time.Duration
	Bytes       uint64
	Rows        uint64
	InProgress  bool
	Final       bool
}

type DrawerAttempt struct {
	JobID      jobs.ID
	State      jobs.State
	Stage      string
	DurationH  string
	Error      string
	AttemptNum int
	FinishedAt string
	WasRetried bool
}

func (h *Handlers) RunDrawer(w http.ResponseWriter, r *http.Request) {
	src := chi.URLParam(r, "src")
	name := chi.URLParam(r, "id")
	run := chi.URLParam(r, "run")
	composite := inventory.ID(src + "/" + name + "/" + run)
	view := RunDrawerView{InventoryID: composite}

	// Info.LoadDuration is the canonical "took N" — keeps drawer + row in sync (per-stage sum excludes manager bookkeeping).
	var infoLoadDuration time.Duration
	if info, ok := h.manager.Get(composite); ok {
		view.HasInventory = true
		view.State = info.State
		view.StateError = info.Error
		infoLoadDuration = info.LoadDuration
	}

	w.Header().Set("HX-Push-Url", "/inventories#run="+string(composite))

	if h.jobStore == nil {
		h.renderHTMLPartial(w, r, "run_drawer.html", "render drawer", view)

		return
	}

	latest, err := h.jobStore.LatestForInventory(r.Context(), composite)
	switch {
	case err == nil:
		view.LatestJob = &latest
		view.Live = latest.State.IsLive()
		view.Stages = drawerStagesFromJob(latest)
		view.PrevAttempts = h.collectPrevAttempts(r, latest)
		est := h.computeBaseline(r, latest)
		view.BaselineMs = est.BaselineMs
		view.BaselineH = est.HumanBaseline
		view.OverallETA = est.OverallETA
		view.LatestTookH = pickTookDuration(infoLoadDuration, latest)
	case errors.Is(err, jobs.ErrStoreNotFound):
	default:
		zerolog.Ctx(r.Context()).Warn().Err(err).
			Stringer("composite", composite).
			Msg("look up latest job for drawer")
	}

	h.renderHTMLPartial(w, r, "run_drawer.html", "render drawer", view)
}

func (h *Handlers) RunDrawerClose(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("HX-Push-Url", "/inventories")
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
			Name:        s.Name,
			Label:       templates.StageLabel(s.Name),
			Description: templates.StageDescription(s.Name),
			DurationH:   templates.DurationFromNs(s.Duration.Nanoseconds()),
			Duration:    s.Duration,
			Err:         s.Err,
			Bytes:       s.Bytes,
			Rows:        s.Rows,
			InProgress:  s.InProgress(),
			Final:       i == last,
		}
		if out[i].InProgress && !s.StartedAt.IsZero() {
			elapsed := time.Since(s.StartedAt)
			if elapsed > 0 {
				out[i].DurationH = humanfmt.Duration(elapsed)
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
			Error:      prev.Error,
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
	}

	return est
}
