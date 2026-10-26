package templates

import (
	"strconv"
	"time"
)

// LiveSegment is one pipeline stage's cell in a live progress bar:
// how full it is and whether it is the stage in flight.
type LiveSegment struct {
	Fill   int
	Active bool
}

// Pipeline stage names (extsort and loader progress) the UI treats
// specially.
const (
	stageDownloading = "downloading"
	stageMerging     = "merging"
	stageBuilding    = "building"
	stageFinalizing  = "finalizing"
	stageFetching    = "fetching"
)

// barStages lists the pipeline stages a progress bar has a cell for when
// the job is at stage. Stages before the first one (queued, preparing,
// initializing) fill nothing.
func barStages(stage string) []string {
	if stage == stageFetching {
		return []string{stageFetching}
	}

	return []string{stageDownloading, stageBuilding, stageFinalizing}
}

// liveSegments lays out a job's progress bar: stages before the current
// one full, the current one at done/total, later ones empty, so the bar
// never moves backwards when a stage starts. A running stage with no
// total yet is Active with no fill (rendered indeterminate). Merging
// spilled runs happens inside the build segment, before the index
// build's own counter starts, so it shows there as indeterminate.
// The script scripts/live.js mirrors this to update bars in place; keep
// the two in step.
func liveSegments(state, stage, step string, done, total int64) []LiveSegment {
	order := barStages(stage)
	cur, counted := -1, true
	for i, s := range order {
		if s == stage {
			cur = i
		}
	}
	if stage == stageMerging {
		cur, counted = 1, false
	}
	live := state == "queued" || state == "running"
	out := make([]LiveSegment, len(order))
	for i := range out {
		switch {
		case state == "succeeded" || i < cur:
			out[i].Fill = 100
		case cur < 0:
			out[i].Active = i == 0 && state == "running"
		case i == cur && counted:
			fill := progressPct(done, total)
			if step != "" {
				// A stage with steps restarts its counter at every step: the
				// first step's share in live.js's stageFill, the most the
				// server knows without the steps before it.
				fill /= 2
			}
			out[i] = LiveSegment{Fill: fill, Active: live}
		case i == cur:
			out[i].Active = live
		}
	}

	return out
}

// clockSince renders the whole time since t for a ticking clock:
// "45s", "1m5s", "2h3m"; "" for a zero or future t. The script's
// formatDuration renders the same, so its first tick doesn't reformat.
func clockSince(now, t time.Time) string {
	if t.IsZero() || !now.After(t) {
		return ""
	}

	return span(now.Sub(t))
}

// span renders d in whole units, as clockSince does: "45s", "1m5s",
// "2h3m".
func span(d time.Duration) string {
	d = d.Truncate(time.Second)
	unit := func(n time.Duration, suffix string) string { return strconv.FormatInt(int64(n), 10) + suffix }
	switch {
	case d < time.Minute:
		return unit(d/time.Second, "s")
	case d < time.Hour:
		out := unit(d/time.Minute, "m")
		if s := d % time.Minute / time.Second; s != 0 {
			out += unit(s, "s")
		}

		return out
	default:
		out := unit(d/time.Hour, "h")
		if m := d % time.Hour / time.Minute; m != 0 {
			out += unit(m, "m")
		}

		return out
	}
}

// NavLink is one primary navigation entry. Page is the Title the page's
// handler renders with, which marks the link current.
type NavLink struct {
	Href  string
	Label string
	Page  string
}

// navLinks lists the primary navigation, in order.
func navLinks() []NavLink {
	return []NavLink{
		{Href: "/", Label: "Dashboard", Page: "Dashboard"},
		{Href: "/inventories", Label: "Inventories", Page: "Inventories"},
		{Href: "/browse", Label: "Browse", Page: "Browse"},
		{Href: "/compare", Label: "Compare", Page: "Compare runs"},
		{Href: "/help", Label: "Help", Page: "Help"},
	}
}

// rfc3339 formats t for data attributes scripts parse; "" for zero.
func rfc3339(t time.Time) string {
	if t.IsZero() {
		return ""
	}

	return t.UTC().Format(time.RFC3339Nano)
}
