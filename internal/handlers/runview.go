package handlers

import (
	"fmt"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/pkg/humanfmt"
	"github.com/eunmann/s3-inv-db/pkg/indexread"
)

// RunStats is what a loaded run holds: its root prefix's totals and the
// monthly cost Browse estimates for the root.
type RunStats struct {
	CostEstimate *CostEstimate // with per-tier costs; nil without tier data
	ObjectsH     string
	BytesH       string
	PrefixesH    string
	CostH        string // "" without tier data
	Tiers        []TierStats
	Objects      uint64
	Bytes        uint64
	CostMicro    uint64
	Prefixes     uint64
}

// Line is the one-line summary rows show:
// "1.5M objects · 549.43 TiB · est. $7.4K/mo".
func (s RunStats) Line() string {
	noun := " objects"
	if s.Objects == 1 {
		noun = " object"
	}
	line := s.ObjectsH + noun + " · " + s.BytesH
	if s.CostH != "" {
		line += " · est. " + s.CostH + "/mo"
	}

	return line
}

// runStats reads a loaded run's root totals. Peek, not WithIndex: a
// page listing runs isn't a use of each, and touching them would
// reorder the eviction LRU. WithTiers adds the per-tier breakdown and
// costs the drawer shows. O(tiers), independent of the index's size.
func (h *Handlers) runStats(id inventory.ID, withTiers bool) (RunStats, error) {
	var s RunStats
	err := h.manager.Peek(id, func(idx *indexread.Index) error {
		pos, ok := idx.Lookup("")
		if !ok {
			return nil
		}
		root := idx.Stats(pos)
		s.Objects, s.Bytes, s.Prefixes = root.ObjectCount, root.TotalBytes, idx.Count()
		breakdown := idx.TierBreakdown(pos)
		if est := h.computeCostEstimate(breakdown, withTiers); est != nil {
			s.CostMicro = est.TotalMicrodollars
			s.CostH = est.TotalFormatted
			if withTiers {
				s.CostEstimate = est
				s.Tiers = assembleTierStats(breakdown)
			}
		}

		return nil
	})
	if err != nil {
		return RunStats{}, fmt.Errorf("root stats of %s: %w", id, err)
	}
	s.ObjectsH = humanfmt.CountUint64(s.Objects)
	s.BytesH = humanfmt.BytesUint64(s.Bytes)
	s.PrefixesH = humanfmt.CountUint64(s.Prefixes)

	return s, nil
}

// domID makes s usable as an HTML id and in a CSS selector: letters,
// digits and '-' stay, every other byte becomes _xx (hex), so distinct
// inputs keep distinct ids.
func domID(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for i := range len(s) {
		c := s[i]
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' {
			b.WriteByte(c)

			continue
		}
		fmt.Fprintf(&b, "_%02x", c)
	}

	return b.String()
}

func browseHref(id inventory.ID) string {
	return "/browse?" + url.Values{"inventory_id": {string(id)}}.Encode()
}

// compareHref compares from with to; an empty to opens Compare with
// only from chosen.
func compareHref(from, to inventory.ID) string {
	q := url.Values{"from": {string(from)}}
	if to != "" {
		q.Set("to", string(to))
	}

	return "/compare?" + q.Encode()
}

// sortRunsNewestFirst orders one configuration's runs newest first; run
// folder names are timestamps, so they sort as strings. A placeholder
// with no run goes last.
func sortRunsNewestFirst(runs []DiscoveredRowView) {
	slices.SortStableFunc(runs, func(a, b DiscoveredRowView) int { return strings.Compare(b.Run, a.Run) })
}

// linkRuns marks the newest run of one configuration's runs (newest
// first) and points each loaded run at the loaded run before it, the
// one "Compare with previous" diffs against.
func linkRuns(runs []DiscoveredRowView) {
	var older inventory.ID
	for i := len(runs) - 1; i >= 0; i-- {
		r := &runs[i]
		if r.Run == "" || !r.State.IsLoaded() {
			continue
		}
		r.PrevLoaded = older
		older = r.CompositeID()
	}
	for i := range runs {
		if runs[i].Run != "" {
			runs[i].IsLatest = true

			break
		}
	}
}

// RunDelta is the change between two loaded runs of one configuration.
type RunDelta struct {
	BytesH  string // "+12.4 GiB"
	CostH   string // "+$3.10/mo"; "" unless both runs are priced
	SinceH  string // the older run's date, "Oct 25"
	Changed bool
}

func newRunDelta(prev, cur RunStats, prevRun string, now time.Time) RunDelta {
	d := RunDelta{
		SinceH:  runDateLabel(prevRun, now),
		Changed: prev.Bytes != cur.Bytes || prev.CostMicro != cur.CostMicro,
	}
	d.BytesH = formatDelta(prev.Bytes, cur.Bytes, int64(cur.Bytes)-int64(prev.Bytes), humanfmt.BytesUint64).DeltaH
	if prev.CostH != "" && cur.CostH != "" {
		d.CostH = formatCostDelta(prev.CostMicro, cur.CostMicro).DeltaH + "/mo"
	}

	return d
}

// Line is "+12.4 GiB · +$3.10/mo since Oct 25", or "No change since
// Oct 25".
func (d RunDelta) Line() string {
	if !d.Changed {
		return "No change since " + d.SinceH
	}
	line := d.BytesH
	if d.CostH != "" {
		line += " · " + d.CostH
	}

	return line + " since " + d.SinceH
}

// runDateLabel is a run folder name as a short date, "Oct 25", with the
// year when it isn't now's. A name that isn't a timestamp comes back
// unchanged.
func runDateLabel(run string, now time.Time) string {
	for _, layout := range []string{"2006-01-02T15-04-05Z", "2006-01-02T15-04Z"} {
		t, err := time.Parse(layout, run)
		if err != nil {
			continue
		}
		if t.Year() == now.Year() {
			return t.Format("Jan 2")
		}

		return t.Format("Jan 2, 2006")
	}

	return run
}
