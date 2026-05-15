package budget

import (
	"fmt"
	"sort"
	"time"

	"github.com/eunmann/s3-inv-db/internal/inventory"
)

// Plan describes how to make room for a pending load: a list of
// inventory IDs to unload first, plus a verdict and (when refused) a
// structured Reason callers can surface verbatim in the UI. A nil-error
// Plan with empty Evict means the load fits as-is.
type Plan struct {
	// Evict is the ordered list of inventory IDs to unload before the
	// new load proceeds. May be empty.
	Evict []inventory.ID

	// FreedBytes is the sum of IndexBytes for everything in Evict.
	FreedBytes uint64

	// EstimateBytes is the byte estimate the planner was asked to fit.
	EstimateBytes uint64

	// Refusal is non-empty when the load cannot proceed even after
	// evicting every auto-loaded run. Surfaced in the UI.
	Refusal string
}

// Fits reports whether the plan actually makes room (Refusal == "").
func (p Plan) Fits() bool { return p.Refusal == "" }

// Config supplies the planner with per-config retention overrides.
// Retention(source, name) returns the configured max-loaded run count
// for that configuration; zero falls back to DefaultRetention.
type Config interface {
	Retention(source, name string) uint32
}

// DefaultRetention is used when Config.Retention returns 0.
const DefaultRetention uint32 = 2

// Planner produces an eviction plan that respects (a) per-config
// retention and (b) the Tracker's remaining capacity. Pinned runs and
// the in-flight loading targets are never evicted.
type Planner struct {
	tracker *Tracker
	config  Config
}

// NewPlanner constructs a Planner. Tracker may not be nil; config may
// be nil (every configuration falls back to DefaultRetention).
func NewPlanner(tracker *Tracker, config Config) *Planner {
	return &Planner{tracker: tracker, config: config}
}

// Input describes the load the planner is asked to fit.
type Input struct {
	// Target identifies the run we're trying to load. Used to skip
	// itself in the candidate list and to read its source/inventory
	// for the per-config retention check.
	Target inventory.ID

	// EstimateBytes is the expected final on-disk size of the new
	// index, post-load. Working/scratch overhead is accounted for via
	// the tracker's headroom; callers may pass an inflated estimate
	// for safety.
	EstimateBytes uint64

	// All is a snapshot of the manager's current Info set. The planner
	// classifies these into "loaded" (eviction candidates) and "other"
	// (ignored). Loaded entries must have non-zero IndexBytes for the
	// planner to consider them — runs with unknown size are skipped
	// (better to refuse than to evict blindly).
	All []inventory.Info
}

// Plan computes the eviction plan for in. Algorithm:
//
//  1. Drop any candidate that's pinned, currently loading, the target
//     itself, or already not-loaded.
//  2. Enforce per-config retention: within the target's source/name,
//     evict oldest auto-loaded so that after this load we sit at
//     retention - 1 (i.e. (retention - 1) old + 1 new = retention).
//  3. If still over budget, evict auto-loaded runs across all configs
//     in LRU order (LastAccessedAt asc; oldest LoadedAt as tiebreak).
//  4. Stop as soon as the freed bytes plus the tracker's existing
//     available are enough to fit in.EstimateBytes.
//  5. If we can't make room without evicting a pinned run, return
//     Refusal explaining what blocked us.
func (p *Planner) Plan(in Input) (Plan, error) {
	if in.EstimateBytes == 0 {
		return Plan{EstimateBytes: 0}, nil
	}
	if p.tracker.Cap() == 0 {
		return Plan{
			EstimateBytes: in.EstimateBytes,
			Refusal:       "disk budget is not configured (--max-index-disk unset)",
		}, nil
	}
	targetSource, targetName, _, ok := in.Target.Split()
	if !ok {
		return Plan{}, fmt.Errorf("target id %q is not a 3-part inventory ID", in.Target)
	}

	retention := DefaultRetention
	if p.config != nil {
		if r := p.config.Retention(targetSource, targetName); r > 0 {
			retention = r
		}
	}

	candidates, pinnedPresent := p.candidates(in)

	// (2) Per-config retention: drop oldest within this configuration.
	plan := Plan{EstimateBytes: in.EstimateBytes}
	configEvict := selectByConfig(candidates, targetSource, targetName, retention)
	for i := range configEvict {
		c := &configEvict[i]
		plan.Evict = append(plan.Evict, c.ID)
		plan.FreedBytes += c.IndexBytes
		candidates = drop(candidates, c.ID)
	}

	// (3+4) Global LRU fill until in.EstimateBytes fits.
	need := requiredBytes(in.EstimateBytes, p.tracker.Available(), plan.FreedBytes)
	if need > 0 {
		sort.SliceStable(candidates, func(i, j int) bool {
			a, b := &candidates[i], &candidates[j]
			if a.LastAccessedAt.Equal(b.LastAccessedAt) {
				return a.LoadedAt.Before(b.LoadedAt)
			}
			return a.LastAccessedAt.Before(b.LastAccessedAt)
		})
		for i := range candidates {
			if need == 0 {
				break
			}
			c := &candidates[i]
			plan.Evict = append(plan.Evict, c.ID)
			plan.FreedBytes += c.IndexBytes
			if c.IndexBytes >= need {
				need = 0
			} else {
				need -= c.IndexBytes
			}
		}
	}

	if need > 0 {
		// Couldn't make enough room out of evictable runs.
		if pinnedPresent {
			plan.Refusal = fmt.Sprintf(
				"need %d bytes after planned evictions but every remaining run is pinned; unpin a run or raise --max-index-disk",
				need)
		} else {
			plan.Refusal = fmt.Sprintf(
				"need %d bytes but only %d available even after evicting every auto-loaded run; raise --max-index-disk",
				in.EstimateBytes, p.tracker.Available()+plan.FreedBytes)
		}
	}
	return plan, nil
}

// candidates filters in.All down to the runs the planner may evict.
func (p *Planner) candidates(in Input) (eligible []inventory.Info, pinnedPresent bool) {
	for i := range in.All {
		info := &in.All[i]
		if info.ID == in.Target {
			continue
		}
		if info.State != inventory.StateLoaded {
			continue
		}
		if info.Pinned {
			pinnedPresent = true
			continue
		}
		if info.IndexBytes == 0 {
			// Unknown size — refuse to evict blindly. Counts toward
			// pinnedPresent so the refusal message is honest.
			pinnedPresent = true
			continue
		}
		eligible = append(eligible, *info)
	}
	return eligible, pinnedPresent
}

// selectByConfig returns the runs within (source,name) that exceed
// retention-1 — i.e. the oldest auto-loaded runs that must come out so
// the new run lands at exactly `retention` loaded.
func selectByConfig(pool []inventory.Info, source, name string, retention uint32) []inventory.Info {
	var inConfig []inventory.Info
	for i := range pool {
		src, n, _, ok := pool[i].ID.Split()
		if !ok {
			continue
		}
		if src == source && n == name {
			inConfig = append(inConfig, pool[i])
		}
	}
	if uint32(len(inConfig)) < retention {
		return nil
	}
	// Sort oldest LoadedAt first; LastAccessedAt as tiebreak.
	sort.SliceStable(inConfig, func(i, j int) bool {
		ai, bi := inConfig[i].LoadedAt, inConfig[j].LoadedAt
		if ai.Equal(bi) {
			return inConfig[i].LastAccessedAt.Before(inConfig[j].LastAccessedAt)
		}
		return ai.Before(bi)
	})
	// Number to drop so that adding the new run lands at retention.
	drop := uint32(len(inConfig)) - (retention - 1)
	if drop > uint32(len(inConfig)) {
		drop = uint32(len(inConfig))
	}
	return inConfig[:drop]
}

func drop(pool []inventory.Info, id inventory.ID) []inventory.Info {
	out := pool[:0]
	for i := range pool {
		if pool[i].ID == id {
			continue
		}
		out = append(out, pool[i])
	}
	return out
}

// requiredBytes returns how many more bytes need freeing beyond what
// the tracker reports as already-available plus the bytes the planner
// has already queued for eviction.
func requiredBytes(estimate, available, alreadyFreed uint64) uint64 {
	have := available + alreadyFreed
	if estimate <= have {
		return 0
	}
	return estimate - have
}

// nowFunc lets tests override time. Production code uses time.Now via
// the default zero value.
type nowFunc func() time.Time

var _ = nowFunc(time.Now)
