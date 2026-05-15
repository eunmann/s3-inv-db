package budget

import (
	"fmt"
	"sort"

	"github.com/eunmann/s3-inv-db/internal/inventory"
)

// Plan is the eviction plan for a pending load.
type Plan struct {
	Evict         []inventory.ID
	FreedBytes    uint64
	EstimateBytes uint64
	Refusal       string
}

// Fits reports whether the plan makes room (Refusal is empty).
func (p Plan) Fits() bool { return p.Refusal == "" }

// Config supplies per-configuration retention overrides.
type Config interface {
	Retention(source, name string) uint32
}

// DefaultRetention is used when Config.Retention returns 0.
const DefaultRetention uint32 = 2

// Planner produces eviction plans honouring per-config retention and
// the Tracker's remaining capacity. Pinned runs are never evicted.
type Planner struct {
	tracker *Tracker
	config  Config
}

func NewPlanner(tracker *Tracker, config Config) *Planner {
	return &Planner{tracker: tracker, config: config}
}

// Input is one load the planner is asked to fit.
type Input struct {
	Target        inventory.ID
	EstimateBytes uint64
	All           []inventory.Info
}

// Plan computes the eviction plan for in.
func (p *Planner) Plan(in Input) (Plan, error) {
	if in.EstimateBytes == 0 {
		return Plan{EstimateBytes: 0}, nil
	}
	if p.tracker.Cap() == 0 {
		return Plan{EstimateBytes: in.EstimateBytes}, nil
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

	// Per-config retention: trim oldest within this configuration first.
	plan := Plan{EstimateBytes: in.EstimateBytes}
	configEvict := selectByConfig(candidates, targetSource, targetName, retention)
	for i := range configEvict {
		c := &configEvict[i]
		plan.Evict = append(plan.Evict, c.ID)
		plan.FreedBytes += c.IndexBytes
		candidates = drop(candidates, c.ID)
	}

	// If still short, evict across configs in LRU order.
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
			// Unknown size — refuse to evict blindly.
			pinnedPresent = true

			continue
		}
		eligible = append(eligible, *info)
	}

	return eligible, pinnedPresent
}

// selectByConfig returns the oldest runs in (source, name) that must
// come out so the new run lands at exactly `retention`.
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
	sort.SliceStable(inConfig, func(i, j int) bool {
		ai, bi := inConfig[i].LoadedAt, inConfig[j].LoadedAt
		if ai.Equal(bi) {
			return inConfig[i].LastAccessedAt.Before(inConfig[j].LastAccessedAt)
		}

		return ai.Before(bi)
	})
	drop := min(uint32(len(inConfig))-(retention-1), uint32(len(inConfig)))

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

func requiredBytes(estimate, available, alreadyFreed uint64) uint64 {
	have := available + alreadyFreed
	if estimate <= have {
		return 0
	}

	return estimate - have
}
