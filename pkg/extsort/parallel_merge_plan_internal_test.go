package extsort

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime/debug"
	"slices"
	"testing"
)

// sizedRuns writes n files whose sizes are 1..n bytes, in reverse order
// of size, so a plan that sorts by size cannot pass by keeping input order.
func sizedRuns(t *testing.T, n int) []string {
	t.Helper()
	dir := t.TempDir()
	paths := make([]string, n)
	for i := range n {
		paths[i] = filepath.Join(dir, fmt.Sprintf("run_%03d", i))
		if err := os.WriteFile(paths[i], make([]byte, n-i), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	return paths
}

func TestPlanRound(t *testing.T) {
	cases := []struct {
		name                     string
		runs, maxFanIn, finalFan int
		wantGroups               []int // group sizes
		wantKept                 int
		partial                  bool // merges only enough to reach finalFan
	}{
		// One small group brings 20 runs under a final fan-in of 16.
		{"just_over_final", 20, 16, 16, []int{5}, 15, true},
		{"one_over_final", 17, 16, 16, []int{2}, 15, true},
		// Excess 36 needs ceil(36/15) = 3 groups of 13.
		{"several_groups", 100, 16, 64, []int{13, 13, 13}, 61, true},
		// No single round reaches 4 runs: merge all, evenly, no singletons.
		{"merge_everything", 17, 4, 4, []int{3, 3, 4, 3, 4}, 0, false},
		// Fan-in 2, odd count: the lone run passes through untouched.
		{"fan_in_two_odd", 5, 2, 2, []int{2, 2}, 1, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			paths := sizedRuns(t, c.runs)
			m := NewParallelMerger(ParallelMergeConfig{MaxFanIn: c.maxFanIn, FinalFanIn: c.finalFan, TempDir: t.TempDir()})
			plan := m.planRound(paths)

			sizes := make([]int, len(plan.groups))
			for i, g := range plan.groups {
				sizes[i] = len(g)
			}
			if !slices.Equal(sizes, c.wantGroups) || len(plan.kept) != c.wantKept {
				t.Fatalf("groups %v kept %d, want groups %v kept %d", sizes, len(plan.kept), c.wantGroups, c.wantKept)
			}
			// Every run is planned exactly once.
			all := slices.Concat(append(plan.groups, plan.kept)...)
			slices.Sort(all)
			want := slices.Sorted(slices.Values(paths))
			if !slices.Equal(all, want) {
				t.Fatalf("plan covers %d runs, want each of %d once", len(all), len(paths))
			}
			// A partial round merges the smallest runs (the last paths).
			if c.partial {
				for _, g := range plan.groups {
					for _, p := range g {
						if idx := slices.Index(paths, p); idx < c.wantKept {
							t.Errorf("merged %s, one of the largest runs", filepath.Base(p))
						}
					}
				}
			}
			after := len(plan.kept) + len(plan.groups)
			if c.partial && after != c.finalFan {
				t.Errorf("partial round leaves %d runs, want exactly %d", after, c.finalFan)
			}
		})
	}
}

func TestFinalFanInFitsMemoryLimit(t *testing.T) {
	const gib = int64(1) << 30
	cases := []struct {
		name        string
		limit       int64
		wantFanIn   int
		wantWorkers int
	}{
		{name: "unset", limit: unsetMemoryLimit, wantFanIn: DefaultFinalFanIn, wantWorkers: 32},
		{name: "1GiB", limit: gib, wantFanIn: 64, wantWorkers: 6},
		{name: "4GiB", limit: 4 * gib, wantFanIn: 64, wantWorkers: 25},
		{name: "256MiB shrinks", limit: gib / 4, wantFanIn: 23, wantWorkers: 1},
		{name: "tiny floors at MaxFanIn", limit: 1, wantFanIn: 16, wantWorkers: 1},
		{name: "huge", limit: 1024 * gib, wantFanIn: DefaultFinalFanIn, wantWorkers: 32},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := finalFanInFor(c.limit, 16, 0); got != c.wantFanIn {
				t.Errorf("finalFanInFor = %d, want %d", got, c.wantFanIn)
			}
			if got := roundWorkersFor(c.limit, 32, 16); got != c.wantWorkers {
				t.Errorf("roundWorkersFor = %d, want %d", got, c.wantWorkers)
			}
			budget, ok := openRunBytes(c.limit)
			if !ok {
				return
			}
			// Every disk round's readers, and the final merge's with
			// their read-ahead, fit the same budget.
			if open := int64(roundWorkersFor(c.limit, 32, 16)) * 16 * openRunHeapBytes; open > budget && budget >= 16*openRunHeapBytes {
				t.Errorf("round holds %d bytes of readers, budget %d", open, budget)
			}
			if final := int64(c.wantFanIn) * (openRunHeapBytes + aheadRunHeapBytes); final > budget && c.wantFanIn > 16 {
				t.Errorf("final merge holds %d bytes of readers, budget %d", final, budget)
			}
		})
	}
}

func TestNewParallelMergerAppliesMemoryLimit(t *testing.T) {
	cases := []struct {
		name           string
		limit          int64
		wantWorkers    int
		wantFinalFanIn int
	}{
		{name: "256MiB", limit: 256 << 20, wantWorkers: 1, wantFinalFanIn: 23},
		{name: "unset", limit: math.MaxInt64, wantWorkers: 32, wantFinalFanIn: DefaultFinalFanIn},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			prev := debug.SetMemoryLimit(c.limit)
			t.Cleanup(func() { debug.SetMemoryLimit(prev) })

			stats := NewParallelMerger(ParallelMergeConfig{NumWorkers: 32, MaxFanIn: 16, TempDir: t.TempDir()}).Statistics()
			if stats.Workers != c.wantWorkers || stats.FinalFanIn != c.wantFinalFanIn {
				t.Fatalf("workers %d final fan-in %d, want %d and %d", stats.Workers, stats.FinalFanIn, c.wantWorkers, c.wantFinalFanIn)
			}
		})
	}
}
