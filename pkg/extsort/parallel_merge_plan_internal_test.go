package extsort

import (
	"fmt"
	"os"
	"path/filepath"
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
	}{
		// One small group brings 20 runs under a final fan-in of 16.
		{"just_over_final", 20, 16, 16, []int{5}, 15},
		{"one_over_final", 17, 16, 16, []int{2}, 15},
		// Excess 36 needs ceil(36/15) = 3 groups of 13.
		{"several_groups", 100, 16, 64, []int{13, 13, 13}, 61},
		// No single round reaches 4 runs: merge all, evenly, no singletons.
		{"merge_everything", 17, 4, 4, []int{3, 3, 4, 3, 4}, 0},
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
			if c.wantKept > 0 {
				for _, g := range plan.groups {
					for _, p := range g {
						if idx := slices.Index(paths, p); idx < c.wantKept {
							t.Errorf("merged %s, one of the largest runs", filepath.Base(p))
						}
					}
				}
			}
			after := len(plan.kept) + len(plan.groups)
			if c.wantKept > 0 && after != c.finalFan {
				t.Errorf("partial round leaves %d runs, want exactly %d", after, c.finalFan)
			}
		})
	}
}
