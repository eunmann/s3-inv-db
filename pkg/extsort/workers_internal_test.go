package extsort

import (
	"runtime"
	"testing"
)

func TestIngestWorkerCount(t *testing.T) {
	for _, c := range []struct {
		name         string
		procs, files int
		want         int
	}{
		{"procs_bound", 4, 100, 4},
		{"files_bound", 64, 3, 3},
		{"unknown_files", 8, 0, 8},
		{"single_proc_floor", 1, 100, 2},
		{"single_file_floor", 16, 1, 2},
	} {
		t.Run(c.name, func(t *testing.T) {
			if got := ingestWorkerCount(c.procs, c.files); got != c.want {
				t.Errorf("ingestWorkerCount(%d, %d) = %d, want %d", c.procs, c.files, got, c.want)
			}
		})
	}
}

func TestMergeWorkerCount(t *testing.T) {
	for _, c := range []struct{ procs, want int }{
		{1, 1}, {2, 1}, {3, 1}, {4, 2}, {64, 32},
	} {
		if got := mergeWorkerCount(c.procs); got != c.want {
			t.Errorf("mergeWorkerCount(%d) = %d, want %d", c.procs, got, c.want)
		}
	}
}

// TestDefaultConfigFollowsGOMAXPROCS: a container CPU quota lowers
// GOMAXPROCS but not NumCPU, so the merge workers must follow
// GOMAXPROCS. Setting it above NumCPU makes the two differ on any host.
func TestDefaultConfigFollowsGOMAXPROCS(t *testing.T) {
	procs := runtime.NumCPU() + 3
	prev := runtime.GOMAXPROCS(procs)
	t.Cleanup(func() { runtime.GOMAXPROCS(prev) })

	if got, want := DefaultConfig().Merge.NumWorkers, mergeWorkerCount(procs); got != want {
		t.Errorf("merge workers = %d with GOMAXPROCS %d, want %d", got, procs, want)
	}
}
