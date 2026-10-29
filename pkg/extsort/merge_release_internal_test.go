package extsort

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/eunmann/s3-inv-db/internal/benchutil"
	"github.com/eunmann/s3-inv-db/pkg/extsort/events"
)

// mergeBuildPipeline returns a pipeline holding runFiles spilled run
// files and memRuns in-memory runs over the same synthetic objects, as
// ingest leaves them for runMergeBuildPhase.
func mergeBuildPipeline(t *testing.T, runFiles, memRuns int) *Pipeline {
	t.Helper()
	objects := benchutil.NewGenerator(benchutil.S3RealisticConfig(20_000)).Generate()
	p := NewPipeline(DefaultConfig(), nil)
	p.tempDir = t.TempDir()
	var mask uint32
	for _, o := range objects {
		mask |= 1 << o.TierID
	}
	p.presentTierMask.Store(mask)

	parts := runFiles + memRuns
	for i := range memRuns {
		agg := NewAggregator(0, 0)
		for j := runFiles + i; j < len(objects); j += parts {
			agg.AddObject(objects[j].Key, objects[j].Size, objects[j].TierID)
		}
		if err := p.keepResidual(agg); err != nil {
			t.Fatalf("keepResidual: %v", err)
		}
	}
	var onDisk []benchutil.FakeObject
	for j, o := range objects {
		if j%parts < runFiles {
			onDisk = append(onDisk, o)
		}
	}
	if runFiles > 0 {
		p.runFiles = writePartitionedRunFiles(t, p.tempDir, onDisk, runFiles)
	}

	return p
}

// runFilesLeft lists the run files still in dir.
func runFilesLeft(t *testing.T, dir string) []string {
	t.Helper()
	left, err := filepath.Glob(filepath.Join(dir, "*.crun"))
	if err != nil {
		t.Fatalf("glob run files: %v", err)
	}

	return left
}

// The final merge's run files are removed once the builder has read
// them, so Finalize's scratch and the index never share the disk with
// them.
func TestMergeBuildRemovesRunsBeforeFinalize(t *testing.T) {
	for _, tc := range []struct {
		name              string
		runFiles, memRuns int
	}{
		{name: "single run", runFiles: 1},
		{name: "runs", runFiles: 3},
		{name: "runs and memory", runFiles: 2, memRuns: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := mergeBuildPipeline(t, tc.runFiles, tc.memRuns)
			if got := len(runFilesLeft(t, p.tempDir)); got != tc.runFiles {
				t.Fatalf("setup wrote %d run files, want %d", got, tc.runFiles)
			}
			var (
				once        sync.Once
				atFinalize  []string
				finalizeRan bool
			)
			p.config.Observe.OnProgress = func(ev events.Progress) {
				if ev.Stage != finalizingStage {
					return
				}
				once.Do(func() {
					finalizeRan = true
					atFinalize = runFilesLeft(t, p.tempDir)
				})
			}

			res, err := p.runMergeBuildPhase(t.Context(), filepath.Join(t.TempDir(), "index"))
			if err != nil {
				t.Fatalf("runMergeBuildPhase: %v", err)
			}
			if res.PrefixCount == 0 {
				t.Fatal("built an empty index")
			}
			if !finalizeRan {
				t.Fatal("no finalizing progress reported")
			}
			if len(atFinalize) != 0 {
				t.Errorf("run files still on disk when Finalize started: %v", atFinalize)
			}
			if left := runFilesLeft(t, p.tempDir); len(left) != 0 {
				t.Errorf("run files left after the build: %v", left)
			}
		})
	}
}

// A build that fails while reading the merge still removes the run
// files and the index directory it created.
func TestMergeBuildFailureRemovesRuns(t *testing.T) {
	for _, tc := range []struct {
		name              string
		runFiles, memRuns int
	}{
		{name: "single run", runFiles: 1},
		{name: "runs and memory", runFiles: 3, memRuns: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := mergeBuildPipeline(t, tc.runFiles, tc.memRuns)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			p.config.Observe.OnProgress = func(ev events.Progress) {
				if ev.Stage == "building" {
					cancel()
				}
			}
			outDir := filepath.Join(t.TempDir(), "index")

			_, err := p.runMergeBuildPhase(ctx, outDir)
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("runMergeBuildPhase err = %v, want context.Canceled", err)
			}
			if left := runFilesLeft(t, p.tempDir); len(left) != 0 {
				t.Errorf("run files left after a failed build: %v", left)
			}
			if _, err := os.Stat(outDir); !errors.Is(err, os.ErrNotExist) {
				t.Errorf("index directory survived a failed build: %v", err)
			}
		})
	}
}
