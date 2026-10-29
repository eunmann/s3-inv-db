package extsort

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/eunmann/s3-inv-db/pkg/format"
	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

// replayCase is a set of rows and the tiers they are declared over.
type replayCase struct {
	name    string
	present []tiers.ID
	rows    []*PrefixRow
}

// replayCases cover every file the replay writes and its edges: 1, 3
// and 16 tiers in both layouts, the multi layout's masks, core patches,
// a deep tree and a wide one, and depths of one row and of fewer rows
// than a chunk.
func replayCases() []replayCase {
	three := []tiers.ID{tiers.GlacierIR, tiers.ExpressOneZone, tiers.Snow}
	ten := make([]tiers.ID, 10)
	for i := range ten {
		ten[i] = tiers.ID(i)
	}

	return []replayCase{
		{"tiers=16/multi", tiers.AllTierIDs(), goldenTierRows(tiers.AllTierIDs(), 0.08, 0)},
		{"tiers=16/dense", tiers.AllTierIDs(), goldenTierRows(tiers.AllTierIDs(), 0.98, 1)},
		{"tiers=3/multi", three, goldenTierRows(three, 0.15, 2)},
		{"tiers=3/dense", three, goldenTierRows(three, 0.95, 3)},
		{"tiers=1", []tiers.ID{tiers.Outposts}, goldenTierRows([]tiers.ID{tiers.Outposts}, 0.7, 4)},
		{"patched core, wide", []tiers.ID{tiers.Standard, tiers.GlacierIR, tiers.DeepArchive}, edgeTreeRows(3000, 900, 2000)},
		{"dense tiers, deep", three, deepTierRows(9, three)},
		{"multi tiers with masks, wide", ten, flatTierRows(5000, func(i int) []tiers.ID {
			if i%20 == 0 {
				return []tiers.ID{ten[i%10], ten[(i+3)%10], ten[(i+7)%10]}
			}

			return []tiers.ID{ten[i%10]}
		})},
	}
}

// buildIndex builds c's rows into a new directory, replaying the spool
// in chunks of markEvery rows on workers goroutines, and returns the
// directory and the builder.
func buildIndex(t *testing.T, c replayCase, markEvery uint64, workers int) (string, *IndexBuilder) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "index")
	b, err := NewIndexBuilder(dir, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	b.spool.markEvery, b.replayWorkers = markEvery, workers
	if err := b.SetPresentTiers(c.present); err != nil {
		t.Fatal(err)
	}
	for _, row := range c.rows {
		if err := b.Add(row); err != nil {
			t.Fatalf("Add(%q): %v", row.Prefix, err)
		}
	}
	if err := b.Finalize(); err != nil {
		t.Fatal(err)
	}

	return dir, b
}

// TestParallelReplayMatchesSequential: the replay in chunks, of any size
// down to one depth block and on any number of workers, writes every
// index file byte for byte as the sequential writers do, and records
// every file's checksum as it writes it.
func TestParallelReplayMatchesSequential(t *testing.T) {
	for _, c := range replayCases() {
		want, _ := buildIndex(t, c, spoolMarkRows, 1)
		wantFiles := indexFiles(t, want)
		for _, split := range []struct {
			markEvery uint64
			workers   int
		}{{64, 8}, {128, 3}, {192, 2}, {320, 4}} {
			t.Run(fmt.Sprintf("%s/rows=%d/workers=%d", c.name, split.markEvery, split.workers), func(t *testing.T) {
				if rows := uint64(len(c.rows)); rows < 2*split.markEvery {
					t.Fatalf("%d rows replay sequentially in chunks of %d", rows, split.markEvery)
				}
				got, b := buildIndex(t, c, split.markEvery, split.workers)
				if gotFiles := indexFiles(t, got); !slices.Equal(gotFiles, wantFiles) {
					t.Errorf("files differ from the sequential replay's:\n%q\nwant\n%q", gotFiles, wantFiles)
				}
				reread, err := format.WriteManifest(got, b.posCount, b.maxDepth, b.checksums, nil)
				if err != nil {
					t.Fatal(err)
				}
				if len(reread) > 0 {
					t.Errorf("files without a checksum from their writer: %v", reread)
				}
			})
		}
	}
}

// spooledBuilder is a builder that has spooled c's rows in chunks of
// markEvery rows, ready for writeLevels, and its segment count.
//
//nolint:gocritic // gocritic wants named returns, nonamedreturns forbids them
func spooledBuilder(t *testing.T, c replayCase, markEvery uint64) (*IndexBuilder, uint32) {
	t.Helper()
	b, err := NewIndexBuilder(filepath.Join(t.TempDir(), "index"), t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(b.cleanup)
	b.spool.markEvery, b.replayWorkers = markEvery, 4
	b.stepProgress = func(string, int64, int64) {}
	if err := b.SetPresentTiers(c.present); err != nil {
		t.Fatal(err)
	}
	for _, row := range c.rows {
		if err := b.Add(row); err != nil {
			t.Fatal(err)
		}
	}
	segments, err := b.finishSpool()
	if err != nil {
		t.Fatal(err)
	}

	return b, segments
}

// TestParallelReplayFailureLeavesNothing: a chunk that cannot be read,
// spool marks that disagree with the rows, more rows than marked and a
// cancelled build each fail the replay with their error, every worker
// stops, and no file the replay writes is left behind.
func TestParallelReplayFailureLeavesNothing(t *testing.T) {
	c := replayCases()[2]
	const markEvery = 64
	for _, tc := range []struct {
		name  string
		spoil func(t *testing.T, b *IndexBuilder)
		ctx   func() context.Context
		want  string
	}{
		{"truncated chunk", func(t *testing.T, b *IndexBuilder) {
			t.Helper()
			sf := b.spool.depths[2]
			if err := os.Truncate(sf.path, int64(sf.marks[3].off)+5); err != nil {
				t.Fatal(err)
			}
		}, context.Background, errSpoolTruncated.Error()},
		{"marks disagree", func(_ *testing.T, b *IndexBuilder) {
			b.spool.depths[2].marks[2].multi++
		}, context.Background, "tier"},
		{"rows past the marks", func(_ *testing.T, b *IndexBuilder) {
			b.spool.depths[2].rows--
		}, context.Background, "more than the"},
		{"cancelled", func(*testing.T, *IndexBuilder) {}, func() context.Context {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()

			return ctx
		}, context.Canceled.Error()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := runtime.NumGoroutine()
			b, segments := spooledBuilder(t, c, markEvery)
			tc.spoil(t, b)
			_, err := b.writeLevels(tc.ctx(), b.levelStarts(), segments)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("writeLevels = %v, want an error holding %q", err, tc.want)
			}
			for _, name := range []string{
				format.CoreStatsFile, format.CorePatchFile, format.CorePatchRowsFile, format.PrefixTreeFile,
				format.DepthOffsetsFile, format.DepthBlocksFile,
				filepath.Join(format.TierStatsDir, format.TierMultiFile), filepath.Join(format.TierStatsDir, format.TierMultiRowsFile),
				filepath.Join(format.TierStatsDir, format.TierMultiOffsetsFile), filepath.Join(format.TierStatsDir, format.TierDenseFile),
			} {
				if _, err := os.Stat(filepath.Join(b.outDir, name)); !errors.Is(err, os.ErrNotExist) {
					t.Errorf("%s left behind: %v", name, err)
				}
			}
			waitGoroutines(t, before)
		})
	}
}

// waitGoroutines fails unless the goroutines fall back to n within a
// second: the replay's workers have stopped.
func waitGoroutines(t *testing.T, n int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for runtime.NumGoroutine() > n {
		if time.Now().After(deadline) {
			t.Fatalf("%d goroutines, %d before the replay", runtime.NumGoroutine(), n)
		}
		time.Sleep(time.Millisecond)
	}
}
