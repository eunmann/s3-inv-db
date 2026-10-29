package format

import (
	"errors"
	"fmt"
	"math/bits"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

// The range writers must write the files their sequential writers do,
// byte for byte, however the rows are cut into ranges and whichever
// goroutine writes which range when: the sequential writers are their
// oracle.

// rangeCuts cuts n rows into parts ranges at random rows that are
// multiples of align, empty and one-row ranges included: the first cut
// is 0, the last n.
func rangeCuts(r *rand.Rand, n uint64, parts int, align uint64) []uint64 {
	cuts := []uint64{0, n}
	for range parts - 1 {
		cuts = append(cuts, min(n, r.Uint64N(n/align+1)*align))
	}
	if n > 2 && align == 1 {
		cuts = append(cuts, 1, n-1)
	}
	slices.Sort(cuts)

	return cuts
}

// writeRanges writes the ranges between cuts on workers goroutines, in
// a random order, each with its own range writer from newRange, then
// merges their parts in row order.
func writeRanges[P any](t *testing.T, r *rand.Rand, cuts []uint64, workers int,
	newRange func() func(lo, hi uint64, part *P) error, merge func(part *P) error,
) {
	t.Helper()
	parts := make([]P, len(cuts)-1)
	order := make(chan int, len(parts))
	for _, i := range r.Perm(len(parts)) {
		order <- i
	}
	close(order)
	errs := make([]error, workers)
	var wg sync.WaitGroup
	for w := range workers {
		write := newRange()
		wg.Go(func() {
			for i := range order {
				if err := write(cuts[i], cuts[i+1], &parts[i]); err != nil {
					errs[w] = fmt.Errorf("range [%d, %d): %w", cuts[i], cuts[i+1], err)

					return
				}
			}
		})
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	for i := range parts {
		if err := merge(&parts[i]); err != nil {
			t.Fatalf("merge range %d: %v", i, err)
		}
	}
}

// checkRecordedSums fails unless sums holds every file under dir with
// the checksum reading it gives.
func checkRecordedSums(t *testing.T, dir string, sums *Checksums) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		path := filepath.Join(dir, e.Name())
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if info.IsDir() {
			checkRecordedSums(t, path, sums)

			continue
		}
		got, ok := sums.lookup(path, info)
		if !ok {
			t.Errorf("%s: no checksum recorded while writing", e.Name())
		} else if want := checksumOf(t, dir, e.Name()); got != want {
			t.Errorf("%s: recorded checksum %s, reading gives %s", e.Name(), got, want)
		}
	}
}

// rangeSplits are the ways the range tests cut rows: into one range,
// a few and many, on one goroutine and on several.
var rangeSplits = []struct{ parts, workers int }{{1, 1}, {3, 2}, {16, 4}, {200, 8}}

func TestCoreStatsRangesMatchWriter(t *testing.T) {
	small := func(n int) []coreTestRow {
		rows := pyramidRows(n, 9)
		for i := range rows {
			rows[i].count, rows[i].bytes = rows[i].count%200, rows[i].bytes%1000
		}

		return rows
	}
	cases := map[string][]coreTestRow{
		"empty":                   nil,
		"one row":                 {{count: 3, bytes: 4, child: 1, mask: 1}},
		"exceptions":              pyramidRows(20_000, 1),
		"exceptions at the edges": edgeRows(5000, 3, 4),
		"no exceptions":           small(3000),
	}
	for name, rows := range cases {
		for _, enc := range []TierMaskEncoding{TierMaskPlain, TierMaskCode} {
			for _, split := range rangeSplits {
				t.Run(fmt.Sprintf("%s/enc=%d/parts=%d", name, enc, split.parts), func(t *testing.T) {
					r := rand.New(rand.NewPCG(uint64(len(rows)), uint64(split.parts)))
					want, got := t.TempDir(), t.TempDir()
					writeCoreFiles(t, want, rows, enc)
					sums := newChecksums(testPiece)
					w, err := NewCoreStatsRanges(got, planCore(rows, uint64(len(rows))), enc, sums)
					if err != nil {
						t.Fatal(err)
					}
					writeRanges(t, r, rangeCuts(r, uint64(len(rows)), split.parts, 1), split.workers,
						func() func(lo, hi uint64, part *CoreStatsPart) error {
							rw := w.NewRange()

							return func(lo, hi uint64, part *CoreStatsPart) error {
								if err := rw.Start(lo, part); err != nil {
									return err
								}
								for _, row := range rows[lo:hi] {
									if err := rw.Add(row.count, row.bytes, row.child, row.mask); err != nil {
										return err
									}
								}

								return rw.Flush()
							}
						}, w.Merge)
					if err := w.Close(); err != nil {
						t.Fatal(err)
					}
					assertSameFiles(t, want, got)
					checkRecordedSums(t, got, sums)
				})
			}
		}
	}
}

// TestCoreStatsRangesRejectRowsOffPlan: rows a range cannot hold, and
// parts that do not continue the ones merged before, fail, and Close
// fails and removes the files until every planned row is merged.
func TestCoreStatsRangesRejectRowsOffPlan(t *testing.T) {
	rows := pyramidRows(2000, 3)
	open := func(t *testing.T) (*CoreStatsRanges, string) {
		t.Helper()
		dir := t.TempDir()
		w, err := NewCoreStatsRanges(dir, planCore(rows, 1), TierMaskPlain, nil)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = w.Abort() })

		return w, dir
	}
	write := func(t *testing.T, w *CoreStatsRanges, lo, hi uint64) *CoreStatsPart {
		t.Helper()
		var part CoreStatsPart
		rw := w.NewRange()
		if err := rw.Start(lo, &part); err != nil {
			t.Fatal(err)
		}
		for _, row := range rows[lo:hi] {
			if err := rw.Add(row.count, row.bytes, row.child, row.mask); err != nil {
				t.Fatal(err)
			}
		}
		if err := rw.Flush(); err != nil {
			t.Fatal(err)
		}

		return &part
	}

	t.Run("child start not past its row", func(t *testing.T) {
		w, _ := open(t)
		var part CoreStatsPart
		rw := w.NewRange()
		if err := rw.Start(10, &part); err != nil {
			t.Fatal(err)
		}
		if err := rw.Add(1, 1, 10, 1); err == nil {
			t.Error("a child start at its own row was written")
		}
	})
	t.Run("range past the rows", func(t *testing.T) {
		w, _ := open(t)
		if err := w.NewRange().Start(uint64(len(rows))+1, &CoreStatsPart{}); err == nil {
			t.Error("a range past the planned rows started")
		}
	})
	t.Run("parts out of order", func(t *testing.T) {
		w, _ := open(t)
		if err := w.Merge(write(t, w, 100, 200)); err == nil {
			t.Error("a part merged before the rows ahead of it")
		}
	})
	t.Run("child starts going back across parts", func(t *testing.T) {
		w, _ := open(t)
		first := write(t, w, 0, 100)
		first.lastChild = rows[150].child
		if err := w.Merge(first); err != nil {
			t.Fatal(err)
		}
		if err := w.Merge(write(t, w, 100, 200)); err == nil {
			t.Error("a part whose child starts go back past the last part's merged")
		}
	})
	t.Run("close short of the plan", func(t *testing.T) {
		w, dir := open(t)
		if err := w.Merge(write(t, w, 0, 1000)); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err == nil {
			t.Fatal("closed with half the planned rows")
		}
		assertSameFiles(t, t.TempDir(), dir)
	})
}

// TestRankedBitsAddBitsMatchesAdd: bits appended in runs of any length,
// at any alignment, write the file adding them one at a time does.
func TestRankedBitsAddBitsMatchesAdd(t *testing.T) {
	for _, n := range []uint64{0, 1, 63, 64, 65, 511, 512, 513, 5000} {
		r := rand.New(rand.NewPCG(n, 2))
		set := make([]bool, n)
		for i := range set {
			set[i] = r.IntN(3) == 0
		}
		want, got := t.TempDir(), t.TempDir()
		ws, err := newRankedBitsWriter(filepath.Join(want, "bits"), nil)
		if err != nil {
			t.Fatal(err)
		}
		for _, bit := range set {
			if err := ws.add(bit); err != nil {
				t.Fatal(err)
			}
		}
		if err := ws.close(); err != nil {
			t.Fatal(err)
		}
		wr, err := newRankedBitsWriter(filepath.Join(got, "bits"), nil)
		if err != nil {
			t.Fatal(err)
		}
		cuts := rangeCuts(r, n, 12, 1)
		for i := range len(cuts) - 1 {
			var words []uint64
			for j := cuts[i]; j < cuts[i+1]; j++ {
				words = setBit(words, j-cuts[i], set[j])
			}
			// Bits past the run are ignored.
			run := cuts[i+1] - cuts[i]
			if run%wordBits != 0 {
				words[len(words)-1] |= ^uint64(0) << (run % wordBits)
			}
			words = append(words, ^uint64(0))
			if err := wr.addBits(words, run); err != nil {
				t.Fatal(err)
			}
		}
		if err := wr.close(); err != nil {
			t.Fatal(err)
		}
		assertSameFiles(t, want, got)
	}
}

func TestTierStatsRangesMatchWriter(t *testing.T) {
	three := []tiers.ID{tiers.Standard, tiers.GlacierIR, tiers.DeepArchive}
	cases := []struct {
		name    string
		present []tiers.ID
		n       int
		p       float64
	}{
		{"tiers=1", []tiers.ID{tiers.Outposts}, 500, 1},
		{"tiers=3/sparse", three, 3000, 0.3},
		{"tiers=3/dense", three, 3000, 0.9},
		{"tiers=3/one row", three, 1, 1},
		{"tiers=16/sparse", tiers.AllTierIDs(), 3000, 0.1},
		{"tiers=16/dense", tiers.AllTierIDs(), 2000, 0.95},
	}
	for _, tc := range cases {
		for _, layout := range []tierLayout{tierLayoutDense, tierLayoutMulti} {
			for _, split := range rangeSplits {
				t.Run(fmt.Sprintf("%s/layout=%d/parts=%d", tc.name, layout, split.parts), func(t *testing.T) {
					r := rand.New(rand.NewPCG(uint64(tc.n), uint64(split.parts)))
					rows := randomTierRows(r, tc.present, tc.n, tc.p, 20, 40)
					want, got := t.TempDir(), t.TempDir()
					writeTierRows(t, want, planTierRows(t, tc.present, rows, layout, nil), tc.present, rows)
					// The multi-tier rows and stored slots before each row.
					multi, stored := make([]uint64, len(rows)+1), make([]uint64, len(rows)+1)
					for i := range rows {
						mask, _ := rowSlots(tc.present, &rows[i])
						multi[i+1], stored[i+1] = multi[i], stored[i]
						if k := uint64(bits.OnesCount16(mask)); k >= 2 {
							multi[i+1]++
							stored[i+1] += k - 1
						}
					}
					sums := newChecksums(testPiece)
					w, err := NewTierStatsRanges(got, planTierRows(t, tc.present, rows, layout, nil), sums)
					if err != nil {
						t.Fatal(err)
					}
					writeRanges(t, r, rangeCuts(r, uint64(len(rows)), split.parts, 1), split.workers,
						func() func(lo, hi uint64, part *TierStatsPart) error {
							rw := w.NewRange()

							return func(lo, hi uint64, part *TierStatsPart) error {
								if err := rw.Start(lo, multi[lo], stored[lo], part); err != nil {
									return err
								}
								for i := lo; i < hi; i++ {
									if err := rw.AddSlots(rowSlots(tc.present, &rows[i])); err != nil {
										return err
									}
								}

								return rw.Flush()
							}
						}, w.Merge)
					if err := w.Close(); err != nil {
						t.Fatal(err)
					}
					assertSameFiles(t, filepath.Join(want, TierStatsDir), filepath.Join(got, TierStatsDir))
					checkRecordedSums(t, got, sums)
				})
			}
		}
	}
}

func TestPrefixTreeRangesMatchWriter(t *testing.T) {
	cases := map[string][]treeTestRow{
		"root only": {{parent: -1}},
		"deep":      levelOrderTreeRows(rand.New(rand.NewPCG(1, 1)), 9, 2, 5),
		"wide":      levelOrderTreeRows(rand.New(rand.NewPCG(2, 2)), 2, 300, 1000),
		"mixed":     levelOrderTreeRows(rand.New(rand.NewPCG(3, 3)), 5, 8, 60),
	}
	for name, rows := range cases {
		for _, split := range rangeSplits {
			t.Run(fmt.Sprintf("%s/parts=%d", name, split.parts), func(t *testing.T) {
				r := rand.New(rand.NewPCG(uint64(len(rows)), uint64(split.parts)))
				want, got := t.TempDir(), t.TempDir()
				writeTreeRows(t, want, rows, nil)
				sums := newChecksums(testPiece)
				ids, segments, maxDist := internTreeRows(t, got, rows, nil)
				w, err := NewPrefixTreeRanges(got, uint64(len(rows)), segments, maxDist, sums)
				if err != nil {
					t.Fatal(err)
				}
				writeRanges(t, r, rangeCuts(r, uint64(len(rows)), split.parts, 1), split.workers,
					func() func(lo, hi uint64, part *PrefixTreePart) error {
						rw := w.NewRange()

						return func(lo, hi uint64, part *PrefixTreePart) error {
							if err := rw.Start(lo, part); err != nil {
								return err
							}
							for i := lo; i < hi; i++ {
								var err error
								if rows[i].parent < 0 {
									err = rw.AddRoot()
								} else {
									err = rw.Add(uint64(rows[i].parent), ids[i])
								}
								if err != nil {
									return err
								}
							}

							return rw.Flush()
						}
					}, w.Merge)
				if err := w.Close(); err != nil {
					t.Fatal(err)
				}
				assertSameFiles(t, want, got)
				info, err := os.Stat(filepath.Join(got, PrefixTreeFile))
				if err != nil {
					t.Fatal(err)
				}
				if sum, ok := sums.lookup(filepath.Join(got, PrefixTreeFile), info); !ok || sum != checksumOf(t, got, PrefixTreeFile) {
					t.Errorf("prefix tree checksum %q recorded (%v), reading gives %s", sum, ok, checksumOf(t, got, PrefixTreeFile))
				}
			})
		}
	}
}

// TestPrefixTreeRangesRejectRowsOffPlan: rows a range cannot hold and a
// Close whose rows are not the planned ones fail, the Close removing the
// file.
func TestPrefixTreeRangesRejectRowsOffPlan(t *testing.T) {
	open := func(t *testing.T, count uint64, segments uint32, maxDist uint64) (*PrefixTreeRanges, *PrefixTreeRange, string) {
		t.Helper()
		dir := t.TempDir()
		w, err := NewPrefixTreeRanges(dir, count, segments, maxDist, nil)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = w.Abort() })
		rw := w.NewRange()

		return w, rw, dir
	}
	t.Run("root past row 0", func(t *testing.T) {
		_, rw, _ := open(t, 4, 2, 3)
		if err := rw.Start(1, &PrefixTreePart{}); err != nil {
			t.Fatal(err)
		}
		if err := rw.AddRoot(); err == nil {
			t.Error("a root at row 1 was written")
		}
	})
	t.Run("parent not before its row", func(t *testing.T) {
		_, rw, _ := open(t, 4, 2, 3)
		if err := rw.Start(2, &PrefixTreePart{}); err != nil {
			t.Fatal(err)
		}
		if err := rw.Add(2, 0); err == nil {
			t.Error("a row that is its own parent was written")
		}
	})
	t.Run("distance past the plan", func(t *testing.T) {
		_, rw, _ := open(t, 4, 2, 1)
		if err := rw.Start(2, &PrefixTreePart{}); err != nil {
			t.Fatal(err)
		}
		if err := rw.Add(0, 0); err == nil {
			t.Error("a parent distance past the planned one was written")
		}
	})
	t.Run("close without the planned widths", func(t *testing.T) {
		w, rw, dir := open(t, 2, 2, 1)
		var part PrefixTreePart
		if err := rw.Start(0, &part); err != nil {
			t.Fatal(err)
		}
		if err := errors.Join(rw.AddRoot(), rw.Add(0, 0), rw.Flush(), w.Merge(&part)); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err == nil {
			t.Fatal("closed with one segment of the two planned")
		}
		assertSameFiles(t, t.TempDir(), dir)
	})
}

// depthCuts cuts the rows of depths of the given sizes, in level order,
// at each depth's start and at random block boundaries within it.
func depthCuts(r *rand.Rand, sizes []uint64, parts int) []uint64 {
	var cuts []uint64
	var start uint64
	for _, size := range sizes {
		for _, c := range rangeCuts(r, size, parts, DepthBlockSize) {
			cuts = append(cuts, start+c)
		}
		start += size
	}

	return slices.Compact(append(cuts, start))
}

func TestDepthIndexRangesMatchWriter(t *testing.T) {
	cases := map[string][]uint64{
		"empty":                {},
		"root only":            {1},
		"block edges":          {1, 63, 64, 65, 128, 129},
		"wide":                 {1, 40, 5000, 300},
		"deep":                 {1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233},
		"trailing empty depth": {1, 70, 0},
	}
	for name, sizes := range cases {
		for _, split := range rangeSplits {
			t.Run(fmt.Sprintf("%s/parts=%d", name, split.parts), func(t *testing.T) {
				r := rand.New(rand.NewPCG(uint64(len(sizes)), uint64(split.parts)))
				type row struct {
					depth        int
					local        uint64
					count, bytes uint64
				}
				var rows []row
				for d, size := range sizes {
					for i := range size {
						rows = append(rows, row{depth: d, local: i, count: r.Uint64N(1 << r.UintN(40)), bytes: r.Uint64() >> r.UintN(64)})
					}
				}
				want, got := t.TempDir(), t.TempDir()
				ws, err := NewDepthIndexWriter(want, nil)
				if err != nil {
					t.Fatal(err)
				}
				for _, row := range rows {
					if err := ws.Add(uint32(row.depth), row.count, row.bytes); err != nil {
						t.Fatal(err)
					}
				}
				if err := ws.Close(); err != nil {
					t.Fatal(err)
				}
				sums := newChecksums(testPiece)
				w, err := NewDepthIndexRanges(got, sizes, sums)
				if err != nil {
					t.Fatal(err)
				}
				writeRanges(t, r, depthCuts(r, sizes, split.parts), split.workers,
					func() func(lo, hi uint64, part *DepthIndexPart) error {
						rw := w.NewRange()

						return func(lo, hi uint64, part *DepthIndexPart) error {
							if lo == hi {
								return nil
							}
							if err := rw.Start(rows[lo].depth, rows[lo].local, part); err != nil {
								return err
							}
							for _, row := range rows[lo:hi] {
								if err := rw.Add(row.count, row.bytes); err != nil {
									return err
								}
							}

							return rw.Flush()
						}
					}, w.Merge)
				if err := w.Close(); err != nil {
					t.Fatal(err)
				}
				assertSameFiles(t, want, got)
				checkRecordedSums(t, got, sums)
			})
		}
	}
}

// TestDepthIndexRangesRejectRowsOffPlan: a range off a block boundary
// or ending inside a block, rows past a depth, parts out of level order,
// a depth with rows below an empty one and a Close short of the rows
// fail, the Close removing the files.
func TestDepthIndexRangesRejectRowsOffPlan(t *testing.T) {
	sizes := []uint64{1, 200}
	open := func(t *testing.T) (*DepthIndexRanges, string) {
		t.Helper()
		dir := t.TempDir()
		w, err := NewDepthIndexRanges(dir, sizes, nil)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = w.Abort() })

		return w, dir
	}
	if _, err := NewDepthIndexRanges(t.TempDir(), []uint64{1, 0, 3}, nil); !errors.Is(err, ErrDepthOrder) {
		t.Errorf("depths with a gap: %v, want ErrDepthOrder", err)
	}
	t.Run("range off a block", func(t *testing.T) {
		w, _ := open(t)
		if err := w.NewRange().Start(1, 10, &DepthIndexPart{}); err == nil {
			t.Error("a range starting inside a block started")
		}
	})
	t.Run("range ending inside a block", func(t *testing.T) {
		w, _ := open(t)
		rw := w.NewRange()
		if err := rw.Start(1, 0, &DepthIndexPart{}); err != nil {
			t.Fatal(err)
		}
		for range 10 {
			if err := rw.Add(1, 1); err != nil {
				t.Fatal(err)
			}
		}
		if err := rw.Flush(); err == nil {
			t.Error("a range ending inside a block flushed")
		}
	})
	t.Run("rows past the depth", func(t *testing.T) {
		w, _ := open(t)
		rw := w.NewRange()
		if err := rw.Start(0, 0, &DepthIndexPart{}); err != nil {
			t.Fatal(err)
		}
		if err := errors.Join(rw.Add(1, 1), rw.Add(1, 1)); err == nil {
			t.Error("two rows written to a depth of one")
		}
	})
	t.Run("parts out of order", func(t *testing.T) {
		w, _ := open(t)
		if err := w.Merge(&DepthIndexPart{depth: 1, first: 64, rows: 64}); err == nil {
			t.Error("a part merged before the rows ahead of it")
		}
	})
	t.Run("close short of the rows", func(t *testing.T) {
		w, dir := open(t)
		if err := w.Merge(&DepthIndexPart{depth: 0, first: 0, rows: 1}); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err == nil {
			t.Fatal("closed with depth 1 unwritten")
		}
		assertSameFiles(t, t.TempDir(), dir)
	})
}

// TestTierStatsRangesRejectRowsOffPlan: a range placed past the plan,
// rows past it, parts merged out of order and a Close short of the plan
// fail, the Close removing every file.
func TestTierStatsRangesRejectRowsOffPlan(t *testing.T) {
	three := []tiers.ID{tiers.Standard, tiers.GlacierIR, tiers.DeepArchive}
	rows := randomTierRows(rand.New(rand.NewPCG(1, 2)), three, 400, 0.5, 8, 20)
	open := func(t *testing.T) (*TierStatsRanges, string) {
		t.Helper()
		dir := t.TempDir()
		w, err := NewTierStatsRanges(dir, planTierRows(t, three, rows, tierLayoutMulti, nil), nil)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = w.Abort() })

		return w, dir
	}
	t.Run("range past the plan", func(t *testing.T) {
		w, _ := open(t)
		if err := w.NewRange().Start(0, w.plan.multiRows+1, 0, &TierStatsPart{}); err == nil {
			t.Error("a range past the planned multi-tier rows started")
		}
	})
	t.Run("rows past the plan", func(t *testing.T) {
		w, _ := open(t)
		rw := w.NewRange()
		if err := rw.Start(uint64(len(rows))-1, w.plan.multiRows, w.plan.multiSlots, &TierStatsPart{}); err != nil {
			t.Fatal(err)
		}
		if err := rw.AddSlots(3, []uint64{1, 1, 1, 1}); err == nil {
			t.Error("a multi-tier row past the planned ones was written")
		}
	})
	t.Run("parts out of order", func(t *testing.T) {
		w, _ := open(t)
		var part TierStatsPart
		if err := w.NewRange().Start(5, 0, 0, &part); err != nil {
			t.Fatal(err)
		}
		if err := w.Merge(&part); err == nil {
			t.Error("a part merged before the rows ahead of it")
		}
	})
	t.Run("close short of the plan", func(t *testing.T) {
		w, dir := open(t)
		if err := w.Close(); err == nil {
			t.Fatal("closed without a row")
		}
		assertSameFiles(t, t.TempDir(), filepath.Join(dir, TierStatsDir))
	})
}
