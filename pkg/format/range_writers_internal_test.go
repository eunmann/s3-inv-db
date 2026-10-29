package format

import (
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
