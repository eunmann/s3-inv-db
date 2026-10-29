package format

import (
	"bytes"
	"errors"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

type tierTestRow struct {
	counts, bytes [tiers.NumTiers]uint64
}

//nolint:gocritic // gocritic wants named returns, nonamedreturns forbids them
func (r *tierTestRow) totals() (uint64, uint64) {
	var c, b uint64
	for id := range tiers.NumTiers {
		c += r.counts[id]
		b += r.bytes[id]
	}

	return c, b
}

// planTierRows plans the rows order lists (all of them, in
// order, when nil) under the given layout.
func planTierRows(t *testing.T, present []tiers.ID, rows []tierTestRow, force tierLayout, order []int) *TierStatsPlan {
	t.Helper()
	plan, err := NewTierStatsPlan(present)
	if err != nil {
		t.Fatal(err)
	}
	plan.force = force
	if order == nil {
		order = make([]int, len(rows))
		for i := range order {
			order[i] = i
		}
	}
	for _, i := range order {
		c, b := rows[i].totals()
		if _, err := plan.Add(&rows[i].counts, &rows[i].bytes, c, b); err != nil {
			t.Fatalf("plan row %d: %v", i, err)
		}
	}

	return plan
}

// writeTierRows writes rows, in order, by plan into dir and returns
// their masks.
func writeTierRows(t *testing.T, dir string, plan *TierStatsPlan, rows []tierTestRow) []uint16 {
	t.Helper()
	tw, err := NewTierStatsWriter(dir, plan)
	if err != nil {
		t.Fatal(err)
	}
	masks := make([]uint16, len(rows))
	for i := range rows {
		if masks[i], err = tw.Add(&rows[i].counts, &rows[i].bytes); err != nil {
			t.Fatalf("Add row %d: %v", i, err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("Close tier writer: %v", err)
	}

	return masks
}

// buildTierIndex writes core stats and tier stats for rows (each its
// own subtree) and a tier manifest, with the given layout, and opens
// both readers.
func buildTierIndex(t *testing.T, present []tiers.ID, rows []tierTestRow, force tierLayout) (*CoreStatsReader, *TierStatsReader) {
	t.Helper()
	dir := t.TempDir()
	plan := planTierRows(t, present, rows, force, nil)
	masks := writeTierRows(t, dir, plan, rows)
	coreRows := make([]coreTestRow, len(rows))
	for i := range rows {
		c, b := rows[i].totals()
		coreRows[i] = coreTestRow{count: c, bytes: b, child: uint64(len(rows)), mask: masks[i]}
	}
	writeCoreFiles(t, dir, coreRows, plan.MaskEncoding())
	if err := tiers.WriteManifest(dir, present); err != nil {
		t.Fatal(err)
	}
	cr, err := OpenCoreStats(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cr.Close() })
	tr, err := OpenTierStats(dir, cr)
	if err != nil {
		t.Fatalf("OpenTierStats: %v", err)
	}
	t.Cleanup(func() { tr.Close() })

	return cr, tr
}

// randomTierRows draws n rows over present: each row holds data in each
// tier with probability p, with counts and bytes up to the given bit
// widths, zero-byte objects included.
func randomTierRows(r *rand.Rand, present []tiers.ID, n int, p float64, countBits, byteBits uint) []tierTestRow {
	rows := make([]tierTestRow, n)
	for i := range rows {
		for _, id := range present {
			if r.Float64() >= p {
				continue
			}
			rows[i].counts[id] = 1 + r.Uint64N(1<<countBits)
			if r.IntN(10) > 0 { // some objects are empty
				rows[i].bytes[id] = r.Uint64N(1 << byteBits)
			}
		}
	}

	return rows
}

func checkBreakdowns(t *testing.T, present []tiers.ID, rows []tierTestRow, tr *TierStatsReader) {
	t.Helper()
	sentinel := TierBreakdown{TierName: "sentinel", Bytes: 1, ObjectCount: 1}
	var buf []TierBreakdown
	for pos := range rows {
		all := tr.BreakdownAll(uint64(pos))
		if len(all) != len(present) {
			t.Fatalf("row %d: BreakdownAll has %d tiers, want %d", pos, len(all), len(present))
		}
		nonZero := 0
		for i, id := range present {
			tb := all[i]
			if tb.TierID != id || tb.ObjectCount != rows[pos].counts[id] || tb.Bytes != rows[pos].bytes[id] {
				t.Fatalf("row %d tier %d: got (%d, %d), want (%d, %d)",
					pos, id, tb.ObjectCount, tb.Bytes, rows[pos].counts[id], rows[pos].bytes[id])
			}
			if tb.ObjectCount != 0 || tb.Bytes != 0 {
				nonZero++
			}
		}
		want := tr.Breakdown(uint64(pos))
		if got := len(want); got != nonZero {
			t.Fatalf("row %d: Breakdown has %d tiers, want %d", pos, got, nonZero)
		}
		// AppendBreakdown keeps what dst holds and appends Breakdown.
		buf = append(buf[:0], sentinel)
		buf = tr.AppendBreakdown(buf, uint64(pos))
		if len(buf) != 1+len(want) || buf[0] != sentinel || !slices.Equal(buf[1:], want) {
			t.Fatalf("row %d: AppendBreakdown = %v, want %v after the sentinel", pos, buf, want)
		}
	}
	if len(rows) > 0 {
		buf = make([]TierBreakdown, 0, maxTierSlots)
		if allocs := testing.AllocsPerRun(10, func() { buf = tr.AppendBreakdown(buf[:0], 0) }); allocs != 0 {
			t.Errorf("AppendBreakdown into a sized buffer: %v allocs, want 0", allocs)
		}
	}
}

func tierDirBytes(t *testing.T, dir string) int64 {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return 0
	}
	if err != nil {
		t.Fatal(err)
	}
	var total int64
	for _, e := range entries {
		info, err := e.Info()
		if err != nil {
			t.Fatal(err)
		}
		total += info.Size()
	}

	return total
}

// v4Bytes is what version 4 stored for the same rows: the smaller of
// 16-byte dense slots for every present tier, or a 2-byte bitmap plus
// 16 bytes per non-empty slot, with an offset per row.
func v4Bytes(present []tiers.ID, rows []tierTestRow) int64 {
	n := int64(len(rows))
	var populated int64
	for i := range rows {
		for _, id := range present {
			if rows[i].counts[id] != 0 || rows[i].bytes[id] != 0 {
				populated++
			}
		}
	}
	dense := HeaderSize + n*int64(len(present))*16
	body := 2*n + 16*populated
	sparse := HeaderSize + body + HeaderSize + (n+1)*int64(byteWidthOf(uint64(body))) + arrayTailPad

	return min(dense, sparse)
}

func TestTierStatsLayoutsRoundTrip(t *testing.T) {
	three := []tiers.ID{tiers.Standard, tiers.GlacierIR, tiers.DeepArchive}
	all := tiers.AllTierIDs()
	for _, tc := range []struct {
		name                string
		present             []tiers.ID
		p                   float64
		countBits, byteBits uint
	}{
		{"three_sparse", three, 0.3, 8, 30},
		{"three_dense", three, 0.9, 8, 30},
		{"all_sparse", all, 0.1, 20, 40},
		{"all_dense", all, 1, 20, 40},
		{"wide_values", three, 0.6, 40, 60},
	} {
		for _, layout := range []struct {
			name  string
			force tierLayout
		}{{"auto", tierLayoutAuto}, {"dense", tierLayoutDense}, {"multi", tierLayoutMulti}} {
			t.Run(tc.name+"/"+layout.name, func(t *testing.T) {
				r := rand.New(rand.NewPCG(uint64(len(tc.name)), uint64(layout.force)))
				rows := randomTierRows(r, tc.present, 3000, tc.p, tc.countBits, tc.byteBits)
				cr, tr := buildTierIndex(t, tc.present, rows, layout.force)
				checkBreakdowns(t, tc.present, rows, tr)

				// v5 tier bytes (files plus the mask in every core row) never
				// exceed what version 4 stored.
				if layout.force != tierLayoutAuto {
					return
				}
				dir := filepath.Dir(cr.mmap.path)
				v5 := tierDirBytes(t, filepath.Join(dir, TierStatsDir)) + int64(len(rows))*int64(cr.Stride().TierMask)
				if v4 := v4Bytes(tc.present, rows); v5 > v4 {
					t.Errorf("v5 tier bytes %d > v4 %d", v5, v4)
				}
			})
		}
	}
}

// TestTierStatsChoosesSmallerLayout checks the size model against the
// files each layout actually writes plus the core rows' tier field, and
// that auto picks the smaller, with the plain mask and past 8 tiers.
func TestTierStatsChoosesSmallerLayout(t *testing.T) {
	four := []tiers.ID{tiers.Standard, tiers.StandardIA, tiers.GlacierIR, tiers.DeepArchive}
	for _, present := range [][]tiers.ID{four, tiers.AllTierIDs()} {
		for _, p := range []float64{0.05, 0.3, 0.95} {
			r := rand.New(rand.NewPCG(7, uint64(p*100)))
			rows := randomTierRows(r, present, 2000, p, 10, 30)
			sizes := map[tierLayout]int64{}
			for _, force := range []tierLayout{tierLayoutDense, tierLayoutMulti, tierLayoutAuto} {
				cr, _ := buildTierIndex(t, present, rows, force)
				sizes[force] = tierDirBytes(t, filepath.Join(filepath.Dir(cr.mmap.path), TierStatsDir)) +
					int64(len(rows))*int64(cr.Stride().TierMask)
			}
			if want := min(sizes[tierLayoutDense], sizes[tierLayoutMulti]); sizes[tierLayoutAuto] != want {
				t.Errorf("%d tiers, p=%.2f: auto stored %d bytes, dense %d, multi %d",
					len(present), p, sizes[tierLayoutAuto], sizes[tierLayoutDense], sizes[tierLayoutMulti])
			}
		}
	}
}

func TestTierStatsSizeModelMatchesFiles(t *testing.T) {
	present := []tiers.ID{tiers.Standard, tiers.GlacierIR, tiers.DeepArchive}
	rows := randomTierRows(rand.New(rand.NewPCG(3, 4)), present, 1500, 0.4, 12, 34)
	for _, force := range []tierLayout{tierLayoutDense, tierLayoutMulti} {
		dir := t.TempDir()
		plan := planTierRows(t, present, rows, force, nil)
		want := plan.denseSize()
		if force == tierLayoutMulti {
			want = plan.multiSize()
		}
		writeTierRows(t, dir, plan, rows)
		if got := tierDirBytes(t, filepath.Join(dir, TierStatsDir)); got != int64(want) {
			t.Errorf("layout %d: files are %d bytes, model says %d", force, got, want)
		}
	}
}

func TestTierStatsSingleTierWritesNothing(t *testing.T) {
	present := []tiers.ID{tiers.GlacierIR}
	rows := []tierTestRow{{}, {}, {}}
	rows[0].counts[tiers.GlacierIR], rows[0].bytes[tiers.GlacierIR] = 2, 50
	rows[2].counts[tiers.GlacierIR] = 1 // an empty object
	cr, tr := buildTierIndex(t, present, rows, tierLayoutAuto)
	if n := tierDirBytes(t, filepath.Join(filepath.Dir(cr.mmap.path), TierStatsDir)); n != 0 {
		t.Errorf("single-tier index wrote %d tier bytes", n)
	}
	checkBreakdowns(t, present, rows, tr)
}

func TestTierStatsPlanRejectsInconsistentRows(t *testing.T) {
	present := []tiers.ID{tiers.Standard, tiers.GlacierIR}
	plan, err := NewTierStatsPlan(present)
	if err != nil {
		t.Fatal(err)
	}
	var row tierTestRow
	row.counts[tiers.Standard], row.bytes[tiers.Standard] = 1, 10

	if _, err := plan.Add(&row.counts, &row.bytes, 2, 10); !errors.Is(err, errTierSumMismatch) {
		t.Errorf("count mismatch: err = %v, want errTierSumMismatch", err)
	}
	if _, err := plan.Add(&row.counts, &row.bytes, 1, 11); !errors.Is(err, errTierSumMismatch) {
		t.Errorf("bytes mismatch: err = %v, want errTierSumMismatch", err)
	}
	row.bytes[tiers.GlacierIR] = 1 << 63
	row.bytes[tiers.Standard] = 1 << 63
	row.counts[tiers.GlacierIR] = 1
	if _, err := plan.Add(&row.counts, &row.bytes, 2, 0); !errors.Is(err, errTierSumMismatch) {
		t.Errorf("overflowing sum: err = %v, want errTierSumMismatch", err)
	}
	var stray tierTestRow
	stray.counts[tiers.DeepArchive] = 1
	if _, err := plan.Add(&stray.counts, &stray.bytes, 1, 0); !errors.Is(err, ErrUndeclaredTierData) {
		t.Errorf("undeclared tier: err = %v, want ErrUndeclaredTierData", err)
	}
	if plan.rows != 0 {
		t.Errorf("plan counted %d refused rows", plan.rows)
	}
}

// TestTierStatsMaskMatchesData: every layout and tier-field encoding
// decodes each row's mask to the slots holding data.
func TestTierStatsMaskMatchesData(t *testing.T) {
	three := []tiers.ID{tiers.Standard, tiers.StandardIA, tiers.GlacierIR}
	for _, present := range [][]tiers.ID{three, tiers.AllTierIDs()} {
		for _, force := range []tierLayout{tierLayoutDense, tierLayoutMulti} {
			rows := randomTierRows(rand.New(rand.NewPCG(9, 9)), present, 500, 0.3, 8, 20)
			_, tr := buildTierIndex(t, present, rows, force)
			for pos := range rows {
				var want uint16
				for slot, id := range present {
					if rows[pos].counts[id] != 0 || rows[pos].bytes[id] != 0 {
						want |= 1 << slot
					}
				}
				var counts, bytesArr [maxTierSlots]uint64
				if got := tr.fill(uint64(pos), &counts, &bytesArr); got != want {
					t.Fatalf("%d tiers, layout %d, row %d: mask %b, want %b", len(present), force, pos, got, want)
				}
			}
		}
	}
}

// TestTierStatsCloseFailureLeavesNothing: a failed Close removes every
// file the writer started.
func TestTierStatsCloseFailureLeavesNothing(t *testing.T) {
	present := []tiers.ID{tiers.Standard, tiers.GlacierIR}
	rows := make([]tierTestRow, 1)
	rows[0].counts[tiers.Standard], rows[0].bytes[tiers.Standard] = 1, 1
	for _, force := range []tierLayout{tierLayoutDense, tierLayoutMulti} {
		dir := t.TempDir()
		w, err := NewTierStatsWriter(dir, planTierRows(t, present, rows, force, nil))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := w.Add(&rows[0].counts, &rows[0].bytes); err != nil {
			t.Fatal(err)
		}
		slots := w.dense
		if slots == nil {
			slots = w.multi
		}
		if err := slots.f.Close(); err != nil { // the final flush now fails
			t.Fatal(err)
		}
		if err := w.Close(); err == nil {
			t.Fatalf("layout %d: Close succeeded with its slot file closed", force)
		}
		if n := tierDirEntries(t, filepath.Join(dir, TierStatsDir)); n != 0 {
			t.Errorf("layout %d: %d files left in tier_stats after a failed Close", force, n)
		}
	}
}

// TestTierStatsWriterRejectsRowsOffPlan: rows that aren't the ones
// planned (more, fewer, or wider) fail the writer, which leaves no
// file behind.
func TestTierStatsWriterRejectsRowsOffPlan(t *testing.T) {
	present := []tiers.ID{tiers.Standard, tiers.GlacierIR, tiers.DeepArchive}
	rows := randomTierRows(rand.New(rand.NewPCG(5, 6)), present, 200, 0.5, 8, 20)
	wide := slices.Clone(rows)
	wide[100].counts[tiers.Standard], wide[100].counts[tiers.GlacierIR] = 1<<40, 1<<40
	for _, force := range []tierLayout{tierLayoutDense, tierLayoutMulti} {
		for _, tc := range []struct {
			name    string
			written []tierTestRow
		}{
			{"fewer", rows[:len(rows)-1]},
			{"more", append(slices.Clone(rows), rows[0])},
			{"wider", wide},
		} {
			dir := t.TempDir()
			w, err := NewTierStatsWriter(dir, planTierRows(t, present, rows, force, nil))
			if err != nil {
				t.Fatal(err)
			}
			for i := range tc.written {
				if _, err = w.Add(&tc.written[i].counts, &tc.written[i].bytes); err != nil {
					break
				}
			}
			if err == nil {
				err = w.Close()
			} else {
				err = errors.Join(err, w.Abort())
			}
			if !errors.Is(err, errTierPlanMismatch) {
				t.Errorf("layout %d, %s rows: err = %v, want errTierPlanMismatch", force, tc.name, err)
			}
			if n := tierDirEntries(t, filepath.Join(dir, TierStatsDir)); n != 0 {
				t.Errorf("layout %d, %s rows: %d files left in tier_stats", force, tc.name, n)
			}
		}
	}
}

// TestTierStatsPlanIgnoresRowOrder: a plan made from the rows in any
// order is the plan made from them in position order, so the files and
// masks written are byte for byte the same.
func TestTierStatsPlanIgnoresRowOrder(t *testing.T) {
	three := []tiers.ID{tiers.Standard, tiers.GlacierIR, tiers.DeepArchive}
	for _, present := range [][]tiers.ID{{tiers.GlacierIR}, three, tiers.AllTierIDs()} {
		for _, p := range []float64{0.05, 0.4, 1} {
			for _, force := range []tierLayout{tierLayoutAuto, tierLayoutDense, tierLayoutMulti} {
				r := rand.New(rand.NewPCG(uint64(len(present)), uint64(p*100)+uint64(force)))
				rows := randomTierRows(r, present, 3000, p, 4+r.UintN(30), 8+r.UintN(50))
				shuffled := planTierRows(t, present, rows, force, r.Perm(len(rows)))
				ordered := planTierRows(t, present, rows, force, nil)
				shuffled.row, ordered.row = tierRow{}, tierRow{} // the last row filled, not a statistic
				if *shuffled != *ordered {
					t.Errorf("%d tiers, p=%.2f, layout %d: plan %+v, in order %+v", len(present), p, force, *shuffled, *ordered)
				}
				gotDir, wantDir := t.TempDir(), t.TempDir()
				gotMasks := writeTierRows(t, gotDir, shuffled, rows)
				wantMasks := writeTierRows(t, wantDir, ordered, rows)
				if !slices.Equal(gotMasks, wantMasks) {
					t.Errorf("%d tiers, p=%.2f, layout %d: masks differ", len(present), p, force)
				}
				assertSameFiles(t, filepath.Join(wantDir, TierStatsDir), filepath.Join(gotDir, TierStatsDir))
			}
		}
	}
}

// assertSameFiles fails unless directories want and got hold the same
// files with the same bytes; a missing directory holds none.
func assertSameFiles(t *testing.T, want, got string) {
	t.Helper()
	read := func(dir string) map[string][]byte {
		files := map[string][]byte{}
		entries, err := os.ReadDir(dir)
		if errors.Is(err, os.ErrNotExist) {
			return files
		}
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range entries {
			data, err := os.ReadFile(filepath.Join(dir, e.Name()))
			if err != nil {
				t.Fatal(err)
			}
			files[e.Name()] = data
		}

		return files
	}
	wantFiles, gotFiles := read(want), read(got)
	if len(gotFiles) != len(wantFiles) {
		t.Errorf("%s holds %d files, want %d", got, len(gotFiles), len(wantFiles))
	}
	for name, data := range wantFiles {
		if !bytes.Equal(gotFiles[name], data) {
			t.Errorf("%s differs from %s", filepath.Join(got, name), filepath.Join(want, name))
		}
	}
}

func tierDirEntries(t *testing.T, dir string) int {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}

	return len(entries)
}

func TestRankedBits(t *testing.T) {
	for _, n := range []uint64{0, 1, 63, 64, 65, 511, 512, 513, 5000} {
		r := rand.New(rand.NewPCG(n, 1))
		set := make([]bool, n)
		path := filepath.Join(t.TempDir(), "bits")
		w, err := newRankedBitsWriter(path)
		if err != nil {
			t.Fatal(err)
		}
		for i := range set {
			set[i] = r.IntN(3) == 0
			if err := w.add(set[i]); err != nil {
				t.Fatal(err)
			}
		}
		if err := w.close(); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Stat(path + ".dir"); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("n=%d: rank directory scratch left behind: %v", n, err)
		}
		rb, err := openRankedBits(path)
		if err != nil {
			t.Fatal(err)
		}
		var want uint64
		for i := range n + 1 {
			if got := rb.rank(i); got != want {
				t.Fatalf("n=%d: rank(%d) = %d, want %d", n, i, got, want)
			}
			if i < n {
				if rb.get(i) != set[i] {
					t.Fatalf("n=%d: get(%d) = %v, want %v", n, i, rb.get(i), set[i])
				}
				if set[i] {
					want++
				}
			}
		}
		rb.close()
	}
}

// TestSlotBitsWidthsAreWidestValues: the OR's widths equal the widest
// value's, at every byte boundary and for random values.
func TestSlotBitsWidthsAreWidestValues(t *testing.T) {
	vals := make([]uint64, 0, 16+1000)
	for k := range 8 {
		vals = append(vals, 1<<(8*k)-1, 1<<(8*k))
	}
	rng := rand.New(rand.NewPCG(1, 2))
	for range 1000 {
		vals = append(vals, rng.Uint64()>>rng.IntN(64))
	}
	var b slotBits
	if w := b.widths(); w != (tierSlotWidths{count: 1, bytes: 1}) {
		t.Fatalf("no values: widths %+v, want 1, 1", w)
	}
	var widest uint8
	for i, v := range vals {
		b.add(v, vals[len(vals)-1-i])
		widest = max(widest, byteWidthOf(v))
		if got := b.widths().count; got != widest {
			t.Fatalf("after %d values: count width %d, want %d", i+1, got, widest)
		}
	}
}
