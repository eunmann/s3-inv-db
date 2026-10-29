package format

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

type coreTestRow struct {
	count, bytes, child uint64
	mask                uint16
}

// planCore plans rows in a shuffled order, with the tail when the plan
// needs it.
func planCore(rows []coreTestRow, seed uint64) *CoreStatsPlan {
	var plan CoreStatsPlan
	for _, i := range rand.New(rand.NewPCG(seed, 3)).Perm(len(rows)) {
		plan.Add(rows[i].count, rows[i].bytes, rows[i].mask)
	}
	if from, ok := plan.ChildStartTail(); ok {
		for _, r := range slices.Backward(rows) {
			if r.child >= from {
				plan.AddTail(r.count, r.bytes)
			}
		}
	}

	return &plan
}

// writeCoreFiles writes rows into dir with a CoreStatsWriter.
func writeCoreFiles(t *testing.T, dir string, rows []coreTestRow, enc TierMaskEncoding) {
	t.Helper()
	w, err := NewCoreStatsWriter(dir, planCore(rows, uint64(len(rows))), enc)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range rows {
		if err := w.Add(r.count, r.bytes, r.child, r.mask); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

// writeCore writes rows and opens them.
func writeCore(t *testing.T, rows []coreTestRow) (string, *CoreStatsReader) {
	t.Helper()
	dir := t.TempDir()
	writeCoreFiles(t, dir, rows, TierMaskPlain)
	cr, err := OpenCoreStats(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cr.Close() })

	return dir, cr
}

// levelOrderChildStarts returns the child starts of a random tree of n
// rows in level order: each row has up to fanout children, more when
// the next row would otherwise have no parent. Fanout 0 is a chain, the
// fewest child starts near the row count.
func levelOrderChildStarts(r *rand.Rand, n, fanout int) []uint64 {
	starts := make([]uint64, n)
	next := 1
	for i := range starts {
		starts[i] = uint64(next)
		c := r.IntN(fanout + 1)
		if next == i+1 {
			c = max(c, 1)
		}
		next += min(c, n-next)
	}

	return starts
}

func checkCore(t *testing.T, cr *CoreStatsReader, rows []coreTestRow) {
	t.Helper()
	for i, want := range rows {
		pos := uint64(i)
		c, b := cr.UnsafeStats(pos)
		if c != want.count || b != want.bytes ||
			cr.UnsafeObjectCount(pos) != want.count || cr.UnsafeTotalBytes(pos) != want.bytes ||
			cr.UnsafeChildStart(pos) != want.child {
			t.Fatalf("row %d = (%d, %d, child %d), want %+v", i, c, b, cr.UnsafeChildStart(pos), want)
		}
	}
}

// pyramidRows is a tree-like shape: most rows are small leaves, a few
// near the top carry huge totals.
func pyramidRows(n int, seed uint64) []coreTestRow {
	r := rand.New(rand.NewPCG(seed, 2))
	starts := levelOrderChildStarts(r, n, 3)
	rows := make([]coreTestRow, n)
	for i := range rows {
		rows[i] = coreTestRow{count: 1 + r.Uint64N(200), bytes: r.Uint64N(1 << 30), child: starts[i], mask: 1}
	}
	for _, i := range []int{0, 1, n / 2} {
		rows[i].count, rows[i].bytes = 1<<40, 1<<60
	}
	rows[3].count = 255 // exactly the 1-byte sentinel: must round-trip

	return rows
}

func TestCorePatchNarrowsFieldsAndStaysExact(t *testing.T) {
	rows := pyramidRows(20_000, 1)
	dir, cr := writeCore(t, rows)
	if s := cr.Stride(); s.Count != 1 || s.ChildStart != 2 || s.Bytes != 4 {
		t.Errorf("stride = %+v, want count 1, bytes 4, child start 2", s)
	}
	if cr.patch == nil {
		t.Fatal("no patch files for a pyramid of values")
	}
	if got := cr.patch.rows.rank(cr.rowCount); got != 4 {
		t.Errorf("exceptions = %d, want 4 (three wide rows, one sentinel value)", got)
	}
	checkCore(t, cr, rows)
	for _, name := range []string{CorePatchFile, CorePatchRowsFile} {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
}

func TestCoreWithoutExceptionsWritesNoPatch(t *testing.T) {
	rows := make([]coreTestRow, 5000)
	for i := range rows {
		rows[i] = coreTestRow{count: uint64(i % 255), bytes: uint64(i), child: min(2*uint64(i)+1, 5000), mask: 1}
	}
	rows[0].count, rows[0].bytes = 255, 0xFFFF // all-ones maxima, no sentinel needed
	dir, cr := writeCore(t, rows)
	if cr.patch != nil {
		t.Error("patch files written for values that all fit")
	}
	if s := cr.Stride(); s.Count != 1 || s.Bytes != 2 {
		t.Errorf("stride = %+v, want count 1, bytes 2", s)
	}
	checkCore(t, cr, rows)
	if _, err := os.Stat(filepath.Join(dir, CorePatchFile)); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("%s exists: %v", CorePatchFile, err)
	}
}

// TestChoosePatchIsSmallest: no other width set within the exception
// cap costs less than the chosen one, and patching beats plain widths on
// a pyramid.
func TestChoosePatchIsSmallest(t *testing.T) {
	rows := pyramidRows(3000, 7)
	var hist patchHistogram
	for _, r := range rows {
		hist.add(r.count, r.bytes, r.child)
	}
	best := choosePatch(uint64(len(rows)), hist.exceptions)
	bestSize := coreFilesSize(t, rows)
	for _, w := range []patchChoice{{count: 8, bytes: 8, size: 8}, {count: 5, bytes: 8, size: 2}, {count: 1, bytes: 1, size: 1}} {
		if w.count == best.count && w.bytes == best.bytes && w.size == best.size {
			continue
		}
		n := uint64(len(rows))
		if cost := patchCost(n, w.count, w.bytes, w.size, hist.exceptions(w.count, w.bytes, w.size)); cost < best.cost {
			t.Errorf("widths %+v cost %d beat the chosen %+v", w, cost, best)
		}
	}
	if bestSize >= int64(len(rows))*(3+8+2+1) {
		t.Errorf("patched core files %d bytes are not smaller than plain widths", bestSize)
	}
}

// TestChoosePatchCapsExceptions: when the smallest layout patches more
// than maxPatchRatePPM of rows, choosePatch takes a wider one within it.
func TestChoosePatchCapsExceptions(t *testing.T) {
	const rows = 10_000
	wide := uint64(rows / 50) // 2% of rows need a 5th bytes byte
	var hist patchHistogram
	hist[1][4][1] = rows - wide
	hist[1][5][1] = wide
	if narrow, full := patchCost(rows, 1, 4, 1, wide), patchCost(rows, 1, 5, 1, 0); narrow >= full {
		t.Fatalf("fixture: 4-byte bytes cost %d does not beat 5-byte %d, so the cap would not bind", narrow, full)
	}
	got := choosePatch(rows, hist.exceptions)
	if got.count != 1 || got.bytes != 5 || got.size != 1 {
		t.Errorf("choosePatch = %+v, want widths 1/5/1", got)
	}
	if got.exceptions*1_000_000 > maxPatchRatePPM*rows {
		t.Errorf("choosePatch exceptions = %d, over the cap of %d per million of %d rows", got.exceptions, maxPatchRatePPM, rows)
	}
}

func coreFilesSize(t *testing.T, rows []coreTestRow) int64 {
	t.Helper()
	dir, _ := writeCore(t, rows)
	var total int64
	for _, name := range []string{CoreStatsFile, CorePatchFile, CorePatchRowsFile} {
		if info, err := os.Stat(filepath.Join(dir, name)); err == nil {
			total += info.Size()
		}
	}

	return total
}

func TestOpenCoreStatsRejectsMismatchedPatch(t *testing.T) {
	dir, cr := writeCore(t, pyramidRows(2000, 3))
	cr.Close()
	other, _ := writeCore(t, pyramidRows(1000, 4))
	src, err := os.OpenRoot(other)
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	dst, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer dst.Close()
	for _, name := range []string{CorePatchFile, CorePatchRowsFile} {
		data, err := src.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		if err := dst.WriteFile(name, data, FilePerm); err != nil {
			t.Fatal(err)
		}
	}
	if r, err := OpenCoreStats(dir); err == nil {
		r.Close()
		t.Fatal("OpenCoreStats accepted another index's patch files")
	}
}

func TestFieldNeed(t *testing.T) {
	for _, tc := range []struct {
		v    uint64
		want uint8
	}{{0, 1}, {254, 1}, {255, 2}, {0xFFFE, 2}, {0xFFFF, 3}, {1 << 56, 8}, {^uint64(0), 8}} {
		if got := fieldNeed(tc.v); got != tc.want {
			t.Errorf("fieldNeed(%#x) = %d, want %d", tc.v, got, tc.want)
		}
	}
	// Every width's boundary agrees with the definition: the narrowest
	// width whose all-ones sentinel is above v.
	need := func(v uint64) uint8 {
		for w := uint8(1); w < 8; w++ {
			if v < widthMask[w] {
				return w
			}
		}

		return 8
	}
	for w := 1; w <= 8; w++ {
		edge := widthMask[w]
		for _, v := range []uint64{edge - 2, edge - 1, edge, edge + 1} {
			if got, want := fieldNeed(v), need(v); got != want {
				t.Errorf("fieldNeed(%#x) = %d, want %d", v, got, want)
			}
		}
	}
}

func TestManifestListsPatchFiles(t *testing.T) {
	dir, cr := writeCore(t, pyramidRows(5000, 9))
	if cr.patch == nil {
		t.Fatal("fixture has no patch")
	}
	for _, name := range []string{
		DepthOffsetsFile, DepthBlocksFile, MPHFile, MPHFSlotsFile, "tiers.json",
		PrefixDictBlobFile, PrefixDictOffsetsFile, PrefixTreeFile,
	} {
		if err := os.WriteFile(filepath.Join(dir, name), nil, FilePerm); err != nil {
			t.Fatal(err)
		}
	}
	if err := WriteManifest(dir, 5000, 1, nil); err != nil {
		t.Fatal(err)
	}
	m, err := ReadManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{CorePatchFile, CorePatchRowsFile} {
		if _, ok := m.Files[name]; !ok {
			t.Errorf("manifest omits %s", name)
		}
	}
}

// writeLegacyCore writes rows into dir with the CoreStatsBuilder.
func writeLegacyCore(t *testing.T, dir string, rows []coreTestRow, enc TierMaskEncoding) {
	t.Helper()
	b, err := NewCoreStatsBuilder(dir, uint64(len(rows)))
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range rows {
		if err := b.Add(r.count, r.bytes, r.child, r.mask); err != nil {
			t.Fatal(err)
		}
	}
	b.SetTierMaskEncoding(enc)
	if err := b.Finalize(); err != nil {
		t.Fatal(err)
	}
}

// assertSameCoreFiles fails unless directories want and got hold the
// same core stats and patch files.
func assertSameCoreFiles(t *testing.T, want, got string) {
	t.Helper()
	for _, name := range []string{CoreStatsFile, CorePatchFile, CorePatchRowsFile} {
		w, werr := os.ReadFile(filepath.Join(want, name))
		g, gerr := os.ReadFile(filepath.Join(got, name))
		if errors.Is(werr, os.ErrNotExist) != errors.Is(gerr, os.ErrNotExist) {
			t.Errorf("%s: legacy %v, writer %v", name, werr, gerr)
		} else if !bytes.Equal(w, g) {
			t.Errorf("%s differs: %d bytes, legacy %d", name, len(g), len(w))
		}
	}
}

// edgeRows is n rows of a tree of the given fanout with small values and
// a few wide ones, some of them on the last rows, whose child starts are
// the widest.
func edgeRows(n, fanout int, seed uint64) []coreTestRow {
	r := rand.New(rand.NewPCG(seed, uint64(n)))
	starts := levelOrderChildStarts(r, n, fanout)
	rows := make([]coreTestRow, n)
	for i := range rows {
		rows[i] = coreTestRow{count: 1 + r.Uint64N(200), bytes: r.Uint64N(1 << 20), child: starts[i], mask: uint16(1 + r.IntN(3))}
	}
	for range min(n, max(1, n/2000)) {
		rows[r.IntN(n)].count = 1 << 20
		rows[n-1-r.IntN(min(n, 300))].bytes = 1 << 40
	}

	return rows
}

// TestCoreChildStartEdgeMatchesLegacy: just above a power of 256 a
// child start one byte narrower than the row count needs can pay, and
// the plan reads the rows whose child start doesn't fit it (the tail) to
// choose; elsewhere it needs no tail. Either way the files are byte for
// byte the builder's, which counts every row's three widths.
func TestCoreChildStartEdgeMatchesLegacy(t *testing.T) {
	narrowed := 0
	for _, tc := range []struct {
		n        int
		needTail bool
	}{{255, true}, {256, false}, {65536, true}, {65600, true}, {65860, true}, {65870, false}} {
		var plan CoreStatsPlan
		for range tc.n {
			plan.Add(1, 1, 1)
		}
		if _, ok := plan.ChildStartTail(); ok != tc.needTail {
			t.Errorf("n=%d: ChildStartTail needed = %v, want %v", tc.n, ok, tc.needTail)
		}
		for _, fanout := range []int{0, 1, 4} {
			for seed := range uint64(3) {
				rows := edgeRows(tc.n, fanout, seed)
				if seed == 2 { // no wide values: only child starts decide
					for i := range rows {
						rows[i].count, rows[i].bytes = 1, 1
					}
				}
				want, got := t.TempDir(), t.TempDir()
				writeLegacyCore(t, want, rows, TierMaskPlain)
				writeCoreFiles(t, got, rows, TierMaskPlain)
				assertSameCoreFiles(t, want, got)
				cr, err := OpenCoreStats(got)
				if err != nil {
					t.Fatal(err)
				}
				if cr.patch != nil && cr.Stride().ChildStart < fieldNeed(uint64(tc.n)) {
					narrowed++
				}
				checkCore(t, cr, rows)
				cr.Close()
			}
		}
	}
	if narrowed == 0 {
		t.Error("no case chose a child start narrower than the row count needs: the tail went untested")
	}
}

// TestCoreStatsWriterMatchesLegacy: over trees of many shapes and value
// ranges, and every tier-field encoding, the writer's files are byte for
// byte the builder's.
func TestCoreStatsWriterMatchesLegacy(t *testing.T) {
	r := rand.New(rand.NewPCG(11, 12))
	for i := range 40 {
		n := []int{0, 1, 2, 100, 254, 257, 3000, 65534, 65535, 65700}[i%10]
		rows := edgeRows(n, r.IntN(6), uint64(i))
		countBits, bytesBits := 1+r.UintN(63), 1+r.UintN(63)
		for j := range rows {
			if r.IntN(4) == 0 {
				rows[j].count, rows[j].bytes = r.Uint64()>>(64-countBits), r.Uint64()>>(64-bytesBits)
			}
			rows[j].mask = uint16(r.Uint32N(1 << r.UintN(17)))
		}
		for _, enc := range []TierMaskEncoding{TierMaskPlain, TierMaskDerived, TierMaskCode} {
			t.Run(fmt.Sprintf("%d/n=%d/enc=%d", i, n, enc), func(t *testing.T) {
				want, got := t.TempDir(), t.TempDir()
				writeLegacyCore(t, want, rows, enc)
				writeCoreFiles(t, got, rows, enc)
				assertSameCoreFiles(t, want, got)
			})
		}
	}
}

// TestCoreStatsWriterNeedsTail: near a power of 256 a plan without its
// tail can't choose the child start's width, so no writer is made.
func TestCoreStatsWriterNeedsTail(t *testing.T) {
	var plan CoreStatsPlan
	for range 65600 {
		plan.Add(1, 1, 1)
	}
	if _, err := NewCoreStatsWriter(t.TempDir(), &plan, TierMaskPlain); !errors.Is(err, errCorePlan) {
		t.Errorf("NewCoreStatsWriter without the tail: err = %v, want errCorePlan", err)
	}
}

// TestCoreStatsWriterRejectsRowsOffPlan: rows that aren't the ones
// planned fail the writer, which leaves no file behind.
func TestCoreStatsWriterRejectsRowsOffPlan(t *testing.T) {
	plain := edgeRows(3000, 3, 1)
	for i := range plain {
		plain[i].count, plain[i].bytes = 1, 1
	}
	patched := pyramidRows(3000, 5)
	for _, tc := range []struct {
		name    string
		planned []coreTestRow
		edit    func(rows []coreTestRow) []coreTestRow
	}{
		{"fewer", plain, func(rows []coreTestRow) []coreTestRow { return rows[:len(rows)-1] }},
		{"more", plain, func(rows []coreTestRow) []coreTestRow { return append(rows, rows[len(rows)-1]) }},
		{"wider", plain, func(rows []coreTestRow) []coreTestRow { rows[7].count = 1 << 20; return rows }},
		{"another exception", patched, func(rows []coreTestRow) []coreTestRow { rows[7].bytes = 1 << 60; return rows }},
		{"one exception fewer", patched, func(rows []coreTestRow) []coreTestRow { rows[1].count, rows[1].bytes = 1, 1; return rows }},
		{"wider mask", plain, func(rows []coreTestRow) []coreTestRow { rows[7].mask = 1 << 15; return rows }},
		{"child start back", plain, func(rows []coreTestRow) []coreTestRow { rows[9].child = rows[8].child - 1; return rows }},
		{"child start at itself", plain, func(rows []coreTestRow) []coreTestRow { rows[9].child = 9; return rows }},
		{"last child start short", plain, func(rows []coreTestRow) []coreTestRow { rows[len(rows)-1].child--; return rows }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			w, err := NewCoreStatsWriter(dir, planCore(tc.planned, 1), TierMaskPlain)
			if err != nil {
				t.Fatal(err)
			}
			for _, r := range tc.edit(slices.Clone(tc.planned)) {
				if err = w.Add(r.count, r.bytes, r.child, r.mask); err != nil {
					break
				}
			}
			if err == nil {
				err = w.Close()
			} else {
				err = errors.Join(err, w.Abort())
			}
			if !errors.Is(err, errCorePlan) {
				t.Errorf("err = %v, want errCorePlan", err)
			}
			if entries, _ := os.ReadDir(dir); len(entries) != 0 {
				t.Errorf("%d files left behind", len(entries))
			}
		})
	}
}
