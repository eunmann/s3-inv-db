package format

import (
	"errors"
	"math/rand/v2"
	"os"
	"path/filepath"
	"testing"
)

type coreTestRow struct{ count, bytes, child uint64 }

// writeCore writes rows (slot-0 tier mask) and opens them.
func writeCore(t *testing.T, rows []coreTestRow) (string, *CoreStatsReader) {
	t.Helper()
	dir := t.TempDir()
	b, err := NewCoreStatsBuilder(dir, uint64(len(rows)))
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range rows {
		if err := b.Add(r.count, r.bytes, r.child, 1); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.Finalize(); err != nil {
		t.Fatal(err)
	}
	cr, err := OpenCoreStats(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cr.Close() })

	return dir, cr
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
// near the top carry huge totals and child starts.
func pyramidRows(n int, seed uint64) []coreTestRow {
	r := rand.New(rand.NewPCG(seed, 2))
	rows := make([]coreTestRow, n)
	for i := range rows {
		rows[i] = coreTestRow{count: 1 + r.Uint64N(200), bytes: r.Uint64N(1 << 30), child: r.Uint64N(200)}
	}
	for _, i := range []int{0, 1, n / 2} {
		rows[i] = coreTestRow{count: 1 << 40, bytes: 1 << 60, child: 1 << 33}
	}
	rows[3].count = 255 // exactly the 1-byte sentinel: must round-trip

	return rows
}

func TestCorePatchNarrowsFieldsAndStaysExact(t *testing.T) {
	rows := pyramidRows(20_000, 1)
	dir, cr := writeCore(t, rows)
	if s := cr.Stride(); s.Count != 1 || s.ChildStart != 1 || s.Bytes != 4 {
		t.Errorf("stride = %+v, want count 1, bytes 4, child start 1", s)
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
		rows[i] = coreTestRow{count: uint64(i % 255), bytes: uint64(i), child: uint64(i)}
	}
	rows[0] = coreTestRow{count: 255, bytes: 0xFFFF, child: 0} // all-ones maxima, no sentinel needed
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
	best := hist.choosePatch(uint64(len(rows)))
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
	got := hist.choosePatch(rows)
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
	if err := WriteManifest(dir, 5000, 1); err != nil {
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
