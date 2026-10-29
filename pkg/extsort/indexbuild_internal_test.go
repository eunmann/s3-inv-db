package extsort

import (
	"errors"
	"fmt"
	"maps"
	"math"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/format"
	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

// TestSegmentOfDoesNotAllocate: segmentOf runs once per prefix, so
// splitting off its last segment must cost no allocation.
func TestSegmentOfDoesNotAllocate(t *testing.T) {
	b, err := NewIndexBuilder(t.TempDir(), t.TempDir())
	if err != nil {
		t.Fatalf("NewIndexBuilder: %v", err)
	}
	defer b.cleanup()
	for _, prefix := range []string{"", "a/"} {
		if err := b.Add(&PrefixRow{Prefix: prefix, Depth: uint16(len(b.stack))}); err != nil {
			t.Fatalf("Add(%q): %v", prefix, err)
		}
	}
	const child = "a/seg/"
	if name, err := b.segmentOf(child); err != nil || name != "seg" {
		t.Fatalf("segmentOf(%q) = %q, %v; want seg", child, name, err)
	}
	allocs := testing.AllocsPerRun(1000, func() {
		if _, err := b.segmentOf(child); err != nil {
			t.Fatalf("segmentOf(%q): %v", child, err)
		}
	})
	if allocs != 0 {
		t.Errorf("segmentOf allocates %v times", allocs)
	}
}

// errInjectedSide is the failure TestWriteLevelsSideFailureEnds gives a
// side.
var errInjectedSide = errors.New("injected side failure")

// TestWriteLevelsSideFailureEnds: a row either side cannot write, past
// more batches than the hand-off holds, ends the replay with that
// side's error instead of leaving the spool reader blocked; the other
// side and the reader stop.
func TestWriteLevelsSideFailureEnds(t *testing.T) {
	const n = 2 * levelBatches * levelBatchRows
	for _, failStats := range []bool{true, false} {
		b, err := NewIndexBuilder(t.TempDir(), t.TempDir())
		if err != nil {
			t.Fatalf("NewIndexBuilder: %v", err)
		}
		if err := b.Add(&PrefixRow{Prefix: ""}); err != nil {
			t.Fatal(err)
		}
		for i := range n {
			row := PrefixRow{Prefix: fmt.Sprintf("p%06d/", i), Depth: 1, Count: 1, TotalBytes: 1}
			row.TierCounts[tiers.Standard], row.TierBytes[tiers.Standard] = 1, 1
			if err := b.Add(&row); err != nil {
				t.Fatalf("Add %d: %v", i, err)
			}
		}
		if err := b.spool.finish(); err != nil {
			t.Fatal(err)
		}
		var rows [2]int
		side := func(i int, fail bool) func(int, *spoolRecord) error {
			return func(int, *spoolRecord) error {
				rows[i]++
				if fail && rows[i] == levelBatchRows/2 {
					return errInjectedSide
				}

				return nil
			}
		}
		_, err = b.replayLevels(side(0, failStats), side(1, !failStats))
		if !errors.Is(err, errInjectedSide) {
			t.Errorf("stats side failing = %v: replay = %v, want the injected error", failStats, err)
		}
		if rows[0] > n || rows[1] > n {
			t.Errorf("sides wrote %v rows after the failure, of %d", rows, n+1)
		}
		b.cleanup()
	}
}

// TestUndeclaredTierFailsTheBuild: a row with data in a tier outside the
// declared set fails the build where the spool plans it, at a later Add
// or at Finalize, never silently.
func TestUndeclaredTierFailsTheBuild(t *testing.T) {
	for _, rows := range []int{10, 3 * spoolBatchRows} {
		b, err := NewIndexBuilder(t.TempDir(), t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		if err := b.SetPresentTiers([]tiers.ID{tiers.Standard}); err != nil {
			t.Fatal(err)
		}
		err = b.Add(&PrefixRow{Prefix: ""})
		for i := 0; err == nil && i < rows; i++ {
			row := PrefixRow{Prefix: fmt.Sprintf("p%06d/", i), Depth: 1, Count: 1, TotalBytes: 1}
			tier := tiers.Standard
			if i == 5 {
				tier = tiers.GlacierIR
			}
			row.TierCounts[tier], row.TierBytes[tier] = 1, 1
			err = b.Add(&row)
		}
		if err == nil {
			err = b.Finalize()
		}
		if !errors.Is(err, format.ErrUndeclaredTierData) {
			t.Errorf("%d rows: err = %v, want ErrUndeclaredTierData", rows, err)
		}
		b.cleanup()
	}
}

// TestTierSumMismatchFailsTheBuild: a row whose tiers don't sum to its
// totals fails the build where the spool plans it, naming the row's
// segment and depth, at a later Add or at Finalize, never silently.
func TestTierSumMismatchFailsTheBuild(t *testing.T) {
	for _, rows := range []int{10, 3 * spoolBatchRows} {
		b, err := NewIndexBuilder(t.TempDir(), t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		err = b.Add(&PrefixRow{Prefix: ""})
		for i := 0; err == nil && i < rows; i++ {
			row := PrefixRow{Prefix: fmt.Sprintf("p%06d/", i), Depth: 1, Count: 2, TotalBytes: 3}
			row.TierCounts[tiers.Standard], row.TierBytes[tiers.Standard] = 1, 1
			row.TierCounts[tiers.Snow], row.TierBytes[tiers.Snow] = 1, 2
			if i == 5 {
				row.TierBytes[tiers.Snow]++
			}
			err = b.Add(&row)
		}
		if err == nil {
			err = b.Finalize()
		}
		const want = `prefix segment "p000005" at depth 1: tier stats: prefix tier stats don't sum to its totals`
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%d rows: err = %v, want it to hold %q", rows, err, want)
		}
		b.cleanup()
	}
}

// TestWriteStatsRowErrorNamesTheRow: a tier stats row the replay cannot
// write is reported by its level-order row and depth: the replay never
// reads the segment names.
func TestWriteStatsRowErrorNamesTheRow(t *testing.T) {
	b, err := NewIndexBuilder(t.TempDir(), t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer b.cleanup()
	if err := b.SetPresentTiers([]tiers.ID{tiers.Standard, tiers.GlacierIR}); err != nil {
		t.Fatal(err)
	}
	for i := range 4 {
		row := PrefixRow{Count: 1, TotalBytes: 1}
		if i > 0 {
			row.Prefix, row.Depth = fmt.Sprintf("p%d/", i), 1
		}
		row.TierCounts[tiers.Standard], row.TierBytes[tiers.Standard] = 1, 1
		if err := b.Add(&row); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := b.finishSpool(); err != nil {
		t.Fatal(err)
	}
	depthW, err := b.openStatsWriters()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = depthW.Abort() }()
	offPlan := spoolRecord{count: 1, bytes: 1, tierMask: 1 << 5, tiers: []uint64{1, 1}}
	err = b.writeStatsRow(depthW, b.levelStarts(), 2, 1, &offPlan)
	const want = "row 2 at depth 1: write tier stats"
	if err == nil || !strings.HasPrefix(err.Error(), want) {
		t.Errorf("err = %v, want it to start %q", err, want)
	}
}

// edgeTreeRows is a root, n1 top-level prefixes and n2 children spread
// over the last spread of them, in key order: level order puts the
// children last, so they and the prefixes just before them hold the
// widest child starts.
func edgeTreeRows(n1, n2, spread int) []*PrefixRow {
	rows := []*PrefixRow{{Prefix: ""}}
	add := func(prefix string, depth uint16, i int) {
		row := &PrefixRow{Prefix: prefix, Depth: depth}
		for j, tier := range []tiers.ID{tiers.Standard, tiers.GlacierIR, tiers.DeepArchive} {
			if (i+j)%(j+2) == 0 || j == 0 {
				row.TierCounts[tier], row.TierBytes[tier] = uint64(1+i%7), uint64(i)<<(10*j)
				row.Count += row.TierCounts[tier]
				row.TotalBytes += row.TierBytes[tier]
			}
		}
		if i%9973 == 0 {
			row.TierCounts[tiers.Standard] += 1 << 33
			row.Count += 1 << 33
		}
		rows = append(rows, row)
	}
	for i := range n1 {
		add(fmt.Sprintf("a%05d/", i), 1, i)
		if parent := i - (n1 - spread); parent >= 0 {
			for c := parent * n2 / spread; c < (parent+1)*n2/spread; c++ {
				add(fmt.Sprintf("a%05d/c%04d/", i, c), 2, i+c)
			}
		}
	}
	for _, row := range rows[1:] {
		rows[0].Count += row.Count * uint64(2-row.Depth)
		rows[0].TotalBytes += row.TotalBytes * uint64(2-row.Depth)
		for id := range rows[0].TierCounts {
			rows[0].TierCounts[id] += row.TierCounts[id] * uint64(2-row.Depth)
			rows[0].TierBytes[id] += row.TierBytes[id] * uint64(2-row.Depth)
		}
	}

	return rows
}

// TestIndexBuilderChildStartTail: builds whose row count is just past a
// power of 256 take the child start one byte narrower when the rows
// whose child start needs the full width (read back from the spool) are
// few, and every row's stats, children and tiers read back exactly
// either way.
func TestIndexBuilderChildStartTail(t *testing.T) {
	present := []tiers.ID{tiers.Standard, tiers.GlacierIR, tiers.DeepArchive}
	for _, tc := range []struct {
		n1, n2, spread int
		narrowed       bool // the child start takes the tail's narrower width
	}{
		{65400, 199, 1, true},   // 65600 prefixes
		{65400, 199, 150, true}, // the tail starts inside depth 1
		{65600, 269, 3, false},  // 65870 prefixes: past the edge
		{200, 54, 20, false},    // 255 prefixes
		{3000, 900, 2000, false},
	} {
		t.Run(fmt.Sprintf("%d+%d", tc.n1, tc.n2), func(t *testing.T) {
			dir := t.TempDir()
			b, err := NewIndexBuilder(dir, t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			if err := b.SetPresentTiers(present); err != nil {
				t.Fatal(err)
			}
			rows := edgeTreeRows(tc.n1, tc.n2, tc.spread)
			for _, row := range rows {
				if err := b.Add(row); err != nil {
					t.Fatal(err)
				}
			}
			if err := b.Finalize(); err != nil {
				t.Fatal(err)
			}
			if narrowed := checkLevelOrderRows(t, dir, rows); narrowed != tc.narrowed {
				t.Errorf("child start narrowed by the tail = %v, want %v", narrowed, tc.narrowed)
			}
		})
	}
}

// checkLevelOrderRows checks the index in dir holds rows (in key order)
// at their level-order positions: each row's totals, tiers and children.
// It reports whether the core stats narrowed the child start below the
// row count's width with patches.
func checkLevelOrderRows(t *testing.T, dir string, rows []*PrefixRow) bool {
	t.Helper()
	byLevel := slices.Clone(rows)
	slices.SortStableFunc(byLevel, func(a, b *PrefixRow) int { return int(a.Depth) - int(b.Depth) })
	children := map[string][]uint64{}
	for pos, row := range byLevel[1:] {
		parent := row.Prefix[:strings.LastIndexByte(row.Prefix[:len(row.Prefix)-1], '/')+1]
		children[parent] = append(children[parent], uint64(pos+1))
	}
	core, err := format.OpenCoreStats(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer core.Close()
	tr, err := format.OpenTierStats(dir, core)
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close()
	for pos, row := range byLevel {
		p := uint64(pos)
		if c, by := core.UnsafeStats(p); c != row.Count || by != row.TotalBytes {
			t.Fatalf("%q: stats (%d, %d), want (%d, %d)", row.Prefix, c, by, row.Count, row.TotalBytes)
		}
		lo, hi := core.UnsafeChildStart(p), core.UnsafeChildStart(p+1)
		want := children[row.Prefix]
		if hi-lo != uint64(len(want)) || (len(want) > 0 && lo != want[0]) {
			t.Fatalf("%q: children [%d, %d), want %v", row.Prefix, lo, hi, want)
		}
		for _, tb := range tr.BreakdownAll(p) {
			if tb.ObjectCount != row.TierCounts[tb.TierID] || tb.Bytes != row.TierBytes[tb.TierID] {
				t.Fatalf("%q tier %d: (%d, %d), want (%d, %d)", row.Prefix, tb.TierID,
					tb.ObjectCount, tb.Bytes, row.TierCounts[tb.TierID], row.TierBytes[tb.TierID])
			}
		}
	}
	_, patchErr := os.Stat(filepath.Join(dir, format.CorePatchFile))

	return patchErr == nil && core.Stride().ChildStart < byteWidthNeed(core.Count())
}

// byteWidthNeed is the bytes v takes when its field's all-ones value is
// a sentinel.
func byteWidthNeed(v uint64) uint8 {
	w := uint8(1)
	for w < 8 && v >= 1<<(8*w)-1 {
		w++
	}

	return w
}

// randomPrefixes is a random tree's prefixes in key order, root first:
// names repeat across parents and fanouts vary, so a row's index within
// its depth can be far below or above its parent's.
func randomPrefixes(r *rand.Rand, depth int) []string {
	var walk func(prefix string, d int) []string
	walk = func(prefix string, d int) []string {
		out := []string{prefix}
		if d == depth {
			return out
		}
		picked := map[string]bool{}
		for range r.IntN([]int{1, 4, 12}[r.IntN(3)]) {
			picked[fmt.Sprintf("s%02d", r.IntN(40))] = true
		}
		for _, name := range slices.Sorted(maps.Keys(picked)) {
			out = append(out, walk(prefix+name+"/", d+1)...)
		}

		return out
	}

	return walk("", 0)
}

// TestIndexBuilderTreeDecodesEveryPrefix: the builder interns segments
// and adds MPHF keys in key order while the tree is in level order;
// every position still decodes to its prefix, every prefix looks up to
// its position, the dictionary lists segments by first appearance in
// key order, and the parent distance takes the width its largest value
// needs.
func TestIndexBuilderTreeDecodesEveryPrefix(t *testing.T) {
	for seed := range uint64(5) {
		prefixes := randomPrefixes(rand.New(rand.NewPCG(seed, 7)), 2+int(seed))
		dir := t.TempDir()
		b, err := NewIndexBuilder(dir, t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		for _, p := range prefixes {
			row := &PrefixRow{Prefix: p, Depth: uint16(strings.Count(p, "/")), Count: 1, TotalBytes: 1}
			row.TierCounts[tiers.Standard], row.TierBytes[tiers.Standard] = 1, 1
			if err := b.Add(row); err != nil {
				t.Fatal(err)
			}
		}
		if err := b.Finalize(); err != nil {
			t.Fatal(err)
		}

		levelOrder := slices.Clone(prefixes)
		slices.SortStableFunc(levelOrder, func(a, b string) int { return strings.Count(a, "/") - strings.Count(b, "/") })
		pos := make(map[string]uint64, len(levelOrder))
		var maxDist uint64
		for i, p := range levelOrder {
			pos[p] = uint64(i)
			if p != "" {
				parent := p[:strings.LastIndexByte(p[:len(p)-1], '/')+1]
				maxDist = max(maxDist, uint64(i)-pos[parent])
			}
		}
		tree, err := format.OpenPrefixTreeReader(dir)
		if err != nil {
			t.Fatal(err)
		}
		for i, want := range levelOrder {
			if got, err := tree.Prefix(uint64(i)); err != nil || got != want {
				t.Fatalf("seed %d: Prefix(%d) = %q, %v; want %q", seed, i, got, err, want)
			}
		}
		tree.Close()

		mphf, err := format.OpenMPHF(dir)
		if err != nil {
			t.Fatal(err)
		}
		for _, p := range prefixes {
			if got, ok := mphf.Lookup(p); !ok || got != pos[p] {
				t.Fatalf("seed %d: Lookup(%q) = %d, %t; want %d", seed, p, got, ok, pos[p])
			}
		}
		mphf.Close()

		dict, err := format.OpenPrefixDictionary(dir)
		if err != nil {
			t.Fatal(err)
		}
		var firstSeen []string
		seen := map[string]bool{}
		for _, p := range prefixes[1:] {
			name := p[strings.LastIndexByte(p[:len(p)-1], '/')+1 : len(p)-1]
			if !seen[name] {
				seen[name] = true
				firstSeen = append(firstSeen, name)
			}
		}
		if dict.Count() != uint64(len(firstSeen)) {
			t.Errorf("seed %d: %d segments, want %d", seed, dict.Count(), len(firstSeen))
		}
		for id, want := range firstSeen {
			if got, err := dict.GetSegment(uint32(id)); err != nil || got != want {
				t.Fatalf("seed %d: segment %d = %q, %v; want %q", seed, id, got, err, want)
			}
		}
		dict.Close()

		data, err := os.ReadFile(filepath.Join(dir, format.PrefixTreeFile))
		if err != nil {
			t.Fatal(err)
		}
		// A plain width: a distance of 255 still fits one byte.
		if got, want := data[format.HeaderSize+1], byteWidthNeed(maxDist-1); got != want {
			t.Errorf("seed %d: parent distance width %d for a largest distance of %d, want %d", seed, got, maxDist, want)
		}
	}
}

// TestMPHFMemoryBudget: the MPHF build gets a share of GOMEMLIMIT, or
// the builder's default without one.
func TestMPHFMemoryBudget(t *testing.T) {
	for _, c := range []struct{ limit, want int64 }{
		{0, 0},
		{unsetMemoryLimit, 0},
		{math.MaxInt64, 0},
		{4 << 30, 1 << 30},
	} {
		if got := mphfMemoryBudget(c.limit); got != c.want {
			t.Errorf("mphfMemoryBudget(%d) = %d, want %d", c.limit, got, c.want)
		}
	}
}

// addFlatRows adds a root and n depth-1 rows.
func addFlatRows(t *testing.T, b *IndexBuilder, n int) error {
	t.Helper()
	if err := b.Add(&PrefixRow{Prefix: ""}); err != nil {
		return err
	}
	for i := range n {
		row := PrefixRow{Prefix: fmt.Sprintf("p%06d/", i), Depth: 1, Count: 1, TotalBytes: 1}
		row.TierCounts[tiers.Standard], row.TierBytes[tiers.Standard] = 1, 1
		if err := b.Add(&row); err != nil {
			return err
		}
	}

	return nil
}

// assertNoScratch fails if dir holds anything after a build cleaned up.
func assertNoScratch(t *testing.T, dir string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		t.Errorf("%s left in %s", e.Name(), dir)
	}
}

// TestFinalizeMPHFFailureCleansUp: an MPHF build that fails (here, it
// cannot create its slot file) fails Finalize, and cleanup leaves no
// scratch and no output.
func TestFinalizeMPHFFailureCleansUp(t *testing.T) {
	tempDir := t.TempDir()
	outDir := filepath.Join(t.TempDir(), "index")
	b, err := NewIndexBuilder(outDir, tempDir)
	if err != nil {
		t.Fatal(err)
	}
	if err := addFlatRows(t, b, 3*levelBatches*levelBatchRows); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(outDir, format.MPHFSlotsFile), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := b.Finalize(); err == nil || !strings.Contains(err.Error(), "MPHF") {
		t.Fatalf("Finalize = %v, want the MPHF build's failure", err)
	}
	b.cleanup()
	assertNoScratch(t, tempDir)
	if _, err := os.Stat(outDir); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("output dir after cleanup: %v", err)
	}
}

// TestMPHFSpillFailureFailsTheBuild: a key the MPHF builder cannot
// spill fails the build where the spool plans it, at a later Add or at
// Finalize, never silently.
func TestMPHFSpillFailureFailsTheBuild(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root writes to a read-only directory")
	}
	tempDir := t.TempDir()
	b, err := NewIndexBuilder(t.TempDir(), tempDir)
	if err != nil {
		t.Fatal(err)
	}
	// The spool's directory exists; the MPHF spill's cannot be created.
	if err := os.Chmod(tempDir, 0o500); err != nil { //nolint:gosec // the test needs a directory nothing can write to
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(tempDir, 0o700) }) //nolint:gosec // restore so t.TempDir can clean up
	err = addFlatRows(t, b, 3*spoolBatchRows)
	if err == nil {
		err = b.Finalize()
	}
	if err == nil || !strings.Contains(err.Error(), "MPHF spill") {
		t.Errorf("err = %v, want the MPHF spill's failure", err)
	}
	b.cleanup()
}

// TestFinalizeLevelsFailureStopsMPHF: writeLevels failing (here, it
// cannot create the prefix tree) while the MPHF builds beside it fails
// Finalize with that error once the MPHF build has stopped, and cleanup
// leaves no scratch and no output.
func TestFinalizeLevelsFailureStopsMPHF(t *testing.T) {
	for _, rows := range []int{10, 70_000} {
		tempDir := t.TempDir()
		outDir := filepath.Join(t.TempDir(), "index")
		b, err := NewIndexBuilder(outDir, tempDir)
		if err != nil {
			t.Fatal(err)
		}
		if err := addFlatRows(t, b, rows); err != nil {
			t.Fatal(err)
		}
		if err := os.Mkdir(filepath.Join(outDir, format.PrefixTreeFile), 0o750); err != nil {
			t.Fatal(err)
		}
		if err := b.Finalize(); err == nil || !strings.Contains(err.Error(), "prefix tree") {
			t.Fatalf("%d rows: Finalize = %v, want the prefix tree's failure", rows, err)
		}
		b.cleanup()
		assertNoScratch(t, tempDir)
		if _, err := os.Stat(outDir); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("%d rows: output dir after cleanup: %v", rows, err)
		}
	}
}
