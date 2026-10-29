package extsort

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"maps"
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
		err = b.replayLevels(side(0, failStats), side(1, !failStats))
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

// TestIndexBuilderCoreAndTierFilesMatchLegacy: the core and tier files
// the builder writes from its plans, the child-start tail included, are
// byte for byte what the legacy writers make from the same rows in
// level order.
func TestIndexBuilderCoreAndTierFilesMatchLegacy(t *testing.T) {
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
			got := t.TempDir()
			b, err := NewIndexBuilder(got, t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			if err := b.SetPresentTiers(present); err != nil {
				t.Fatal(err)
			}
			for _, row := range edgeTreeRows(tc.n1, tc.n2, tc.spread) {
				if err := b.Add(row); err != nil {
					t.Fatal(err)
				}
			}
			if err := b.Finalize(); err != nil {
				t.Fatal(err)
			}
			want := t.TempDir()
			narrowed := writeLegacyRowFiles(t, got, want, present)
			if narrowed != tc.narrowed {
				t.Errorf("child start narrowed by the tail = %v, want %v", narrowed, tc.narrowed)
			}
			for _, name := range []string{
				format.CoreStatsFile, format.CorePatchFile, format.CorePatchRowsFile,
				filepath.Join(format.TierStatsDir, format.TierDenseFile), filepath.Join(format.TierStatsDir, format.TierMultiFile),
				filepath.Join(format.TierStatsDir, format.TierMultiRowsFile), filepath.Join(format.TierStatsDir, format.TierMultiOffsetsFile),
				filepath.Join(format.TierStatsDir, format.TierMultiMaskFile),
			} {
				w, werr := os.ReadFile(filepath.Join(want, name))
				g, gerr := os.ReadFile(filepath.Join(got, name))
				if errors.Is(werr, fs.ErrNotExist) != errors.Is(gerr, fs.ErrNotExist) || !bytes.Equal(w, g) {
					t.Errorf("%s differs: built %d bytes (%v), legacy %d (%v)", name, len(g), gerr, len(w), werr)
				}
			}
		})
	}
}

// writeLegacyRowFiles reads the built index in dir back in position
// order into the legacy core and tier writers in out, and reports
// whether the built core stats narrowed the child start below the row
// count's width with patches.
func writeLegacyRowFiles(t *testing.T, dir, out string, present []tiers.ID) bool {
	t.Helper()
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
	cb, err := format.NewCoreStatsBuilder(out, core.Count())
	if err != nil {
		t.Fatal(err)
	}
	tw, err := format.NewTierStatsScratchWriter(out, present)
	if err != nil {
		t.Fatal(err)
	}
	for pos := range core.Count() {
		var counts, byteCounts [tiers.NumTiers]uint64
		for _, tb := range tr.BreakdownAll(pos) {
			counts[tb.TierID], byteCounts[tb.TierID] = tb.ObjectCount, tb.Bytes
		}
		c, by := core.UnsafeStats(pos)
		mask, err := tw.Add(&counts, &byteCounts, c, by)
		if err != nil {
			t.Fatal(err)
		}
		if err := cb.Add(c, by, core.UnsafeChildStart(pos), mask); err != nil {
			t.Fatal(err)
		}
	}
	cb.SetTierMaskEncoding(tw.MaskEncoding())
	if err := errors.Join(tw.Close(), cb.Finalize()); err != nil {
		t.Fatal(err)
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
// in key order while the tree is in level order; every position still
// decodes to its prefix, the dictionary lists segments by first
// appearance in key order, and the parent distance takes the width its
// largest value needs.
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
