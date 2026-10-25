package extsort_test

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/extsort"
	"github.com/eunmann/s3-inv-db/pkg/format"
	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

// withTotals sets a row's totals to the sum of its tiers, as the
// aggregator does.
func withTotals(r *extsort.PrefixRow) *extsort.PrefixRow {
	r.Count, r.TotalBytes = 0, 0
	for id := range tiers.NumTiers {
		r.Count += r.TierCounts[id]
		r.TotalBytes += r.TierBytes[id]
	}

	return r
}

// withRoot returns rows behind a root row whose tier stats are their
// sum: the builder takes the root "" first, before its children.
func withRoot(rows ...*extsort.PrefixRow) []*extsort.PrefixRow {
	root := &extsort.PrefixRow{Prefix: ""}
	for _, r := range rows {
		for id := range tiers.NumTiers {
			root.TierCounts[id] += r.TierCounts[id]
			root.TierBytes[id] += r.TierBytes[id]
		}
	}

	return append([]*extsort.PrefixRow{root}, rows...)
}

// openTiers opens the core and tier stats of the index in dir.
func openTiers(t *testing.T, dir string) *format.TierStatsReader {
	t.Helper()
	core, err := format.OpenCoreStats(dir)
	if err != nil {
		t.Fatalf("OpenCoreStats: %v", err)
	}
	t.Cleanup(func() { core.Close() })
	tsr, err := format.OpenTierStats(dir, core)
	if err != nil {
		t.Fatalf("OpenTierStats: %v", err)
	}
	t.Cleanup(func() { tsr.Close() })

	return tsr
}

// TestTierStats_PresentTiersOnly pins the tier slots to the tiers
// declared via SetPresentTiers, not the compile-time NumTiers: a
// regression that ignored the declared set would silently inflate index
// size.
func TestTierStats_PresentTiersOnly(t *testing.T) {
	dir := t.TempDir()
	outDir := filepath.Join(dir, "idx")
	tempDir := filepath.Join(dir, "tmp")
	if err := os.MkdirAll(tempDir, 0o750); err != nil {
		t.Fatal(err)
	}
	b, err := extsort.NewIndexBuilder(outDir, tempDir)
	if err != nil {
		t.Fatalf("NewIndexBuilder: %v", err)
	}
	if err := b.SetPresentTiers([]tiers.ID{tiers.Standard, tiers.GlacierFR, tiers.DeepArchive}); err != nil {
		t.Fatalf("SetPresentTiers: %v", err)
	}

	// Use exactly 3 tiers: Standard, GlacierFR, DeepArchive.
	rows := []*extsort.PrefixRow{
		{Prefix: "a/", Depth: 1},
		{Prefix: "b/", Depth: 1},
		{Prefix: "c/", Depth: 1},
		{Prefix: "d/", Depth: 1},
	}
	rows[0].TierCounts[tiers.Standard] = 1
	rows[0].TierBytes[tiers.Standard] = 100
	rows[1].TierCounts[tiers.GlacierFR] = 2
	rows[1].TierBytes[tiers.GlacierFR] = 200
	rows[2].TierCounts[tiers.DeepArchive] = 3
	rows[2].TierBytes[tiers.DeepArchive] = 300
	rows[3].TierCounts[tiers.Standard] = 4
	rows[3].TierBytes[tiers.Standard] = 400

	rows = withRoot(rows...)
	for _, r := range rows {
		if err := b.Add(withTotals(r)); err != nil {
			t.Fatalf("Add %q: %v", r.Prefix, err)
		}
	}
	if err := b.FinalizeWithContext(t.Context()); err != nil {
		t.Fatalf("Finalize: %v", err)
	}

	// Whichever layout the writer picked, tier_stats/ must stay within
	// version 4's dense bound for the declared tiers (presentTiers × N ×
	// a 16-byte slot): the "no waste on absent tiers" invariant.
	const presentTiers, v4SlotBytes = 3, 16
	denseUpperBound := int64(format.HeaderSize) + int64(len(rows))*int64(presentTiers)*v4SlotBytes
	tierDir := filepath.Join(outDir, "tier_stats")
	entries, err := os.ReadDir(tierDir)
	if err != nil {
		t.Fatalf("read tier_stats dir: %v", err)
	}
	var totalSize int64
	for _, e := range entries {
		info, err := e.Info()
		if err != nil {
			t.Fatalf("stat %s: %v", e.Name(), err)
		}
		totalSize += info.Size()
	}
	if totalSize > denseUpperBound {
		t.Errorf("tier_stats/ total bytes = %d, exceeds dense upper bound %d (N=%d × presentTiers=%d × %d + header)",
			totalSize, denseUpperBound, len(rows), presentTiers, v4SlotBytes)
	}

	// Round-trip through the reader: values must come back correct.
	tsr := openTiers(t, outDir)

	check := func(pos uint64, tier tiers.ID, wantCount, wantBytes uint64) {
		t.Helper()
		var gotCount, gotBytes uint64
		for _, tb := range tsr.BreakdownAll(pos) {
			if tb.TierID == tier {
				gotCount, gotBytes = tb.ObjectCount, tb.Bytes
			}
		}
		if gotCount != wantCount || gotBytes != wantBytes {
			t.Errorf("pos %d tier %d: got (%d, %d), want (%d, %d)", pos, tier, gotCount, gotBytes, wantCount, wantBytes)
		}
	}
	// Position 0 is the root, holding every row's tiers.
	check(0, tiers.Standard, 5, 500)
	check(1, tiers.Standard, 1, 100)
	check(2, tiers.GlacierFR, 2, 200)
	check(3, tiers.DeepArchive, 3, 300)
	check(4, tiers.Standard, 4, 400)
	// Tiers not in a row's data still appear in BreakdownAll with zeros.
	check(1, tiers.GlacierFR, 0, 0)
}

// TestTierStats_PreorderAlignment guards the invariant that each
// prefix's tier stats sit at exactly its preorder position: a
// regression that dropped or duplicated a row would silently misalign
// tier data with prefix positions.
func TestTierStats_PreorderAlignment(t *testing.T) {
	dir := t.TempDir()
	outDir := filepath.Join(dir, "idx")
	tempDir := filepath.Join(dir, "tmp")
	if err := os.MkdirAll(tempDir, 0o750); err != nil {
		t.Fatal(err)
	}

	b, err := extsort.NewIndexBuilder(outDir, tempDir)
	if err != nil {
		t.Fatalf("NewIndexBuilder: %v", err)
	}

	row0 := &extsort.PrefixRow{Prefix: "a/", Depth: 1}
	row0.TierCounts[tiers.Standard] = 1
	row0.TierBytes[tiers.Standard] = 100
	row1 := &extsort.PrefixRow{Prefix: "b/", Depth: 1}
	row1.TierCounts[tiers.Standard] = 1
	row1.TierBytes[tiers.Standard] = 200
	row2 := &extsort.PrefixRow{Prefix: "c/", Depth: 1}
	row2.TierCounts[tiers.GlacierFR] = 1
	row2.TierBytes[tiers.GlacierFR] = 300
	row3 := &extsort.PrefixRow{Prefix: "d/", Depth: 1}
	row3.TierCounts[tiers.Standard] = 1
	row3.TierBytes[tiers.Standard] = 400

	for _, r := range withRoot(row0, row1, row2, row3) {
		if err := b.Add(withTotals(r)); err != nil {
			t.Fatalf("Add %q: %v", r.Prefix, err)
		}
	}
	if err := b.FinalizeWithContext(t.Context()); err != nil {
		t.Fatalf("Finalize: %v", err)
	}

	tsr := openTiers(t, outDir)

	// Position 0 is the root, holding every row's tiers.
	wantStd := []uint64{3, 1, 1, 0, 1}
	wantStdBytes := []uint64{700, 100, 200, 0, 400}
	wantGl := []uint64{1, 0, 0, 1, 0}
	wantGlBytes := []uint64{300, 0, 0, 300, 0}

	for pos := range uint64(5) {
		breakdown := tsr.BreakdownAll(pos)
		gotStd, gotStdBytes, gotGl, gotGlBytes := uint64(0), uint64(0), uint64(0), uint64(0)
		for _, tb := range breakdown {
			if tb.TierID == tiers.Standard {
				gotStd, gotStdBytes = tb.ObjectCount, tb.Bytes
			}
			if tb.TierID == tiers.GlacierFR {
				gotGl, gotGlBytes = tb.ObjectCount, tb.Bytes
			}
		}
		if gotStd != wantStd[pos] || gotStdBytes != wantStdBytes[pos] {
			t.Errorf("pos %d Standard: got (%d, %d), want (%d, %d)", pos, gotStd, gotStdBytes, wantStd[pos], wantStdBytes[pos])
		}
		if gotGl != wantGl[pos] || gotGlBytes != wantGlBytes[pos] {
			t.Errorf("pos %d GlacierFR: got (%d, %d), want (%d, %d)", pos, gotGl, gotGlBytes, wantGl[pos], wantGlBytes[pos])
		}
	}
}

// TestEmptyIndex_EmptyPresentTiers covers the empty-index path the
// pipeline takes when there are no run files: SetPresentTiers is called
// with an empty set and no rows are added. The lazy tier writer must
// never be created (so NewTierStatsWriter's non-empty requirement is
// not tripped), no manifest is written, and OpenTierStats returns a
// usable empty reader.
func TestEmptyIndex_EmptyPresentTiers(t *testing.T) {
	outDir := filepath.Join(t.TempDir(), "idx")
	b, err := extsort.NewIndexBuilder(outDir, "")
	if err != nil {
		t.Fatalf("NewIndexBuilder: %v", err)
	}
	if err := b.SetPresentTiers(nil); err != nil {
		t.Fatalf("SetPresentTiers(nil): %v", err)
	}
	if err := b.FinalizeWithContext(t.Context()); err != nil {
		t.Fatalf("Finalize: %v", err)
	}

	tsr := openTiers(t, outDir)
	if tsr.HasTierData() {
		t.Error("empty index should report no tier data")
	}
}

// TestIndexBuilder_RejectsMissingParent: the index stores each prefix
// as its parent plus one segment, so the root comes first and no
// prefix may skip a level.
func TestIndexBuilder_RejectsMissingParent(t *testing.T) {
	for _, tc := range []struct {
		name     string
		prefixes []string
	}{
		{"no root", []string{"a/"}},
		{"skipped level", []string{"", "a/b/"}},
		{"not slash-terminated", []string{"", "a"}},
		{"second root", []string{"", ""}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b, err := extsort.NewIndexBuilder(filepath.Join(t.TempDir(), "idx"), "")
			if err != nil {
				t.Fatal(err)
			}
			var last error
			for _, p := range tc.prefixes {
				row := &extsort.PrefixRow{Prefix: p, Count: 1, TotalBytes: 1}
				row.TierCounts[tiers.Standard], row.TierBytes[tiers.Standard] = 1, 1
				if last = b.Add(row); last != nil {
					break
				}
			}
			if !errors.Is(last, extsort.ErrMissingParent) {
				t.Errorf("err = %v, want ErrMissingParent", last)
			}
		})
	}
}
