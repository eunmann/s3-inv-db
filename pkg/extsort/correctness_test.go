package extsort_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/extsort"
	"github.com/eunmann/s3-inv-db/pkg/format"
	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

// TestTierStatsRow_PackedStride pins the row stride to the number of
// *present* tiers declared via SetPresentTiers, not the compile-time
// NumTiers. The sparse layout is written directly during construction;
// a regression that ignored the declared set would silently inflate
// index size — the whole point of the hybrid layout.
func TestTierStatsRow_PackedStride(t *testing.T) {
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

	for _, r := range rows {
		if err := b.Add(r); err != nil {
			t.Fatalf("Add %q: %v", r.Prefix, err)
		}
	}
	if err := b.FinalizeWithContext(t.Context()); err != nil {
		t.Fatalf("Finalize: %v", err)
	}

	// The tier_stats writer may have picked dense or sparse layout
	// based on the populated-slot cost model. Whichever it chose, the
	// total bytes used by tier_stats/ must not exceed the dense upper
	// bound (presentTiers × N × slot); that's the "no waste on absent
	// tiers" invariant this test exists to pin.
	const presentTiers = 3
	denseUpperBound := int64(format.HeaderSize) + int64(len(rows))*int64(presentTiers)*int64(format.TierStatsSlotBytes)
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
			totalSize, denseUpperBound, len(rows), presentTiers, format.TierStatsSlotBytes)
	}

	// Round-trip through the reader: values must come back correct.
	tsr, err := format.OpenTierStats(outDir)
	if err != nil {
		t.Fatalf("OpenTierStats: %v", err)
	}
	defer tsr.Close()

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
	check(0, tiers.Standard, 1, 100)
	check(1, tiers.GlacierFR, 2, 200)
	check(2, tiers.DeepArchive, 3, 300)
	check(3, tiers.Standard, 4, 400)
	// Tiers not in any row's data must still appear in BreakdownAll with zeros.
	check(0, tiers.GlacierFR, 0, 0)
}

// TestTierStatsRow_PreorderAlignment guards the invariant that the
// row-major tier-stats file places each prefix's stats at exactly its
// preorder position. The original per-tier writers required explicit
// backfill when a tier first appeared partway through a build; the
// row-major writer always emits all NumTiers slots so no backfill is
// needed, but a regression that dropped or duplicated a row would
// silently misalign tier data with prefix positions — exactly the
// failure mode this test is here to catch.
func TestTierStatsRow_PreorderAlignment(t *testing.T) {
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

	for _, r := range []*extsort.PrefixRow{row0, row1, row2, row3} {
		if err := b.Add(r); err != nil {
			t.Fatalf("Add %q: %v", r.Prefix, err)
		}
	}
	if err := b.FinalizeWithContext(t.Context()); err != nil {
		t.Fatalf("Finalize: %v", err)
	}

	tsr, err := format.OpenTierStats(outDir)
	if err != nil {
		t.Fatalf("OpenTierStats: %v", err)
	}
	defer tsr.Close()

	wantStd := []uint64{1, 1, 0, 1}
	wantStdBytes := []uint64{100, 200, 0, 400}
	wantGl := []uint64{0, 0, 1, 0}
	wantGlBytes := []uint64{0, 0, 300, 0}

	for pos := range uint64(4) {
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
// never be created (so NewTierStatsRowWriter's non-empty requirement is
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

	tsr, err := format.OpenTierStats(outDir)
	if err != nil {
		t.Fatalf("OpenTierStats: %v", err)
	}
	defer tsr.Close()
	if tsr.HasTierData() {
		t.Error("empty index should report no tier data")
	}
}
