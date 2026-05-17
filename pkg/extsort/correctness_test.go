package extsort_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/extsort"
	"github.com/eunmann/s3-inv-db/pkg/format"
	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

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
	if err := b.FinalizeWithContext(context.Background()); err != nil {
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
