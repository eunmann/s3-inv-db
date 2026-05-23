package format_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/format"
	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

// TestTierStatsReader_MissingDir verifies OpenTierStats returns a
// usable empty reader (HasTierData() == false) when no tier data
// exists at the given index directory. Row-format read/write
// behaviour is covered by extsort.TestTierStatsRow_PreorderAlignment.
func TestTierStatsReader_MissingDir(t *testing.T) {
	dir := t.TempDir()

	reader, err := format.OpenTierStats(dir)
	if err != nil {
		t.Fatalf("OpenTierStats: %v", err)
	}
	if reader == nil {
		t.Fatal("expected non-nil reader when tiers.json is absent")
	}
	defer reader.Close()
	if reader.HasTierData() {
		t.Error("HasTierData should be false when tiers.json absent")
	}
}

// writeDenseRow creates a tier_stats_row.bin under dir with one row
// per (counts, bytes) entry, using the production writer (dense
// NumTiers-slot stride).
func writeDenseRow(t *testing.T, dir string, rows []struct {
	counts [tiers.NumTiers]uint64
	bytes  [tiers.NumTiers]uint64
},
) string {
	t.Helper()
	w, err := format.NewTierStatsRowWriter(dir)
	if err != nil {
		t.Fatalf("NewTierStatsRowWriter: %v", err)
	}
	for i := range rows {
		if err := w.Add(&rows[i].counts, &rows[i].bytes); err != nil {
			t.Fatalf("Add row %d: %v", i, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}

	return filepath.Join(dir, "tier_stats", "tier_stats_row.bin")
}

// TestPackTierStatsRow_MissingFile_IsNoOp covers the
// "file already absent" path (re-finalize, or a build that never
// wrote tier_stats). The function must return nil so callers don't
// need to special-case it.
func TestPackTierStatsRow_MissingFile_IsNoOp(t *testing.T) {
	dir := t.TempDir()
	// Don't create tier_stats subdir or file.
	if err := format.PackTierStatsRow(dir, []tiers.ID{tiers.Standard}); err != nil {
		t.Errorf("PackTierStatsRow on missing file = %v, want nil", err)
	}
}

// TestPackTierStatsRow_NoTiers_RemovesFile covers the empty-present-list
// path. The dense file written during ingest is dead weight (all zero
// rows) when no tier ever produced data; it must be removed so the
// reader doesn't trip on a stride/manifest mismatch.
func TestPackTierStatsRow_NoTiers_RemovesFile(t *testing.T) {
	dir := t.TempDir()
	rowFile := writeDenseRow(t, dir, []struct {
		counts [tiers.NumTiers]uint64
		bytes  [tiers.NumTiers]uint64
	}{
		{}, {}, {},
	})
	if _, err := os.Stat(rowFile); err != nil {
		t.Fatalf("expected dense file to exist before pack: %v", err)
	}

	if err := format.PackTierStatsRow(dir, nil); err != nil {
		t.Fatalf("PackTierStatsRow: %v", err)
	}
	if _, err := os.Stat(rowFile); !os.IsNotExist(err) {
		t.Errorf("expected row file removed when no tiers present, got err=%v", err)
	}
}

// TestPackTierStatsRow_AllTiersPresent_IsNoOp covers the
// "dense already optimal" skip. When every tier has data globally,
// packing would not reduce stride; the file must be left untouched
// rather than rewritten through the rename dance.
func TestPackTierStatsRow_AllTiersPresent_IsNoOp(t *testing.T) {
	dir := t.TempDir()
	var row struct {
		counts [tiers.NumTiers]uint64
		bytes  [tiers.NumTiers]uint64
	}
	for i := range tiers.NumTiers {
		row.counts[i] = uint64(i + 1)
		row.bytes[i] = uint64((i + 1) * 100)
	}
	rowFile := writeDenseRow(t, dir, []struct {
		counts [tiers.NumTiers]uint64
		bytes  [tiers.NumTiers]uint64
	}{row, row, row})

	before, err := os.Stat(rowFile)
	if err != nil {
		t.Fatalf("stat before: %v", err)
	}

	allTiers := make([]tiers.ID, tiers.NumTiers)
	for i := range allTiers {
		allTiers[i] = tiers.ID(i)
	}
	if err := format.PackTierStatsRow(dir, allTiers); err != nil {
		t.Fatalf("PackTierStatsRow: %v", err)
	}

	after, err := os.Stat(rowFile)
	if err != nil {
		t.Fatalf("stat after: %v", err)
	}
	if after.Size() != before.Size() {
		t.Errorf("file size changed %d → %d when all tiers present (should be no-op)",
			before.Size(), after.Size())
	}
}

// TestPackTierStatsRow_Idempotent confirms a second pack is a no-op
// when the file is already packed. PackTierStatsRow inspects the
// header's Width and returns early when it doesn't match the dense
// stride.
func TestPackTierStatsRow_Idempotent(t *testing.T) {
	dir := t.TempDir()
	var row struct {
		counts [tiers.NumTiers]uint64
		bytes  [tiers.NumTiers]uint64
	}
	row.counts[tiers.Standard] = 7
	row.bytes[tiers.Standard] = 700
	rowFile := writeDenseRow(t, dir, []struct {
		counts [tiers.NumTiers]uint64
		bytes  [tiers.NumTiers]uint64
	}{row, row})

	present := []tiers.ID{tiers.Standard}
	if err := format.PackTierStatsRow(dir, present); err != nil {
		t.Fatalf("first pack: %v", err)
	}
	first, err := os.Stat(rowFile)
	if err != nil {
		t.Fatalf("stat after first pack: %v", err)
	}

	if err := format.PackTierStatsRow(dir, present); err != nil {
		t.Errorf("second pack should be a no-op, got: %v", err)
	}
	second, err := os.Stat(rowFile)
	if err != nil {
		t.Fatalf("stat after second pack: %v", err)
	}
	if second.Size() != first.Size() {
		t.Errorf("size changed across repeat packs: %d → %d", first.Size(), second.Size())
	}
}
