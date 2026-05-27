package format_test

import (
	"encoding/binary"
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

type tierRow struct {
	counts [tiers.NumTiers]uint64
	bytes  [tiers.NumTiers]uint64
}

// writeRows writes a tier_stats_row.bin under dir for the given present
// tiers and rows, using the production writer. Returns the file path.
func writeRows(t *testing.T, dir string, present []tiers.ID, rows []tierRow) string {
	t.Helper()
	w, err := format.NewTierStatsRowWriter(dir, present)
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

// TestTierStatsRowWriter_SparseStrideAndRoundTrip verifies the writer
// emits a packed stride of len(present) slots directly and that the
// reader recovers each declared tier's (count, bytes) from the slot it
// was written to.
func TestTierStatsRowWriter_SparseStrideAndRoundTrip(t *testing.T) {
	dir := t.TempDir()
	present := []tiers.ID{tiers.Standard, tiers.GlacierIR, tiers.DeepArchive}

	var row tierRow
	row.counts[tiers.Standard] = 10
	row.bytes[tiers.Standard] = 1000
	row.counts[tiers.GlacierIR] = 20
	row.bytes[tiers.GlacierIR] = 2000
	row.counts[tiers.DeepArchive] = 30
	row.bytes[tiers.DeepArchive] = 3000

	writeRows(t, dir, present, []tierRow{row, row})

	reader, err := format.OpenTierStatsRow(dir)
	if err != nil {
		t.Fatalf("OpenTierStatsRow: %v", err)
	}
	defer reader.Close()

	if got := reader.SlotCount(); got != len(present) {
		t.Errorf("SlotCount = %d, want %d (sparse stride, not dense %d)",
			got, len(present), tiers.NumTiers)
	}
	if reader.Count() != 2 {
		t.Fatalf("Count = %d, want 2", reader.Count())
	}

	// Slot order is sorted tier ID: Standard(0), GlacierIR(3), DeepArchive(5).
	wantCounts := []uint64{10, 20, 30}
	wantBytes := []uint64{1000, 2000, 3000}
	raw := reader.UnsafeRow(0)
	for slot := range present {
		off := slot * format.TierStatsSlotBytes
		gotCount := binary.LittleEndian.Uint64(raw[off : off+8])
		gotBytes := binary.LittleEndian.Uint64(raw[off+8 : off+16])
		if gotCount != wantCounts[slot] || gotBytes != wantBytes[slot] {
			t.Errorf("slot %d = (%d, %d), want (%d, %d)",
				slot, gotCount, gotBytes, wantCounts[slot], wantBytes[slot])
		}
	}
}

// TestTierStatsRowWriter_RejectsUndeclaredTierData is the regression
// guard for the present-tier mask: if a row carries data for a tier the
// caller did not declare present, the writer must fail loudly rather
// than silently drop that tier's count/bytes (a mask-threading bug).
func TestTierStatsRowWriter_RejectsUndeclaredTierData(t *testing.T) {
	dir := t.TempDir()
	w, err := format.NewTierStatsRowWriter(dir, []tiers.ID{tiers.Standard})
	if err != nil {
		t.Fatalf("NewTierStatsRowWriter: %v", err)
	}

	var counts, bytes [tiers.NumTiers]uint64
	counts[tiers.GlacierIR] = 1 // undeclared tier
	bytes[tiers.GlacierIR] = 100

	if err := w.Add(&counts, &bytes); err == nil {
		t.Fatal("Add with data for an undeclared tier should fail, got nil")
	}
}

// TestNewTierStatsRowWriter_EmptyPresent_Errors confirms the writer
// refuses an empty present set: an index with no tier data writes no
// row file at all (the builder skips the writer entirely).
func TestNewTierStatsRowWriter_EmptyPresent_Errors(t *testing.T) {
	if _, err := format.NewTierStatsRowWriter(t.TempDir(), nil); err == nil {
		t.Fatal("NewTierStatsRowWriter with empty present should error, got nil")
	}
}

// TestTierStatsRowWriter_AllTiersDenseStride confirms declaring every
// tier yields the full dense stride.
func TestTierStatsRowWriter_AllTiersDenseStride(t *testing.T) {
	dir := t.TempDir()
	var row tierRow
	for i := range tiers.NumTiers {
		row.counts[i] = uint64(i + 1)
		row.bytes[i] = uint64((i + 1) * 100)
	}
	writeRows(t, dir, tiers.AllTierIDs(), []tierRow{row})

	reader, err := format.OpenTierStatsRow(dir)
	if err != nil {
		t.Fatalf("OpenTierStatsRow: %v", err)
	}
	defer reader.Close()
	if got := reader.SlotCount(); got != int(tiers.NumTiers) {
		t.Errorf("SlotCount = %d, want %d", got, tiers.NumTiers)
	}
}
