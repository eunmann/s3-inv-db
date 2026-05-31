package format_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/format"
	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

// TestTierStatsSparse_RoundTrip builds an index whose populated-tier
// distribution makes sparse the smaller layout. Confirms that
// (a) the sparse files are written and the dense file is gone,
// (b) Breakdown round-trips every (pos, tier) pair correctly.
func TestTierStatsSparse_RoundTrip(t *testing.T) {
	present := []tiers.ID{tiers.Standard, tiers.GlacierFR, tiers.DeepArchive, tiers.ITFrequent, tiers.ITArchive}
	dir := t.TempDir()
	outDir := filepath.Join(dir, "idx")

	w, err := format.NewTierStatsRowWriter(outDir, present)
	if err != nil {
		t.Fatalf("NewTierStatsRowWriter: %v", err)
	}

	// 8 rows, each populating exactly 1 of 5 present slots — sparse
	// is the clear winner (1×16 + 2 bitmap + 8 offset ≪ 5×16).
	rows := []struct {
		tier  tiers.ID
		count uint64
		bytes uint64
	}{
		{tiers.Standard, 1, 100},
		{tiers.Standard, 2, 200},
		{tiers.GlacierFR, 3, 300},
		{tiers.DeepArchive, 4, 400},
		{tiers.ITFrequent, 5, 500},
		{tiers.ITArchive, 6, 600},
		{tiers.Standard, 7, 700},
		{tiers.DeepArchive, 8, 800},
	}
	for _, r := range rows {
		var counts, bytes [tiers.NumTiers]uint64
		counts[r.tier] = r.count
		bytes[r.tier] = r.bytes
		if err := w.Add(&counts, &bytes); err != nil {
			t.Fatalf("Add: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Sparse should have been picked. Dense file should be gone;
	// sparse files should exist.
	tierDir := filepath.Join(outDir, "tier_stats")
	if _, err := os.Stat(filepath.Join(tierDir, format.TierStatsRowFile)); !os.IsNotExist(err) {
		t.Errorf("dense file still present after sparse conversion: err=%v", err)
	}
	if _, err := os.Stat(filepath.Join(tierDir, format.TierStatsSparseFile)); err != nil {
		t.Errorf("sparse rows file missing: %v", err)
	}
	if _, err := os.Stat(filepath.Join(tierDir, format.TierStatsSparseOffsetsFile)); err != nil {
		t.Errorf("sparse offsets file missing: %v", err)
	}

	// Need a tiers.json so OpenTierStats can find the manifest.
	if err := tiers.WriteManifest(outDir, present); err != nil {
		t.Fatalf("WriteManifest: %v", err)
	}

	r, err := format.OpenTierStats(outDir)
	if err != nil {
		t.Fatalf("OpenTierStats: %v", err)
	}
	defer r.Close()

	for pos, want := range rows {
		breakdown := r.Breakdown(uint64(pos))
		if len(breakdown) != 1 {
			t.Errorf("pos %d: got %d non-zero tiers, want 1", pos, len(breakdown))

			continue
		}
		got := breakdown[0]
		if got.TierID != want.tier || got.ObjectCount != want.count || got.Bytes != want.bytes {
			t.Errorf("pos %d: got (tier=%d, count=%d, bytes=%d), want (tier=%d, count=%d, bytes=%d)",
				pos, got.TierID, got.ObjectCount, got.Bytes, want.tier, want.count, want.bytes)
		}
	}
}

// TestTierStatsSparse_DenseWinsOnSingleTier verifies the heuristic
// keeps the dense format when there's only one present tier — sparse
// is always worse in that regime (bitmap + offset > nothing).
func TestTierStatsSparse_DenseWinsOnSingleTier(t *testing.T) {
	present := []tiers.ID{tiers.Standard}
	dir := t.TempDir()
	outDir := filepath.Join(dir, "idx")

	w, err := format.NewTierStatsRowWriter(outDir, present)
	if err != nil {
		t.Fatalf("NewTierStatsRowWriter: %v", err)
	}
	for i := range 100 {
		var counts, bytes [tiers.NumTiers]uint64
		//nolint:gosec // tiers.Standard is a typed enum < NumTiers
		counts[tiers.Standard] = uint64(i + 1)
		//nolint:gosec // tiers.Standard is a typed enum < NumTiers
		bytes[tiers.Standard] = uint64((i + 1) * 100)
		if err := w.Add(&counts, &bytes); err != nil {
			t.Fatalf("Add: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	tierDir := filepath.Join(outDir, "tier_stats")
	if _, err := os.Stat(filepath.Join(tierDir, format.TierStatsRowFile)); err != nil {
		t.Errorf("dense file missing after single-tier build: %v", err)
	}
	if _, err := os.Stat(filepath.Join(tierDir, format.TierStatsSparseFile)); !os.IsNotExist(err) {
		t.Errorf("sparse file should NOT exist for single-tier build: err=%v", err)
	}
}

// TestTierStatsSparse_DenseWinsOnFullyPopulated verifies the
// heuristic keeps the dense format when every row populates every
// present-tier slot — sparse would add the bitmap + offsets overhead
// for no compression.
func TestTierStatsSparse_DenseWinsOnFullyPopulated(t *testing.T) {
	present := []tiers.ID{tiers.Standard, tiers.GlacierFR, tiers.DeepArchive}
	dir := t.TempDir()
	outDir := filepath.Join(dir, "idx")

	w, err := format.NewTierStatsRowWriter(outDir, present)
	if err != nil {
		t.Fatalf("NewTierStatsRowWriter: %v", err)
	}
	for i := range 100 {
		var counts, bytes [tiers.NumTiers]uint64
		for _, tid := range present {
			counts[tid] = uint64(i + 1)
			bytes[tid] = uint64((i + 1) * 100)
		}
		if err := w.Add(&counts, &bytes); err != nil {
			t.Fatalf("Add: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	tierDir := filepath.Join(outDir, "tier_stats")
	if _, err := os.Stat(filepath.Join(tierDir, format.TierStatsRowFile)); err != nil {
		t.Errorf("dense file missing after fully-populated build: %v", err)
	}
	if _, err := os.Stat(filepath.Join(tierDir, format.TierStatsSparseFile)); !os.IsNotExist(err) {
		t.Errorf("sparse file should NOT exist for fully-populated build: err=%v", err)
	}
}
