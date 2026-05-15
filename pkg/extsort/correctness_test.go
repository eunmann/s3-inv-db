package extsort

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/format"
	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

// TestTierWriterBackfillAlignment verifies that lazy tier-writer creation
// correctly backfills zeros so tier columns align with preorder positions
// when a tier appears for the first time partway through a build.
func TestTierWriterBackfillAlignment(t *testing.T) {
	dir := t.TempDir()
	outDir := filepath.Join(dir, "idx")
	tempDir := filepath.Join(dir, "tmp")
	if err := os.MkdirAll(tempDir, 0o750); err != nil {
		t.Fatal(err)
	}

	b, err := NewIndexBuilder(outDir, tempDir, false)
	if err != nil {
		t.Fatalf("NewIndexBuilder: %v", err)
	}

	row0 := &PrefixRow{Prefix: "a/", Depth: 1}
	row0.TierCounts[tiers.Standard] = 1
	row0.TierBytes[tiers.Standard] = 100
	row1 := &PrefixRow{Prefix: "b/", Depth: 1}
	row1.TierCounts[tiers.Standard] = 1
	row1.TierBytes[tiers.Standard] = 200
	row2 := &PrefixRow{Prefix: "c/", Depth: 1}
	row2.TierCounts[tiers.GlacierFR] = 1
	row2.TierBytes[tiers.GlacierFR] = 300
	row3 := &PrefixRow{Prefix: "d/", Depth: 1}
	row3.TierCounts[tiers.Standard] = 1
	row3.TierBytes[tiers.Standard] = 400

	for _, r := range []*PrefixRow{row0, row1, row2, row3} {
		if err := b.Add(r); err != nil {
			t.Fatalf("Add %q: %v", r.Prefix, err)
		}
	}
	if err := b.FinalizeWithContext(context.Background()); err != nil {
		t.Fatalf("Finalize: %v", err)
	}

	stdCount, err := format.OpenArray(filepath.Join(outDir, "tier_stats", "standard_count.u64"))
	if err != nil {
		t.Fatalf("open standard_count: %v", err)
	}
	defer stdCount.Close()
	if got, want := stdCount.Count(), uint64(4); got != want {
		t.Errorf("standard_count length = %d, want %d", got, want)
	}
	for i, want := range []uint64{1, 1, 0, 1} {
		got, _ := stdCount.GetU64(uint64(i))
		if got != want {
			t.Errorf("standard_count[%d] = %d, want %d", i, got, want)
		}
	}

	glCount, err := format.OpenArray(filepath.Join(outDir, "tier_stats", "glacier_fr_count.u64"))
	if err != nil {
		t.Fatalf("open glacier_fr_count: %v", err)
	}
	defer glCount.Close()
	if got, want := glCount.Count(), uint64(4); got != want {
		t.Errorf("glacier_fr_count length = %d, want %d", got, want)
	}
	for i, want := range []uint64{0, 0, 1, 0} {
		got, _ := glCount.GetU64(uint64(i))
		if got != want {
			t.Errorf("glacier_fr_count[%d] = %d, want %d", i, got, want)
		}
	}
}
