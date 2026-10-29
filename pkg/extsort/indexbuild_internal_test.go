package extsort

import (
	"fmt"
	"strings"
	"testing"

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

// TestWriteLevelsSideFailureEnds: a row the stats side cannot write,
// past more batches than the hand-off holds, fails Finalize instead of
// leaving the spool reader blocked; the names side and reader stop.
func TestWriteLevelsSideFailureEnds(t *testing.T) {
	b, err := NewIndexBuilder(t.TempDir(), t.TempDir())
	if err != nil {
		t.Fatalf("NewIndexBuilder: %v", err)
	}
	defer b.cleanup()
	if err := b.SetPresentTiers([]tiers.ID{tiers.Standard}); err != nil {
		t.Fatal(err)
	}
	if err := b.Add(&PrefixRow{Prefix: ""}); err != nil {
		t.Fatal(err)
	}
	const n = 2 * levelBatches * levelBatchRows
	for i := range n {
		row := PrefixRow{Prefix: fmt.Sprintf("p%06d/", i), Depth: 1, Count: 1, TotalBytes: 1}
		tier := tiers.Standard
		if i == levelBatchRows/2 {
			tier = tiers.GlacierIR // not declared: the tier writer refuses it
		}
		row.TierCounts[tier], row.TierBytes[tier] = 1, 1
		if err := b.Add(&row); err != nil {
			t.Fatalf("Add %d: %v", i, err)
		}
	}
	if err := b.Finalize(); err == nil || !strings.Contains(err.Error(), "tier") {
		t.Fatalf("Finalize = %v, want the tier writer's error", err)
	}
}
