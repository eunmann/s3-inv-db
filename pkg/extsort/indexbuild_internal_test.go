package extsort

import "testing"

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
