package extsort

import "testing"

// TestAddToPrefixTreeDoesNotAllocate: addToPrefixTree runs once per
// prefix, so a prefix whose last segment is already interned must cost
// no allocation.
func TestAddToPrefixTreeDoesNotAllocate(t *testing.T) {
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
	if err := b.addToPrefixTree(child); err != nil {
		t.Fatalf("addToPrefixTree(%q): %v", child, err)
	}
	allocs := testing.AllocsPerRun(1000, func() {
		if err := b.addToPrefixTree(child); err != nil {
			t.Fatalf("addToPrefixTree(%q): %v", child, err)
		}
	})
	if allocs != 0 {
		t.Errorf("addToPrefixTree of a known segment allocates %v times", allocs)
	}
}
