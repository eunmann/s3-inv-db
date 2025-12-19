package format

import (
	"reflect"
	"testing"
)

func TestDepthIndexBuilderEmpty(t *testing.T) {
	dir := t.TempDir()
	b := NewDepthIndexBuilder()

	if err := b.Build(dir); err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	idx, err := OpenDepthIndex(dir)
	if err != nil {
		t.Fatalf("OpenDepthIndex failed: %v", err)
	}
	defer idx.Close()

	positions, err := idx.GetPositionsAtDepth(0)
	if err != nil {
		t.Fatalf("GetPositionsAtDepth failed: %v", err)
	}
	if len(positions) != 0 {
		t.Errorf("expected no positions, got %v", positions)
	}
}

func TestDepthIndexBuilderSimple(t *testing.T) {
	dir := t.TempDir()
	b := NewDepthIndexBuilder()

	// Add positions at various depths
	// Depth 0: pos 0 (root)
	// Depth 1: pos 1, 4
	// Depth 2: pos 2, 3, 5
	b.Add(0, 0) // root
	b.Add(1, 1) // a/
	b.Add(2, 2) // a/x/
	b.Add(3, 2) // a/y/
	b.Add(4, 1) // b/
	b.Add(5, 2) // b/z/

	if err := b.Build(dir); err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	if b.MaxDepth() != 2 {
		t.Errorf("MaxDepth = %d, want 2", b.MaxDepth())
	}

	idx, err := OpenDepthIndex(dir)
	if err != nil {
		t.Fatalf("OpenDepthIndex failed: %v", err)
	}
	defer idx.Close()

	// Verify depth 0
	pos0, err := idx.GetPositionsAtDepth(0)
	if err != nil {
		t.Fatalf("GetPositionsAtDepth(0) failed: %v", err)
	}
	if !reflect.DeepEqual(pos0, []uint64{0}) {
		t.Errorf("depth 0 positions = %v, want [0]", pos0)
	}

	// Verify depth 1
	pos1, err := idx.GetPositionsAtDepth(1)
	if err != nil {
		t.Fatalf("GetPositionsAtDepth(1) failed: %v", err)
	}
	if !reflect.DeepEqual(pos1, []uint64{1, 4}) {
		t.Errorf("depth 1 positions = %v, want [1, 4]", pos1)
	}

	// Verify depth 2
	pos2, err := idx.GetPositionsAtDepth(2)
	if err != nil {
		t.Fatalf("GetPositionsAtDepth(2) failed: %v", err)
	}
	if !reflect.DeepEqual(pos2, []uint64{2, 3, 5}) {
		t.Errorf("depth 2 positions = %v, want [2, 3, 5]", pos2)
	}

	// Verify depth 3 (doesn't exist)
	pos3, err := idx.GetPositionsAtDepth(3)
	if err != nil {
		t.Fatalf("GetPositionsAtDepth(3) failed: %v", err)
	}
	if len(pos3) != 0 {
		t.Errorf("depth 3 positions = %v, want []", pos3)
	}
}

func TestDepthIndexSubtreeQuery(t *testing.T) {
	dir := t.TempDir()
	b := NewDepthIndexBuilder()

	// Build a tree:
	// 0: root (subtree 0-5)
	// 1: a/   (subtree 1-3)
	// 2: a/x/ (subtree 2)
	// 3: a/y/ (subtree 3)
	// 4: b/   (subtree 4-5)
	// 5: b/z/ (subtree 5)
	b.Add(0, 0)
	b.Add(1, 1)
	b.Add(2, 2)
	b.Add(3, 2)
	b.Add(4, 1)
	b.Add(5, 2)

	if err := b.Build(dir); err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	idx, err := OpenDepthIndex(dir)
	if err != nil {
		t.Fatalf("OpenDepthIndex failed: %v", err)
	}
	defer idx.Close()

	// Query depth 2 positions in subtree of a/ [1, 3]
	posInA, err := idx.GetPositionsInSubtree(2, 1, 3)
	if err != nil {
		t.Fatalf("GetPositionsInSubtree failed: %v", err)
	}
	if !reflect.DeepEqual(posInA, []uint64{2, 3}) {
		t.Errorf("depth 2 in a/ subtree = %v, want [2, 3]", posInA)
	}

	// Query depth 2 positions in subtree of b/ [4, 5]
	posInB, err := idx.GetPositionsInSubtree(2, 4, 5)
	if err != nil {
		t.Fatalf("GetPositionsInSubtree failed: %v", err)
	}
	if !reflect.DeepEqual(posInB, []uint64{5}) {
		t.Errorf("depth 2 in b/ subtree = %v, want [5]", posInB)
	}

	// Query depth 1 in root subtree
	posDepth1, err := idx.GetPositionsInSubtree(1, 0, 5)
	if err != nil {
		t.Fatalf("GetPositionsInSubtree failed: %v", err)
	}
	if !reflect.DeepEqual(posDepth1, []uint64{1, 4}) {
		t.Errorf("depth 1 in root subtree = %v, want [1, 4]", posDepth1)
	}
}

func TestDepthIterator(t *testing.T) {
	dir := t.TempDir()
	b := NewDepthIndexBuilder()

	b.Add(0, 0)
	b.Add(1, 1)
	b.Add(2, 2)
	b.Add(3, 2)
	b.Add(4, 1)
	b.Add(5, 2)

	if err := b.Build(dir); err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	idx, err := OpenDepthIndex(dir)
	if err != nil {
		t.Fatalf("OpenDepthIndex failed: %v", err)
	}
	defer idx.Close()

	// Iterate depth 2 in subtree [1, 3] (a/ subtree)
	it, err := idx.NewDepthIterator(2, 1, 3)
	if err != nil {
		t.Fatalf("NewDepthIterator failed: %v", err)
	}

	var positions []uint64
	for it.Next() {
		positions = append(positions, it.Pos())
	}

	if !reflect.DeepEqual(positions, []uint64{2, 3}) {
		t.Errorf("iterator positions = %v, want [2, 3]", positions)
	}
}

func TestDepthIteratorEmpty(t *testing.T) {
	dir := t.TempDir()
	b := NewDepthIndexBuilder()
	b.Add(0, 0)
	b.Add(1, 1)

	if err := b.Build(dir); err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	idx, err := OpenDepthIndex(dir)
	if err != nil {
		t.Fatalf("OpenDepthIndex failed: %v", err)
	}
	defer idx.Close()

	// Query depth 5 (doesn't exist)
	it, err := idx.NewDepthIterator(5, 0, 10)
	if err != nil {
		t.Fatalf("NewDepthIterator failed: %v", err)
	}

	if it.Next() {
		t.Error("expected no positions")
	}
}

func TestDepthIndexOutOfRange(t *testing.T) {
	dir := t.TempDir()
	b := NewDepthIndexBuilder()
	b.Add(0, 0)

	if err := b.Build(dir); err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	idx, err := OpenDepthIndex(dir)
	if err != nil {
		t.Fatalf("OpenDepthIndex failed: %v", err)
	}
	defer idx.Close()

	// Query beyond max depth
	positions, err := idx.GetPositionsAtDepth(100)
	if err != nil {
		t.Fatalf("GetPositionsAtDepth failed: %v", err)
	}
	if len(positions) != 0 {
		t.Errorf("expected empty positions for out of range depth")
	}
}

func TestDepthIndexLarge(t *testing.T) {
	dir := t.TempDir()
	b := NewDepthIndexBuilder()

	// Create 10 levels with varying positions
	for d := uint32(0); d < 10; d++ {
		for p := uint64(0); p < 100; p++ {
			pos := uint64(d)*100 + p
			b.Add(pos, d)
		}
	}

	if err := b.Build(dir); err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	idx, err := OpenDepthIndex(dir)
	if err != nil {
		t.Fatalf("OpenDepthIndex failed: %v", err)
	}
	defer idx.Close()

	if idx.MaxDepth() != 9 {
		t.Errorf("MaxDepth = %d, want 9", idx.MaxDepth())
	}

	// Verify each depth has 100 positions
	for d := uint32(0); d < 10; d++ {
		positions, err := idx.GetPositionsAtDepth(d)
		if err != nil {
			t.Fatalf("GetPositionsAtDepth(%d) failed: %v", d, err)
		}
		if len(positions) != 100 {
			t.Errorf("depth %d has %d positions, want 100", d, len(positions))
		}
	}

	// Test subtree query
	// Positions at depth 5 in range [500, 550] should be 500-550 (51 positions)
	positions, err := idx.GetPositionsInSubtree(5, 500, 550)
	if err != nil {
		t.Fatalf("GetPositionsInSubtree failed: %v", err)
	}
	if len(positions) != 51 {
		t.Errorf("got %d positions, want 51", len(positions))
	}
	if positions[0] != 500 {
		t.Errorf("first position = %d, want 500", positions[0])
	}
	if positions[50] != 550 {
		t.Errorf("last position = %d, want 550", positions[50])
	}
}

func TestDepthIndexBinarySearch(t *testing.T) {
	dir := t.TempDir()
	b := NewDepthIndexBuilder()

	// Add sparse positions at depth 1
	// Positions: 10, 20, 30, 40, 50
	for _, p := range []uint64{10, 20, 30, 40, 50} {
		b.Add(p, 1)
	}

	if err := b.Build(dir); err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	idx, err := OpenDepthIndex(dir)
	if err != nil {
		t.Fatalf("OpenDepthIndex failed: %v", err)
	}
	defer idx.Close()

	tests := []struct {
		start, end uint64
		expected   []uint64
	}{
		{0, 100, []uint64{10, 20, 30, 40, 50}},
		{15, 35, []uint64{20, 30}},
		{10, 10, []uint64{10}},
		{30, 30, []uint64{30}},
		{0, 9, nil},
		{51, 100, nil},
		{25, 25, nil},
	}

	for _, tc := range tests {
		positions, err := idx.GetPositionsInSubtree(1, tc.start, tc.end)
		if err != nil {
			t.Fatalf("GetPositionsInSubtree(%d, %d) failed: %v", tc.start, tc.end, err)
		}
		if !reflect.DeepEqual(positions, tc.expected) {
			t.Errorf("GetPositionsInSubtree(%d, %d) = %v, want %v", tc.start, tc.end, positions, tc.expected)
		}
	}
}

func TestDepthIteratorCount(t *testing.T) {
	dir := t.TempDir()
	b := NewDepthIndexBuilder()

	for p := uint64(0); p < 10; p++ {
		b.Add(p, 1)
	}

	if err := b.Build(dir); err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	idx, err := OpenDepthIndex(dir)
	if err != nil {
		t.Fatalf("OpenDepthIndex failed: %v", err)
	}
	defer idx.Close()

	it, err := idx.NewDepthIterator(1, 3, 7)
	if err != nil {
		t.Fatalf("NewDepthIterator failed: %v", err)
	}

	// Should have 5 positions (3, 4, 5, 6, 7)
	if it.Count() != 5 {
		t.Errorf("Count = %d, want 5", it.Count())
	}

	// Consume some
	it.Next()
	it.Next()

	// Should have 3 remaining
	if it.Count() != 3 {
		t.Errorf("Count after 2 Next = %d, want 3", it.Count())
	}
}
