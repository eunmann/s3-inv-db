package extsort

import (
	"fmt"
	"slices"
	"strings"
	"testing"
)

// tableKey is a node's key: its parent's index and its segment.
type tableKey struct {
	parent uint32
	seg    string
}

// addTableKey looks k up in agg's node table under hash h, which the
// test picks, creating its node as Aggregator.child does when absent.
func addTableKey(agg *Aggregator, h uint32, k tableKey) (uint32, bool) {
	idx, found := agg.children.getOrAdd(agg, h, k.parent, k.seg, uint32(agg.used))
	if !found {
		_, n := agg.newNode(k.parent)
		n.row.Prefix = agg.segs.clone(k.parent, k.seg)
	}

	return idx, found
}

// collidingKeys returns n distinct keys, each differing from another in
// the parent's low byte alone, its high byte alone, the segment's bytes
// alone or the segment's length alone.
func collidingKeys(n int) []tableKey {
	keys := make([]tableKey, 0, n)
	for i := 0; len(keys) < n; i++ {
		seg := fmt.Sprintf("s%03d/", i/4)
		switch i % 4 {
		case 0:
			keys = append(keys, tableKey{parent: 7, seg: seg})
		case 1:
			keys = append(keys, tableKey{parent: 8, seg: seg})
		case 2:
			keys = append(keys, tableKey{parent: 7, seg: strings.TrimSuffix(seg, "/") + "x/"})
		default:
			keys = append(keys, tableKey{parent: 1<<24 + 7, seg: seg})
		}
	}

	return keys
}

// Under hash functions that collide as much as possible, every key is
// added once, found again as the node it was added as, and never taken
// for another key with the same hash, across growth through several
// powers of two.
func TestNodeTableForcedCollisions(t *testing.T) {
	for name, hash := range map[string]func(i int) uint32{
		"all equal":          func(int) uint32 { return 0x5eed },
		"same low bits":      func(i int) uint32 { return uint32(i%3)<<20 | 5 },
		"two values":         func(i int) uint32 { return uint32(i % 2) },
		"one bucket per key": func(i int) uint32 { return uint32(i) },
	} {
		t.Run(name, func(t *testing.T) {
			keys := collidingKeys(1500)
			agg := NewAggregator(1, 0)
			added := make([]uint32, len(keys))
			sizes := []int{len(agg.children.slots)}
			for i, k := range keys {
				idx, found := addTableKey(agg, hash(i), k)
				if found {
					t.Fatalf("key %d %+v: found node %d (%+v) before it was added", i, k, idx, keys[idx])
				}
				if idx != uint32(i) {
					t.Fatalf("key %d %+v: added as node %d", i, k, idx)
				}
				added[i] = idx
				if n := len(agg.children.slots); n != sizes[len(sizes)-1] {
					sizes = append(sizes, n)
				}
			}
			if len(sizes) < 5 {
				t.Fatalf("table sizes %v: want growth across several powers of two", sizes)
			}
			for i, k := range keys {
				idx, found := addTableKey(agg, hash(i), k)
				if !found || idx != added[i] {
					t.Fatalf("key %d %+v again: node %d found=%v, want node %d", i, k, idx, found, added[i])
				}
			}
			if agg.children.count != len(keys) || agg.used != len(keys) {
				t.Fatalf("table counts %d nodes and the aggregator has %d, want %d", agg.children.count, agg.used, len(keys))
			}
		})
	}
}

// A key that matches a stored one's hash but not its key is absent: find
// reports the empty slot its probe ends at and changes nothing.
func TestNodeTableFindAbsent(t *testing.T) {
	agg := NewAggregator(1, 0)
	const h = 0xabc
	for _, k := range []tableKey{{3, "b/"}, {4, "a/"}, {3, "ab/"}} {
		addTableKey(agg, h, k)
	}
	before := slices.Clone(agg.children.slots)
	for _, k := range []tableKey{{3, "a/"}, {4, "b/"}, {3, "a"}, {3, "abc/"}, {5, "a/"}, {3, "bb"}} {
		pos, found := agg.children.find(agg, h, k.parent, k.seg)
		if node := agg.children.slots[pos].node; found || node != 0 {
			t.Errorf("find %+v: slot %d holding node %d, found=%v; want an empty slot", k, pos, int64(node)-1, found)
		}
	}
	for i, k := range []tableKey{{3, "b/"}, {4, "a/"}, {3, "ab/"}} {
		pos, found := agg.children.find(agg, h, k.parent, k.seg)
		if node := agg.children.slots[pos].node; !found || node != uint32(i)+1 {
			t.Errorf("find %+v: slot %d holding node %d, found=%v; want node %d", k, pos, int64(node)-1, found, i)
		}
	}
	if !slices.Equal(before, agg.children.slots) {
		t.Error("find changed the table")
	}
}

// Growing places every node by its stored hash, without reading a key:
// the test's hashes are not the keys' real ones, so a table that rehashed
// the keys would put the nodes off the probe sequences of the stored
// hashes.
func TestNodeTableGrowsFromStoredHashes(t *testing.T) {
	agg := NewAggregator(1, 0)
	keys := collidingKeys(14)
	for i, k := range keys {
		addTableKey(agg, uint32(i*0x9e37), k)
	}
	if agg.children.count != maxTableLoad(len(agg.children.slots)) {
		t.Fatalf("table holds %d of %d slots, want it full", agg.children.count, len(agg.children.slots))
	}
	agg.children.grow()
	mask := uint32(len(agg.children.slots) - 1)
	for i := range keys {
		h := uint32(i * 0x9e37)
		pos := h & mask
		for step := uint32(1); agg.children.slots[pos].node != uint32(i)+1; step++ {
			if agg.children.slots[pos].node == 0 {
				t.Fatalf("node %d is not on its hash's probe sequence", i)
			}
			pos = (pos + step) & mask
		}
	}
}

// The real hash: keys found again after every growth, and a reset keeps
// the slots for the next spill with no key in them.
func TestNodeTableHashAndReset(t *testing.T) {
	agg := NewAggregator(1, 0)
	keys := collidingKeys(5000)
	for i, k := range keys {
		if idx, found := agg.getOrAddChild(k.parent, k.seg, uint32(agg.used)); found {
			t.Fatalf("key %d %+v: found node %d before it was added", i, k, idx)
		}
		_, n := agg.newNode(k.parent)
		n.row.Prefix = agg.segs.clone(k.parent, k.seg)
	}
	for i, k := range keys {
		if idx, found := agg.getOrAddChild(k.parent, k.seg, uint32(agg.used)); !found || idx != uint32(i) {
			t.Fatalf("key %d %+v: node %d found=%v, want node %d", i, k, idx, found, i)
		}
	}
	slots := len(agg.children.slots)
	if slots != tableSlotsFor(len(keys)) {
		t.Fatalf("%d keys in %d slots, want %d", len(keys), slots, tableSlotsFor(len(keys)))
	}

	agg.children.reset()
	if len(agg.children.slots) != slots || agg.children.count != 0 {
		t.Fatalf("after reset: %d slots holding %d, want %d holding 0", len(agg.children.slots), agg.children.count, slots)
	}
	for _, k := range keys[:100] {
		if _, found := agg.children.find(agg, agg.children.hash(k.parent, k.seg), k.parent, k.seg); found {
			t.Fatalf("after reset: %+v still found", k)
		}
	}
}

// Same segment under different parents, and different segments under
// one parent, hash apart, and a seed of its own gives each table its own
// hashes.
func TestNodeTableHashSpreads(t *testing.T) {
	a, b := newNodeTable(0), newNodeTable(0)
	seen := map[uint32]bool{}
	for p := range uint32(1000) {
		seen[a.hash(p, "x/")] = true
	}
	for i := range 1000 {
		seen[a.hash(0, fmt.Sprintf("%d/", i))] = true
	}
	if len(seen) < 1999 {
		t.Errorf("2000 keys hash to %d values", len(seen))
	}
	same := 0
	for p := range uint32(100) {
		if a.hash(p, "x/") == b.hash(p, "x/") {
			same++
		}
	}
	if same > 1 {
		t.Errorf("two tables agree on %d of 100 hashes", same)
	}
}
