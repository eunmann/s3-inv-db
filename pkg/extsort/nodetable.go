package extsort

import (
	"hash/maphash"
	"unsafe"
)

// nodeTable finds a node of the main tree by its key, its parent's index
// and its segment. It is an open-addressing table of 8-byte slots with
// no pointers in them, so the GC never scans it: a node's index and 32
// bits of its key's hash, whose low bits pick the first slot to probe.
// A lookup hashes the parent and the segment where they are, with no key
// built, and reads a node's key only when the stored hash matches.
// Growing rehashes from the stored hashes without reading a key. No node
// leaves the table until it is reset: mapSegment sets a node's parent
// before adding it, and a node it merges into another is never added.
type nodeTable struct {
	slots []nodeSlot
	count int
	seed  maphash.Seed
}

// nodeSlot is a node's index plus one, so a zeroed slot is empty, and
// its key's hash.
type nodeSlot struct {
	node uint32
	hash uint32
}

// nodeSlotBytes is a slot's size: at the maximum load of 7/8 and half
// that just after doubling, 9.1 to 18.3 B per node.
const nodeSlotBytes = int64(unsafe.Sizeof(nodeSlot{}))

// minTableSlots is the smallest table; every size is a power of two, so
// triangular probing visits every slot.
const minTableSlots = 16

// newNodeTable returns a table with room for entries nodes and a seed of
// its own: keys come from the inventory, so no fixed seed may let them
// pile into a few slots.
func newNodeTable(entries int) nodeTable {
	return nodeTable{slots: make([]nodeSlot, tableSlotsFor(entries)), seed: maphash.MakeSeed()}
}

// tableSlotsFor returns how many slots hold n nodes within the maximum
// load.
func tableSlotsFor(n int) int {
	slots := minTableSlots
	for n > maxTableLoad(slots) {
		slots *= 2
	}

	return slots
}

// maxLoadEighths is the maximum load, in eighths of the slots.
const maxLoadEighths = 7

// maxTableLoad returns how many nodes a table of the given size holds.
func maxTableLoad(slots int) int {
	return slots / 8 * maxLoadEighths
}

// Constants of the hash's mixing steps: the golden ratio's, which spreads
// the parent's index over 64 bits, and MurmurHash3's 64-bit finalizer's.
const (
	hashParentMul = 0x9e3779b97f4a7c15
	hashFinalMul1 = 0xff51afd7ed558ccd
	hashFinalMul2 = 0xc4ceb9fe1a85ec53
	hashFinalBits = 33
)

// hash returns the hash of parent's child with segment seg. Adding the
// parent's index times an odd constant to the segment's seeded hash is
// one-to-one in the parent, so two children of different parents with
// the same segment never share a 64-bit hash, and the finalizer, also
// one-to-one, spreads that sum into the low 32 bits kept.
func (t *nodeTable) hash(parent uint32, seg string) uint32 {
	h := maphash.String(t.seed, seg) + uint64(parent)*hashParentMul
	h ^= h >> hashFinalBits
	h *= hashFinalMul1
	h ^= h >> hashFinalBits
	h *= hashFinalMul2
	h ^= h >> hashFinalBits

	return uint32(h)
}

// find returns the slot of the node of a's main tree whose key is parent
// and seg, given h, that key's hash, and true; or the empty slot where
// the probe for it ends and false. The table must have an empty slot.
func (t *nodeTable) find(a *Aggregator, h, parent uint32, seg string) (uint32, bool) {
	mask := uint32(len(t.slots) - 1)
	pos := h & mask
	for step := uint32(1); ; step++ {
		s := t.slots[pos]
		if s.node == 0 {
			return pos, false
		}
		if s.hash == h && a.hasKey(s.node-1, parent, seg) {
			return pos, true
		}
		pos = (pos + step) & mask
	}
}

// getOrAdd returns the node of a's main tree whose key is parent and seg,
// given h, that key's hash, and true; or adds node idx under that key and
// returns idx and false.
func (t *nodeTable) getOrAdd(a *Aggregator, h, parent uint32, seg string, idx uint32) (uint32, bool) {
	// Growing first keeps the slot find returns valid.
	if t.count >= maxTableLoad(len(t.slots)) {
		t.grow()
	}
	pos, found := t.find(a, h, parent, seg)
	if found {
		return t.slots[pos].node - 1, true
	}
	t.slots[pos] = nodeSlot{node: idx + 1, hash: h}
	t.count++

	return idx, false
}

// grow doubles the table, placing each node by its stored hash.
func (t *nodeTable) grow() {
	slots := make([]nodeSlot, max(minTableSlots, 2*len(t.slots)))
	mask := uint32(len(slots) - 1)
	for _, s := range t.slots {
		if s.node == 0 {
			continue
		}
		pos := s.hash & mask
		for step := uint32(1); slots[pos].node != 0; step++ {
			pos = (pos + step) & mask
		}
		slots[pos] = s
	}
	t.slots = slots
}

// reset empties the table and keeps its slots for the next spill.
func (t *nodeTable) reset() {
	clear(t.slots)
	t.count = 0
}

// release empties the table and drops its slots.
func (t *nodeTable) release() {
	t.slots, t.count = nil, 0
}

// heapBytes returns the size of the table's slots.
func (t *nodeTable) heapBytes() int64 {
	return int64(len(t.slots)) * nodeSlotBytes
}

// A node's key is its parent's index, little-endian in nodeKeyHeader
// bytes, followed by its segment. The segment arena stores every
// non-root node's key where the node says, so the table compares a key
// from the node and the arena alone.
const nodeKeyHeader = 4
