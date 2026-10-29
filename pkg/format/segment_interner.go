package format

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/bits"
	"math/rand/v2"
	"path/filepath"
	"unsafe"
)

// PrefixSegmentInterner gives each prefix's last segment an ID in the
// segment dictionary (prefix_dict.bin), which prefix_tree.bin rows
// reference. A segment it remembers gets the ID it already has; any
// other is appended to the dictionary under the next ID.
//
// It remembers segments within a memory budget, in two generations of
// a pointer-free hash table, each held to half of it. New segments go
// into the current generation; a segment found in the previous one is
// copied into the current one with its ID. Once the current generation
// is full the previous one is dropped and an empty one takes the
// current's place. A segment named at least once per generation keeps
// one ID; one that comes back only after a full generation of other new
// segments gets a second entry and ID. Every ID names its segment
// exactly, and the dictionary has at most one entry per prefix. While
// the distinct segments fit the budget, the dictionary is each of them
// once, in the order first seen.
type PrefixSegmentInterner struct {
	blobWriter *BlobWriter
	seed       uint64
	// cur takes new segments; old is the generation before it.
	cur, old segmentTable
	// genBytes is the most one generation may hold.
	genBytes int
	nextID   uint32
	done     bool
}

// NewPrefixSegmentInterner creates a segment interner that writes the
// dictionary to outDir, recording its files' checksums in sums, and
// remembers segments in at most budget bytes of heap, beside the
// arrays a growing table copies from until the next GC.
func NewPrefixSegmentInterner(outDir string, budget int64, sums *Checksums) (*PrefixSegmentInterner, error) {
	writer, err := NewBlobWriter(filepath.Join(outDir, PrefixDictBlobFile), filepath.Join(outDir, PrefixDictOffsetsFile), sums)
	if err != nil {
		return nil, fmt.Errorf("create prefix dict blob writer: %w", err)
	}

	return &PrefixSegmentInterner{
		blobWriter: writer,
		seed:       rand.Uint64(),
		genBytes:   int(min(max(budget, 0)/2, math.MaxInt32)),
	}, nil
}

// ErrTooManySegments is returned when an index would need more
// dictionary entries than a segment ID can number.
var ErrTooManySegments = errors.New("too many distinct prefix segments")

// Intern returns the ID for segment, appending it to the dictionary
// under a new ID unless it is remembered. It copies what it keeps, so
// segment may alias a buffer the caller reuses.
func (si *PrefixSegmentInterner) Intern(segment string) (uint32, error) {
	h := segmentHash(si.seed, segment)
	if id, ok := si.cur.find(h, segment); ok {
		return id, nil
	}
	if id, ok := si.old.find(h, segment); ok {
		si.remember(h, segment, id)

		return id, nil
	}
	if si.nextID == math.MaxUint32 {
		return 0, fmt.Errorf("%w: more than %d dictionary entries", ErrTooManySegments, uint64(math.MaxUint32))
	}
	if err := si.blobWriter.WriteString(segment); err != nil {
		return 0, fmt.Errorf("write segment: %w", err)
	}
	id := si.nextID
	si.nextID++
	si.remember(h, segment, id)

	return id, nil
}

// remember adds segment to the current generation, starting a new one
// when it is full. A segment over a quarter of a generation does not
// start one: it is remembered only while it fits.
func (si *PrefixSegmentInterner) remember(h uint64, segment string, id uint32) {
	if si.cur.insert(si.seed, h, segment, id, si.genBytes) || len(segment) > si.genBytes/4 {
		return
	}
	si.cur, si.old = si.old, si.cur
	si.cur.reset()
	si.cur.insert(si.seed, h, segment, id, si.genBytes)
}

// heapBytes is the heap the generations' arrays hold.
func (si *PrefixSegmentInterner) heapBytes() int {
	return si.cur.bytes() + si.old.bytes()
}

// Count returns the number of dictionary entries written.
func (si *PrefixSegmentInterner) Count() uint32 {
	return si.nextID
}

// Close writes the dictionary's last offset and closes its files. It
// drops the generations, so Count is all that stays usable. After an
// error, Abort removes the files.
func (si *PrefixSegmentInterner) Close() error {
	si.cur, si.old = segmentTable{}, segmentTable{}
	if err := si.blobWriter.Close(); err != nil {
		return err
	}
	si.done = true

	return nil
}

// Abort removes the dictionary's files unless Close finished them.
// Idempotent.
func (si *PrefixSegmentInterner) Abort() error {
	if si.done {
		return nil
	}
	si.cur, si.old = segmentTable{}, segmentTable{}
	si.done = true

	return si.blobWriter.Abort()
}

// segmentTable is one generation: an open-addressing table of slots,
// each a hash tag over an entry's index, and the entries, which place
// each segment's bytes in names. Nothing in it is a pointer, so the
// garbage collector scans none of it, and a miss reads only slots. A
// lookup compares the segment's bytes, so a hash collision costs a
// probe, never a wrong ID.
type segmentTable struct {
	// slots is a power of two long, at most three quarters used; 0 is
	// an empty slot.
	slots   []uint64
	entries []segmentEntry
	names   []byte
}

// segmentEntry is a remembered segment, names[off:off+n], and its ID.
type segmentEntry struct {
	off, n, id uint32
}

const (
	segmentEntryBytes = int(unsafe.Sizeof(segmentEntry{}))
	segmentSlotBytes  = 8
	minSegmentSlots   = 64
	minSegmentNames   = 1 << 10
)

// segmentTag is a slot's upper half: the hash's, its low bit set so no
// used slot is 0.
func segmentTag(h uint64) uint64 { return (h>>32 | 1) << 32 }

// find returns segment's ID if the table holds it.
func (t *segmentTable) find(h uint64, segment string) (uint32, bool) {
	if len(t.slots) == 0 {
		return 0, false
	}
	mask := uint64(len(t.slots) - 1)
	tag := segmentTag(h)
	for i := h & mask; ; i = (i + 1) & mask {
		s := t.slots[i]
		if s == 0 {
			return 0, false
		}
		if s&^math.MaxUint32 == tag {
			e := t.entries[uint32(s)]
			if int(e.n) == len(segment) && string(t.names[e.off:e.off+e.n]) == segment {
				return e.id, true
			}
		}
	}
}

// insert adds segment, which the table does not hold, growing its
// arrays while they stay within limit bytes: false when they can't.
func (t *segmentTable) insert(seed, h uint64, segment string, id uint32, limit int) bool {
	if !t.reserve(seed, len(segment), limit) {
		return false
	}
	off := len(t.names)
	t.names = append(t.names, segment...)
	t.entries = append(t.entries, segmentEntry{off: uint32(off), n: uint32(len(segment)), id: id})
	t.place(h, uint32(len(t.entries)-1))

	return true
}

// place puts entry i, whose segment hashes to h, in its slot.
func (t *segmentTable) place(h uint64, i uint32) {
	mask := uint64(len(t.slots) - 1)
	j := h & mask
	for t.slots[j] != 0 {
		j = (j + 1) & mask
	}
	t.slots[j] = segmentTag(h) | uint64(i)
}

// reserve makes room for one more segment of n bytes, doubling what is
// short while every array stays within limit bytes.
func (t *segmentTable) reserve(seed uint64, n, limit int) bool {
	slots, entries, names := len(t.slots), cap(t.entries), cap(t.names)
	if 4*(len(t.entries)+1) > 3*slots {
		slots = max(2*slots, minSegmentSlots)
	}
	if len(t.entries) == entries {
		entries = max(2*entries, minSegmentSlots/2)
	}
	for len(t.names)+n > names {
		names = max(2*names, minSegmentNames)
	}
	if slots*segmentSlotBytes+entries*segmentEntryBytes+names > limit {
		return false
	}
	if entries != cap(t.entries) {
		t.entries = append(make([]segmentEntry, 0, entries), t.entries...)
	}
	if names != cap(t.names) {
		t.names = append(make([]byte, 0, names), t.names...)
	}
	if slots != len(t.slots) {
		t.slots = make([]uint64, slots)
		for i, e := range t.entries {
			t.place(segmentHash(seed, unsafe.String(unsafe.SliceData(t.names[e.off:]), e.n)), uint32(i))
		}
	}

	return true
}

// reset empties the table, keeping its arrays.
func (t *segmentTable) reset() {
	clear(t.slots)
	t.entries, t.names = t.entries[:0], t.names[:0]
}

// bytes is the heap the table's arrays hold.
func (t *segmentTable) bytes() int {
	return len(t.slots)*segmentSlotBytes + cap(t.entries)*segmentEntryBytes + cap(t.names)
}

// segmentHash hashes s with seed as wyhash does: 16 bytes at a time,
// each pair of words folded by a 128-bit multiply. On the short segments
// most keys have, maphash.String's chain of calls cost a third more.
func segmentHash(seed uint64, s string) uint64 {
	const p0, p1, p2 = 0xa0761d6478bd642f, 0xe7037ed1a0b428db, 0x8ebc6af09c88c6e3
	b := unsafe.Slice(unsafe.StringData(s), len(s))
	n := len(b)
	seed ^= p0
	var lo, hi uint64
	switch {
	case n > 16:
		for len(b) > 16 {
			seed = segmentMix(binary.LittleEndian.Uint64(b)^p1, binary.LittleEndian.Uint64(b[8:])^seed)
			b = b[16:]
		}
		b = unsafe.Slice(unsafe.StringData(s), n)
		lo, hi = binary.LittleEndian.Uint64(b[n-16:]), binary.LittleEndian.Uint64(b[n-8:])
	case n >= 8:
		lo, hi = binary.LittleEndian.Uint64(b), binary.LittleEndian.Uint64(b[n-8:])
	case n >= 4:
		lo, hi = uint64(binary.LittleEndian.Uint32(b)), uint64(binary.LittleEndian.Uint32(b[n-4:]))
	case n > 0:
		lo = uint64(b[0])<<16 | uint64(b[n>>1])<<8 | uint64(b[n-1])
	}

	return segmentMix(p1^uint64(n), segmentMix(lo^p1, hi^seed)^p2)
}

func segmentMix(a, b uint64) uint64 {
	hi, lo := bits.Mul64(a, b)

	return hi ^ lo
}
