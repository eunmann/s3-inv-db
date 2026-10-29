package format

import (
	"bufio"
	"cmp"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"math/bits"
	"os"
	"slices"
	"unsafe"
)

// bbhash is a partitioned BBHash minimal perfect hash (Limasset et al.,
// "Fast and Scalable Minimal Perfect Hashing for Massive Key Sets",
// 2017) over 64-bit key hashes. The top partBits bits of a key choose
// its partition, and each partition is a BBHash of its own keys. Each
// level gives the keys that collided on every earlier level a bit
// vector of about gamma bits per key; a key lands on the first level
// where its bit is its own.
//
// Partition p owns the slots [base_p, base_p+n_p), base_p being the
// keys of the partitions before it. Its placed keys take the first of
// them, by the number of set bits before theirs within the partition;
// the keys its levels cannot place (equal 64-bit hashes always collide)
// take the rest, in the order of its fallback entries. Every
// partition's bits lie back to back, so one rank directory, holding one
// count per 512-bit block, counts set bits across them all: a key's
// slot is that count plus its partition's slotDelta. A lookup costs
// O(levels) whatever the key count.
type bbhash struct {
	// Bits is every partition's levels' bit vectors, back to back; its
	// capacity reaches the end of the last rank block (see rankOf).
	bits     []uint64
	ranks    []uint64     // ranks[b] = set bits in bits[:b*rankBlockWords]
	levels   []bbLevel    // every partition's levels, partition by partition
	fallback []bbFallback // every partition's, each sorted by compareFallback
	parts    []bbPart
	// partShift is 64 minus the partition bits: a key's partition is
	// key>>partShift, 0 for every key at 64 (a Go shift past the width).
	partShift uint
	placed    uint64 // keys placed in the levels: every set bit
	count     uint64 // placed plus the fallback entries
}

// bbPart locates one partition's levels and fallback entries in the
// table's, and maps its ranks and fallback indexes to slots.
type bbPart struct {
	firstLevel uint32
	numLevels  uint32
	// slotDelta is the partition's slot base minus the set bits before
	// its bits: a placed key's slot is its rank plus it.
	slotDelta uint64
	fbStart   uint64
	fbEnd     uint64
	// fbSlot is the slot of the partition's first fallback entry: its
	// slot base plus its placed keys.
	fbSlot uint64
}

// bbFallback is a key the levels left unplaced. The fingerprint tells
// apart keys that share a hash, so the pair is the lookup key.
type bbFallback struct {
	hash, fp uint64
}

// bbLevel locates one level's bit vector, in words: inside the table's
// bits once loaded, inside its partition's bits while building.
type bbLevel struct {
	offset uint64
	words  uint64
}

const (
	// RankBlockWords is the rank directory's block size in words: one
	// 8-byte count per 512 bits (12.5% over the bit vectors).
	rankBlockWords = 8
	rankBlockBits  = rankBlockWords * 64
	// MaxBBHashLevels bounds a partition's build. At gamma 2 about 39%
	// of a level's keys collide, so distinct keys are placed within ~30
	// levels even at 2^40 keys; keys still unplaced here go to the
	// fallback table.
	maxBBHashLevels = 64
	// BbhashGamma is the bits per key of each level: smaller tables
	// for more levels and slower builds; 2.0 is the paper's default.
	bbhashGamma = 2.0
	// BbhashIOBuffer sizes mph.bin's read and write buffers.
	bbhashIOBuffer = 1 << 20
	// MaxPartBits bounds the partition bits: 2^16 partitions.
	maxPartBits = 16
)

// splitmix64's constants: the level's odd increment, then the
// finalizer's shifts and multipliers.
const (
	splitmixGamma  = 0x9e3779b97f4a7c15
	splitmixMul1   = 0xbf58476d1ce4e5b9
	splitmixMul2   = 0x94d049bb133111eb
	splitmixShift1 = 30
	splitmixShift2 = 27
	splitmixShift3 = 31
)

// ErrBBHashCorrupt means mph.bin does not describe a valid table.
var errBBHashCorrupt = errors.New("bbhash: corrupt MPHF file")

// bbPartition is one partition's table as the build makes it and
// mph.bin stores it: its key count, its levels (offsets within bits),
// their bit vectors and its sorted fallback entries.
type bbPartition struct {
	n        uint64
	levels   []bbLevel
	bits     []uint64
	fallback []bbFallback
}

// partScratch is one build worker's buffers, reused across partitions
// so a partition costs no allocation once they reach its size: the
// partition's records, the keys colliding on the level being built,
// the level's marks, the partition's rank directory and its slot
// layout.
type partScratch struct {
	hashes, fps, pos []uint64
	work             []uint64
	marks            []uint64
	ranks            []uint64
	layout           []byte
}

// buildPartition builds the levels and fallback of the partition whose
// keys are hashes, with fingerprints fps, into out, reusing out's
// slices. Equal keys collide on every level, so the build stops at the
// first level that places no key rather than add levels that cannot
// help; the keys still unplaced become the fallback entries. Two keys
// equal in both hash and fingerprint cannot be told apart by any
// lookup, so they fail the build.
//
// Level 0's marks come from one pass over hashes. Each level's placed
// bits, the ones a single key set, then give its count of colliding
// keys by a popcount; one pass copies the colliding keys to s.work
// (compacting them there in place from level 1 on) and the next pass
// marks them on the next level.
func buildPartition(hashes, fps []uint64, s *partScratch, out *bbPartition) error {
	out.n = uint64(len(hashes))
	out.levels, out.bits, out.fallback = out.levels[:0], out.bits[:0], out.fallback[:0]
	if len(hashes) == 0 {
		return nil
	}
	if cap(s.work) < len(hashes) {
		s.work = make([]uint64, len(hashes))
	}
	keys := hashes
	words := levelWords(len(keys))
	marks := s.levelMarks(words)
	markKeys(keys, 0, marks)
	for level := 0; ; level++ {
		placed := placedBits(marks)
		rest := len(keys) - placed
		if placed > 0 {
			offset := uint64(len(out.bits))
			out.levels = append(out.levels, bbLevel{offset: offset, words: words})
			out.bits = append(out.bits, marks[:words]...)
		}
		if rest == 0 {
			return nil
		}
		if placed == 0 {
			// Only keys equal on every level are left: the leftovers.
			keys = append(s.work[:0], keys...)

			break
		}
		lvBits := out.bits[len(out.bits)-int(words):]
		next := s.work[:len(keys)]
		m := 0
		for _, k := range keys {
			i := bbPosition(k, level, words)
			next[m] = k
			m += int(^lvBits[i/64] >> (i % 64) & 1)
		}
		keys = next[:m]
		if level+1 == maxBBHashLevels {
			break
		}
		words = levelWords(len(keys))
		marks = s.levelMarks(words)
		markKeys(keys, level+1, marks)
	}

	return partitionFallback(keys, hashes, fps, out)
}

// levelMarks returns s's marks cleared for a level of words words.
func (s *partScratch) levelMarks(words uint64) []uint64 {
	if uint64(cap(s.marks)) < 2*words {
		s.marks = make([]uint64, 2*words)
	}
	m := s.marks[:2*words]
	clear(m)

	return m
}

// partitionFallback sets out's fallback entries: the (hash,
// fingerprint) of every key whose hash is a leftover, sorted. Every key
// with a leftover hash is unplaced, since keys sharing a hash take the
// same position on every level; leftovers is sorted in place.
func partitionFallback(leftovers, hashes, fps []uint64, out *bbPartition) error {
	slices.Sort(leftovers)
	leftovers = slices.Compact(leftovers)
	for i, h := range hashes {
		if _, found := slices.BinarySearch(leftovers, h); found {
			out.fallback = append(out.fallback, bbFallback{hash: h, fp: fps[i]})
		}
	}
	slices.SortFunc(out.fallback, compareFallback)
	for j := 1; j < len(out.fallback); j++ {
		if out.fallback[j] == out.fallback[j-1] {
			return fmt.Errorf("%w: two prefixes share key hash %#x and fingerprint %#x",
				ErrMPHFKeyCollision, out.fallback[j].hash, out.fallback[j].fp)
		}
	}

	return nil
}

// compareFallback orders fallback entries by hash, then fingerprint.
func compareFallback(a, b bbFallback) int {
	if c := cmp.Compare(a.hash, b.hash); c != 0 {
		return c
	}

	return cmp.Compare(a.fp, b.fp)
}

// levelWords sizes a level for n keys: gamma bits per key, at least one word.
func levelWords(n int) uint64 {
	return max(1, uint64(math.Ceil(bbhashGamma*float64(n)/64)))
}

// markKeys marks keys on a level whose marks hold, for each word of its
// bit vector, a set word and then a collided word: a key sets its bit in
// the set word, and in the collided word when another key already set
// it. Side by side, the two words share a cache line, so a colliding
// key's second OR does not miss.
func markKeys(keys []uint64, level int, marks []uint64) {
	words := uint64(len(marks) / 2)
	for _, k := range keys {
		i := bbPosition(k, level, words)
		w, bit := 2*(i/64), uint64(1)<<(i%64)
		marks[w+1] |= marks[w] & bit
		marks[w] |= bit
	}
}

// placedBits packs into the first half of marks the level's bits that
// exactly one key set, and returns their count: the keys the level
// places.
func placedBits(marks []uint64) int {
	words := len(marks) / 2
	placed := 0
	for w := range words {
		marks[w] = marks[2*w] &^ marks[2*w+1]
		placed += bits.OnesCount64(marks[w])
	}

	return placed
}

// bbPosition maps key to its bit index in a level of the given size: a
// splitmix64 finalizer over the key and level, reduced to the bit count
// by a multiply-shift instead of a modulo.
func bbPosition(key uint64, level int, words uint64) uint64 {
	x := key ^ (uint64(level)+1)*splitmixGamma
	x ^= x >> splitmixShift1
	x *= splitmixMul1
	x ^= x >> splitmixShift2
	x *= splitmixMul2
	x ^= x >> splitmixShift3
	i, _ := bits.Mul64(x, words*64)

	return i
}

// findBit returns the bit of the first of levels on which key's bit is
// set: the bit that placed it, if it was placed.
func findBit(levelBits []uint64, levels []bbLevel, key uint64) (uint64, bool) {
	for level, lv := range levels {
		i := lv.offset*64 + bbPosition(key, level, lv.words)
		if levelBits[i/64]&(1<<(i%64)) != 0 {
			return i, true
		}
	}

	return 0, false
}

// find returns the slot of a key the levels placed. A fallback key
// misses here; a key outside the build set either misses or lands on
// some other key's slot.
func (h *bbhash) find(key uint64) (uint64, bool) {
	p := &h.parts[key>>h.partShift]
	i, ok := findBit(h.bits, h.levels[p.firstLevel:p.firstLevel+p.numLevels], key)
	if !ok {
		return 0, false
	}

	return rankOf(h.bits, h.ranks, i) + p.slotDelta, true
}

// findFallback returns the slot of the fallback entry (key, fp).
func (h *bbhash) findFallback(key, fp uint64) (uint64, bool) {
	p := &h.parts[key>>h.partShift]
	j, ok := slices.BinarySearchFunc(h.fallback[p.fbStart:p.fbEnd], bbFallback{hash: key, fp: fp}, compareFallback)
	if !ok {
		return 0, false
	}

	return p.fbSlot + uint64(j), true
}

// rankOf counts the set bits of levelBits before bit i: the directory's
// count for i's block, the bits below i in its word, and the popcounts
// of the block's words before that word. It popcounts all the block's
// words, masking off those from i's word on, so the loop always runs
// rankBlockWords times and never mispredicts where i falls. The words
// past the end of levelBits in the last block are within its capacity
// and masked off.
func rankOf(levelBits, ranks []uint64, i uint64) uint64 {
	block := i / rankBlockBits
	words := (*[rankBlockWords]uint64)(levelBits[block*rankBlockWords : (block+1)*rankBlockWords])
	at := i / 64 % rankBlockWords
	r := ranks[block] + uint64(bits.OnesCount64(words[at]&(1<<(i%64)-1)))
	// Bit j is set for each word j before i's word.
	before := uint64(1)<<at - 1
	for j, w := range words {
		r += uint64(bits.OnesCount64(w & -(before >> j & 1)))
	}

	return r
}

// rankDirectory fills ranks with levelBits' rank directory, reusing its
// memory, and returns it with levelBits grown to the capacity rankOf
// reads.
//
//nolint:gocritic // gocritic wants named returns, nonamedreturns forbids them
func rankDirectory(levelBits, ranks []uint64) ([]uint64, []uint64) {
	levelBits = slices.Grow(levelBits, rankBlockWords-1)
	ranks = slices.Grow(ranks[:0], (len(levelBits)+rankBlockWords-1)/rankBlockWords)
	var r uint64
	for i, w := range levelBits {
		if i%rankBlockWords == 0 {
			ranks = append(ranks, r)
		}
		r += uint64(bits.OnesCount64(w))
	}

	return levelBits, ranks
}

// newBBHashTable starts an empty table of 2^partBits partitions; the
// partitions follow in order (addPart), then finish.
func newBBHashTable(partBits uint, bitsCap uint64) *bbhash {
	return &bbhash{
		bits:      make([]uint64, 0, bitsCap),
		parts:     make([]bbPart, 0, 1<<partBits),
		partShift: 64 - partBits,
	}
}

// addPart records the next partition, holding n keys, whose levels,
// bits and fallback entries are those appended to the table's since
// levelsFrom, bitsFrom and fbFrom. It checks what lookups rely on: the
// partition's set bits and fallback entries count its keys, and its
// fallback entries belong to it and are strictly sorted.
func (h *bbhash) addPart(n uint64, levelsFrom, bitsFrom, fbFrom int) error {
	p := uint64(len(h.parts))
	var placed uint64
	for _, w := range h.bits[bitsFrom:] {
		placed += uint64(bits.OnesCount64(w))
	}
	fb := h.fallback[fbFrom:]
	if placed+uint64(len(fb)) != n {
		return fmt.Errorf("%w: partition %d has %d keys, %d placed and %d in the fallback",
			errBBHashCorrupt, p, n, placed, len(fb))
	}
	for j, e := range fb {
		if e.hash>>h.partShift != p {
			return fmt.Errorf("%w: partition %d holds fallback hash %#x", errBBHashCorrupt, p, e.hash)
		}
		if j > 0 && compareFallback(fb[j-1], e) >= 0 {
			return fmt.Errorf("%w: partition %d's fallback entry %d out of order", errBBHashCorrupt, p, j)
		}
	}
	h.parts = append(h.parts, bbPart{
		firstLevel: uint32(levelsFrom),
		numLevels:  uint32(len(h.levels) - levelsFrom),
		slotDelta:  h.count - h.placed,
		fbStart:    uint64(fbFrom),
		fbEnd:      uint64(len(h.fallback)),
		fbSlot:     h.count + placed,
	})
	h.placed += placed
	h.count += n

	return nil
}

// finish builds the rank directory once every partition is added.
func (h *bbhash) finish() {
	h.bits, h.ranks = rankDirectory(h.bits, h.ranks)
}

// writePartition appends one partition to mph.bin: its key count, its
// level count, each level's word count, every level's words, then its
// fallback entry count and each entry's hash and fingerprint, all
// little-endian uint64. A bufio.Writer error is sticky, so the first
// failed write ends it.
func writePartition(w *bufio.Writer, p *bbPartition) error {
	if err := putU64(w, p.n); err != nil {
		return err
	}
	if err := putU64(w, uint64(len(p.levels))); err != nil {
		return err
	}
	for _, lv := range p.levels {
		if err := putU64(w, lv.words); err != nil {
			return err
		}
	}
	for _, word := range p.bits {
		if err := putU64(w, word); err != nil {
			return err
		}
	}
	if err := putU64(w, uint64(len(p.fallback))); err != nil {
		return err
	}
	for _, e := range p.fallback {
		if err := putU64(w, e.hash); err != nil {
			return err
		}
		if err := putU64(w, e.fp); err != nil {
			return err
		}
	}

	return nil
}

// bbhashReader reads mph.bin's words, counting the bytes left.
type bbhashReader struct {
	r    *bufio.Reader
	left uint64
	word [8]byte
}

func (r *bbhashReader) u64() (uint64, error) {
	if r.left < 8 {
		return 0, fmt.Errorf("%w: truncated", errBBHashCorrupt)
	}
	if _, err := io.ReadFull(r.r, r.word[:]); err != nil {
		return 0, fmt.Errorf("%w: %w", errBBHashCorrupt, err)
	}
	r.left -= 8

	return binary.LittleEndian.Uint64(r.word[:]), nil
}

// readBBHash loads a table mph.bin holds: the partition bits, then
// each partition as writePartition wrote it. Every count is checked
// against the bytes left before anything is sized by it.
func readBBHash(path string) (*bbhash, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open MPHF file: %w", err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat MPHF file: %w", err)
	}
	r := &bbhashReader{r: bufio.NewReaderSize(f, bbhashIOBuffer), left: uint64(info.Size())}
	partBits, err := r.u64()
	if err != nil {
		return nil, err
	}
	// A partition takes at least three words.
	if partBits > maxPartBits || r.left/24 < 1<<partBits {
		return nil, fmt.Errorf("%w: %d partition bits in %d bytes", errBBHashCorrupt, partBits, info.Size())
	}
	h := newBBHashTable(uint(partBits), r.left/8+rankBlockWords)
	for range 1 << partBits {
		if err := h.readPartition(r); err != nil {
			return nil, err
		}
	}
	if r.left != 0 {
		return nil, fmt.Errorf("%w: %d trailing bytes", errBBHashCorrupt, r.left)
	}
	if h.count == 0 {
		return nil, fmt.Errorf("%w: no keys", errBBHashCorrupt)
	}
	h.finish()

	return h, nil
}

// readPartition reads the next partition into the table.
func (h *bbhash) readPartition(r *bbhashReader) error {
	levelsFrom, bitsFrom, fbFrom := len(h.levels), len(h.bits), len(h.fallback)
	n, err := r.u64()
	if err != nil {
		return err
	}
	numLevels, err := r.u64()
	if err != nil {
		return err
	}
	if numLevels > maxBBHashLevels {
		return fmt.Errorf("%w: partition %d has %d levels", errBBHashCorrupt, len(h.parts), numLevels)
	}
	var total uint64
	for range numLevels {
		words, err := r.u64()
		if err != nil {
			return err
		}
		if words == 0 || words > r.left/8 || total+words > r.left/8 {
			return fmt.Errorf("%w: partition %d has a level of %d words", errBBHashCorrupt, len(h.parts), words)
		}
		h.levels = append(h.levels, bbLevel{offset: uint64(bitsFrom) + total, words: words})
		total += words
	}
	// Read straight into the words' memory, then decode in place: a
	// no-op on little-endian hosts and no second copy of the table.
	// The bits' capacity was sized from the file, so this stays in it.
	h.bits = h.bits[:bitsFrom+int(total)]
	raw := unsafe.Slice((*byte)(unsafe.Pointer(unsafe.SliceData(h.bits[bitsFrom:]))), total*8)
	if _, err := io.ReadFull(r.r, raw); err != nil {
		return fmt.Errorf("%w: %w", errBBHashCorrupt, err)
	}
	r.left -= total * 8
	for i := bitsFrom; i < len(h.bits); i++ {
		h.bits[i] = binary.LittleEndian.Uint64(raw[(i-bitsFrom)*8:])
	}
	numFallback, err := r.u64()
	if err != nil {
		return err
	}
	if numFallback > r.left/16 {
		return fmt.Errorf("%w: %d fallback entries in %d bytes", errBBHashCorrupt, numFallback, r.left)
	}
	for range numFallback {
		var e bbFallback
		if e.hash, err = r.u64(); err != nil {
			return err
		}
		if e.fp, err = r.u64(); err != nil {
			return err
		}
		h.fallback = append(h.fallback, e)
	}

	return h.addPart(n, levelsFrom, bitsFrom, fbFrom)
}
