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
	"sync"
	"sync/atomic"
	"unsafe"
)

// bbhash is a BBHash minimal perfect hash (Limasset et al., "Fast and
// Scalable Minimal Perfect Hashing for Massive Key Sets", 2017) over
// 64-bit key hashes. Each level gives the keys that collided on every
// earlier level a bit vector of about gamma bits per key; a key lands
// on the first level where its bit is its own, and its slot is the
// number of set bits before that bit across all levels. A rank
// directory holding one count per 512-bit block bounds that count to a
// few popcounts, so a lookup costs O(levels) whatever the key count.
//
// Keys the levels cannot place (equal 64-bit hashes always collide)
// live in a fallback table after the levels' slots.
type bbhash struct {
	// Bits is every level's bit vector, back to back; its capacity
	// reaches the end of the last rank block (see rank).
	bits     []uint64
	ranks    []uint64 // ranks[b] = set bits in bits[:b*rankBlockWords]
	levels   []bbLevel
	fallback []bbFallback // sorted by compareFallback; entry j owns slot placed+j
	placed   uint64       // keys placed in the levels: every set bit
	count    uint64       // placed plus the fallback entries
}

// bbFallback is a key the levels left unplaced. The fingerprint tells
// apart keys that share a hash, so the pair is the lookup key.
type bbFallback struct {
	hash, fp uint64
}

// bbLevel locates one level's bit vector inside bbhash.bits.
type bbLevel struct {
	offset uint64
	words  uint64
}

const (
	// RankBlockWords is the rank directory's block size in words: one
	// 8-byte count per 512 bits (12.5% over the bit vectors).
	rankBlockWords = 8
	rankBlockBits  = rankBlockWords * 64
	// MaxBBHashLevels bounds the build. At gamma 2 about 39% of a
	// level's keys collide, so distinct keys are placed within ~30
	// levels even at 2^40 keys; keys still unplaced here go to the
	// fallback table.
	maxBBHashLevels = 64
	// BbhashGamma is the bits per key of each level: smaller tables
	// for more levels and slower builds; 2.0 is the paper's default.
	bbhashGamma = 2.0
	// BbhashIOBuffer sizes mph.bin's read and write buffers.
	bbhashIOBuffer = 1 << 20
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

// newBBHash builds the table over keys. Equal keys collide on every
// level, so the build stops at the first level that places no key
// rather than add levels that cannot help, and passes the keys still
// unplaced to fallback, whose entries (sorted by compareFallback) become
// the fallback table. It reads keys in place (they may be a read-only
// mmap) and copies only the keys that collide on a level into the next
// one, so the leftovers never alias keys.
func newBBHash(keys []uint64, fallback func(leftovers []uint64) ([]bbFallback, error)) (*bbhash, error) {
	return newBBHashWith(keys, heapKeys, fallback, nil)
}

// keyBuffer returns room for n keys and a function releasing it.
type keyBuffer func(n int) ([]uint64, func() error, error)

// heapKeys is the keyBuffer of a build that keeps every level's keys on
// the heap.
//
//nolint:gocritic // gocritic wants named returns, nonamedreturns forbids them
func heapKeys(n int) ([]uint64, func() error, error) {
	return make([]uint64, n), func() error { return nil }, nil
}

// newBBHashWith is newBBHash taking each level's colliding keys from
// keyBuf, so a large build can keep them off the heap; a level's keys
// are released once the next level is built. Scan, when non-nil,
// counts the keys each pass reads (see bbhashScanEstimate).
func newBBHashWith(keys []uint64, keyBuf keyBuffer, fallback func(leftovers []uint64) ([]bbFallback, error), scan *stepCounter) (*bbhash, error) {
	release := func() error { return nil }
	h, buildErr := buildBBHashLevels(keys, keyBuf, fallback, &release, scan)
	if err := errors.Join(buildErr, release()); err != nil {
		return nil, err
	}

	return h, nil
}

// buildBBHashLevels builds the levels and the fallback; *release frees
// the keys of the level built last, which the caller releases.
//
// One pass marks the first level's bits. Each level's placed bits, the
// ones a single key set, then give its count of colliding keys by a
// popcount, and one pass over its keys both copies the colliding ones
// out and marks them on the next level.
func buildBBHashLevels(keys []uint64, keyBuf keyBuffer, fallback func(leftovers []uint64) ([]bbFallback, error), release *func() error, scan *stepCounter) (*bbhash, error) {
	firstWords := levelWords(len(keys))
	// The levels after the first hold ~65% as many bits again.
	h := &bbhash{bits: make([]uint64, 0, firstWords*7/4+maxBBHashLevels)}
	var marks []uint64
	if len(keys) > 0 {
		marks = make([]uint64, 2*firstWords)
		markLevel(keys, 0, marks, scan)
	}
	for level := 0; len(keys) > 0; level++ {
		lvBits, placed := placedBits(marks)
		if placed > 0 {
			offset := uint64(len(h.bits))
			h.levels = append(h.levels, bbLevel{offset: offset, words: uint64(len(lvBits))})
			h.bits = append(h.bits, lvBits...)
			lvBits = h.bits[offset:]
		}
		rest := len(keys) - placed
		if rest == 0 {
			keys = nil

			break
		}
		// A level that places no key holds only keys equal on every
		// level: they are the leftovers, copied so they never alias keys.
		last := placed == 0 || level+1 == maxBBHashLevels
		marks = nil
		if !last {
			marks = make([]uint64, 2*levelWords(rest))
		}
		next, nextRelease, err := keyBuf(rest)
		if err != nil {
			return nil, err
		}
		nextLevelKeys(keys, level, lvBits, next, marks, scan)
		if err := (*release)(); err != nil {
			return nil, errors.Join(err, nextRelease())
		}
		*release = nextRelease
		keys = next
		if last {
			break
		}
	}
	if len(keys) > 0 {
		var err error
		if h.fallback, err = fallback(keys); err != nil {
			return nil, err
		}
	}
	h.buildRanks()

	return h, nil
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

// bbhashScanEstimate is about how many key reads building n keys takes,
// the total of its progress: the first level's marking reads every key,
// and each level's pass reads its keys once. A key collides on a level
// with probability 1-e^(-1/gamma), so the levels hold n·e^(1/gamma)
// keys in all.
func bbhashScanEstimate(n int) int64 {
	return int64(math.Ceil(float64(n) * (1 + math.Exp(1/bbhashGamma))))
}

// markLevel marks keys on level (see markKeys) and counts them on scan.
func markLevel(keys []uint64, level int, marks []uint64, scan *stepCounter) {
	forChunks(len(keys), scan, func(_, lo, hi int) {
		markKeys(keys[lo:hi], level, marks)
	})
}

// markKeys marks keys on a level whose marks hold, for each word of its
// bit vector, a set word and then a collided word: a key sets its bit in
// the set word, and in the collided word when another key already set
// it. Side by side, the two words share a cache line, so a colliding
// key's second OR does not miss. Atomic ORs make the result independent
// of the order the workers run in.
func markKeys(keys []uint64, level int, marks []uint64) {
	words := uint64(len(marks) / 2)
	for _, k := range keys {
		i := bbPosition(k, level, words)
		w, bit := 2*(i/64), uint64(1)<<(i%64)
		if atomic.OrUint64(&marks[w], bit)&bit != 0 {
			atomic.OrUint64(&marks[w+1], bit)
		}
	}
}

// placedBits packs into the first half of marks the level's bits that
// exactly one key set, and returns them with their count: the keys the
// level places.
//
//nolint:gocritic // gocritic wants named returns, nonamedreturns forbids them
func placedBits(marks []uint64) ([]uint64, int) {
	words := len(marks) / 2
	placed := 0
	for w := range words {
		marks[w] = marks[2*w] &^ marks[2*w+1]
		placed += bits.OnesCount64(marks[w])
	}

	return marks[:words], placed
}

// collectBlock is how many keys a worker tests before it hands the
// colliding ones on, so its buffer stays in cache.
const collectBlock = 1 << 12

// nextLevelKeys copies to next, sized to hold them exactly, the keys
// whose bit is not in placed, the bits level placed, and marks them on
// level+1 in marks unless marks is nil. Each worker gathers a
// block's colliding keys without a branch, then claims room for them in
// next, so they land in no set order: neither the levels nor the
// leftovers depend on it.
func nextLevelKeys(keys []uint64, level int, placed, next, marks []uint64, scan *stepCounter) {
	words := uint64(len(placed))
	bufs := make([]uint64, mphfWorkerCount(len(keys))*collectBlock)
	var filled atomic.Int64
	forChunks(len(keys), scan, func(c, lo, hi int) {
		buf := bufs[c*collectBlock : (c+1)*collectBlock]
		for from := lo; from < hi; from += collectBlock {
			n := 0
			for _, k := range keys[from:min(from+collectBlock, hi)] {
				i := bbPosition(k, level, words)
				buf[n] = k
				n += int(^placed[i/64] >> (i % 64) & 1)
			}
			at := int(filled.Add(int64(n))) - n
			copy(next[at:at+n], buf[:n])
			if marks != nil {
				markKeys(buf[:n], level+1, marks)
			}
		}
	})
}

// forChunks splits [0, n) into mphfWorkerCount(n) contiguous chunks and
// runs them concurrently. Each chunk's worker calls fn on the chunk's
// blocks of up to progressBlock items, in order, and counts each block
// on progress.
func forChunks(n int, progress *stepCounter, fn func(chunk, lo, hi int)) {
	workers := mphfWorkerCount(n)
	size := (n + workers - 1) / workers
	var wg sync.WaitGroup
	for c := range workers {
		lo, hi := c*size, min((c+1)*size, n)
		if lo >= hi {
			continue
		}
		wg.Go(func() {
			// A new variable, not lo: a closure that advanced lo would
			// move it to the heap, one allocation per worker and pass.
			for from := lo; from < hi; from += progressBlock {
				to := min(from+progressBlock, hi)
				fn(c, from, to)
				progress.add(to - from)
			}
		})
	}
	wg.Wait()
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

// find returns the slot in [0, placed) of a key the levels placed. A
// fallback key misses here; a key outside the build set either misses
// or lands on some other key's slot.
func (h *bbhash) find(key uint64) (uint64, bool) {
	for level, lv := range h.levels {
		i := lv.offset*64 + bbPosition(key, level, lv.words)
		if h.bits[i/64]&(1<<(i%64)) != 0 {
			return h.rank(i), true
		}
	}

	return 0, false
}

// findFallback returns the slot of the fallback entry (key, fp).
func (h *bbhash) findFallback(key, fp uint64) (uint64, bool) {
	j, ok := slices.BinarySearchFunc(h.fallback, bbFallback{hash: key, fp: fp}, compareFallback)
	if !ok {
		return 0, false
	}

	return h.placed + uint64(j), true
}

// rank counts the set bits before bit i of bits: the directory's count
// for i's block, the bits below i in its word, and the popcounts of the
// block's words before that word. It popcounts all the block's words,
// masking off those from i's word on, so the loop always runs
// rankBlockWords times and never mispredicts where i falls. The words
// past the end of bits in the last block are within its capacity and
// masked off.
func (h *bbhash) rank(i uint64) uint64 {
	block := i / rankBlockBits
	words := (*[rankBlockWords]uint64)(h.bits[block*rankBlockWords : (block+1)*rankBlockWords])
	at := i / 64 % rankBlockWords
	r := h.ranks[block] + uint64(bits.OnesCount64(words[at]&(1<<(i%64)-1)))
	// Bit j is set for each word j before i's word.
	before := uint64(1)<<at - 1
	for j, w := range words {
		r += uint64(bits.OnesCount64(w & -(before >> j & 1)))
	}

	return r
}

// buildRanks fills the rank directory and the key counts, and gives
// bits the capacity rank reads.
func (h *bbhash) buildRanks() {
	h.bits = slices.Grow(h.bits, rankBlockWords-1)
	h.ranks = make([]uint64, (len(h.bits)+rankBlockWords-1)/rankBlockWords)
	var r uint64
	for i, w := range h.bits {
		if i%rankBlockWords == 0 {
			h.ranks[i/rankBlockWords] = r
		}
		r += uint64(bits.OnesCount64(w))
	}
	h.placed = r
	h.count = r + uint64(len(h.fallback))
}

// writeFile stores the table as mph.bin: the level count, each level's
// word count, every level's words, then the fallback entry count and
// each entry's hash and fingerprint, all little-endian uint64. An
// entry's slot is implied by its index. The rank directory is rebuilt
// on load.
func (h *bbhash) writeFile(path string) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create MPHF file: %w", err)
	}
	if err := h.writeTo(f); err != nil {
		return errors.Join(fmt.Errorf("write MPHF file: %w", err), f.Close(), os.Remove(path))
	}
	startWriteback(f)
	if err := f.Close(); err != nil {
		return fmt.Errorf("close MPHF file: %w", err)
	}

	return nil
}

// writeTo encodes the table into dst, stopping at the first failed
// write: a bufio.Writer error is sticky, so every later word would fail
// too.
func (h *bbhash) writeTo(dst io.Writer) error {
	w := bufio.NewWriterSize(dst, bbhashIOBuffer)
	if err := putU64(w, uint64(len(h.levels))); err != nil {
		return err
	}
	for _, lv := range h.levels {
		if err := putU64(w, lv.words); err != nil {
			return err
		}
	}
	for _, word := range h.bits {
		if err := putU64(w, word); err != nil {
			return err
		}
	}
	if err := putU64(w, uint64(len(h.fallback))); err != nil {
		return err
	}
	for _, e := range h.fallback {
		if err := putU64(w, e.hash); err != nil {
			return err
		}
		if err := putU64(w, e.fp); err != nil {
			return err
		}
	}
	if err := w.Flush(); err != nil {
		return fmt.Errorf("flush: %w", err)
	}

	return nil
}

// readBBHash loads a table written by writeFile.
func readBBHash(path string) (*bbhash, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open MPHF file: %w", err)
	}
	defer f.Close()
	r := bufio.NewReaderSize(f, bbhashIOBuffer)

	var word [8]byte
	readU64 := func() (uint64, error) {
		if _, err := io.ReadFull(r, word[:]); err != nil {
			return 0, fmt.Errorf("%w: %w", errBBHashCorrupt, err)
		}

		return binary.LittleEndian.Uint64(word[:]), nil
	}
	numLevels, err := readU64()
	if err != nil {
		return nil, err
	}
	if numLevels > maxBBHashLevels {
		return nil, fmt.Errorf("%w: %d levels", errBBHashCorrupt, numLevels)
	}
	info, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat MPHF file: %w", err)
	}
	size := uint64(info.Size())
	h := &bbhash{levels: make([]bbLevel, numLevels)}
	var total uint64
	for i := range h.levels {
		words, err := readU64()
		if err != nil {
			return nil, err
		}
		h.levels[i] = bbLevel{offset: total, words: words}
		total += words
		if words == 0 || total > size/8 {
			return nil, fmt.Errorf("%w: level %d has %d words", errBBHashCorrupt, i, words)
		}
	}
	bitsEnd := 8 * (1 + numLevels + total)
	if size < bitsEnd+8 {
		return nil, fmt.Errorf("%w: %d bytes, want at least %d", errBBHashCorrupt, size, bitsEnd+8)
	}

	// Read straight into the words' memory, then decode in place: a
	// no-op on little-endian hosts and no second copy of the table.
	h.bits = make([]uint64, total, total+rankBlockWords-1)
	raw := unsafe.Slice((*byte)(unsafe.Pointer(unsafe.SliceData(h.bits))), len(h.bits)*8)
	if _, err := io.ReadFull(r, raw); err != nil {
		return nil, fmt.Errorf("%w: %w", errBBHashCorrupt, err)
	}
	for i := range h.bits {
		h.bits[i] = binary.LittleEndian.Uint64(raw[i*8:])
	}
	if err := h.readFallback(readU64, size-bitsEnd-8); err != nil {
		return nil, err
	}
	if numLevels == 0 && len(h.fallback) == 0 {
		return nil, fmt.Errorf("%w: no keys", errBBHashCorrupt)
	}
	h.buildRanks()

	return h, nil
}

// readFallback reads the fallback entry count and the entries. They
// must fill the file's remaining rest bytes exactly and be strictly
// sorted, since lookups binary-search them.
func (h *bbhash) readFallback(readU64 func() (uint64, error), rest uint64) error {
	n, err := readU64()
	if err != nil {
		return err
	}
	if n > rest/16 || rest != 16*n {
		return fmt.Errorf("%w: %d fallback entries in %d bytes", errBBHashCorrupt, n, rest)
	}
	h.fallback = make([]bbFallback, n)
	for j := range h.fallback {
		e := &h.fallback[j]
		if e.hash, err = readU64(); err != nil {
			return err
		}
		if e.fp, err = readU64(); err != nil {
			return err
		}
		if j > 0 && compareFallback(h.fallback[j-1], *e) >= 0 {
			return fmt.Errorf("%w: fallback entry %d out of order", errBBHashCorrupt, j)
		}
	}

	return nil
}
