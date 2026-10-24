package format

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"math/bits"
	"os"
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
type bbhash struct {
	bits   []uint64 // every level's bit vector, back to back
	ranks  []uint64 // ranks[b] = set bits in bits[:b*rankBlockWords]
	levels []bbLevel
	count  uint64 // keys placed: every set bit
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
	// MaxBBHashLevels bounds the build. At gamma 2 about 39% of a
	// level's keys collide, so distinct keys are placed within ~30
	// levels even at 2^40 keys; keys still unplaced here are duplicates.
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

var (
	// ErrBBHashUnplaced means some keys collided on every level, which
	// in practice means they are equal.
	errBBHashUnplaced = errors.New("bbhash: keys left unplaced")
	// ErrBBHashCorrupt means mph.bin does not describe a valid table.
	errBBHashCorrupt = errors.New("bbhash: corrupt MPHF file")
)

// newBBHash builds the table over keys, which must be distinct. It
// reads keys in place (they may be an mmap) and copies only the keys
// that collide on a level into the next one.
func newBBHash(keys []uint64) (*bbhash, error) {
	firstWords := levelWords(len(keys))
	// The levels after the first hold ~65% as many bits again.
	h := &bbhash{bits: make([]uint64, 0, firstWords*7/4+maxBBHashLevels)}
	for level := 0; len(keys) > 0; level++ {
		if level == maxBBHashLevels {
			return nil, fmt.Errorf("%w: %d keys after %d levels", errBBHashUnplaced, len(keys), level)
		}
		words := levelWords(len(keys))
		set, collided := make([]uint64, words), make([]uint64, words)
		markLevel(keys, level, set, collided)
		for w := range set {
			set[w] &^= collided[w]
		}
		keys = collidedKeys(keys, level, words, collided)
		h.levels = append(h.levels, bbLevel{offset: uint64(len(h.bits)), words: words})
		h.bits = append(h.bits, set...)
	}
	h.buildRanks()

	return h, nil
}

// levelWords sizes a level for n keys: gamma bits per key, at least one word.
func levelWords(n int) uint64 {
	return max(1, uint64(math.Ceil(bbhashGamma*float64(n)/64)))
}

// markLevel sets each key's bit in set, and in collided when another
// key already set it. Atomic ORs make the result independent of the
// order the workers run in.
func markLevel(keys []uint64, level int, set, collided []uint64) {
	words := uint64(len(set))
	forChunks(len(keys), func(_, lo, hi int) {
		for _, k := range keys[lo:hi] {
			i := bbPosition(k, level, words)
			w, bit := i/64, uint64(1)<<(i%64)
			if atomic.OrUint64(&set[w], bit)&bit != 0 {
				atomic.OrUint64(&collided[w], bit)
			}
		}
	})
}

// collidedKeys returns the keys whose bit collided, in input order:
// one pass counts each chunk's share, a second copies into place.
func collidedKeys(keys []uint64, level int, words uint64, collided []uint64) []uint64 {
	isCollided := func(k uint64) bool {
		i := bbPosition(k, level, words)

		return collided[i/64]&(1<<(i%64)) != 0
	}
	counts := make([]int, mphfWorkerCount(len(keys)))
	forChunks(len(keys), func(c, lo, hi int) {
		for _, k := range keys[lo:hi] {
			if isCollided(k) {
				counts[c]++
			}
		}
	})
	total := 0
	for c, n := range counts {
		counts[c] = total
		total += n
	}
	next := make([]uint64, total)
	forChunks(len(keys), func(c, lo, hi int) {
		at := counts[c]
		for _, k := range keys[lo:hi] {
			if isCollided(k) {
				next[at] = k
				at++
			}
		}
	})

	return next
}

// forChunks splits [0, n) into mphfWorkerCount(n) contiguous chunks and
// runs fn on each concurrently.
func forChunks(n int, fn func(chunk, lo, hi int)) {
	workers := mphfWorkerCount(n)
	size := (n + workers - 1) / workers
	var wg sync.WaitGroup
	for c := range workers {
		lo, hi := c*size, min((c+1)*size, n)
		if lo >= hi {
			continue
		}
		wg.Go(func() { fn(c, lo, hi) })
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

// find returns key's slot in [0, count) and true. A key outside the
// build set either misses (false) or lands on some other key's slot.
func (h *bbhash) find(key uint64) (uint64, bool) {
	for level, lv := range h.levels {
		i := bbPosition(key, level, lv.words)
		w, bit := lv.offset+i/64, uint64(1)<<(i%64)
		if h.bits[w]&bit != 0 {
			return h.rank(w, bit), true
		}
	}

	return 0, false
}

// rank counts the set bits before bit in word w.
func (h *bbhash) rank(w, bit uint64) uint64 {
	block := w / rankBlockWords
	r := h.ranks[block]
	for j := block * rankBlockWords; j < w; j++ {
		r += uint64(bits.OnesCount64(h.bits[j]))
	}

	return r + uint64(bits.OnesCount64(h.bits[w]&(bit-1)))
}

// buildRanks fills the rank directory and the key count.
func (h *bbhash) buildRanks() {
	h.ranks = make([]uint64, (len(h.bits)+rankBlockWords-1)/rankBlockWords)
	var r uint64
	for i, w := range h.bits {
		if i%rankBlockWords == 0 {
			h.ranks[i/rankBlockWords] = r
		}
		r += uint64(bits.OnesCount64(w))
	}
	h.count = r
}

// writeFile stores the table as mph.bin: the level count, each level's
// word count, then every level's words, all little-endian uint64. The
// rank directory is rebuilt on load.
func (h *bbhash) writeFile(path string) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create MPHF file: %w", err)
	}
	if err := h.writeTo(f); err != nil {
		return errors.Join(fmt.Errorf("write MPHF file: %w", err), f.Close(), os.Remove(path))
	}
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
	if numLevels == 0 || numLevels > maxBBHashLevels {
		return nil, fmt.Errorf("%w: %d levels", errBBHashCorrupt, numLevels)
	}
	info, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat MPHF file: %w", err)
	}
	h := &bbhash{levels: make([]bbLevel, numLevels)}
	var total uint64
	for i := range h.levels {
		words, err := readU64()
		if err != nil {
			return nil, err
		}
		h.levels[i] = bbLevel{offset: total, words: words}
		total += words
		if words == 0 || total > uint64(info.Size())/8 {
			return nil, fmt.Errorf("%w: level %d has %d words", errBBHashCorrupt, i, words)
		}
	}
	if want := 8 * (1 + numLevels + total); uint64(info.Size()) != want {
		return nil, fmt.Errorf("%w: %d bytes, want %d", errBBHashCorrupt, info.Size(), want)
	}

	// Read straight into the words' memory, then decode in place: a
	// no-op on little-endian hosts and no second copy of the table.
	h.bits = make([]uint64, total)
	raw := unsafe.Slice((*byte)(unsafe.Pointer(unsafe.SliceData(h.bits))), len(h.bits)*8)
	if _, err := io.ReadFull(r, raw); err != nil {
		return nil, fmt.Errorf("%w: %w", errBBHashCorrupt, err)
	}
	for i := range h.bits {
		h.bits[i] = binary.LittleEndian.Uint64(raw[i*8:])
	}
	h.buildRanks()

	return h, nil
}
