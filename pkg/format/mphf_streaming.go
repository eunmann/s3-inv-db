package format

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sync"
	"time"

	"github.com/eunmann/s3-inv-db/pkg/logging"
	"golang.org/x/sys/unix"
)

// StreamingMPHFBuilder builds a minimal perfect hash function for prefix
// strings with memory bounded whatever the prefix count: each prefix's
// key hash, preorder position and fingerprint go to disk-backed arrays,
// 8 bytes apiece, and the prefix strings themselves are not kept.
type StreamingMPHFBuilder struct {
	// Disk-backed: hashes, positions, fingerprints. At billion-prefix
	// scale these would otherwise be ~24 GiB of heap. Backed by mmap'd
	// files in tempDir; bbhash receives the mmap'd slice directly via
	// unsafe.Slice so there's no copy to heap during construction.
	hashes       *u64DiskArray
	preorderPos  *u64DiskArray
	fingerprints *u64DiskArray
	tempDir      string

	// keyHash and fingerprint are hashString and computeFingerprint;
	// tests replace them to force collisions.
	keyHash     func(prefix string) uint64
	fingerprint func(prefix string) uint64

	count uint64
	built bool
}

// ErrMPHFAlreadyBuilt is returned when Build is called more than
// once on the same StreamingMPHFBuilder. The first call consumes the
// disk-backed arrays, so a second would nil-deref on them. Surfaced as
// a named error so callers can distinguish double-Build from other
// Build failures.
var ErrMPHFAlreadyBuilt = errors.New("StreamingMPHFBuilder.Build already called")

// NewStreamingMPHFBuilder creates a new streaming MPHF builder. TempDir
// holds the disk-backed u64 arrays.
func NewStreamingMPHFBuilder(tempDir string) (*StreamingMPHFBuilder, error) {
	hashes, err := newU64DiskArray(tempDir, "mphf_hashes")
	if err != nil {
		return nil, err
	}
	preorderPos, err := newU64DiskArray(tempDir, "mphf_preorderpos")
	if err != nil {
		return nil, errors.Join(err, hashes.Close())
	}
	fingerprints, err := newU64DiskArray(tempDir, "mphf_fingerprints")
	if err != nil {
		return nil, errors.Join(err, preorderPos.Close(), hashes.Close())
	}

	return &StreamingMPHFBuilder{
		hashes:       hashes,
		preorderPos:  preorderPos,
		fingerprints: fingerprints,
		tempDir:      tempDir,
		keyHash:      hashString,
		fingerprint:  computeFingerprint,
	}, nil
}

// Add adds a prefix at the given preorder position. All three
// per-prefix uint64 values (hash / preorderPos / fingerprint) are
// appended to disk-backed arrays rather than heap slices, so memory
// usage during Add stays flat regardless of prefix count.
func (b *StreamingMPHFBuilder) Add(prefix string, pos uint64) error {
	if err := b.hashes.Append(b.keyHash(prefix)); err != nil {
		return fmt.Errorf("append hash: %w", err)
	}
	if err := b.preorderPos.Append(pos); err != nil {
		return fmt.Errorf("append preorder pos: %w", err)
	}
	if err := b.fingerprints.Append(b.fingerprint(prefix)); err != nil {
		return fmt.Errorf("append fingerprint: %w", err)
	}
	b.count++

	return nil
}

// Count returns the number of prefixes added.
func (b *StreamingMPHFBuilder) Count() uint64 {
	return b.count
}

// Close closes the builder and removes its temporary files, joining
// every array's cleanup error so a failed close or removal (typical on
// a full disk mid-build) still surfaces.
func (b *StreamingMPHFBuilder) Close() error {
	var errs []error
	// Disk-backed arrays may have been already closed during a
	// successful Build; nil-guard so Close stays idempotent.
	for _, a := range []*u64DiskArray{b.hashes, b.preorderPos, b.fingerprints} {
		if a == nil {
			continue
		}
		if err := a.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	b.hashes = nil
	b.preorderPos = nil
	b.fingerprints = nil

	return errors.Join(errs...)
}

// Build's memory usage is bounded by buffer sizes, not by prefix count.
func (b *StreamingMPHFBuilder) Build(outDir string) error {
	if b.built {
		return ErrMPHFAlreadyBuilt
	}
	b.built = true
	log := logging.L()

	if b.count == 0 {
		log.Debug().Msg("MPHF: writing empty (zero prefixes)")

		return b.writeEmpty(outDir)
	}

	// Freeze the disk-backed arrays so their backing files are mmap'd
	// and exposable as []uint64. bbhash reads the slice; the values
	// live in the page cache, not the Go heap.
	if err := b.hashes.Freeze(); err != nil {
		return fmt.Errorf("freeze hashes: %w", err)
	}
	if err := b.preorderPos.Freeze(); err != nil {
		return fmt.Errorf("freeze preorderPos: %w", err)
	}
	if err := b.fingerprints.Freeze(); err != nil {
		return fmt.Errorf("freeze fingerprints: %w", err)
	}

	n := int(b.count)
	bbhashStart := time.Now()
	mph, err := b.buildBBHash()
	if err != nil {
		return err
	}
	log.Debug().Dur("bbhash_ms", time.Since(bbhashStart)).Msg("MPHF: bbhash built")

	if err := mph.writeFile(filepath.Join(outDir, MPHFile)); err != nil {
		return err
	}

	mapStart := time.Now()
	if err := writeSlots(outDir, b.tempDir, n, mph, b.hashes.Slice(), b.fingerprints.Slice(), b.preorderPos.Slice()); err != nil {
		return err
	}
	log.Debug().Dur("write_ms", time.Since(mapStart)).Msg("MPHF: slots written")

	if err := b.hashes.Close(); err != nil {
		return fmt.Errorf("close hashes: %w", err)
	}
	b.hashes = nil
	if err := b.fingerprints.Close(); err != nil {
		return fmt.Errorf("close fingerprints: %w", err)
	}
	b.fingerprints = nil
	if err := b.preorderPos.Close(); err != nil {
		return fmt.Errorf("close preorderPos: %w", err)
	}
	b.preorderPos = nil

	log.Debug().Msg("MPHF: build complete")

	return nil
}

// buildBBHash builds the MPHF over the frozen key hashes, straight from
// their disk-backed array: no reverse map and no sorted copy, so the
// finalize holds little beyond bbhash's own bit vectors. Keys the levels
// leave unplaced, chiefly prefixes that share a 64-bit key hash, go to
// the fallback table.
func (b *StreamingMPHFBuilder) buildBBHash() (*bbhash, error) {
	hashes, fps := b.hashes.Slice(), b.fingerprints.Slice()
	mph, err := newBBHash(hashes, func(leftovers []uint64) ([]bbFallback, error) {
		return fallbackEntries(leftovers, hashes, fps)
	})
	if err != nil {
		return nil, err
	}
	if len(mph.fallback) > 0 {
		logging.L().Debug().Int("fallback_keys", len(mph.fallback)).Msg("MPHF: keys in the fallback table")
	}
	if mph.count != uint64(len(hashes)) {
		return nil, fmt.Errorf("%w: %d keys in the levels and %d in the fallback, %d added",
			ErrMPHFLookupFailed, mph.placed, len(mph.fallback), len(hashes))
	}

	return mph, nil
}

// fallbackEntries returns the (hash, fingerprint) of every key whose
// hash is a leftover, sorted, from one parallel pass over the key
// arrays. Every key with a leftover hash is unplaced: keys sharing a
// hash take the same position on every level. Two keys equal in both
// hashes cannot be told apart by any lookup, so they fail the build.
func fallbackEntries(leftovers, hashes, fps []uint64) ([]bbFallback, error) {
	slices.Sort(leftovers)
	leftovers = slices.Compact(leftovers)
	chunks := make([][]bbFallback, mphfWorkerCount(len(hashes)))
	forChunks(len(hashes), func(c, lo, hi int) {
		for i := lo; i < hi; i++ {
			if _, found := slices.BinarySearch(leftovers, hashes[i]); found {
				chunks[c] = append(chunks[c], bbFallback{hash: hashes[i], fp: fps[i]})
			}
		}
	})
	entries := slices.Concat(chunks...)
	slices.SortFunc(entries, compareFallback)
	for j := 1; j < len(entries); j++ {
		if entries[j] == entries[j-1] {
			return nil, fmt.Errorf("%w: two prefixes share key hash %#x and fingerprint %#x",
				ErrMPHFKeyCollision, entries[j].hash, entries[j].fp)
		}
	}

	return entries, nil
}

// mphfWorkerCount caps parallelism at the smaller of NumCPU and the
// per-worker amortised cost threshold — there's no point spinning up
// 64 workers for 10k prefixes. Floor of 1 keeps callers off zero-divide.
func mphfWorkerCount(n int) int {
	const minPerWorker = 4096
	w := runtime.NumCPU()
	if maxFromWork := n / minPerWorker; maxFromWork < w {
		w = maxFromWork
	}
	if w < 1 {
		w = 1
	}

	return w
}

func (b *StreamingMPHFBuilder) writeEmpty(outDir string) error {
	// Create empty mph file
	mphPath := filepath.Join(outDir, MPHFile)
	if err := os.WriteFile(mphPath, nil, FilePerm); err != nil {
		return fmt.Errorf("write empty mph: %w", err)
	}

	// An empty slot file keeps the index's file set the same at any size.
	return writeSlots(outDir, b.tempDir, 0, nil, nil, nil, nil)
}

// writeSlots materialises mph_slots.bin: it maps every slot to its
// prefix (slotOwners), then each worker fills a 1 MiB buffer in slot
// order and pwrites it to a disjoint range of the output file.
// Sequential writes touch each page once; scattered writes indexed by
// hash position measured ~3x write amplification on EBS gp3.
//
// File layout: Header (Count = slots, Width = slot bytes), a 1-byte
// descriptor (position width), the slots (8-byte fingerprint, then the
// position at that width), and an 8-byte zero tail pad for masked loads
// at the last slot.
func writeSlots(outDir, tempDir string, n int, mph *bbhash, hashes, fps, poss []uint64) error {
	if len(poss) != n || len(fps) != n {
		return fmt.Errorf("%w: fps=%d poss=%d n=%d", errMPHFArrayLengthMismatch, len(fps), len(poss), n)
	}
	var maxPos uint64
	if n > 0 {
		maxPos = slices.Max(poss)
	}
	posW := int(byteWidthOf(maxPos))
	slotW := mphfFingerprintBytes + posW
	path := filepath.Join(outDir, MPHFSlotsFile)
	dataBytes := int64(n) * int64(slotW)
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create MPHF slots: %w", err)
	}
	cleanup := func(primary error) error {
		return errors.Join(primary, f.Close(), removeIfErr(path))
	}
	if err := f.Truncate(int64(mphfSlotsDataOff) + dataBytes + mphfSlotsTailPad); err != nil {
		return cleanup(fmt.Errorf("truncate MPHF slots: %w", err))
	}
	head := EncodeHeader(Header{Magic: MagicNumber, Version: Version, Count: uint64(n), Width: uint32(slotW)})
	if _, err := f.Write(append(head, byte(posW))); err != nil {
		return cleanup(fmt.Errorf("write MPHF slots header: %w", err))
	}

	if n > 0 {
		// Concurrent pwrite to disjoint ranges is POSIX-safe; each worker
		// writes its own [start,end) slot range.
		owners, err := newSlotOwners(tempDir, mph, hashes, fps)
		if err != nil {
			return cleanup(err)
		}
		numWorkers := max(min(runtime.NumCPU(), n), 1)
		slotsPerWorker := (n + numWorkers - 1) / numWorkers
		errs := make([]error, numWorkers)
		var wg sync.WaitGroup
		for w := range numWorkers {
			start := w * slotsPerWorker
			end := min(start+slotsPerWorker, n)
			if start >= end {
				continue
			}
			wg.Go(func() { errs[w] = writeSlotRange(f, owners, fps, poss, posW, start, end) })
		}
		wg.Wait()
		if err := errors.Join(errors.Join(errs...), owners.close()); err != nil {
			return cleanup(err)
		}
	}
	if err := f.Sync(); err != nil {
		return cleanup(fmt.Errorf("sync MPHF slots: %w", err))
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close MPHF slots: %w", err)
	}

	return nil
}

// slotOwners maps every MPHF slot to the index of the prefix whose key
// hash lands there, so the slot file can be written in slot order. It
// lives in an mmapped scratch file in the build's temp dir, not the
// heap: at a billion prefixes it is gigabytes, which the page cache can
// hold or evict under the build's memory limit. Entries are 4 bytes up
// to 2^32 prefixes, 8 beyond.
type slotOwners struct {
	scratchMapping

	width int
}

// scratchMapping is a writable mapping of a scratch file.
type scratchMapping struct {
	path string
	data []byte
}

// newSlotOwners fills the map with parallel bbhash lookups, through the
// fallback table for keys the levels miss. The MPHF is a bijection, so
// every slot is written by exactly one lookup.
func newSlotOwners(tempDir string, mph *bbhash, hashes, fps []uint64) (*slotOwners, error) {
	n := len(hashes)
	o := &slotOwners{width: 4}
	if uint64(n) > math.MaxUint32 {
		o.width = 8
	}
	var err error
	o.scratchMapping, err = mapScratchFile(tempDir, "mphf_slot_owners_*.tmp", n*o.width)
	if err != nil {
		return nil, fmt.Errorf("slot owners: %w", err)
	}

	numWorkers := mphfWorkerCount(n)
	chunk := (n + numWorkers - 1) / numWorkers
	errs := make([]error, numWorkers)
	var wg sync.WaitGroup
	for w := range numWorkers {
		start, end := w*chunk, min((w+1)*chunk, n)
		if start >= end {
			continue
		}
		wg.Go(func() {
			for i := start; i < end; i++ {
				slot, ok := mph.find(hashes[i])
				if !ok {
					slot, ok = mph.findFallback(hashes[i], fps[i])
				}
				if !ok || slot >= uint64(n) {
					errs[w] = fmt.Errorf("%w: slot %d (found %t) for prefix %d", ErrMPHFLookupFailed, slot, ok, i)

					return
				}
				o.set(int(slot), uint64(i))
			}
		})
	}
	wg.Wait()
	if err := errors.Join(errs...); err != nil {
		return nil, errors.Join(err, o.close())
	}

	return o, nil
}

// mapScratchFile creates a temp file of size bytes in dir and maps it
// writable. The file's blocks are reserved first: a write to an
// unbacked page of a sparse mapping on a full volume is a SIGBUS that
// kills the process, where a failed reservation is an error the build
// cleans up after.
func mapScratchFile(dir, pattern string, size int) (scratchMapping, error) {
	f, err := os.CreateTemp(dir, pattern)
	if err != nil {
		return scratchMapping{}, fmt.Errorf("create scratch: %w", err)
	}
	m := scratchMapping{path: f.Name()}
	err = preallocate(f, 0, int64(size))
	if err == nil {
		m.data, err = unix.Mmap(int(f.Fd()), 0, size, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return scratchMapping{}, errors.Join(fmt.Errorf("map scratch %s: %w", m.path, err), removeIfErr(m.path))
	}

	return m, nil
}

func (o *slotOwners) set(slot int, owner uint64) {
	if o.width == 4 {
		binary.LittleEndian.PutUint32(o.data[slot*4:], uint32(owner))

		return
	}
	binary.LittleEndian.PutUint64(o.data[slot*8:], owner)
}

func (o *slotOwners) get(slot int) uint64 {
	if o.width == 4 {
		return uint64(binary.LittleEndian.Uint32(o.data[slot*4:]))
	}

	return binary.LittleEndian.Uint64(o.data[slot*8:])
}

// close unmaps and removes the scratch file.
func (o *slotOwners) close() error {
	var err error
	if o.data != nil {
		err = unix.Munmap(o.data)
		o.data = nil
	}

	return errors.Join(err, removeIfErr(o.path))
}

// writeSlotRange writes slots [start, end) of the slot file.
func writeSlotRange(f *os.File, owners *slotOwners, fps, poss []uint64, posW, start, end int) error {
	slotW := mphfFingerprintBytes + posW
	const bufBytes = 1 << 20
	bufSlots := bufBytes / slotW
	buf := make([]byte, bufSlots*slotW+8)
	for chunk := start; chunk < end; chunk += bufSlots {
		cEnd := min(chunk+bufSlots, end)
		for j := chunk; j < cEnd; j++ {
			i := owners.get(j)
			off := (j - chunk) * slotW
			binary.LittleEndian.PutUint64(buf[off:], fps[i])
			// An 8-byte store, overwritten by the next slot: only posW
			// bytes are the slot's.
			binary.LittleEndian.PutUint64(buf[off+mphfFingerprintBytes:], poss[i])
		}
		blen := (cEnd - chunk) * slotW
		fileOff := int64(mphfSlotsDataOff) + int64(chunk)*int64(slotW)
		if _, err := f.WriteAt(buf[:blen], fileOff); err != nil {
			return fmt.Errorf("write MPHF slot %d: %w", chunk, err)
		}
	}

	return nil
}

// errMPHFArrayLengthMismatch fires when the writer's fingerprints and
// positions slices have differing lengths — a programming bug; both
// must come from the same parallel-lookup pass.
var errMPHFArrayLengthMismatch = errors.New("mphf fingerprints/positions length mismatch")

// MPHFSlotsFile maps each MPHF slot to its prefix's fingerprint and
// preorder position.
const MPHFSlotsFile = "mph_slots.bin"

// mph_slots.bin layout constants (see writeSlots).
const (
	mphfSlotsDescBytes   = 1
	mphfSlotsDataOff     = HeaderSize + mphfSlotsDescBytes
	mphfFingerprintBytes = 8
	mphfSlotsTailPad     = 8
)
