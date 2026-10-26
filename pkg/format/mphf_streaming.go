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
	"unsafe"

	"github.com/eunmann/s3-inv-db/pkg/logging"
	"golang.org/x/sys/unix"
)

// StreamingMPHFBuilder builds a minimal perfect hash function for prefix
// strings with memory bounded whatever the prefix count: each prefix's
// key hash and fingerprint go to disk-backed arrays, 8 bytes apiece, and
// the prefix strings themselves are not kept. A prefix's position is the
// order it was added in: the index builder adds them in level order.
type StreamingMPHFBuilder struct {
	// Disk-backed: hashes, fingerprints. At billion-prefix scale these
	// would otherwise be ~16 GiB of heap. Backed by mmap'd
	// files in tempDir; bbhash receives the mmap'd slice directly via
	// unsafe.Slice so there's no copy to heap during construction.
	hashes       *u64DiskArray
	fingerprints *u64DiskArray
	tempDir      string

	// keyHash and fingerprint are hashString and computeFingerprint;
	// tests replace them to force collisions.
	keyHash     func(prefix string) uint64
	fingerprint func(prefix string) uint64

	// progress, when set, receives the build's steps (see SetProgress).
	progress StepProgress

	count uint64
	built bool
}

// The MPHF build's steps, as SetProgress's callback names them.
const (
	// StepBuildHash builds the hash levels; it counts key reads against
	// an estimate (bbhashScanEstimate).
	StepBuildHash = "Building the prefix hash"
	// StepPlaceSlots looks up every prefix's slot; it counts prefixes.
	StepPlaceSlots = "Placing hash slots"
	// StepSlotFile writes mph_slots.bin in slot order; it counts slots.
	StepSlotFile = "Writing hash slots"
)

// SetProgress makes Build report its steps to fn. Call it before Build.
func (b *StreamingMPHFBuilder) SetProgress(fn StepProgress) { b.progress = fn }

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
	fingerprints, err := newU64DiskArray(tempDir, "mphf_fingerprints")
	if err != nil {
		return nil, errors.Join(err, hashes.Close())
	}

	return &StreamingMPHFBuilder{
		hashes:       hashes,
		fingerprints: fingerprints,
		tempDir:      tempDir,
		keyHash:      hashString,
		fingerprint:  computeFingerprint,
	}, nil
}

// Add adds the next prefix; its position is the number of prefixes
// added before it. Its hash and fingerprint are appended to disk-backed
// arrays rather than heap slices, so memory usage during Add stays flat
// regardless of prefix count.
func (b *StreamingMPHFBuilder) Add(prefix string) error {
	return b.AddHashed(b.KeyHashes(prefix))
}

// KeyHashes returns the key hash and fingerprint Add stores for prefix,
// for a caller that hashes prefixes before it knows their positions.
//
//nolint:gocritic // gocritic wants named returns, nonamedreturns forbids them
func (b *StreamingMPHFBuilder) KeyHashes(prefix string) (uint64, uint64) {
	return b.keyHash(prefix), b.fingerprint(prefix)
}

// AddHashed adds the next prefix by the key hash and fingerprint
// KeyHashes returned for it.
func (b *StreamingMPHFBuilder) AddHashed(keyHash, fingerprint uint64) error {
	if err := b.hashes.Append(keyHash); err != nil {
		return fmt.Errorf("append hash: %w", err)
	}
	if err := b.fingerprints.Append(fingerprint); err != nil {
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
	for _, a := range []*u64DiskArray{b.hashes, b.fingerprints} {
		if a == nil {
			continue
		}
		if err := a.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	b.hashes = nil
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
	if err := b.fingerprints.Freeze(); err != nil {
		return fmt.Errorf("freeze fingerprints: %w", err)
	}

	n := int(b.count)
	bbhashStart := time.Now()
	scan := newStepCounter(b.progress, StepBuildHash, bbhashScanEstimate(n))
	mph, err := b.buildBBHash(scan)
	if err != nil {
		return err
	}
	scan.finish()
	log.Debug().Dur("bbhash_ms", time.Since(bbhashStart)).Msg("MPHF: bbhash built")

	if err := mph.writeFile(filepath.Join(outDir, MPHFile)); err != nil {
		return err
	}

	mapStart := time.Now()
	if err := writeSlots(outDir, b.tempDir, n, mph, b.hashes.Slice(), b.fingerprints.Slice(), b.progress); err != nil {
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

	log.Debug().Msg("MPHF: build complete")

	return nil
}

// buildBBHash builds the MPHF over the frozen key hashes, straight from
// their disk-backed array: no reverse map and no sorted copy, so the
// finalize holds little beyond bbhash's own bit vectors. Keys the levels
// leave unplaced, chiefly prefixes that share a 64-bit key hash, go to
// the fallback table.
func (b *StreamingMPHFBuilder) buildBBHash(scan *stepCounter) (*bbhash, error) {
	hashes, fps := b.hashes.Slice(), b.fingerprints.Slice()
	mph, err := newBBHashWith(hashes, b.scratchKeys, func(leftovers []uint64) ([]bbFallback, error) {
		return fallbackEntries(leftovers, hashes, fps)
	}, scan)
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

// scratchKeysMinBytes is the smallest level key buffer kept in a
// scratch file rather than on the heap.
const scratchKeysMinBytes = 1 << 20

// scratchKeys is the bbhash build's keyBuffer: the keys that collide on
// a level (~39% of the level's) go to an mmapped scratch file in the
// build's temp dir, so the build's heap doesn't grow with the prefix
// count; small levels stay on the heap.
//
//nolint:gocritic // gocritic wants named returns, nonamedreturns forbids them
func (b *StreamingMPHFBuilder) scratchKeys(n int) ([]uint64, func() error, error) {
	if n*8 < scratchKeysMinBytes {
		return heapKeys(n)
	}
	m, err := mapScratchFile(b.tempDir, "mphf_level_keys_*.tmp", n*8)
	if err != nil {
		return nil, nil, fmt.Errorf("level keys: %w", err)
	}

	keys := unsafe.Slice((*uint64)(unsafe.Pointer(unsafe.SliceData(m.data))), n)

	return keys, m.close, nil
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
	forChunks(len(hashes), nil, func(c, lo, hi int) {
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
// at the last slot. Progress, when non-nil, receives the StepPlaceSlots
// and StepSlotFile steps.
func writeSlots(outDir, tempDir string, n int, mph *bbhash, hashes, fps []uint64, progress StepProgress) error {
	if len(fps) != n {
		return fmt.Errorf("%w: fps=%d n=%d", errMPHFArrayLengthMismatch, len(fps), n)
	}
	// Prefix i is at position i, so positions run to n-1.
	posW := int(byteWidthOf(uint64(max(n, 1) - 1)))
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
		placed := newStepCounter(progress, StepPlaceSlots, int64(n))
		owners, err := newSlotOwners(tempDir, mph, hashes, fps, placed)
		if err != nil {
			return cleanup(err)
		}
		placed.finish()
		written := newStepCounter(progress, StepSlotFile, int64(n))
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
			wg.Go(func() { errs[w] = writeSlotRange(f, owners, fps, posW, start, end, written) })
		}
		wg.Wait()
		if err := errors.Join(errors.Join(errs...), owners.close()); err != nil {
			return cleanup(err)
		}
		written.finish()
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
// every slot is written by exactly one lookup. Placed counts the
// prefixes looked up.
func newSlotOwners(tempDir string, mph *bbhash, hashes, fps []uint64, placed *stepCounter) (*slotOwners, error) {
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
				if (i-start+1)%progressBlock == 0 {
					placed.add(progressBlock)
				}
			}
			placed.add((end - start) % progressBlock)
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
	return o.scratchMapping.close()
}

// close unmaps the scratch file and removes it.
func (m *scratchMapping) close() error {
	var err error
	if m.data != nil {
		err = unix.Munmap(m.data)
		m.data = nil
	}

	return errors.Join(err, removeIfErr(m.path))
}

// writeSlotRange writes slots [start, end) of the slot file, counting
// them on written.
func writeSlotRange(f *os.File, owners *slotOwners, fps []uint64, posW, start, end int, written *stepCounter) error {
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
			binary.LittleEndian.PutUint64(buf[off+mphfFingerprintBytes:], i)
		}
		blen := (cEnd - chunk) * slotW
		fileOff := int64(mphfSlotsDataOff) + int64(chunk)*int64(slotW)
		if _, err := f.WriteAt(buf[:blen], fileOff); err != nil {
			return fmt.Errorf("write MPHF slot %d: %w", chunk, err)
		}
		written.add(cEnd - chunk)
	}

	return nil
}

// errMPHFArrayLengthMismatch fires when the writer's fingerprints and
// the prefix count differ — a programming bug.
var errMPHFArrayLengthMismatch = errors.New("mphf fingerprints/positions length mismatch")

// MPHFSlotsFile maps each MPHF slot to its prefix's fingerprint and
// position.
const MPHFSlotsFile = "mph_slots.bin"

// mph_slots.bin layout constants (see writeSlots).
const (
	mphfSlotsDescBytes   = 1
	mphfSlotsDataOff     = HeaderSize + mphfSlotsDescBytes
	mphfFingerprintBytes = 8
	mphfSlotsTailPad     = 8
)
