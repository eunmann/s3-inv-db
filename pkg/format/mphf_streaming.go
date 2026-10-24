package format

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sync"
	"time"
	"unsafe"

	"github.com/eunmann/s3-inv-db/pkg/logging"
	"github.com/klauspost/compress/zstd"
)

// StreamingMPHFBuilder builds a minimal perfect hash function for prefix strings
// while keeping memory usage bounded by writing prefixes to disk during construction.
//
// Memory usage:
// - ~8 bytes per prefix for hash keys (bbhash's input)
// - ~8 bytes per prefix for preorder positions
// - ~8 bytes per prefix for pre-computed fingerprints (optimization)
// - Temporary buffers for I/O
//
// This is much more memory efficient than MPHFBuilder which stores all
// prefix strings in memory (~50+ bytes per prefix average).
type StreamingMPHFBuilder struct {
	// Disk-backed: hashes, positions, fingerprints. At billion-prefix
	// scale these would otherwise be ~24 GiB of heap. Backed by mmap'd
	// files in tempDir; bbhash receives the mmap'd slice directly via
	// unsafe.Slice so there's no copy to heap during construction.
	hashes       *u64DiskArray
	preorderPos  *u64DiskArray
	fingerprints *u64DiskArray

	// Temp file for prefix strings. The bytes flow:
	//   Add() → tempWriter (bufio, batches per-Add lengths+payloads)
	//         → tempEncoder (zstd, fastest level)
	//         → tempFile
	// On Build(), tempWriter.Flush + tempEncoder.Close finalize the
	// zstd frame, then the read side opens a fresh decoder on the
	// seeked file. Compression is on because the prefix stream is
	// large (sum of all S3 key prefixes, ~50 GB at 1B prefixes) and
	// highly compressible (shared path components, written then read
	// exactly once sequentially — no mmap requirement).
	tempFile    *os.File
	tempEncoder *zstd.Encoder
	tempWriter  *bufio.Writer
	tempPath    string
	tempDir     string

	// keyHash is hashString; tests replace it to force key collisions.
	keyHash func(prefix string, seed uint64) uint64

	// Stats
	count      uint64
	totalBytes uint64
	bufferSize int

	built bool
}

// ErrMPHFAlreadyBuilt is returned when Build is called more than
// once on the same StreamingMPHFBuilder. The writer chain is
// finalized in the first call, so a second call would nil-deref on
// the tempWriter flush. Surfaced as a named error so callers can
// distinguish double-Build from other Build failures.
var ErrMPHFAlreadyBuilt = errors.New("StreamingMPHFBuilder.Build already called")

// NewStreamingMPHFBuilder creates a new streaming MPHF builder. TempDir
// holds prefix strings plus the disk-backed u64 arrays.
func NewStreamingMPHFBuilder(tempDir string) (*StreamingMPHFBuilder, error) {
	tempFile, err := os.CreateTemp(tempDir, "mphf_prefixes_*.tmp")
	if err != nil {
		return nil, fmt.Errorf("create temp file: %w", err)
	}

	hashes, err := newU64DiskArray(tempDir, "mphf_hashes")
	if err != nil {
		tempFile.Close()
		os.Remove(tempFile.Name())

		return nil, err
	}
	preorderPos, err := newU64DiskArray(tempDir, "mphf_preorderpos")
	if err != nil {
		hashes.Close()
		tempFile.Close()
		os.Remove(tempFile.Name())

		return nil, err
	}
	fingerprints, err := newU64DiskArray(tempDir, "mphf_fingerprints")
	if err != nil {
		preorderPos.Close()
		hashes.Close()
		tempFile.Close()
		os.Remove(tempFile.Name())

		return nil, err
	}

	enc, err := zstd.NewWriter(tempFile, zstd.WithEncoderLevel(zstd.SpeedFastest))
	if err != nil {
		fingerprints.Close()
		preorderPos.Close()
		hashes.Close()
		tempFile.Close()
		os.Remove(tempFile.Name())

		return nil, fmt.Errorf("create prefix zstd encoder: %w", err)
	}

	b := &StreamingMPHFBuilder{
		hashes:       hashes,
		preorderPos:  preorderPos,
		fingerprints: fingerprints,
		tempFile:     tempFile,
		tempEncoder:  enc,
		tempWriter:   bufio.NewWriterSize(enc, 1024*1024),
		tempPath:     tempFile.Name(),
		tempDir:      tempDir,
		keyHash:      hashString,
		bufferSize:   1024 * 1024,
	}

	return b, nil
}

// Add adds a prefix at the given preorder position. All three
// per-prefix uint64 values (hash / preorderPos / fingerprint) are
// appended to disk-backed arrays rather than heap slices, so memory
// usage during Add stays flat regardless of prefix count.
func (b *StreamingMPHFBuilder) Add(prefix string, pos uint64) error {
	if err := b.hashes.Append(b.keyHash(prefix, 0)); err != nil {
		return fmt.Errorf("append hash: %w", err)
	}
	if err := b.preorderPos.Append(pos); err != nil {
		return fmt.Errorf("append preorder pos: %w", err)
	}
	if err := b.fingerprints.Append(computeFingerprint(prefix)); err != nil {
		return fmt.Errorf("append fingerprint: %w", err)
	}

	// Write prefix to temp file with length prefix.
	if err := putU32(b.tempWriter, uint32(len(prefix))); err != nil {
		return fmt.Errorf("write prefix length: %w", err)
	}
	if _, err := b.tempWriter.WriteString(prefix); err != nil {
		return fmt.Errorf("write prefix: %w", err)
	}

	b.count++
	b.totalBytes += uint64(len(prefix))

	return nil
}

// Count returns the number of prefixes added.
func (b *StreamingMPHFBuilder) Count() uint64 {
	return b.count
}

// Close closes the builder and removes temporary files. Aggregates
// the cleanup errors so callers don't lose visibility when the
// tempfile flush or close fails (typical on disk-full mid-build).
func (b *StreamingMPHFBuilder) Close() error {
	var errs []error
	if b.tempWriter != nil {
		if err := b.tempWriter.Flush(); err != nil {
			errs = append(errs, fmt.Errorf("flush mphf tempfile: %w", err))
		}
		b.tempWriter = nil
	}
	if b.tempEncoder != nil {
		if err := b.tempEncoder.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close mphf temp encoder: %w", err))
		}
		b.tempEncoder = nil
	}
	if b.tempFile != nil {
		if err := b.tempFile.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close mphf tempfile: %w", err))
		}
		b.tempFile = nil
		if err := os.Remove(b.tempPath); err != nil && !os.IsNotExist(err) {
			errs = append(errs, fmt.Errorf("remove mphf tempfile: %w", err))
		}
	}
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

	if err := b.finalizePrefixWriter(); err != nil {
		return err
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
	mph, seed, err := b.buildBBHash()
	if err != nil {
		return err
	}
	log.Debug().Dur("bbhash_ms", time.Since(bbhashStart)).Msg("MPHF: bbhash built")

	if err := mph.writeFile(filepath.Join(outDir, MPHFile)); err != nil {
		return err
	}

	mapStart := time.Now()
	if err := writeCombinedMmap(outDir, n, mph, b.hashes.Slice(), b.fingerprints.Slice(), b.preorderPos.Slice(), seed); err != nil {
		return err
	}
	log.Debug().Dur("write_ms", time.Since(mapStart)).Msg("MPHF: combined fp+pos written")

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

	log.Debug().Msg("MPHF: writing prefix blob")

	if err := b.writePrefixBlobDictionary(outDir); err != nil {
		return fmt.Errorf("write dict prefix blob: %w", err)
	}

	log.Debug().Msg("MPHF: build complete")

	return nil
}

// maxMPHFSeedAttempts bounds the rehashes after a key-hash collision.
// Each fresh seed collides again with probability ~n²/2⁶⁵ (≈2.7% at
// 1B prefixes), so eight in a row means something other than chance.
const maxMPHFSeedAttempts = 8

// buildBBHash builds the MPHF over the frozen key hashes, straight from
// their disk-backed array: no reverse map and no sorted copy, so the
// finalize holds little beyond bbhash's own bit vectors. Bbhash cannot
// place two equal keys; only when it fails are the hashes checked for a
// duplicate (one sorted copy), and then every prefix is rehashed with a
// new seed. Seed 0 is the hash Add computed.
func (b *StreamingMPHFBuilder) buildBBHash() (*bbhash, uint64, error) {
	var seed uint64
	for attempt := 1; ; attempt++ {
		mph, err := newBBHash(b.hashes.Slice())
		if err == nil {
			return mph, seed, nil
		}
		if !hasDuplicateHash(b.hashes.Slice()) {
			return nil, 0, fmt.Errorf("build MPHF: %w", err)
		}
		if attempt > maxMPHFSeedAttempts {
			return nil, 0, fmt.Errorf("%w: duplicates remain after %d seeds", ErrMPHFKeyCollision, maxMPHFSeedAttempts)
		}
		seed = mphfSeed(uint64(attempt))
		logging.L().Debug().Uint64("seed", seed).Msg("MPHF: duplicate key hash, rehashing with a new seed")
		if err := b.rehashKeys(seed); err != nil {
			return nil, 0, err
		}
	}
}

// hasDuplicateHash reports whether two key hashes are equal. It sorts a
// copy, so it runs only after bbhash has already failed.
func hasDuplicateHash(hashes []uint64) bool {
	sorted := slices.Clone(hashes)
	slices.Sort(sorted)
	for i := 1; i < len(sorted); i++ {
		if sorted[i] == sorted[i-1] {
			return true
		}
	}

	return false
}

// mphfSeed derives the seed for a retry with the splitmix64 finalizer
// so every bit of the FNV offset basis changes, not just the low ones
// a small counter would touch; low FNV-1a output bits depend only on
// low basis bits.
func mphfSeed(attempt uint64) uint64 {
	const (
		gamma  = 0x9e3779b97f4a7c15
		mul1   = 0xbf58476d1ce4e5b9
		mul2   = 0x94d049bb133111eb
		shift1 = 30
		shift2 = 27
		shift3 = 31
	)
	z := attempt * gamma
	z = (z ^ (z >> shift1)) * mul1
	z = (z ^ (z >> shift2)) * mul2

	return z ^ (z >> shift3)
}

// rehashKeys replaces the frozen key-hash array with the hashes of the
// spooled prefixes under seed.
func (b *StreamingMPHFBuilder) rehashKeys(seed uint64) error {
	hashes, err := newU64DiskArray(b.tempDir, "mphf_hashes")
	if err != nil {
		return err
	}
	err = b.forEachPrefix(func(_ int, prefix []byte) error {
		return hashes.Append(b.keyHash(unsafe.String(unsafe.SliceData(prefix), len(prefix)), seed))
	})
	if err == nil {
		err = hashes.Freeze()
	}
	if err != nil {
		return errors.Join(fmt.Errorf("rehash MPHF keys: %w", err), hashes.Close())
	}
	old := b.hashes
	b.hashes = hashes
	if err := old.Close(); err != nil {
		return fmt.Errorf("close superseded hashes: %w", err)
	}

	return nil
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

// finalizePrefixWriter flushes the bufio writer into the zstd encoder
// and finalizes the frame so the read side can open a fresh decoder on
// the seeked file. After this both tempWriter and tempEncoder are
// spent; Close still owns closing/removing the underlying file.
func (b *StreamingMPHFBuilder) finalizePrefixWriter() error {
	if err := b.tempWriter.Flush(); err != nil {
		return fmt.Errorf("flush temp file: %w", err)
	}
	b.tempWriter = nil
	if err := b.tempEncoder.Close(); err != nil {
		return fmt.Errorf("close prefix encoder: %w", err)
	}
	b.tempEncoder = nil

	return nil
}

// openPrefixReader rewinds the prefix temp file and returns a buffered
// reader over a fresh zstd decoder. Caller owns Close on the decoder.
func (b *StreamingMPHFBuilder) openPrefixReader() (*bufio.Reader, *zstd.Decoder, error) {
	if _, err := b.tempFile.Seek(0, 0); err != nil {
		return nil, nil, fmt.Errorf("seek temp file: %w", err)
	}
	dec, err := zstd.NewReader(b.tempFile)
	if err != nil {
		return nil, nil, fmt.Errorf("open prefix zstd reader: %w", err)
	}

	return bufio.NewReaderSize(dec, b.bufferSize), dec, nil
}

// forEachPrefix streams the spooled prefixes in Add order. The slice
// passed to fn is reused for the next prefix.
func (b *StreamingMPHFBuilder) forEachPrefix(fn func(i int, prefix []byte) error) error {
	reader, dec, err := b.openPrefixReader()
	if err != nil {
		return err
	}
	defer dec.Close()

	var lenBuf [4]byte
	prefixBuf := make([]byte, 0, 256)
	for i := range int(b.count) {
		if _, err := io.ReadFull(reader, lenBuf[:]); err != nil {
			return fmt.Errorf("read prefix length at %d: %w", i, err)
		}
		prefixLen := binary.LittleEndian.Uint32(lenBuf[:])
		if cap(prefixBuf) < int(prefixLen) {
			prefixBuf = make([]byte, prefixLen)
		}
		prefixBuf = prefixBuf[:prefixLen]
		if _, err := io.ReadFull(reader, prefixBuf); err != nil {
			return fmt.Errorf("read prefix at %d: %w", i, err)
		}
		if err := fn(i, prefixBuf); err != nil {
			return err
		}
	}

	return nil
}

// writePrefixBlobDictionary writes prefixes as dictionary-encoded
// segment-ID sequences (prefix_dict.ids.u32 + prefix_dict.prefix_off.u64).
func (b *StreamingMPHFBuilder) writePrefixBlobDictionary(outDir string) error {
	writer, err := NewDictPrefixWriter(outDir)
	if err != nil {
		return fmt.Errorf("create dict prefix writer: %w", err)
	}
	err = b.forEachPrefix(func(i int, prefix []byte) error {
		if err := writer.WritePrefix(prefix); err != nil {
			return fmt.Errorf("write prefix %d to dict blob: %w", i, err)
		}

		return nil
	})
	if err != nil {
		writer.Close()

		return err
	}

	return writer.Close()
}

func (b *StreamingMPHFBuilder) writeEmpty(outDir string) error {
	// Create empty mph file
	mphPath := filepath.Join(outDir, MPHFile)
	if err := os.WriteFile(mphPath, nil, FilePerm); err != nil {
		return fmt.Errorf("write empty mph: %w", err)
	}

	// Create empty combined fp+pos array (new format).
	combinedPath := filepath.Join(outDir, CombinedMPHFArrayFile)
	combinedWriter, err := NewArrayWriter(combinedPath, 8)
	if err != nil {
		return fmt.Errorf("create empty combined fp+pos writer: %w", err)
	}
	if err := combinedWriter.Close(); err != nil {
		return fmt.Errorf("close empty combined fp+pos writer: %w", err)
	}

	writer, err := NewDictPrefixWriter(outDir)
	if err != nil {
		return fmt.Errorf("create empty dict prefix writer: %w", err)
	}
	if err := writer.Close(); err != nil {
		return fmt.Errorf("close empty dict prefix writer: %w", err)
	}

	return nil
}

// writeCombinedMmap materialises mph_fp_pos.u64: it maps every slot to
// its prefix (slotOwners), then each worker fills a 1 MiB buffer in slot
// order and pwrites it to a disjoint range of the output file.
//
// The previous version mmap'd the file and scattered random writes
// indexed by hashPositions. On EBS gp3 that hit ~3× write amplification
// — page-cache evict/reload churn (CPU profile showed 91% in
// page-fault wait). Sequential writes touch each page exactly once.
//
// File layout: 20-byte header (count = 2N, width = 8) followed by
// 2N×8 bytes of interleaved (fp, pos, fp, pos, …) data, then the 8-byte
// key-hash seed.
func writeCombinedMmap(outDir string, n int, mph *bbhash, hashes, fps, poss []uint64, seed uint64) error {
	if len(fps) != n || len(poss) != n {
		return fmt.Errorf("%w: fps=%d poss=%d n=%d", errMPHFArrayLengthMismatch, len(fps), len(poss), n)
	}
	path := filepath.Join(outDir, CombinedMPHFArrayFile)
	dataBytes := int64(2*n) * 8
	totalBytes := int64(HeaderSize) + dataBytes + mphfSeedBytes
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create combined file: %w", err)
	}
	cleanup := func(primary error) error {
		return errors.Join(primary, f.Close(), removeIfErr(path))
	}
	if err := f.Truncate(totalBytes); err != nil {
		return cleanup(fmt.Errorf("truncate combined file: %w", err))
	}

	if _, err := f.Write(EncodeHeader(Header{
		Magic:   MagicNumber,
		Version: Version,
		Count:   uint64(2 * n),
		Width:   8,
	})); err != nil {
		return cleanup(fmt.Errorf("write combined header: %w", err))
	}

	if n == 0 {
		if err := f.Sync(); err != nil {
			return cleanup(fmt.Errorf("sync combined: %w", err))
		}
		if err := f.Close(); err != nil {
			return fmt.Errorf("close combined: %w", err)
		}

		return nil
	}

	// Concurrent pwrite to disjoint ranges is POSIX-safe; each worker
	// writes its own [start,end) slot range.
	invMap, err := slotOwners(mph, hashes)
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
		wg.Go(func() { errs[w] = writeCombinedSlots(f, invMap, fps, poss, start, end) })
	}
	wg.Wait()
	for _, werr := range errs {
		if werr != nil {
			return cleanup(werr)
		}
	}
	var seedBuf [mphfSeedBytes]byte
	binary.LittleEndian.PutUint64(seedBuf[:], seed)
	if _, err := f.WriteAt(seedBuf[:], int64(HeaderSize)+dataBytes); err != nil {
		return cleanup(fmt.Errorf("write combined seed: %w", err))
	}

	if err := f.Sync(); err != nil {
		return cleanup(fmt.Errorf("sync combined: %w", err))
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close combined: %w", err)
	}

	return nil
}

// slotOwners returns, for every MPHF slot, the index of the prefix whose
// key hash lands there, found with parallel Find calls. The MPHF is a
// bijection, so each slot is written by exactly one worker.
func slotOwners(mph *bbhash, hashes []uint64) ([]uint32, error) {
	n := len(hashes)
	if uint64(n) > math.MaxUint32 {
		return nil, fmt.Errorf("%w: %d prefixes exceed the uint32 slot-owner index", ErrMPHFTooManyKeys, n)
	}
	owners := make([]uint32, n)
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
				if !ok || slot >= uint64(n) {
					errs[w] = fmt.Errorf("%w: slot %d (found %t) for prefix %d", ErrMPHFLookupFailed, slot, ok, i)

					return
				}
				owners[slot] = uint32(i)
			}
		})
	}
	wg.Wait()

	return owners, errors.Join(errs...)
}

// writeCombinedSlots fills slots [start, end) of the combined file in
// 1 MiB buffers, each slot the (fp, pos) of the prefix invMap assigns.
func writeCombinedSlots(f *os.File, invMap []uint32, fps, poss []uint64, start, end int) error {
	const slotBytes = 16
	const bufSlots = 1 << 16 // 1 MiB / 16 B
	buf := make([]byte, bufSlots*slotBytes)
	for chunk := start; chunk < end; chunk += bufSlots {
		cEnd := min(chunk+bufSlots, end)
		for j := chunk; j < cEnd; j++ {
			i := invMap[j]
			off := (j - chunk) * slotBytes
			binary.LittleEndian.PutUint64(buf[off:off+8], fps[i])
			binary.LittleEndian.PutUint64(buf[off+8:off+slotBytes], poss[i])
		}
		blen := (cEnd - chunk) * slotBytes
		fileOff := int64(HeaderSize) + int64(chunk)*slotBytes
		if _, err := f.WriteAt(buf[:blen], fileOff); err != nil {
			return fmt.Errorf("write combined slot %d: %w", chunk, err)
		}
	}

	return nil
}

// errMPHFArrayLengthMismatch fires when the writer's fingerprints and
// positions slices have differing lengths — a programming bug; both
// must come from the same parallel-lookup pass.
var errMPHFArrayLengthMismatch = errors.New("mphf fingerprints/positions length mismatch")

// CombinedMPHFArrayFile is the filename used for the combined
// interleaved fingerprint+position table. Slot i holds the
// fingerprint at offset 2i and the position at 2i+1, so a single
// cache line covers both reads on a Lookup.
const CombinedMPHFArrayFile = "mph_fp_pos.u64"

// mphfSeedBytes is the size of the seed word mph_fp_pos.u64 carries
// after its slots.
const mphfSeedBytes = 8
