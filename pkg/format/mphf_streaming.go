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

	"github.com/eunmann/s3-inv-db/pkg/logging"
	"github.com/relab/bbhash"
)

// StreamingMPHFBuilder builds a minimal perfect hash function for prefix strings
// while keeping memory usage bounded by writing prefixes to disk during construction.
//
// Memory usage:
// - ~8 bytes per prefix for hash keys (unavoidable for bbhash)
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

	// Temp file for prefix strings
	tempFile   *os.File
	tempWriter *bufio.Writer
	tempPath   string

	// Stats
	count      uint64
	totalBytes uint64
	bufferSize int

	usePrefixDict bool
}

// StreamingMPHFOption configures a StreamingMPHFBuilder.
type StreamingMPHFOption func(*StreamingMPHFBuilder)

// WithPrefixDictionary enables dictionary-encoded prefix storage.
func WithPrefixDictionary() StreamingMPHFOption {
	return func(b *StreamingMPHFBuilder) {
		b.usePrefixDict = true
	}
}

// NewStreamingMPHFBuilder creates a new streaming MPHF builder.
// TempDir holds prefix strings plus the disk-backed u64 arrays.
func NewStreamingMPHFBuilder(tempDir string, opts ...StreamingMPHFOption) (*StreamingMPHFBuilder, error) {
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

	b := &StreamingMPHFBuilder{
		hashes:       hashes,
		preorderPos:  preorderPos,
		fingerprints: fingerprints,
		tempFile:     tempFile,
		tempWriter:   bufio.NewWriterSize(tempFile, 1024*1024),
		tempPath:     tempFile.Name(),
		bufferSize:   1024 * 1024,
	}

	for _, opt := range opts {
		opt(b)
	}

	return b, nil
}

// Add adds a prefix at the given preorder position. All three
// per-prefix uint64 values (hash / preorderPos / fingerprint) are
// appended to disk-backed arrays rather than heap slices, so memory
// usage during Add stays flat regardless of prefix count.
func (b *StreamingMPHFBuilder) Add(prefix string, pos uint64) error {
	prefixBytes := []byte(prefix)

	if err := b.hashes.Append(hashBytes(prefixBytes)); err != nil {
		return fmt.Errorf("append hash: %w", err)
	}
	if err := b.preorderPos.Append(pos); err != nil {
		return fmt.Errorf("append preorder pos: %w", err)
	}
	if err := b.fingerprints.Append(computeFingerprintBytes(prefixBytes)); err != nil {
		return fmt.Errorf("append fingerprint: %w", err)
	}

	// Write prefix to temp file with length prefix.
	var lenBuf [4]byte
	binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(prefixBytes)))
	if _, err := b.tempWriter.Write(lenBuf[:]); err != nil {
		return fmt.Errorf("write prefix length: %w", err)
	}
	if _, err := b.tempWriter.Write(prefixBytes); err != nil {
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
	}
	if b.tempFile != nil {
		if err := b.tempFile.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close mphf tempfile: %w", err))
		}
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

// Build constructs the MPHF and writes it to the output directory.
// Memory usage during Build is bounded by buffer sizes, not by prefix count.
//
// Optimization notes:
//   - ReverseMap: Uses bbhash's ReverseMap to avoid expensive Find() calls (~17x faster).
//   - Option 4: Uses pre-computed fingerprints from Add phase (no recomputation).
func (b *StreamingMPHFBuilder) Build(outDir string) error {
	log := logging.L()

	if b.count == 0 {
		log.Debug().Msg("MPHF: writing empty (zero prefixes)")

		return b.writeEmpty(outDir)
	}

	// Flush and close temp writer
	if err := b.tempWriter.Flush(); err != nil {
		return fmt.Errorf("flush temp file: %w", err)
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

	// Build MPHF with gamma=2.0 and ReverseMap for fast position lookup
	log.Debug().
		Uint64("hash_count", b.count).
		Msg("MPHF: calling bbhash.New with ReverseMap")

	bbhashStart := time.Now()
	const bbhashGamma = 2.0
	// bbhash's per-partition build runs concurrently via errgroup; the
	// default partitions=1 leaves it serial. Pinning to NumCPU lets all
	// cores work the N×4M-key partitions in parallel. bbhash internally
	// caps at maxPartitions=255 (uint8 partition index).
	mph, err := bbhash.New(b.hashes.Slice(),
		bbhash.Gamma(bbhashGamma),
		bbhash.Partitions(runtime.NumCPU()),
		bbhash.WithReverseMap(),
	)
	if err != nil {
		return fmt.Errorf("build MPHF: %w", err)
	}
	bbhashDuration := time.Since(bbhashStart)

	log.Debug().
		Dur("bbhash_ms", bbhashDuration).
		Msg("MPHF: bbhash.New complete")

	// Write MPHF to disk
	mphPath := filepath.Join(outDir, MPHFile)
	mphFile, err := os.Create(mphPath)
	if err != nil {
		return fmt.Errorf("create mph file: %w", err)
	}

	data, err := mph.MarshalBinary()
	if err != nil {
		mphFile.Close()
		os.Remove(mphPath)

		return fmt.Errorf("marshal MPHF: %w", err)
	}
	if _, err := mphFile.Write(data); err != nil {
		mphFile.Close()
		os.Remove(mphPath)

		return fmt.Errorf("write MPHF: %w", err)
	}
	mphFile.Close()

	// Compute hash positions using ReverseMap (avoids expensive Find() calls)
	log.Debug().Msg("MPHF: computing hash positions via ReverseMap")
	posStart := time.Now()

	n := int(b.count)
	hashPositions, err := b.computeHashPositionsParallelSort(mph, n)
	if err != nil {
		return err
	}

	log.Debug().
		Dur("pos_ms", time.Since(posStart)).
		Msg("MPHF: hash positions computed")

	// Free hashes now — bbhash + lookup are done with them. The
	// underlying mmap stays around until builder.Close.
	if err := b.hashes.Close(); err != nil {
		return fmt.Errorf("close hashes: %w", err)
	}
	b.hashes = nil

	// =========================================================================
	// OPTIMIZATION: scatter fingerprints + positions directly into an
	// mmap'd output file. Previous path allocated two N-element heap
	// slices, filled them, then handed them to writeArraysParallel
	// which serialised to disk. With direct-mmap-write the scatter
	// loop writes through the page cache: zero heap intermediate.
	// =========================================================================
	log.Debug().Msg("MPHF: writing combined fp+pos via mmap")
	mapStart := time.Now()
	if err := writeCombinedMmap(outDir, n, hashPositions, b.fingerprints.Slice(), b.preorderPos.Slice()); err != nil {
		return err
	}
	log.Debug().
		Dur("write_ms", time.Since(mapStart)).
		Msg("MPHF: combined fp+pos written")

	if err := b.fingerprints.Close(); err != nil {
		return fmt.Errorf("close fingerprints: %w", err)
	}
	b.fingerprints = nil
	if err := b.preorderPos.Close(); err != nil {
		return fmt.Errorf("close preorderPos: %w", err)
	}
	b.preorderPos = nil
	runtime.GC()

	log.Debug().Msg("MPHF: writing prefix blob")

	if b.usePrefixDict {
		if err := b.writePrefixBlobDictionary(outDir); err != nil {
			return fmt.Errorf("write dict prefix blob: %w", err)
		}
	} else {
		if err := b.writePrefixBlobPreorder(outDir); err != nil {
			return fmt.Errorf("write prefix blob: %w", err)
		}
	}

	log.Debug().Msg("MPHF: build complete")

	return nil
}

// hashIdxPair packs a (hash, original-index) tuple for the sorted-array
// lookup variant. The 12-byte layout (vs Go's ~24-byte map entry) keeps
// the table inside L3 for one more decade of input scale.
type hashIdxPair struct {
	hash uint64
	idx  uint32
}

// computeHashPositionsParallelSort replaces the map with a sorted
// (hash, idx) array and binary-searches it in parallel across
// runtime.NumCPU() workers. Trades O(n) map build for O(n log n) sort,
// betting that the cache-friendlier flat array plus parallelism wins
// at scale where the map exceeds L3.
//
// Uint32 indices cap input at ~4B prefixes; the build pipeline tracks
// uint64 counts elsewhere but no realistic MPHF input approaches 2^32.
func (b *StreamingMPHFBuilder) computeHashPositionsParallelSort(mph *bbhash.BBHash2, n int) ([]int, error) {
	if uint64(n) > math.MaxUint32 {
		return nil, fmt.Errorf("%w: %d prefixes exceeds uint32 index range used by the sorted-array variant", ErrMPHFAmbiguousKey, n)
	}
	pairs := make([]hashIdxPair, n)
	for i, h := range b.hashes.Slice() {
		pairs[i] = hashIdxPair{hash: h, idx: uint32(i)}
	}
	parallelRadixSortByHashByte(pairs)

	hashPositions := make([]int, n)
	numWorkers := mphfWorkerCount(n)
	chunkSize := (n + numWorkers - 1) / numWorkers
	errs := make([]error, numWorkers)
	var wg sync.WaitGroup
	for w := range numWorkers {
		start := uint64(w*chunkSize) + 1
		end := min(uint64((w+1)*chunkSize)+1, uint64(n)+1)
		if start >= end {
			continue
		}
		wg.Go(func() {
			for mphPos := start; mphPos < end; mphPos++ {
				key := mph.Key(mphPos)
				if key == 0 {
					errs[w] = fmt.Errorf("%w: Key(%d) returned 0", ErrMPHFAmbiguousKey, mphPos)

					return
				}
				pos, ok := slices.BinarySearchFunc(pairs, key, func(p hashIdxPair, target uint64) int {
					switch {
					case p.hash < target:
						return -1
					case p.hash > target:
						return 1
					default:
						return 0
					}
				})
				if !ok {
					errs[w] = fmt.Errorf("%w: Key(%d) returned %d", ErrMPHFUnknownHash, mphPos, key)

					return
				}
				hashPositions[pairs[pos].idx] = int(mphPos - 1)
			}
		})
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return nil, err
		}
	}

	return hashPositions, nil
}

// parallelRadixSortByHashByte sorts pairs by hash with one MSD pass on
// the top byte (256 buckets) followed by parallel pdqsort within each
// bucket. Beats single-threaded slices.SortFunc once n outgrows L3,
// which is where the sort phase had been spending its time.
func parallelRadixSortByHashByte(pairs []hashIdxPair) {
	const (
		radixBuckets  = 256
		topByteShift  = 56
		bucketIdxMask = radixBuckets - 1
	)
	n := len(pairs)
	if n < 2 {
		return
	}

	var counts [radixBuckets]int
	for i := range pairs {
		counts[(pairs[i].hash>>topByteShift)&bucketIdxMask]++
	}
	// Cumulative sum: starts[i] = sum of counts[0..i). Bucket bkt spans
	// [starts[bkt], starts[bkt] + counts[bkt]).
	var starts [radixBuckets]int
	sum := 0
	for i, c := range &counts {
		starts[i] = sum
		sum += c
	}

	scratch := make([]hashIdxPair, n)
	cursors := starts
	for i := range pairs {
		b := (pairs[i].hash >> topByteShift) & bucketIdxMask
		scratch[cursors[b]] = pairs[i]
		cursors[b]++
	}
	copy(pairs, scratch)

	cmp := func(a, b hashIdxPair) int {
		switch {
		case a.hash < b.hash:
			return -1
		case a.hash > b.hash:
			return 1
		default:
			return 0
		}
	}

	numWorkers := runtime.NumCPU()
	sem := make(chan struct{}, numWorkers)
	var wg sync.WaitGroup
	for bkt, c := range &counts {
		if c < 2 {
			continue
		}
		s := starts[bkt]
		e := s + c
		sem <- struct{}{}
		wg.Add(1)
		go func(s, e int) {
			defer wg.Done()
			defer func() { <-sem }()
			slices.SortFunc(pairs[s:e], cmp)
		}(s, e)
	}
	wg.Wait()
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

// writePrefixBlobPreorder writes prefixes in preorder (original Add order) for GetPrefix.
// This reads from the temp file in sequence.
func (b *StreamingMPHFBuilder) writePrefixBlobPreorder(outDir string) error {
	blobPath := filepath.Join(outDir, PrefixBlobFile)
	offsetsPath := filepath.Join(outDir, PrefixOffsetsFile)

	writer, err := NewBlobWriter(blobPath, offsetsPath)
	if err != nil {
		return fmt.Errorf("create blob writer: %w", err)
	}

	// Seek to start of temp file
	if _, err := b.tempFile.Seek(0, 0); err != nil {
		writer.Close()

		return fmt.Errorf("seek temp file: %w", err)
	}
	reader := bufio.NewReaderSize(b.tempFile, b.bufferSize)

	// Read all prefixes in original (preorder) order and write to blob
	var lenBuf [4]byte
	n := int(b.count)

	// Reusable buffer for reading prefixes
	prefixBuf := make([]byte, 0, 256)

	for i := range n {
		// Read prefix length
		if _, err := io.ReadFull(reader, lenBuf[:]); err != nil {
			writer.Close()

			return fmt.Errorf("read prefix length at %d: %w", i, err)
		}
		prefixLen := binary.LittleEndian.Uint32(lenBuf[:])

		// Grow buffer if needed
		if cap(prefixBuf) < int(prefixLen) {
			prefixBuf = make([]byte, prefixLen)
		}
		prefixBuf = prefixBuf[:prefixLen]

		// Read prefix
		if _, err := io.ReadFull(reader, prefixBuf); err != nil {
			writer.Close()

			return fmt.Errorf("read prefix at %d: %w", i, err)
		}

		// Write to blob (WriteBytes avoids string conversion)
		if err := writer.WriteBytes(prefixBuf); err != nil {
			writer.Close()

			return fmt.Errorf("write prefix %d to blob: %w", i, err)
		}
	}

	return writer.Close()
}

// writePrefixBlobDictionary writes prefixes as dictionary-encoded
// segment-ID sequences (prefix_dict.ids.u32 + prefix_dict.prefix_off.u64).
func (b *StreamingMPHFBuilder) writePrefixBlobDictionary(outDir string) error {
	writer, err := NewDictPrefixWriter(outDir)
	if err != nil {
		return fmt.Errorf("create dict prefix writer: %w", err)
	}

	if _, err := b.tempFile.Seek(0, 0); err != nil {
		writer.Close()

		return fmt.Errorf("seek temp file: %w", err)
	}
	reader := bufio.NewReaderSize(b.tempFile, b.bufferSize)

	var lenBuf [4]byte
	n := int(b.count)

	prefixBuf := make([]byte, 0, 256)

	for i := range n {
		if _, err := io.ReadFull(reader, lenBuf[:]); err != nil {
			writer.Close()

			return fmt.Errorf("read prefix length at %d: %w", i, err)
		}
		prefixLen := binary.LittleEndian.Uint32(lenBuf[:])

		if cap(prefixBuf) < int(prefixLen) {
			prefixBuf = make([]byte, prefixLen)
		}
		prefixBuf = prefixBuf[:prefixLen]

		if _, err := io.ReadFull(reader, prefixBuf); err != nil {
			writer.Close()

			return fmt.Errorf("read prefix at %d: %w", i, err)
		}

		if err := writer.WritePrefix(string(prefixBuf)); err != nil {
			writer.Close()

			return fmt.Errorf("write prefix %d to dict blob: %w", i, err)
		}
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

	if b.usePrefixDict {
		writer, err := NewDictPrefixWriter(outDir)
		if err != nil {
			return fmt.Errorf("create empty dict prefix writer: %w", err)
		}
		if err := writer.Close(); err != nil {
			return fmt.Errorf("close empty dict prefix writer: %w", err)
		}

		return nil
	}

	blobPath := filepath.Join(outDir, PrefixBlobFile)
	offsetsPath := filepath.Join(outDir, PrefixOffsetsFile)
	writer, err := NewBlobWriter(blobPath, offsetsPath)
	if err != nil {
		return fmt.Errorf("create empty blob writer: %w", err)
	}
	if err := writer.Close(); err != nil {
		return fmt.Errorf("close empty blob writer: %w", err)
	}

	return nil
}

// writeCombinedMmap materialises mph_fp_pos.u64 by inverting the
// permutation once, then having each worker fill a 1 MiB buffer in
// slot order and pwrite it to a disjoint range of the output file.
//
// The previous version mmap'd the file and scattered random writes
// indexed by hashPositions. On EBS gp3 that hit ~3× write amplification
// — page-cache evict/reload churn (CPU profile showed 91% in
// page-fault wait). Sequential writes touch each page exactly once.
//
// File layout matches the existing combined format: 20-byte header
// (count = 2N, width = 8) followed by 2N×8 bytes of interleaved
// (fp, pos, fp, pos, …) data.
func writeCombinedMmap(outDir string, n int, hashPositions []int, fps, poss []uint64) error {
	if len(fps) != n || len(poss) != n {
		return fmt.Errorf("%w: fps=%d poss=%d n=%d", errMPHFArrayLengthMismatch, len(fps), len(poss), n)
	}
	path := filepath.Join(outDir, CombinedMPHFArrayFile)
	dataBytes := int64(2*n) * 8
	totalBytes := int64(HeaderSize) + dataBytes
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create combined file: %w", err)
	}
	if err := f.Truncate(totalBytes); err != nil {
		f.Close()
		os.Remove(path)

		return fmt.Errorf("truncate combined file: %w", err)
	}

	if _, err := f.Write(EncodeHeader(Header{
		Magic:   MagicNumber,
		Version: Version,
		Count:   uint64(2 * n),
		Width:   8,
	})); err != nil {
		f.Close()
		os.Remove(path)

		return fmt.Errorf("write combined header: %w", err)
	}

	if n == 0 {
		if err := f.Sync(); err != nil {
			f.Close()
			os.Remove(path)

			return fmt.Errorf("sync combined: %w", err)
		}
		if err := f.Close(); err != nil {
			return fmt.Errorf("close combined: %w", err)
		}

		return nil
	}

	// Invert hashPositions: invMap[slot] = source index. Lets each
	// worker emit slots in order without lookups against random
	// indices. uint32 indexing assumes n ≤ MaxUint32, which
	// computeHashPositionsParallelSort already enforces upstream.
	invMap := make([]uint32, n)
	for i, slot := range hashPositions {
		invMap[slot] = uint32(i)
	}

	const slotBytes = 16
	const bufSlots = 1 << 16 // 1 MiB / 16 B
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
		wg.Add(1)
		go func(w, start, end int) {
			defer wg.Done()
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
					errs[w] = fmt.Errorf("write combined slot %d: %w", chunk, err)

					return
				}
			}
		}(w, start, end)
	}
	wg.Wait()
	for _, werr := range errs {
		if werr != nil {
			f.Close()
			os.Remove(path)

			return werr
		}
	}

	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(path)

		return fmt.Errorf("sync combined: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close combined: %w", err)
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
// cache line covers both reads on a Lookup. Replaces the older
// separate mph_fp.u64 / mph_pos.u64 files; openMPHFArrays falls back
// to the separate format when this file isn't present.
const CombinedMPHFArrayFile = "mph_fp_pos.u64"
