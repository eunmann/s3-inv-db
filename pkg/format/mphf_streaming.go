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

// PrefixEncoding specifies how prefix strings are stored in the index.
type PrefixEncoding int

const (
	// PrefixEncodingRaw stores prefixes as raw UTF-8 strings concatenated in a blob.
	// This is the legacy format with prefix_blob.bin and prefix_offsets.u64.
	PrefixEncodingRaw PrefixEncoding = iota

	// PrefixEncodingSegDict splits prefixes into "/"-delimited segments,
	// interns unique segments into a dictionary, and stores each prefix
	// as a sequence of uint32 segment IDs. This provides significant size
	// reduction when prefixes share common path components.
	PrefixEncodingSegDict
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
	// In-memory: hashes, positions, and fingerprints (24 bytes per prefix total)
	hashes       []uint64
	preorderPos  []uint64
	fingerprints []uint64 // Pre-computed during Add phase

	// Temp file for prefix strings
	tempFile   *os.File
	tempWriter *bufio.Writer
	tempPath   string

	// Stats
	count      uint64
	totalBytes uint64
	bufferSize int

	// Prefix encoding configuration
	prefixEncoding PrefixEncoding
}

// StreamingMPHFOption configures a StreamingMPHFBuilder.
type StreamingMPHFOption func(*StreamingMPHFBuilder)

// WithPrefixEncoding sets the prefix encoding method.
func WithPrefixEncoding(enc PrefixEncoding) StreamingMPHFOption {
	return func(b *StreamingMPHFBuilder) {
		b.prefixEncoding = enc
	}
}

// NewStreamingMPHFBuilder creates a new streaming MPHF builder.
// The tempDir is used for temporary storage of prefix strings.
func NewStreamingMPHFBuilder(tempDir string, opts ...StreamingMPHFOption) (*StreamingMPHFBuilder, error) {
	// Create temp file for prefix strings
	tempFile, err := os.CreateTemp(tempDir, "mphf_prefixes_*.tmp")
	if err != nil {
		return nil, fmt.Errorf("create temp file: %w", err)
	}

	b := &StreamingMPHFBuilder{
		hashes:         make([]uint64, 0, 1024),
		preorderPos:    make([]uint64, 0, 1024),
		fingerprints:   make([]uint64, 0, 1024),
		tempFile:       tempFile,
		tempWriter:     bufio.NewWriterSize(tempFile, 1024*1024), // 1MB buffer
		tempPath:       tempFile.Name(),
		bufferSize:     1024 * 1024,
		prefixEncoding: PrefixEncodingRaw, // Default to raw encoding
	}

	for _, opt := range opts {
		opt(b)
	}

	return b, nil
}

// Add adds a prefix at the given preorder position.
// The prefix is written to disk immediately; only the hash and fingerprint are kept in memory.
func (b *StreamingMPHFBuilder) Add(prefix string, pos uint64) error {
	// Convert to bytes once, reuse for all operations
	prefixBytes := []byte(prefix)

	// Store hash in memory (8 bytes) - used for BBHash construction
	b.hashes = append(b.hashes, hashBytes(prefixBytes))

	// Store position in memory (8 bytes)
	b.preorderPos = append(b.preorderPos, pos)

	// Pre-compute fingerprint now (8 bytes) - avoids recomputing later
	// This is Option 4: compute fingerprint during Add phase
	b.fingerprints = append(b.fingerprints, computeFingerprintBytes(prefixBytes))

	// Write prefix to temp file with length prefix
	// Format: [4-byte length][prefix bytes]
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

	// Build MPHF with gamma=2.0 and ReverseMap for fast position lookup
	log.Debug().
		Uint64("hash_count", b.count).
		Msg("MPHF: calling bbhash.New with ReverseMap")

	bbhashStart := time.Now()
	const bbhashGamma = 2.0
	mph, err := bbhash.New(b.hashes, bbhash.Gamma(bbhashGamma), bbhash.WithReverseMap())
	if err != nil {
		return fmt.Errorf("build MPHF: %w", err)
	}
	bbhashDuration := time.Since(bbhashStart)

	log.Debug().
		Dur("bbhash_ms", bbhashDuration).
		Msg("MPHF: bbhash.New complete")

	// Write MPHF to disk
	mphPath := filepath.Join(outDir, "mph.bin")
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
	hashPositions, err := b.computeHashPositionsParallelMap(mph, n)
	if err != nil {
		return err
	}

	log.Debug().
		Dur("pos_ms", time.Since(posStart)).
		Msg("MPHF: hash positions computed")

	// Free hashes now - we have all positions
	b.hashes = nil

	// =========================================================================
	// OPTIMIZATION: Use pre-computed fingerprints (Option 4)
	// =========================================================================
	// Fingerprints were computed during Add() phase, so we just copy them
	// to the output array at the correct positions. No recomputation needed.

	log.Debug().Msg("MPHF: mapping fingerprints and positions to output arrays")
	mapStart := time.Now()

	outputFingerprints := make([]uint64, n)
	outputPreorderPos := make([]uint64, n)

	// Simple copy loop - no hash computation, no I/O
	for i, hashPos := range hashPositions {
		outputFingerprints[hashPos] = b.fingerprints[i]
		outputPreorderPos[hashPos] = b.preorderPos[i]
	}

	log.Debug().
		Dur("map_ms", time.Since(mapStart)).
		Msg("MPHF: arrays mapped")

	// Free source arrays
	b.fingerprints = nil
	b.preorderPos = nil
	runtime.GC()

	log.Debug().Msg("MPHF: writing fingerprints and positions (parallel)")

	// Write fingerprints and positions in parallel since they are independent files
	if err := writeArraysParallel(outDir, outputFingerprints, outputPreorderPos); err != nil {
		return err
	}

	log.Debug().Msg("MPHF: writing prefix blob")

	// Write prefix blob in preorder (original order)
	switch b.prefixEncoding {
	case PrefixEncodingSegDict:
		if err := b.writePrefixBlobSegmented(outDir); err != nil {
			return fmt.Errorf("write segmented prefix blob: %w", err)
		}
	default:
		if err := b.writePrefixBlobPreorder(outDir); err != nil {
			return fmt.Errorf("write prefix blob: %w", err)
		}
	}

	log.Debug().Msg("MPHF: build complete")

	return nil
}

// computeHashPositionsReverseMap computes MPHF hash positions using the ReverseMap.
// This avoids expensive Find() calls by iterating through the ReverseMap to build
// a forward mapping. Approximately 17x faster than calling Find() for each hash.
//
// Baseline implementation: serial map build, serial lookup loop. Retained as the
// reference variant for benchmarking; production callers use whichever variant
// wins on representative workloads.
func (b *StreamingMPHFBuilder) computeHashPositionsReverseMap(mph *bbhash.BBHash2, n int) ([]int, error) {
	// Build hash → original index map
	hashToOrigIdx := make(map[uint64]int, n)
	for i, h := range b.hashes {
		hashToOrigIdx[h] = i
	}

	// Use ReverseMap to get positions without calling Find()
	hashPositions := make([]int, n)
	for mphPos := uint64(1); mphPos <= uint64(n); mphPos++ {
		key := mph.Key(mphPos)
		if key == 0 {
			// Key value 0 is ambiguous (could be sentinel or actual key)
			// This should be extremely rare for FNV hashes
			return nil, fmt.Errorf("%w: Key(%d) returned 0", ErrMPHFAmbiguousKey, mphPos)
		}
		origIdx, ok := hashToOrigIdx[key]
		if !ok {
			return nil, fmt.Errorf("%w: Key(%d) returned %d", ErrMPHFUnknownHash, mphPos, key)
		}
		hashPositions[origIdx] = int(mphPos - 1)
	}

	return hashPositions, nil
}

// computeHashPositionsParallelMap is the baseline variant with the lookup
// loop parallelised across runtime.NumCPU() workers. The map is built once
// (serial) and then read concurrently by every worker. Concurrent map reads
// are race-free in Go as long as no goroutine writes — which holds here.
//
// Writes to hashPositions are race-free because each mphPos maps to a
// unique origIdx (the MPHF is a perfect hash), so workers never touch the
// same slot.
func (b *StreamingMPHFBuilder) computeHashPositionsParallelMap(mph *bbhash.BBHash2, n int) ([]int, error) {
	hashToOrigIdx := make(map[uint64]int, n)
	for i, h := range b.hashes {
		hashToOrigIdx[h] = i
	}

	hashPositions := make([]int, n)
	numWorkers := mphfWorkerCount(n)

	chunkSize := (n + numWorkers - 1) / numWorkers
	errs := make([]error, numWorkers)
	var wg sync.WaitGroup
	for w := range numWorkers {
		start := uint64(w*chunkSize) + 1 // mph.Key uses 1-based positions
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
				origIdx, ok := hashToOrigIdx[key]
				if !ok {
					errs[w] = fmt.Errorf("%w: Key(%d) returned %d", ErrMPHFUnknownHash, mphPos, key)

					return
				}
				hashPositions[origIdx] = int(mphPos - 1)
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
	for i, h := range b.hashes {
		pairs[i] = hashIdxPair{hash: h, idx: uint32(i)}
	}
	slices.SortFunc(pairs, func(a, b hashIdxPair) int {
		switch {
		case a.hash < b.hash:
			return -1
		case a.hash > b.hash:
			return 1
		default:
			return 0
		}
	})

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

// prefixChunkItem holds data for one prefix during parallel processing.
type prefixChunkItem struct {
	index       int    // Original index in the prefix sequence
	prefixBytes []byte // The prefix data (shared slice into chunk buffer)
	offset      uint64 // Cumulative offset in temp file
}

// prefixChunkReader reads length-prefixed prefix data in chunks.
type prefixChunkReader struct {
	reader        io.Reader
	n             int // Total number of prefixes to read
	chunkSize     int
	processed     int
	currentOffset uint64
	lenBuf        [4]byte
}

// newPrefixChunkReader creates a reader for chunked prefix data.
func newPrefixChunkReader(reader io.Reader, n, chunkSize int) *prefixChunkReader {
	return &prefixChunkReader{
		reader:    reader,
		n:         n,
		chunkSize: chunkSize,
	}
}

// ReadChunk reads the next chunk of prefixes. Returns nil when all prefixes are read.
func (r *prefixChunkReader) ReadChunk() ([]prefixChunkItem, error) {
	if r.processed >= r.n {
		return nil, nil
	}

	// Determine chunk size for this iteration
	remaining := r.n - r.processed
	thisChunk := min(remaining, r.chunkSize)

	// Pre-allocate buffer for prefixes (estimate 24 bytes average)
	const estimatedAvgPrefixLen = 24
	chunkBuffer := make([]byte, 0, thisChunk*estimatedAvgPrefixLen)
	items := make([]prefixChunkItem, 0, thisChunk)

	for range thisChunk {
		// Read prefix length
		if _, err := io.ReadFull(r.reader, r.lenBuf[:]); err != nil {
			return nil, fmt.Errorf("read prefix length at %d: %w", r.processed, err)
		}
		prefixLen := binary.LittleEndian.Uint32(r.lenBuf[:])

		// Ensure buffer has capacity for this prefix
		start := len(chunkBuffer)
		if cap(chunkBuffer)-start < int(prefixLen) {
			// Need to grow - double capacity plus this prefix
			newCap := cap(chunkBuffer)*2 + int(prefixLen)
			newBuf := make([]byte, start, newCap)
			copy(newBuf, chunkBuffer)
			chunkBuffer = newBuf
		}

		// Extend buffer and read prefix directly into it
		chunkBuffer = chunkBuffer[:start+int(prefixLen)]
		if _, err := io.ReadFull(r.reader, chunkBuffer[start:]); err != nil {
			return nil, fmt.Errorf("read prefix at %d: %w", r.processed, err)
		}

		// Create item with slice into the contiguous buffer
		items = append(items, prefixChunkItem{
			index:       r.processed,
			prefixBytes: chunkBuffer[start : start+int(prefixLen)],
			offset:      r.currentOffset,
		})

		r.currentOffset += uint64(4 + prefixLen)
		r.processed++
	}

	return items, nil
}

// computeFingerprintsParallel reads prefixes from the temp file and computes
// fingerprints in parallel using a worker pool with chunked processing.
func (b *StreamingMPHFBuilder) computeFingerprintsParallel(
	reader *bufio.Reader,
	mph *bbhash.BBHash2,
	n int,
	fingerprints []uint64,
	preorderPositions []uint64,
	orderedPrefixOffsets []uint64,
) error {
	numWorkers := max(runtime.NumCPU(), 1)

	const chunkSize = 50000
	workChan := make(chan []prefixChunkItem, numWorkers*2)
	errChan := make(chan error, numWorkers)

	// Start workers
	var wg sync.WaitGroup
	for range numWorkers {
		wg.Go(func() {
			b.fingerprintWorker(workChan, errChan, mph, fingerprints, preorderPositions, orderedPrefixOffsets)
		})
	}

	// Read and dispatch chunks
	chunkReader := newPrefixChunkReader(reader, n, chunkSize)
	err := b.dispatchChunks(chunkReader, workChan, errChan, &wg)
	if err != nil {
		return err
	}

	// Check for any final errors
	select {
	case err := <-errChan:
		return err
	default:
	}

	return nil
}

// fingerprintWorker processes prefix chunks and computes fingerprints.
func (b *StreamingMPHFBuilder) fingerprintWorker(
	workChan <-chan []prefixChunkItem,
	errChan chan<- error,
	mph *bbhash.BBHash2,
	fingerprints []uint64,
	preorderPositions []uint64,
	orderedPrefixOffsets []uint64,
) {
	for items := range workChan {
		for _, item := range items {
			keyHash := hashBytes(item.prefixBytes)
			hashVal := mph.Find(keyHash)
			if hashVal == 0 {
				select {
				case errChan <- fmt.Errorf("%w at index %d", ErrMPHFLookupFailed, item.index):
				default:
				}

				return
			}
			hashPos := int(hashVal - 1)

			fingerprints[hashPos] = computeFingerprintBytes(item.prefixBytes)
			preorderPositions[hashPos] = b.preorderPos[item.index]
			orderedPrefixOffsets[hashPos] = item.offset
		}
	}
}

// dispatchChunks reads prefix chunks and sends them to workers.
func (b *StreamingMPHFBuilder) dispatchChunks(
	chunkReader *prefixChunkReader,
	workChan chan<- []prefixChunkItem,
	errChan <-chan error,
	wg *sync.WaitGroup,
) error {
	defer func() {
		close(workChan)
		wg.Wait()
	}()

	for {
		items, err := chunkReader.ReadChunk()
		if err != nil {
			return err
		}
		if items == nil {
			return nil
		}

		workChan <- items

		// Check for worker errors
		select {
		case err := <-errChan:
			return err
		default:
		}
	}
}

// writePrefixBlobPreorder writes prefixes in preorder (original Add order) for GetPrefix.
// This reads from the temp file in sequence.
func (b *StreamingMPHFBuilder) writePrefixBlobPreorder(outDir string) error {
	blobPath := filepath.Join(outDir, "prefix_blob.bin")
	offsetsPath := filepath.Join(outDir, "prefix_offsets.u64")

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

// writePrefixBlobSegmented writes prefixes using segment dictionary compression.
// This reads from the temp file in sequence and writes segmented encoding files.
func (b *StreamingMPHFBuilder) writePrefixBlobSegmented(outDir string) error {
	writer, err := NewSegmentedPrefixWriter(outDir)
	if err != nil {
		return fmt.Errorf("create segmented prefix writer: %w", err)
	}

	// Seek to start of temp file
	if _, err := b.tempFile.Seek(0, 0); err != nil {
		writer.Close()

		return fmt.Errorf("seek temp file: %w", err)
	}
	reader := bufio.NewReaderSize(b.tempFile, b.bufferSize)

	// Read all prefixes in original (preorder) order and write to segmented blob
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

		// Write to segmented blob
		if err := writer.WritePrefix(string(prefixBuf)); err != nil {
			writer.Close()

			return fmt.Errorf("write prefix %d to segmented blob: %w", i, err)
		}
	}

	return writer.Close()
}

func (b *StreamingMPHFBuilder) writeEmpty(outDir string) error {
	// Create empty mph file
	mphPath := filepath.Join(outDir, "mph.bin")
	if err := os.WriteFile(mphPath, nil, indexFilePerm); err != nil {
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

	// Create empty prefix files based on encoding
	switch b.prefixEncoding {
	case PrefixEncodingSegDict:
		writer, err := NewSegmentedPrefixWriter(outDir)
		if err != nil {
			return fmt.Errorf("create empty segmented prefix writer: %w", err)
		}

		return writer.Close()
	default:
		blobPath := filepath.Join(outDir, "prefix_blob.bin")
		offsetsPath := filepath.Join(outDir, "prefix_offsets.u64")
		writer, err := NewBlobWriter(blobPath, offsetsPath)
		if err != nil {
			return fmt.Errorf("create empty blob writer: %w", err)
		}

		return writer.Close()
	}
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

// writeArraysParallel writes the combined interleaved fingerprints+
// positions array. Single file, single header — half the cache misses
// of the previous separate-array layout on the Lookup hot path. Writes
// the combined file only; new builds no longer emit the legacy
// mph_fp.u64 / mph_pos.u64 pair.
func writeArraysParallel(outDir string, fingerprints, positions []uint64) error {
	if len(fingerprints) != len(positions) {
		return fmt.Errorf("%w: %d vs %d", errMPHFArrayLengthMismatch, len(fingerprints), len(positions))
	}
	combined := make([]uint64, len(fingerprints)*2)
	for i := range fingerprints {
		combined[2*i] = fingerprints[i]
		combined[2*i+1] = positions[i]
	}

	path := filepath.Join(outDir, CombinedMPHFArrayFile)
	w, err := NewArrayWriter(path, 8)
	if err != nil {
		return fmt.Errorf("create combined fp+pos writer: %w", err)
	}
	if err := w.WriteU64Batch(combined); err != nil {
		w.Close()

		return fmt.Errorf("write combined fp+pos: %w", err)
	}
	if err := w.Close(); err != nil {
		return fmt.Errorf("close combined fp+pos: %w", err)
	}

	return nil
}
