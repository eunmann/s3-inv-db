package format

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
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
}

// NewStreamingMPHFBuilder creates a new streaming MPHF builder.
// The tempDir is used for temporary storage of prefix strings.
func NewStreamingMPHFBuilder(tempDir string) (*StreamingMPHFBuilder, error) {
	// Create temp file for prefix strings
	tempFile, err := os.CreateTemp(tempDir, "mphf_prefixes_*.tmp")
	if err != nil {
		return nil, fmt.Errorf("create temp file: %w", err)
	}

	return &StreamingMPHFBuilder{
		hashes:       make([]uint64, 0, 1024),
		preorderPos:  make([]uint64, 0, 1024),
		fingerprints: make([]uint64, 0, 1024),
		tempFile:     tempFile,
		tempWriter:   bufio.NewWriterSize(tempFile, 1024*1024), // 1MB buffer
		tempPath:     tempFile.Name(),
		bufferSize:   1024 * 1024,
	}, nil
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

// Close closes the builder and removes temporary files.
func (b *StreamingMPHFBuilder) Close() error {
	if b.tempWriter != nil {
		b.tempWriter.Flush()
	}
	if b.tempFile != nil {
		b.tempFile.Close()
		os.Remove(b.tempPath)
	}
	return nil
}

// Build constructs the MPHF and writes it to the output directory.
// Memory usage during Build is bounded by buffer sizes, not by prefix count.
//
// Optimization notes:
//   - Option 2: Computes all hash positions in a tight loop for cache efficiency.
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

	// Build MPHF with gamma=2.0 (good space/time tradeoff)
	log.Debug().
		Uint64("hash_count", b.count).
		Msg("MPHF: calling bbhash.New (CPU-intensive)")

	bbhashStart := time.Now()
	mph, err := bbhash.New(b.hashes, bbhash.Gamma(2.0))
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

	// Compute all hash positions in parallel (Option 2 optimization)
	log.Debug().Msg("MPHF: computing hash positions (parallel)")
	findStart := time.Now()

	n := int(b.count)
	hashPositions, err := b.computeHashPositionsParallel(mph, n)
	if err != nil {
		return err
	}

	log.Debug().
		Dur("find_ms", time.Since(findStart)).
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
	if err := b.writePrefixBlobPreorder(outDir); err != nil {
		return fmt.Errorf("write prefix blob: %w", err)
	}

	log.Debug().Msg("MPHF: build complete")

	return nil
}

// computeHashPositionsParallel computes MPHF hash positions for all prefixes in parallel.
// This is an optimization that avoids interleaving Find() calls with I/O and fingerprint
// computation, keeping the BBHash bitvectors hot in cache across worker threads.
func (b *StreamingMPHFBuilder) computeHashPositionsParallel(mph *bbhash.BBHash2, n int) ([]int, error) {
	hashPositions := make([]int, n)

	numWorkers := runtime.NumCPU()
	if numWorkers < 1 {
		numWorkers = 1
	}

	var findErr error
	var findErrOnce sync.Once
	var wg sync.WaitGroup

	chunkSize := (n + numWorkers - 1) / numWorkers
	for w := range numWorkers {
		start := w * chunkSize
		end := start + chunkSize
		if end > n {
			end = n
		}
		if start >= n {
			break
		}

		wg.Add(1)
		go func(start, end int) {
			defer wg.Done()
			for i := start; i < end; i++ {
				hashVal := mph.Find(b.hashes[i])
				if hashVal == 0 {
					findErrOnce.Do(func() {
						findErr = fmt.Errorf("MPHF lookup failed for prefix at index %d", i)
					})
					return
				}
				hashPositions[i] = int(hashVal - 1)
			}
		}(start, end)
	}
	wg.Wait()

	if findErr != nil {
		return nil, findErr
	}

	return hashPositions, nil
}

// prefixChunkItem holds data for one prefix during parallel processing.
type prefixChunkItem struct {
	index       int    // Original index in the prefix sequence
	prefixBytes []byte // The prefix data (shared slice into chunk buffer)
	offset      uint64 // Cumulative offset in temp file
}

// computeFingerprintsParallel reads prefixes from the temp file and computes
// fingerprints in parallel using a worker pool with chunked processing.
//
//nolint:gocognit // Parallel processing with chunked I/O requires complex control flow
func (b *StreamingMPHFBuilder) computeFingerprintsParallel(
	reader *bufio.Reader,
	mph *bbhash.BBHash2,
	n int,
	fingerprints []uint64,
	preorderPositions []uint64,
	orderedPrefixOffsets []uint64,
) error {
	// Determine number of workers (use all available CPUs)
	numWorkers := runtime.NumCPU()
	if numWorkers < 1 {
		numWorkers = 1
	}

	// Chunk size: process 50K prefixes per batch to balance memory vs parallelism
	const chunkSize = 50000

	// Channels for work distribution
	type workItem struct {
		items []prefixChunkItem
	}
	workChan := make(chan workItem, numWorkers*2)
	errChan := make(chan error, numWorkers)

	// Start workers
	var wg sync.WaitGroup
	for range numWorkers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for work := range workChan {
				for _, item := range work.items {
					// Compute hash for MPHF lookup
					keyHash := hashBytes(item.prefixBytes)
					hashVal := mph.Find(keyHash)
					if hashVal == 0 {
						select {
						case errChan <- fmt.Errorf("MPHF lookup failed for prefix at index %d", item.index):
						default:
						}
						return
					}
					hashPos := int(hashVal - 1)

					// Store results (each hashPos is unique, so no race)
					fingerprints[hashPos] = computeFingerprintBytes(item.prefixBytes)
					preorderPositions[hashPos] = b.preorderPos[item.index]
					orderedPrefixOffsets[hashPos] = item.offset
				}
			}
		}()
	}

	// Read prefixes in chunks and send to workers
	var lenBuf [4]byte
	currentOffset := uint64(0)
	processed := 0

	// Estimate average prefix length for buffer sizing (paths are typically 20-40 bytes)
	const estimatedAvgPrefixLen = 24

	for processed < n {
		// Determine chunk size for this iteration
		remaining := n - processed
		thisChunk := chunkSize
		if remaining < thisChunk {
			thisChunk = remaining
		}

		// Pre-allocate a single contiguous buffer for all prefixes in this chunk.
		// This eliminates per-prefix allocations - we'll use slices into this buffer.
		chunkBuffer := make([]byte, 0, thisChunk*estimatedAvgPrefixLen)
		items := make([]prefixChunkItem, 0, thisChunk)

		for range thisChunk {
			// Read prefix length
			if _, err := io.ReadFull(reader, lenBuf[:]); err != nil {
				close(workChan)
				wg.Wait()
				return fmt.Errorf("read prefix length at %d: %w", processed, err)
			}
			prefixLen := binary.LittleEndian.Uint32(lenBuf[:])

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
			if _, err := io.ReadFull(reader, chunkBuffer[start:]); err != nil {
				close(workChan)
				wg.Wait()
				return fmt.Errorf("read prefix at %d: %w", processed, err)
			}

			// Create item with slice into the contiguous buffer (no allocation)
			items = append(items, prefixChunkItem{
				index:       processed,
				prefixBytes: chunkBuffer[start : start+int(prefixLen)],
				offset:      currentOffset,
			})

			currentOffset += uint64(4 + prefixLen)
			processed++
		}

		// Send chunk to workers
		workChan <- workItem{items: items}

		// Check for worker errors
		select {
		case err := <-errChan:
			close(workChan)
			wg.Wait()
			return err
		default:
		}
	}

	// Close work channel and wait for workers to finish
	close(workChan)
	wg.Wait()

	// Check for any final errors
	select {
	case err := <-errChan:
		return err
	default:
	}

	return nil
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

func (b *StreamingMPHFBuilder) writeEmpty(outDir string) error {
	// Create empty mph file
	mphPath := filepath.Join(outDir, "mph.bin")
	if err := os.WriteFile(mphPath, nil, 0o644); err != nil {
		return fmt.Errorf("write empty mph: %w", err)
	}

	// Create empty fingerprint array
	fpPath := filepath.Join(outDir, "mph_fp.u64")
	fpWriter, err := NewArrayWriter(fpPath, 8)
	if err != nil {
		return fmt.Errorf("create empty fingerprint writer: %w", err)
	}
	if err := fpWriter.Close(); err != nil {
		return fmt.Errorf("close empty fingerprint writer: %w", err)
	}

	// Create empty position array
	posPath := filepath.Join(outDir, "mph_pos.u64")
	posWriter, err := NewArrayWriter(posPath, 8)
	if err != nil {
		return fmt.Errorf("create empty position writer: %w", err)
	}
	if err := posWriter.Close(); err != nil {
		return fmt.Errorf("close empty position writer: %w", err)
	}

	// Create empty prefix blob files
	blobPath := filepath.Join(outDir, "prefix_blob.bin")
	offsetsPath := filepath.Join(outDir, "prefix_offsets.u64")
	writer, err := NewBlobWriter(blobPath, offsetsPath)
	if err != nil {
		return fmt.Errorf("create empty blob writer: %w", err)
	}
	return writer.Close()
}

// writeArraysParallel writes fingerprints and positions arrays in parallel.
// Since these are independent files, parallel writes can utilize disk bandwidth better.
// Uses WriteU64Batch for efficient bulk writes.
func writeArraysParallel(outDir string, fingerprints, positions []uint64) error {
	var wg sync.WaitGroup
	var fpErr, posErr error

	wg.Add(2)

	// Write fingerprints
	go func() {
		defer wg.Done()
		fpPath := filepath.Join(outDir, "mph_fp.u64")
		fpWriter, err := NewArrayWriter(fpPath, 8)
		if err != nil {
			fpErr = fmt.Errorf("create fingerprint writer: %w", err)
			return
		}
		if err := fpWriter.WriteU64Batch(fingerprints); err != nil {
			fpWriter.Close()
			fpErr = fmt.Errorf("write fingerprints: %w", err)
			return
		}
		if err := fpWriter.Close(); err != nil {
			fpErr = fmt.Errorf("close fingerprint writer: %w", err)
		}
	}()

	// Write positions
	go func() {
		defer wg.Done()
		posPath := filepath.Join(outDir, "mph_pos.u64")
		posWriter, err := NewArrayWriter(posPath, 8)
		if err != nil {
			posErr = fmt.Errorf("create position writer: %w", err)
			return
		}
		if err := posWriter.WriteU64Batch(positions); err != nil {
			posWriter.Close()
			posErr = fmt.Errorf("write positions: %w", err)
			return
		}
		if err := posWriter.Close(); err != nil {
			posErr = fmt.Errorf("close position writer: %w", err)
		}
	}()

	wg.Wait()

	if fpErr != nil {
		return fpErr
	}
	return posErr
}
