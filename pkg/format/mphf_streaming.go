package format

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"

	"github.com/relab/bbhash"
)

// StreamingMPHFBuilder builds a minimal perfect hash function for prefix strings
// while keeping memory usage bounded by writing prefixes to disk during construction.
//
// Memory usage:
// - ~8 bytes per prefix for hash keys (unavoidable for bbhash)
// - ~8 bytes per prefix for preorder positions
// - Temporary buffers for I/O
//
// This is much more memory efficient than MPHFBuilder which stores all
// prefix strings in memory (~50+ bytes per prefix average).
type StreamingMPHFBuilder struct {
	// In-memory: only hashes and positions (16 bytes per prefix total)
	hashes      []uint64
	preorderPos []uint64

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
		hashes:      make([]uint64, 0, 1024),
		preorderPos: make([]uint64, 0, 1024),
		tempFile:    tempFile,
		tempWriter:  bufio.NewWriterSize(tempFile, 1024*1024), // 1MB buffer
		tempPath:    tempFile.Name(),
		bufferSize:  1024 * 1024,
	}, nil
}

// Add adds a prefix at the given preorder position.
// The prefix is written to disk immediately; only the hash is kept in memory.
func (b *StreamingMPHFBuilder) Add(prefix string, pos uint64) error {
	// Store hash in memory (8 bytes)
	b.hashes = append(b.hashes, hashString(prefix))

	// Store position in memory (8 bytes)
	b.preorderPos = append(b.preorderPos, pos)

	// Write prefix to temp file with length prefix
	// Format: [4-byte length][prefix bytes]
	prefixBytes := []byte(prefix)
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
func (b *StreamingMPHFBuilder) Build(outDir string) error {
	if b.count == 0 {
		return b.writeEmpty(outDir)
	}

	// Flush and close temp writer
	if err := b.tempWriter.Flush(); err != nil {
		return fmt.Errorf("flush temp file: %w", err)
	}

	// Build MPHF with gamma=2.0 (good space/time tradeoff)
	mph, err := bbhash.New(b.hashes, bbhash.Gamma(2.0))
	if err != nil {
		return fmt.Errorf("build MPHF: %w", err)
	}

	// Free hashes now that MPHF is built
	b.hashes = nil
	runtime.GC()

	// Write MPHF
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

	// Create output arrays - preallocate at correct size
	n := int(b.count)
	fingerprints := make([]uint64, n)
	preorderPositions := make([]uint64, n)
	orderedPrefixOffsets := make([]uint64, n) // For rebuilding prefix order

	// Seek to start of temp file to re-read prefixes
	if _, err := b.tempFile.Seek(0, 0); err != nil {
		return fmt.Errorf("seek temp file: %w", err)
	}
	reader := bufio.NewReaderSize(b.tempFile, b.bufferSize)

	// Stream through prefixes, compute fingerprints and positions
	var lenBuf [4]byte
	currentOffset := uint64(0)

	for i := range n {
		// Read prefix length
		if _, err := io.ReadFull(reader, lenBuf[:]); err != nil {
			return fmt.Errorf("read prefix length at %d: %w", i, err)
		}
		prefixLen := binary.LittleEndian.Uint32(lenBuf[:])

		// Read prefix
		prefixBuf := make([]byte, prefixLen)
		if _, err := io.ReadFull(reader, prefixBuf); err != nil {
			return fmt.Errorf("read prefix at %d: %w", i, err)
		}
		prefix := string(prefixBuf)

		// Look up hash position
		keyHash := hashString(prefix)
		hashVal := mph.Find(keyHash)
		if hashVal == 0 {
			return fmt.Errorf("MPHF lookup failed for prefix at index %d", i)
		}
		hashPos := int(hashVal - 1) // Convert to 0-indexed

		// Store fingerprint, position, and offset at hash position
		fingerprints[hashPos] = computeFingerprint(prefix)
		preorderPositions[hashPos] = b.preorderPos[i]
		orderedPrefixOffsets[hashPos] = currentOffset

		currentOffset += uint64(4 + prefixLen) // Length prefix + string
	}

	// Free preorderPos now
	b.preorderPos = nil

	// Write fingerprints
	fpPath := filepath.Join(outDir, "mph_fp.u64")
	fpWriter, err := NewArrayWriter(fpPath, 8)
	if err != nil {
		return fmt.Errorf("create fingerprint writer: %w", err)
	}
	for _, fp := range fingerprints {
		if err := fpWriter.WriteU64(fp); err != nil {
			fpWriter.Close()
			return fmt.Errorf("write fingerprint: %w", err)
		}
	}
	if err := fpWriter.Close(); err != nil {
		return fmt.Errorf("close fingerprint writer: %w", err)
	}

	// Write preorder positions
	posPath := filepath.Join(outDir, "mph_pos.u64")
	posWriter, err := NewArrayWriter(posPath, 8)
	if err != nil {
		return fmt.Errorf("create position writer: %w", err)
	}
	for _, p := range preorderPositions {
		if err := posWriter.WriteU64(p); err != nil {
			posWriter.Close()
			return fmt.Errorf("write preorder position: %w", err)
		}
	}
	if err := posWriter.Close(); err != nil {
		return fmt.Errorf("close position writer: %w", err)
	}

	// Now write prefix blob in MPHF-ordered sequence
	// We need to re-read prefixes in hash order
	if err := b.writePrefixBlobOrdered(outDir, mph, orderedPrefixOffsets); err != nil {
		return fmt.Errorf("write prefix blob: %w", err)
	}

	return nil
}

// writePrefixBlobOrdered writes prefixes in preorder (not hash order) for GetPrefix.
// This requires re-reading from temp file.
func (b *StreamingMPHFBuilder) writePrefixBlobOrdered(outDir string, _ *bbhash.BBHash2, _ []uint64) error {
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

	for i := range n {
		// Read prefix length
		if _, err := io.ReadFull(reader, lenBuf[:]); err != nil {
			writer.Close()
			return fmt.Errorf("read prefix length at %d: %w", i, err)
		}
		prefixLen := binary.LittleEndian.Uint32(lenBuf[:])

		// Read prefix
		prefixBuf := make([]byte, prefixLen)
		if _, err := io.ReadFull(reader, prefixBuf); err != nil {
			writer.Close()
			return fmt.Errorf("read prefix at %d: %w", i, err)
		}

		// Write to blob
		if err := writer.WriteString(string(prefixBuf)); err != nil {
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
