// Package fileutil provides file utilities for resumable builds with tmp+mv semantics.
package fileutil

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/eunmann/s3-inv-db/pkg/logging"
)

// Header format constants (must match pkg/format/format.go)
const (
	headerMagicNumber = 0x53334944 // "S3ID"
	headerVersion     = 1
	headerSize        = 24 // 4+4+8+4+4 bytes
)

// Exists returns true if the file exists.
func Exists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// IsNonEmpty returns true if the file exists and has non-zero size.
func IsNonEmpty(path string) bool {
	info, err := os.Stat(path)
	if err != nil {
		return false
	}
	return info.Size() > 0
}

// ColumnFileValid checks if a columnar array file exists and has valid header/size.
// Returns true if the file is valid and can be skipped during resume.
func ColumnFileValid(path string, expectedN uint64, expectedWidth uint32) bool {
	f, err := os.Open(path)
	if err != nil {
		return false
	}
	defer f.Close()

	// Read header
	headerBuf := make([]byte, headerSize)
	n, err := f.Read(headerBuf)
	if err != nil || n < headerSize {
		return false
	}

	// Parse header: magic(4) + version(4) + count(8) + width(4) + reserved(4)
	magic := binary.LittleEndian.Uint32(headerBuf[0:4])
	version := binary.LittleEndian.Uint32(headerBuf[4:8])
	count := binary.LittleEndian.Uint64(headerBuf[8:16])
	width := binary.LittleEndian.Uint32(headerBuf[16:20])

	// Validate header fields
	if magic != headerMagicNumber {
		return false
	}
	if version != headerVersion {
		return false
	}
	if count != expectedN {
		return false
	}
	if width != expectedWidth {
		return false
	}

	// Check file size matches expected
	info, err := f.Stat()
	if err != nil {
		return false
	}
	expectedSize := int64(headerSize) + int64(expectedN)*int64(expectedWidth)
	return info.Size() == expectedSize
}

// BlobFileValid checks if a blob file and its offsets file are valid.
// offsetsN should be N+1 (includes sentinel).
func BlobFileValid(blobPath, offsetsPath string, offsetsN uint64) bool {
	// Check offsets file is valid
	if !ColumnFileValid(offsetsPath, offsetsN, 8) {
		return false
	}

	// Check blob file exists and is non-empty (or empty if offsetsN == 1, meaning 0 strings)
	if offsetsN == 1 {
		return Exists(blobPath)
	}

	// Read last offset from offsets file to verify blob size
	f, err := os.Open(offsetsPath)
	if err != nil {
		return false
	}
	defer f.Close()

	// Seek to last offset value
	lastOffsetPos := int64(headerSize) + int64(offsetsN-1)*8
	if _, err := f.Seek(lastOffsetPos, 0); err != nil {
		return false
	}

	var lastOffsetBuf [8]byte
	if _, err := f.Read(lastOffsetBuf[:]); err != nil {
		return false
	}
	lastOffset := binary.LittleEndian.Uint64(lastOffsetBuf[:])

	// Check blob file size matches last offset
	blobInfo, err := os.Stat(blobPath)
	if err != nil {
		return false
	}
	return blobInfo.Size() == int64(lastOffset)
}

// WriteTmpThenMove writes to a temporary file then atomically moves it to the final path.
// The writeFunc receives the temporary path and should write the complete file.
// On success, the file is moved to outPath atomically.
func WriteTmpThenMove(tmpDir, outPath string, writeFunc func(tmpPath string) error) error {
	// Ensure tmp directory exists
	if err := os.MkdirAll(tmpDir, 0755); err != nil {
		return fmt.Errorf("create tmp dir: %w", err)
	}

	// Create temp file path
	tmpPath := filepath.Join(tmpDir, filepath.Base(outPath)+".tmp")

	// Write to temp file
	if err := writeFunc(tmpPath); err != nil {
		os.Remove(tmpPath) // Clean up on error
		return err
	}

	// Fsync the temp file
	if err := syncFile(tmpPath); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("sync temp file: %w", err)
	}

	// Ensure output directory exists
	outDir := filepath.Dir(outPath)
	if err := os.MkdirAll(outDir, 0755); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("create output dir: %w", err)
	}

	// Atomic move
	if err := os.Rename(tmpPath, outPath); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("rename temp to final: %w", err)
	}

	return nil
}

// syncFile opens, syncs, and closes a file.
func syncFile(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	err = f.Sync()
	f.Close()
	return err
}

// CleanupTmpFiles removes all .tmp files in the given directory recursively.
func CleanupTmpFiles(dir string) error {
	log := logging.L()

	var removed int
	err := filepath.Walk(dir, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			// Continue walking even if individual paths fail
			return nil //nolint:nilerr
		}
		if !info.IsDir() && strings.HasSuffix(path, ".tmp") {
			if rmErr := os.Remove(path); rmErr == nil {
				removed++
			}
		}
		return nil
	})

	if removed > 0 {
		log.Debug().Int("files_removed", removed).Str("dir", dir).Msg("cleaned up tmp files")
	}

	return err
}

// MPHFFilesValid checks if all MPHF-related files exist and are valid.
func MPHFFilesValid(outDir string, prefixCount uint64) bool {
	mphPath := filepath.Join(outDir, "mph.bin")
	fpPath := filepath.Join(outDir, "mph_fp.u64")
	posPath := filepath.Join(outDir, "mph_pos.u64")
	blobPath := filepath.Join(outDir, "prefix_blob.bin")
	offsetsPath := filepath.Join(outDir, "prefix_offsets.u64")

	// Check mph.bin exists (can be empty for 0 prefixes)
	if !Exists(mphPath) {
		return false
	}

	// For non-empty prefix sets, validate the supporting files
	if prefixCount == 0 {
		// Empty case: files should exist but may be minimal
		return Exists(fpPath) && Exists(posPath) && Exists(blobPath) && Exists(offsetsPath)
	}

	// Fingerprints and positions should have prefixCount elements
	if !ColumnFileValid(fpPath, prefixCount, 8) {
		return false
	}
	if !ColumnFileValid(posPath, prefixCount, 8) {
		return false
	}

	// Blob should have prefixCount strings (offsets has prefixCount+1)
	if !BlobFileValid(blobPath, offsetsPath, prefixCount+1) {
		return false
	}

	return true
}

// DepthIndexValid checks if depth index files are valid.
func DepthIndexValid(outDir string, maxDepth uint32, nodeCount uint64) bool {
	offsetsPath := filepath.Join(outDir, "depth_offsets.u64")
	positionsPath := filepath.Join(outDir, "depth_positions.u64")

	// Offsets has maxDepth+2 elements (0..maxDepth + sentinel)
	if !ColumnFileValid(offsetsPath, uint64(maxDepth)+2, 8) {
		return false
	}

	// Positions has nodeCount elements
	if !ColumnFileValid(positionsPath, nodeCount, 8) {
		return false
	}

	return true
}
