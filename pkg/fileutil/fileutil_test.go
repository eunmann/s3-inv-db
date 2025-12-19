package fileutil

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
)

// Local header type for tests (matches pkg/format/format.go)
type testHeader struct {
	Magic    uint32
	Version  uint32
	Count    uint64
	Width    uint32
	Reserved uint32
}

func encodeTestHeader(h testHeader) []byte {
	buf := make([]byte, headerSize)
	binary.LittleEndian.PutUint32(buf[0:4], h.Magic)
	binary.LittleEndian.PutUint32(buf[4:8], h.Version)
	binary.LittleEndian.PutUint64(buf[8:16], h.Count)
	binary.LittleEndian.PutUint32(buf[16:20], h.Width)
	binary.LittleEndian.PutUint32(buf[20:24], h.Reserved)
	return buf
}

func TestExists(t *testing.T) {
	tmpDir := t.TempDir()

	// Test non-existent file
	if Exists(filepath.Join(tmpDir, "nonexistent")) {
		t.Error("Exists returned true for non-existent file")
	}

	// Test existing file
	path := filepath.Join(tmpDir, "exists.txt")
	if err := os.WriteFile(path, []byte("content"), 0644); err != nil {
		t.Fatal(err)
	}
	if !Exists(path) {
		t.Error("Exists returned false for existing file")
	}
}

func TestIsNonEmpty(t *testing.T) {
	tmpDir := t.TempDir()

	// Test non-existent file
	if IsNonEmpty(filepath.Join(tmpDir, "nonexistent")) {
		t.Error("IsNonEmpty returned true for non-existent file")
	}

	// Test empty file
	emptyPath := filepath.Join(tmpDir, "empty.txt")
	if err := os.WriteFile(emptyPath, []byte{}, 0644); err != nil {
		t.Fatal(err)
	}
	if IsNonEmpty(emptyPath) {
		t.Error("IsNonEmpty returned true for empty file")
	}

	// Test non-empty file
	nonEmptyPath := filepath.Join(tmpDir, "nonempty.txt")
	if err := os.WriteFile(nonEmptyPath, []byte("content"), 0644); err != nil {
		t.Fatal(err)
	}
	if !IsNonEmpty(nonEmptyPath) {
		t.Error("IsNonEmpty returned false for non-empty file")
	}
}

func TestColumnFileValid(t *testing.T) {
	tmpDir := t.TempDir()

	// Test non-existent file
	if ColumnFileValid(filepath.Join(tmpDir, "nonexistent"), 10, 8) {
		t.Error("ColumnFileValid returned true for non-existent file")
	}

	// Create a valid column file (u64)
	validPath := filepath.Join(tmpDir, "valid.u64")
	createValidColumnFile(t, validPath, 10, 8)
	if !ColumnFileValid(validPath, 10, 8) {
		t.Error("ColumnFileValid returned false for valid file")
	}

	// Create a valid column file (u32)
	validU32Path := filepath.Join(tmpDir, "valid.u32")
	createValidColumnFile(t, validU32Path, 20, 4)
	if !ColumnFileValid(validU32Path, 20, 4) {
		t.Error("ColumnFileValid returned false for valid u32 file")
	}

	// Test with wrong count
	if ColumnFileValid(validPath, 5, 8) {
		t.Error("ColumnFileValid returned true for wrong count")
	}

	// Test with wrong width
	if ColumnFileValid(validPath, 10, 4) {
		t.Error("ColumnFileValid returned true for wrong width")
	}

	// Test file with wrong size (truncated)
	truncatedPath := filepath.Join(tmpDir, "truncated.u64")
	createValidColumnFile(t, truncatedPath, 10, 8)
	// Truncate the file
	if err := os.Truncate(truncatedPath, headerSize+5*8); err != nil {
		t.Fatal(err)
	}
	if ColumnFileValid(truncatedPath, 10, 8) {
		t.Error("ColumnFileValid returned true for truncated file")
	}

	// Test file with bad magic
	badMagicPath := filepath.Join(tmpDir, "badmagic.u64")
	createColumnFileWithBadMagic(t, badMagicPath, 10, 8)
	if ColumnFileValid(badMagicPath, 10, 8) {
		t.Error("ColumnFileValid returned true for file with bad magic")
	}
}

func TestBlobFileValid(t *testing.T) {
	tmpDir := t.TempDir()

	// Test with non-existent offsets file
	if BlobFileValid(
		filepath.Join(tmpDir, "blob.bin"),
		filepath.Join(tmpDir, "offsets.u64"),
		11,
	) {
		t.Error("BlobFileValid returned true for non-existent files")
	}

	// Create valid blob and offsets files
	blobPath := filepath.Join(tmpDir, "valid_blob.bin")
	offsetsPath := filepath.Join(tmpDir, "valid_offsets.u64")
	createValidBlobFiles(t, blobPath, offsetsPath, 10)
	if !BlobFileValid(blobPath, offsetsPath, 11) {
		t.Error("BlobFileValid returned false for valid files")
	}

	// Test with empty blob (0 strings, offsetsN=1)
	emptyBlobPath := filepath.Join(tmpDir, "empty_blob.bin")
	emptyOffsetsPath := filepath.Join(tmpDir, "empty_offsets.u64")
	createValidColumnFile(t, emptyOffsetsPath, 1, 8) // Just the sentinel
	if err := os.WriteFile(emptyBlobPath, []byte{}, 0644); err != nil {
		t.Fatal(err)
	}
	if !BlobFileValid(emptyBlobPath, emptyOffsetsPath, 1) {
		t.Error("BlobFileValid returned false for empty blob")
	}
}

func TestWriteTmpThenMove(t *testing.T) {
	tmpDir := t.TempDir()
	outDir := t.TempDir()
	outPath := filepath.Join(outDir, "output.txt")

	// Test successful write
	content := []byte("test content")
	err := WriteTmpThenMove(tmpDir, outPath, func(tmpPath string) error {
		return os.WriteFile(tmpPath, content, 0644)
	})
	if err != nil {
		t.Fatalf("WriteTmpThenMove failed: %v", err)
	}

	// Verify output file exists with correct content
	got, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("Failed to read output file: %v", err)
	}
	if string(got) != string(content) {
		t.Errorf("Content mismatch: got %q, want %q", got, content)
	}

	// Verify tmp file doesn't exist
	tmpPath := filepath.Join(tmpDir, "output.txt.tmp")
	if Exists(tmpPath) {
		t.Error("Tmp file still exists after successful write")
	}
}

func TestWriteTmpThenMoveError(t *testing.T) {
	tmpDir := t.TempDir()
	outDir := t.TempDir()
	outPath := filepath.Join(outDir, "output.txt")

	// Test write function error
	err := WriteTmpThenMove(tmpDir, outPath, func(tmpPath string) error {
		return os.ErrPermission
	})
	if err == nil {
		t.Error("WriteTmpThenMove should have failed")
	}

	// Verify tmp file doesn't exist (cleaned up)
	tmpPath := filepath.Join(tmpDir, "output.txt.tmp")
	if Exists(tmpPath) {
		t.Error("Tmp file exists after failed write")
	}

	// Verify output file doesn't exist
	if Exists(outPath) {
		t.Error("Output file exists after failed write")
	}
}

func TestCleanupTmpFiles(t *testing.T) {
	tmpDir := t.TempDir()

	// Create some .tmp files and regular files
	tmpFile1 := filepath.Join(tmpDir, "file1.tmp")
	tmpFile2 := filepath.Join(tmpDir, "subdir", "file2.tmp")
	regularFile := filepath.Join(tmpDir, "regular.txt")

	if err := os.MkdirAll(filepath.Join(tmpDir, "subdir"), 0755); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{tmpFile1, tmpFile2, regularFile} {
		if err := os.WriteFile(path, []byte("content"), 0644); err != nil {
			t.Fatal(err)
		}
	}

	if err := CleanupTmpFiles(tmpDir); err != nil {
		t.Fatalf("CleanupTmpFiles failed: %v", err)
	}

	// Verify .tmp files are removed
	if Exists(tmpFile1) {
		t.Error("tmpFile1 still exists")
	}
	if Exists(tmpFile2) {
		t.Error("tmpFile2 still exists")
	}

	// Verify regular file still exists
	if !Exists(regularFile) {
		t.Error("regularFile was removed")
	}
}

func TestMPHFFilesValid(t *testing.T) {
	tmpDir := t.TempDir()

	// Test with missing files
	if MPHFFilesValid(tmpDir, 10) {
		t.Error("MPHFFilesValid returned true for missing files")
	}

	// Create valid MPHF files
	createValidMPHFFiles(t, tmpDir, 10)
	if !MPHFFilesValid(tmpDir, 10) {
		t.Error("MPHFFilesValid returned false for valid files")
	}

	// Test with wrong count
	if MPHFFilesValid(tmpDir, 5) {
		t.Error("MPHFFilesValid returned true for wrong count")
	}
}

func TestDepthIndexValid(t *testing.T) {
	tmpDir := t.TempDir()

	// Test with missing files
	if DepthIndexValid(tmpDir, 5, 100) {
		t.Error("DepthIndexValid returned true for missing files")
	}

	// Create valid depth index files
	createValidDepthIndexFiles(t, tmpDir, 5, 100)
	if !DepthIndexValid(tmpDir, 5, 100) {
		t.Error("DepthIndexValid returned false for valid files")
	}

	// Test with wrong maxDepth
	if DepthIndexValid(tmpDir, 3, 100) {
		t.Error("DepthIndexValid returned true for wrong maxDepth")
	}

	// Test with wrong nodeCount
	if DepthIndexValid(tmpDir, 5, 50) {
		t.Error("DepthIndexValid returned true for wrong nodeCount")
	}
}

// Helper functions to create test files

func createValidColumnFile(t *testing.T, path string, count uint64, width uint32) {
	t.Helper()

	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	// Write header
	header := testHeader{
		Magic:   headerMagicNumber,
		Version: headerVersion,
		Count:   count,
		Width:   width,
	}
	headerBuf := encodeTestHeader(header)
	if _, err := f.Write(headerBuf); err != nil {
		t.Fatal(err)
	}

	// Write data
	data := make([]byte, count*uint64(width))
	if _, err := f.Write(data); err != nil {
		t.Fatal(err)
	}
}

func createColumnFileWithBadMagic(t *testing.T, path string, count uint64, width uint32) {
	t.Helper()

	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	// Write header with bad magic
	header := testHeader{
		Magic:   0xDEADBEEF, // Bad magic
		Version: headerVersion,
		Count:   count,
		Width:   width,
	}
	headerBuf := encodeTestHeader(header)
	if _, err := f.Write(headerBuf); err != nil {
		t.Fatal(err)
	}

	// Write data
	data := make([]byte, count*uint64(width))
	if _, err := f.Write(data); err != nil {
		t.Fatal(err)
	}
}

func createValidBlobFiles(t *testing.T, blobPath, offsetsPath string, n int) {
	t.Helper()

	// Create offsets file with n+1 entries (includes sentinel)
	offsetCount := uint64(n + 1)
	createValidColumnFile(t, offsetsPath, offsetCount, 8)

	// Update offsets with actual values
	f, err := os.OpenFile(offsetsPath, os.O_RDWR, 0644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	// Seek past header and write offsets
	if _, err := f.Seek(headerSize, 0); err != nil {
		t.Fatal(err)
	}

	totalBlobSize := uint64(0)
	for i := 0; i <= n; i++ {
		var buf [8]byte
		binary.LittleEndian.PutUint64(buf[:], totalBlobSize)
		if _, err := f.Write(buf[:]); err != nil {
			t.Fatal(err)
		}
		if i < n {
			totalBlobSize += 10 // Each string is 10 bytes
		}
	}

	// Create blob file with correct size
	blobData := make([]byte, totalBlobSize)
	if err := os.WriteFile(blobPath, blobData, 0644); err != nil {
		t.Fatal(err)
	}
}

func createValidMPHFFiles(t *testing.T, outDir string, prefixCount uint64) {
	t.Helper()

	// Create mph.bin (non-empty)
	mphPath := filepath.Join(outDir, "mph.bin")
	if err := os.WriteFile(mphPath, []byte("mph data"), 0644); err != nil {
		t.Fatal(err)
	}

	// Create fingerprints (prefixCount elements, 8 bytes each)
	fpPath := filepath.Join(outDir, "mph_fp.u64")
	createValidColumnFile(t, fpPath, prefixCount, 8)

	// Create positions (prefixCount elements, 8 bytes each)
	posPath := filepath.Join(outDir, "mph_pos.u64")
	createValidColumnFile(t, posPath, prefixCount, 8)

	// Create blob files (prefixCount strings)
	blobPath := filepath.Join(outDir, "prefix_blob.bin")
	offsetsPath := filepath.Join(outDir, "prefix_offsets.u64")
	createValidBlobFiles(t, blobPath, offsetsPath, int(prefixCount))
}

func createValidDepthIndexFiles(t *testing.T, outDir string, maxDepth uint32, nodeCount uint64) {
	t.Helper()

	// Create offsets (maxDepth+2 elements)
	offsetsPath := filepath.Join(outDir, "depth_offsets.u64")
	createValidColumnFile(t, offsetsPath, uint64(maxDepth)+2, 8)

	// Create positions (nodeCount elements)
	positionsPath := filepath.Join(outDir, "depth_positions.u64")
	createValidColumnFile(t, positionsPath, nodeCount, 8)
}
