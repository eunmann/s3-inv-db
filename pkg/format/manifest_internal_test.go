package format

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"
)

func TestWriteAndReadManifest(t *testing.T) {
	dir := t.TempDir()

	// Create the two artifact files the manifest enumerator always
	// includes; their names must match requiredTopLevel.
	testFiles := map[string][]byte{
		CoreStatsFile: []byte("test data 1"),
		MPHFSlotsFile: []byte("test data 2"),
	}

	for name, data := range testFiles {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatalf("WriteFile failed: %v", err)
		}
	}

	// Write manifest
	if _, err := WriteManifest(dir, 100, 5, nil, nil); err != nil {
		t.Fatalf("WriteManifest failed: %v", err)
	}

	// Read manifest
	manifest, err := ReadManifest(dir)
	if err != nil {
		t.Fatalf("ReadManifest failed: %v", err)
	}

	// Verify fields
	if manifest.Version != ManifestVersion {
		t.Errorf("Version = %d, want %d", manifest.Version, ManifestVersion)
	}
	if manifest.NodeCount != 100 {
		t.Errorf("NodeCount = %d, want 100", manifest.NodeCount)
	}
	if manifest.MaxDepth != 5 {
		t.Errorf("MaxDepth = %d, want 5", manifest.MaxDepth)
	}

	// Check files
	for name, data := range testFiles {
		info, ok := manifest.Files[name]
		if !ok {
			t.Errorf("File %q not in manifest", name)

			continue
		}
		if info.Size != int64(len(data)) {
			t.Errorf("File %q size = %d, want %d", name, info.Size, len(data))
		}
		if info.Checksum == "" {
			t.Errorf("File %q has empty checksum", name)
		}
	}
}

func TestVerifyManifest(t *testing.T) {
	dir := t.TempDir()

	// Create test files
	path := filepath.Join(dir, CoreStatsFile)
	data := []byte("test data for verification")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	// Write manifest
	if _, err := WriteManifest(dir, 50, 3, nil, nil); err != nil {
		t.Fatalf("WriteManifest failed: %v", err)
	}

	// Read manifest
	manifest, err := ReadManifest(dir)
	if err != nil {
		t.Fatalf("ReadManifest failed: %v", err)
	}

	// Verify should pass
	if err := VerifyManifest(dir, manifest); err != nil {
		t.Errorf("VerifyManifest failed: %v", err)
	}

	// Corrupt the file
	if err := os.WriteFile(path, []byte("corrupted data"), 0o600); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	// Verify should now fail
	if err := VerifyManifest(dir, manifest); err == nil {
		t.Error("VerifyManifest should fail after corruption")
	}
}

// testPiece is the checksum piece size the tests hash with, small so a
// multi-piece file stays small.
const testPiece = 4 << 10

// checksumOf returns checksumFiles's checksum of one file in dir.
func checksumOf(t *testing.T, dir, name string) string {
	t.Helper()
	info, err := os.Stat(filepath.Join(dir, name))
	if err != nil {
		t.Fatal(err)
	}
	sums, err := checksumFiles(dir, map[string]int64{name: info.Size()}, testPiece, nil)
	if err != nil {
		t.Fatalf("checksumFiles: %v", err)
	}

	return sums[name]
}

func TestChecksumFilesDeterministic(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.bin")
	if err := os.WriteFile(path, []byte("hello world"), 0o600); err != nil {
		t.Fatal(err)
	}
	first, second := checksumOf(t, dir, "test.bin"), checksumOf(t, dir, "test.bin")
	if first != second {
		t.Error("checksum not deterministic")
	}
	if err := os.WriteFile(path, []byte("different"), 0o600); err != nil {
		t.Fatal(err)
	}
	if checksumOf(t, dir, "test.bin") == first {
		t.Error("different data gave same checksum")
	}
}

// TestChecksumFilesPieces pins the checksum's definition: the CRC-32C
// of the pieces' CRC-32Cs, each 4 bytes big-endian, as 8 hex digits;
// here two full pieces and one byte.
func TestChecksumFilesPieces(t *testing.T) {
	table := crc32.MakeTable(crc32.Castagnoli)
	if got := crc32.Checksum([]byte("123456789"), table); got != 0xe3069283 {
		t.Fatalf("CRC-32C check value = %08x, want e3069283", got)
	}
	dir := t.TempDir()
	data := bytes.Repeat([]byte{0xa5}, 2*testPiece+1)
	data[testPiece] = 1 // the pieces differ
	if err := os.WriteFile(filepath.Join(dir, "big.bin"), data, 0o600); err != nil {
		t.Fatal(err)
	}
	var digests []byte
	for off := 0; off < len(data); off += testPiece {
		digests = binary.BigEndian.AppendUint32(digests, crc32.Checksum(data[off:min(off+testPiece, len(data))], table))
	}
	if got, want := checksumOf(t, dir, "big.bin"), fmt.Sprintf("%08x", crc32.Checksum(digests, table)); got != want {
		t.Errorf("checksum = %s, want %s", got, want)
	}

	if err := os.WriteFile(filepath.Join(dir, "empty.bin"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	wantEmpty := fmt.Sprintf("%08x", crc32.Checksum(make([]byte, 4), table))
	if got := checksumOf(t, dir, "empty.bin"); got != wantEmpty {
		t.Errorf("empty file checksum = %s, want %s", got, wantEmpty)
	}
}

// TestVerifyManifestDetectsFlippedByte: one changed bit, the size
// unchanged, fails verification by its checksum.
func TestVerifyManifestDetectsFlippedByte(t *testing.T) {
	dir := t.TempDir()
	const size = 3 << 20
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i * 31)
	}
	if err := os.WriteFile(filepath.Join(dir, CoreStatsFile), data, FilePerm); err != nil {
		t.Fatal(err)
	}
	if _, err := WriteManifest(dir, 1, 1, nil, nil); err != nil {
		t.Fatal(err)
	}
	m, err := ReadManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifyManifest(dir, m); err != nil {
		t.Fatalf("VerifyManifest before corruption: %v", err)
	}
	f, err := os.OpenFile(filepath.Join(dir, CoreStatsFile), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	b := []byte{data[size/2] ^ 0x01}
	if _, err := f.WriteAt(b, size/2); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if err := VerifyManifest(dir, m); !errors.Is(err, ErrChecksumMismatch) {
		t.Errorf("VerifyManifest = %v, want ErrChecksumMismatch", err)
	}
}

// TestChecksumFilesShortFile: a file shorter than the size it was
// listed with (it shrank since) fails rather than getting its prefix's
// checksum, among other files' pieces hashing in parallel.
func TestChecksumFilesShortFile(t *testing.T) {
	dir := t.TempDir()
	sizes := make(map[string]int64)
	for i := range 8 {
		name := fmt.Sprintf("f%d.bin", i)
		if err := os.WriteFile(filepath.Join(dir, name), bytes.Repeat([]byte{byte(i)}, 3*testPiece), 0o600); err != nil {
			t.Fatal(err)
		}
		sizes[name] = 3 * testPiece
	}
	sizes["f5.bin"] = 5 * testPiece
	if _, err := checksumFiles(dir, sizes, testPiece, nil); !errors.Is(err, ErrSizeMismatch) {
		t.Errorf("checksumFiles = %v, want ErrSizeMismatch", err)
	}
}

func TestVerifyManifestRejectsOtherVersions(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, CoreStatsFile), []byte("rows"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := WriteManifest(dir, 1, 1, nil, nil); err != nil {
		t.Fatalf("WriteManifest: %v", err)
	}
	m, err := ReadManifest(dir)
	if err != nil {
		t.Fatalf("ReadManifest: %v", err)
	}
	m.Version = ManifestVersion - 1
	if err := VerifyManifest(dir, m); !errors.Is(err, ErrManifestVersion) {
		t.Errorf("VerifyManifest = %v, want ErrManifestVersion", err)
	}
}

// TestWriteManifestRereadsOnlyFilesWithoutChecksums: files whose
// writers recorded a checksum are not read; a file written without one,
// or rewritten since its checksum, is, and every checksum verifies.
func TestWriteManifestRereadsOnlyFilesWithoutChecksums(t *testing.T) {
	dir := t.TempDir()
	sums := NewChecksums()
	for _, name := range []string{CoreStatsFile, PrefixTreeFile, MPHFSlotsFile} {
		if err := WriteFile(filepath.Join(dir, name), []byte(name+" rows"), sums); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(dir, MPHFile), []byte("unhashed"), FilePerm); err != nil {
		t.Fatal(err)
	}
	rewritten := filepath.Join(dir, MPHFSlotsFile)
	if err := os.WriteFile(rewritten, []byte("other slots"), FilePerm); err != nil {
		t.Fatal(err)
	}
	later := time.Now().Add(time.Second)
	if err := os.Chtimes(rewritten, later, later); err != nil {
		t.Fatal(err)
	}

	reread, err := WriteManifest(dir, 1, 1, sums, nil)
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{MPHFile, MPHFSlotsFile}; !slices.Equal(reread, want) {
		t.Errorf("reread %v, want %v", reread, want)
	}
	m, err := ReadManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Files) != 4 {
		t.Errorf("manifest lists %d files, want 4", len(m.Files))
	}
	if err := VerifyManifest(dir, m); err != nil {
		t.Errorf("VerifyManifest: %v", err)
	}
}
