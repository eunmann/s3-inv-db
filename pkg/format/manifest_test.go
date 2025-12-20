package format

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestWriteAndReadManifest(t *testing.T) {
	dir := t.TempDir()

	// Create some test files
	testFiles := map[string][]byte{
		"subtree_end.u64": []byte("test data 1"),
		"depth.u32":       []byte("test data 2"),
	}

	for name, data := range testFiles {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, data, 0644); err != nil {
			t.Fatalf("WriteFile failed: %v", err)
		}
	}

	// Write manifest
	if err := WriteManifest(dir, 100, 5); err != nil {
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
	path := filepath.Join(dir, "subtree_end.u64")
	data := []byte("test data for verification")
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	// Write manifest
	if err := WriteManifest(dir, 50, 3); err != nil {
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
	if err := os.WriteFile(path, []byte("corrupted data"), 0644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	// Verify should now fail
	if err := VerifyManifest(dir, manifest); err == nil {
		t.Error("VerifyManifest should fail after corruption")
	}
}

func TestChecksumFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.bin")

	// Write known data
	data := []byte("hello world")
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	checksum1, err := checksumFile(path)
	if err != nil {
		t.Fatalf("checksumFile failed: %v", err)
	}

	// Same data should give same checksum
	checksum2, err := checksumFile(path)
	if err != nil {
		t.Fatalf("checksumFile failed: %v", err)
	}

	if checksum1 != checksum2 {
		t.Error("checksum not deterministic")
	}

	// Different data should give different checksum
	if err := os.WriteFile(path, []byte("different"), 0644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	checksum3, err := checksumFile(path)
	if err != nil {
		t.Fatalf("checksumFile failed: %v", err)
	}

	if checksum1 == checksum3 {
		t.Error("different data gave same checksum")
	}
}

func TestWriteFileSync(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.bin")

	data := []byte("test data")
	if err := writeFileSync(path, data); err != nil {
		t.Fatalf("writeFileSync failed: %v", err)
	}

	// Read back
	read, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}

	if !bytes.Equal(read, data) {
		t.Errorf("read = %q, want %q", read, data)
	}
}

func TestSyncDir(t *testing.T) {
	dir := t.TempDir()

	// Create a file in the directory
	path := filepath.Join(dir, "test.bin")
	if err := os.WriteFile(path, []byte("data"), 0644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	// Sync should not error
	if err := SyncDir(dir); err != nil {
		t.Errorf("SyncDir failed: %v", err)
	}
}
