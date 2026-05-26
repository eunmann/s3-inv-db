package format_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/format"
)

// TestMmapFileCloseIdempotent guards against a double-Munmap regression:
// the first Close must release the mapping, the second must be a no-op.
func TestMmapFileCloseIdempotent(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "mmap.bin")
	if err := os.WriteFile(path, []byte("hello world"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}

	mf, err := format.OpenMmap(path)
	if err != nil {
		t.Fatalf("OpenMmap: %v", err)
	}

	if err := mf.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}

	if err := mf.Close(); err != nil {
		t.Fatalf("second Close must be a no-op, got: %v", err)
	}
}

// TestMmapFileCloseEmpty exercises the size==0 branch where data is nil
// at construction time; Close must still be safely callable.
func TestMmapFileCloseEmpty(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "empty.bin")
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}

	mf, err := format.OpenMmap(path)
	if err != nil {
		t.Fatalf("OpenMmap: %v", err)
	}

	if err := mf.Close(); err != nil {
		t.Fatalf("Close on empty mmap: %v", err)
	}
	if err := mf.Close(); err != nil {
		t.Fatalf("second Close on empty mmap: %v", err)
	}
}
