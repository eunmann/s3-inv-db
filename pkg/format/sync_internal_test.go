package format

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
)

func TestSyncData(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.bin")
	if err := os.WriteFile(path, []byte("payload"), FilePerm); err != nil {
		t.Fatal(err)
	}
	if err := SyncData(path); err != nil {
		t.Fatalf("SyncData: %v", err)
	}
	if err := SyncData(path + ".missing"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("SyncData(missing) = %v, want ErrNotExist", err)
	}
}

func TestSyncDir(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "data.bin"), []byte("payload"), FilePerm); err != nil {
		t.Fatal(err)
	}
	if err := SyncDir(dir); err != nil {
		t.Fatalf("SyncDir: %v", err)
	}
	if err := SyncDir(filepath.Join(dir, "missing")); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("SyncDir(missing) = %v, want ErrNotExist", err)
	}
}
