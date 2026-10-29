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
