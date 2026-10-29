package format

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// openDescriptorsOf counts this process's descriptors open on path.
func openDescriptorsOf(t *testing.T, path string) int {
	t.Helper()
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatalf("list descriptors: %v", err)
	}
	n := 0
	for _, e := range entries {
		if target, err := os.Readlink("/proc/self/fd/" + e.Name()); err == nil && target == path {
			n++
		}
	}

	return n
}

// TestStartWritebackClosesItsDescriptor: the writeback runs on a
// duplicate descriptor that it closes once the writes are submitted,
// and leaves the file's contents alone.
func TestStartWritebackClosesItsDescriptor(t *testing.T) {
	path := filepath.Join(t.TempDir(), "final.bin")
	data := bytes.Repeat([]byte{0x5a}, 4<<20)
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(data); err != nil {
		t.Fatal(err)
	}
	startWriteback(f)
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for openDescriptorsOf(t, path) > 0 {
		if time.Now().After(deadline) {
			t.Fatal("writeback descriptor still open")
		}
		time.Sleep(time.Millisecond)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("file contents changed")
	}
}
