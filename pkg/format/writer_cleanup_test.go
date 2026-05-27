//nolint:testpackage // intentional internal access for cleanup tests
package format

// Tests in this file need unexported access (w.file, removeIfErr) to
// exercise the cleanup paths, so they live in the format package
// rather than format_test.

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

// TestArrayWriter_CleanupRemovesPartialOnCloseError forces a write
// failure during Close by closing the underlying *os.File out from
// under the writer. Before the fix, Close would discard the file.Close
// error and leave a half-written partial on disk. After the fix, the
// failure path removes the partial output.
func TestArrayWriter_CleanupRemovesPartialOnCloseError(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "array.bin")

	w, err := NewArrayWriter(path, 8)
	if err != nil {
		t.Fatalf("NewArrayWriter: %v", err)
	}

	// Pull the rug out: close the underlying file directly. The
	// subsequent Close on ArrayWriter then fails at Flush (the buffered
	// writer needs to push bytes to a now-closed fd) and must clean up.
	if err := w.file.Close(); err != nil {
		t.Fatalf("pre-close underlying: %v", err)
	}

	if err := w.Close(); err == nil {
		t.Fatal("Close should fail with the underlying file closed")
	}

	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("partial file still present after Close failure: stat err=%v", err)
	}
}

// TestRemoveIfErr_AbsentPathReturnsNil exercises the helper in
// isolation: a missing file should not be reported as an error since
// the caller is already on a fatal path.
func TestRemoveIfErr_AbsentPathReturnsNil(t *testing.T) {
	if err := removeIfErr(filepath.Join(t.TempDir(), "never-existed")); err != nil {
		t.Errorf("removeIfErr on missing path = %v, want nil", err)
	}
}

// TestTierStatsRowWriter_CleanupRemovesPartialOnCloseError mirrors the
// ArrayWriter cleanup contract for the row-major tier-stats writer:
// a Close-time failure must not leave a corrupt file on disk.
func TestTierStatsRowWriter_CleanupRemovesPartialOnCloseError(t *testing.T) {
	dir := t.TempDir()
	w, err := NewTierStatsRowWriter(dir, []tiers.ID{tiers.Standard})
	if err != nil {
		t.Fatalf("NewTierStatsRowWriter: %v", err)
	}

	// Close the underlying file under the writer; the next Flush fails.
	if err := w.file.Close(); err != nil {
		t.Fatalf("pre-close underlying: %v", err)
	}

	if err := w.Close(); err == nil {
		t.Fatal("Close should fail with the underlying file closed")
	}

	if _, err := os.Stat(w.path); !os.IsNotExist(err) {
		t.Fatalf("partial tier-stats file still present: stat err=%v", err)
	}
}
