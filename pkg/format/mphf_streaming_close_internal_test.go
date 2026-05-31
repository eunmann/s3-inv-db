package format

import (
	"errors"
	"os"
	"strings"
	"testing"
)

func TestStreamingMPHFBuilder_CloseSurfacesErrors(t *testing.T) {
	dir := t.TempDir()
	b, err := NewStreamingMPHFBuilder(dir)
	if err != nil {
		t.Fatalf("NewStreamingMPHFBuilder: %v", err)
	}
	if err := b.Add("a/", 1); err != nil {
		t.Fatalf("Add: %v", err)
	}

	// Close the tempfile out from under the builder so the deferred
	// Flush+Close inside Close() returns errors that the old impl would
	// have silently swallowed. After this, every cleanup call should
	// fail, and Close must report something.
	if err := b.tempFile.Close(); err != nil {
		t.Fatalf("preempt tempFile.Close: %v", err)
	}

	err = b.Close()
	if err == nil {
		t.Fatal("Close() returned nil after preempting tempfile; expected aggregated cleanup error")
	}
	// The aggregated message should mention at least one of the
	// cleanup phases so operators have a starting point.
	got := err.Error()
	if !strings.Contains(got, "mphf tempfile") {
		t.Errorf("Close error = %q; expected mention of mphf tempfile", got)
	}
}

func TestStreamingMPHFBuilder_BuildTwiceRejected(t *testing.T) {
	// Build finalizes the prefix writer chain on its first call; a
	// second call would have nil-derefed b.tempWriter.Flush. Surface
	// ErrMPHFAlreadyBuilt instead. Empty-count path goes through
	// writeEmpty without nilling the writer chain, so it would
	// previously have silently re-run writeEmpty — also guarded.
	dir := t.TempDir()
	outDir := t.TempDir()
	b, err := NewStreamingMPHFBuilder(dir)
	if err != nil {
		t.Fatalf("NewStreamingMPHFBuilder: %v", err)
	}
	defer b.Close()

	if err := b.Add("a/", 0); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if err := b.Build(outDir); err != nil {
		t.Fatalf("first Build: %v", err)
	}
	err = b.Build(outDir)
	if !errors.Is(err, ErrMPHFAlreadyBuilt) {
		t.Errorf("second Build err = %v, want ErrMPHFAlreadyBuilt", err)
	}
}

func TestStreamingMPHFBuilder_BuildTwiceRejectedEmpty(t *testing.T) {
	// Same guard, but on the count==0 fast path that writes empty
	// index files via writeEmpty.
	b, err := NewStreamingMPHFBuilder(t.TempDir())
	if err != nil {
		t.Fatalf("NewStreamingMPHFBuilder: %v", err)
	}
	defer b.Close()

	outDir := t.TempDir()
	if err := b.Build(outDir); err != nil {
		t.Fatalf("first Build: %v", err)
	}
	err = b.Build(outDir)
	if !errors.Is(err, ErrMPHFAlreadyBuilt) {
		t.Errorf("second Build err = %v, want ErrMPHFAlreadyBuilt", err)
	}
}

func TestStreamingMPHFBuilder_CloseClean(t *testing.T) {
	dir := t.TempDir()
	b, err := NewStreamingMPHFBuilder(dir)
	if err != nil {
		t.Fatalf("NewStreamingMPHFBuilder: %v", err)
	}
	if err := b.Add("a/", 1); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("Close on a healthy builder returned %v", err)
	}
	if _, err := os.Stat(b.tempPath); !os.IsNotExist(err) {
		t.Errorf("tempfile %s not removed: %v", b.tempPath, err)
	}
}
