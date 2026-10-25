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
	if err := b.Add("a/"); err != nil {
		t.Fatalf("Add: %v", err)
	}

	// Close an array's file out from under the builder: Close must
	// report the failure rather than swallow it.
	if err := b.hashes.file.Close(); err != nil {
		t.Fatalf("preempt hashes file close: %v", err)
	}

	err = b.Close()
	if err == nil {
		t.Fatal("Close() returned nil after preempting an array's file; expected an aggregated cleanup error")
	}
	if got := err.Error(); !strings.Contains(got, "u64 disk array close") {
		t.Errorf("Close error = %q; expected mention of the array close", got)
	}
}

func TestStreamingMPHFBuilder_BuildTwiceRejected(t *testing.T) {
	// Build consumes the disk-backed arrays on its first call; a
	// second call would nil-deref them. The empty-count path goes
	// through writeEmpty without touching the arrays, so it would
	// silently re-run writeEmpty; both are guarded.
	dir := t.TempDir()
	outDir := t.TempDir()
	b, err := NewStreamingMPHFBuilder(dir)
	if err != nil {
		t.Fatalf("NewStreamingMPHFBuilder: %v", err)
	}
	defer b.Close()

	if err := b.Add("a/"); err != nil {
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
	if err := b.Add("a/"); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("Close on a healthy builder returned %v", err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Errorf("temp files left after Close: %v", entries)
	}
}
