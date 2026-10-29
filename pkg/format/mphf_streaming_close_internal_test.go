package format

import (
	"errors"
	"os"
	"strings"
	"testing"
)

func TestStreamingMPHFBuilder_CloseSurfacesErrors(t *testing.T) {
	dir := t.TempDir()
	b := NewStreamingMPHFBuilder(dir)
	// Enough keys in bucket 1 to fill its buffer, so it has a file.
	for i := range spillBufBytes/spillRecordBytes + 1 {
		if err := b.AddKey(1<<(64-spillBits)|uint64(i), 0, uint64(i)); err != nil {
			t.Fatalf("AddKey: %v", err)
		}
	}

	// Close a spill file out from under the builder: Close must report
	// the failure rather than swallow it.
	if err := b.spill.files[1].f.Close(); err != nil {
		t.Fatalf("preempt spill file close: %v", err)
	}

	err := b.Close()
	if err == nil {
		t.Fatal("Close() returned nil after preempting a spill file; expected a cleanup error")
	}
	if got := err.Error(); !strings.Contains(got, "close MPHF spill") {
		t.Errorf("Close error = %q; expected mention of the spill close", got)
	}
	if entries, _ := os.ReadDir(dir); len(entries) != 0 {
		t.Errorf("temp files left after Close: %v", entries)
	}
}

func TestStreamingMPHFBuilder_BuildTwiceRejected(t *testing.T) {
	// The first Build consumes the spill files; a second, or another
	// key after it, is refused.
	outDir := t.TempDir()
	b := NewStreamingMPHFBuilder(t.TempDir())
	defer b.Close()

	if err := b.Add("a/"); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if err := b.Build(outDir); err != nil {
		t.Fatalf("first Build: %v", err)
	}
	if err := b.Build(outDir); !errors.Is(err, ErrMPHFAlreadyBuilt) {
		t.Errorf("second Build err = %v, want ErrMPHFAlreadyBuilt", err)
	}
	if err := b.Add("b/"); !errors.Is(err, ErrMPHFAlreadyBuilt) {
		t.Errorf("Add after Build err = %v, want ErrMPHFAlreadyBuilt", err)
	}
}

func TestStreamingMPHFBuilder_BuildTwiceRejectedEmpty(t *testing.T) {
	b := NewStreamingMPHFBuilder(t.TempDir())
	defer b.Close()

	outDir := t.TempDir()
	if err := b.Build(outDir); err != nil {
		t.Fatalf("first Build: %v", err)
	}
	if err := b.Build(outDir); !errors.Is(err, ErrMPHFAlreadyBuilt) {
		t.Errorf("second Build err = %v, want ErrMPHFAlreadyBuilt", err)
	}
}

func TestStreamingMPHFBuilder_CloseClean(t *testing.T) {
	dir := t.TempDir()
	b := NewStreamingMPHFBuilder(dir)
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

// A successful build leaves no spill file, and Close after it neither
// fails nor touches the output.
func TestStreamingMPHFBuilder_BuildRemovesSpill(t *testing.T) {
	tmp, outDir := t.TempDir(), t.TempDir()
	b := NewStreamingMPHFBuilder(tmp)
	for i := range 70_000 {
		if err := b.Add(prefixFromInt(i + 1)); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.Build(outDir); err != nil {
		t.Fatal(err)
	}
	if entries, err := os.ReadDir(tmp); err != nil || len(entries) != 0 {
		t.Fatalf("temp dir after Build: %v (%v)", entries, err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("Close after Build: %v", err)
	}
	m, err := OpenMPHF(outDir)
	if err != nil {
		t.Fatalf("OpenMPHF after Close: %v", err)
	}
	m.Close()
}
