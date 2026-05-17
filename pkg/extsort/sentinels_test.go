package extsort_test

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/extsort"
)

// TestRunFileReader_RejectsBadMagic verifies the sentinel
// ErrInvalidMagic surfaces when a run file's first 4 bytes don't match
// the expected magic. Catches a class of bugs where a corrupted or
// wrong-format file passes silently into the merge pipeline.
func TestRunFileReader_RejectsBadMagic(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.bin")
	// Header layout begins with magic. Any 4 bytes != runFileMagic
	// trip the sentinel; pad the rest so the read doesn't fail with
	// "short read" first.
	junk := append([]byte{0xDE, 0xAD, 0xBE, 0xEF}, make([]byte, 64)...)
	if err := os.WriteFile(path, junk, 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	_, err := extsort.OpenRunFile(path, 4096)
	if !errors.Is(err, extsort.ErrInvalidMagic) {
		t.Fatalf("err = %v, want ErrInvalidMagic", err)
	}
}

// TestRunFileReader_RejectsBadMagicCompressed verifies the same for
// the compressed run-file reader.
func TestRunFileReader_RejectsBadMagicCompressed(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.crun")
	junk := append([]byte{0xDE, 0xAD, 0xBE, 0xEF}, make([]byte, 64)...)
	if err := os.WriteFile(path, junk, 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	_, err := extsort.OpenCompressedRunFile(path, 4096)
	if !errors.Is(err, extsort.ErrInvalidMagic) {
		t.Fatalf("err = %v, want ErrInvalidMagic", err)
	}
}
