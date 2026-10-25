package indexread_test

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/format"
	"github.com/eunmann/s3-inv-db/pkg/indexread"
)

// TestOpen_RejectsFilesThatDisagreeOnCount: an index whose prefix tree
// comes from a different index (a mixed or damaged file set) fails to
// open instead of answering with the wrong rows.
func TestOpen_RejectsFilesThatDisagreeOnCount(t *testing.T) {
	small := setupTestIndex(t)
	if err := buildIndexFromCSV(t, small, "Key,Size\na/x,1\n"); err != nil {
		t.Fatal(err)
	}
	big := setupTestIndex(t)
	if err := buildIndexFromCSV(t, big, "Key,Size\na/x,1\nb/y,2\nc/z,3\n"); err != nil {
		t.Fatal(err)
	}
	if idx, err := indexread.Open(small); err != nil {
		t.Fatalf("Open(intact index): %v", err)
	} else {
		_ = idx.Close()
	}

	data, err := os.ReadFile(filepath.Join(big, format.PrefixTreeFile))
	if err != nil {
		t.Fatal(err)
	}
	root, err := os.OpenRoot(small)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	if err := root.WriteFile(format.PrefixTreeFile, data, format.FilePerm); err != nil {
		t.Fatal(err)
	}
	idx, err := indexread.Open(small)
	if !errors.Is(err, indexread.ErrIndexInconsistent) {
		if idx != nil {
			_ = idx.Close()
		}
		t.Fatalf("Open(mixed files) err = %v, want ErrIndexInconsistent", err)
	}
}

// TestLookup_OutOfRangeSlotPositionIsMiss: an MPHF slot whose
// fingerprint matches but whose stored position is past the prefix
// count (a damaged mph_slots.bin) is reported as a miss, not as a row.
func TestLookup_OutOfRangeSlotPositionIsMiss(t *testing.T) {
	dir := setupTestIndex(t)
	if err := buildIndexFromCSV(t, dir, "Key,Size\na/x,1\nb/y,2\n"); err != nil {
		t.Fatal(err)
	}
	prefixes := []string{"", "a/", "b/"}
	intact, err := indexread.Open(dir)
	if err != nil {
		t.Fatalf("Open(intact index): %v", err)
	}
	for _, p := range prefixes {
		if _, ok := intact.Lookup(p); !ok {
			t.Errorf("intact Lookup(%q) missed", p)
		}
	}
	if err := intact.Close(); err != nil {
		t.Fatal(err)
	}

	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	data, err := root.ReadFile(format.MPHFSlotsFile)
	if err != nil {
		t.Fatal(err)
	}
	header, err := format.DecodeHeader(data[:format.HeaderSize])
	if err != nil {
		t.Fatal(err)
	}
	// mph_slots.bin: header, 1-byte position width, then Count slots of
	// an 8-byte fingerprint followed by the position.
	const fingerprintBytes = 8
	dataOff := uint64(format.HeaderSize + 1)
	for slot := range header.Count {
		off := dataOff + slot*uint64(header.Width)
		for i := off + fingerprintBytes; i < off+uint64(header.Width); i++ {
			data[i] = 0xFF
		}
	}
	if err := root.WriteFile(format.MPHFSlotsFile, data, format.FilePerm); err != nil {
		t.Fatal(err)
	}

	idx, err := indexread.Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer idx.Close()
	for _, p := range prefixes {
		if pos, ok := idx.Lookup(p); ok {
			t.Errorf("Lookup(%q) = %d, true; want miss (count %d)", p, pos, idx.Count())
		}
		if s, ok := idx.StatsForPrefix(p); ok {
			t.Errorf("StatsForPrefix(%q) = %+v, true; want miss", p, s)
		}
	}
}
