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

// TestDescendants_RejectChildStartOutsideItsDepth: a damaged child_start
// in core_stats.bin (past the row count, or outside the next depth's
// rows) makes every descendant query return ErrIndexInconsistent
// instead of panicking, scanning a wrapped-around span or answering
// with rows from another depth.
func TestDescendants_RejectChildStartOutsideItsDepth(t *testing.T) {
	// Rows in level order: 0 "", 1 a/, 2 b/, 3 a/x/, 4 a/y/, 5 b/z/.
	const csv = "Key,Size\na/x/1,1\na/y/2,2\nb/z/3,3\n"
	tests := []struct {
		name     string
		row      uint64
		value    uint64
		pos      uint64
		relDepth int
	}{
		{"children end past the row count", 2, 9, 1, 1},
		{"children start before their depth", 1, 0, 1, 1},
		{"grandchildren start before their depth", 1, 0, 0, 2},
		{"root children start past the row count", 0, 200, 0, 2},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dir := setupTestIndex(t)
			if err := buildIndexFromCSV(t, dir, csv); err != nil {
				t.Fatal(err)
			}
			setChildStart(t, dir, tc.row, tc.value)
			idx, err := indexread.Open(dir)
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			defer idx.Close()

			if span, err := idx.DescendantSpan(tc.pos, tc.relDepth); !errors.Is(err, indexread.ErrIndexInconsistent) {
				t.Fatalf("DescendantSpan = %+v, %v; want ErrIndexInconsistent", span, err)
			}
			if got, err := idx.DescendantsAtDepth(tc.pos, tc.relDepth); !errors.Is(err, indexread.ErrIndexInconsistent) {
				t.Errorf("DescendantsAtDepth = %v, %v; want ErrIndexInconsistent", got, err)
			}
			for _, f := range []indexread.Filter{{}, {MinCount: 1}} {
				if page, err := idx.DescendantsPage(tc.pos, tc.relDepth, f, 0, 10); !errors.Is(err, indexread.ErrIndexInconsistent) {
					t.Errorf("DescendantsPage(filter %+v) = %+v, %v; want ErrIndexInconsistent", f, page, err)
				}
			}
			if got, err := idx.TopByBytes(tc.pos, tc.relDepth, 10); !errors.Is(err, indexread.ErrIndexInconsistent) {
				t.Errorf("TopByBytes = %v, %v; want ErrIndexInconsistent", got, err)
			}
		})
	}
}

// setChildStart overwrites row's child_start in dir's core_stats.bin.
func setChildStart(t *testing.T, dir string, row, value uint64) {
	t.Helper()
	r, err := format.OpenCoreStats(dir)
	if err != nil {
		t.Fatal(err)
	}
	stride := r.Stride()
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	data, err := root.ReadFile(format.CoreStatsFile)
	if err != nil {
		t.Fatal(err)
	}
	off := uint64(format.HeaderSize+format.CoreStatsStrideBytes) + row*uint64(stride.RowBytes()) + uint64(stride.Count) + uint64(stride.Bytes)
	for i := range uint64(stride.ChildStart) {
		data[off+i] = byte(value >> (8 * i))
	}
	if err := root.WriteFile(format.CoreStatsFile, data, format.FilePerm); err != nil {
		t.Fatal(err)
	}
}
