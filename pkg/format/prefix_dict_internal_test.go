package format

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSplitPrefix(t *testing.T) {
	tests := []struct {
		prefix   string
		expected []string
	}{
		{"", []string{""}},
		{"data/", []string{"data", ""}},
		{"data/2024/", []string{"data", "2024", ""}},
		{"data/2024/01/", []string{"data", "2024", "01", ""}},
		{"a/b/c", []string{"a", "b", "c"}},
		{"single", []string{"single"}},
		{"///", []string{"", "", "", ""}},
	}

	for _, tc := range tests {
		got := SplitPrefix(tc.prefix)
		if len(got) != len(tc.expected) {
			t.Errorf("SplitPrefix(%q) = %v, want %v", tc.prefix, got, tc.expected)

			continue
		}
		for i := range got {
			if got[i] != tc.expected[i] {
				t.Errorf("SplitPrefix(%q)[%d] = %q, want %q", tc.prefix, i, got[i], tc.expected[i])
			}
		}
	}
}

func TestPrefixSegmentInternerBasic(t *testing.T) {
	dir := t.TempDir()

	interner, err := NewPrefixSegmentInterner(dir, 1<<20, nil)
	if err != nil {
		t.Fatalf("NewPrefixSegmentInterner failed: %v", err)
	}

	id1, err := interner.Intern("data")
	if err != nil {
		t.Fatalf("Intern(data) failed: %v", err)
	}
	if id1 != 0 {
		t.Errorf("first ID = %d, want 0", id1)
	}

	id2, err := interner.Intern("2024")
	if err != nil {
		t.Fatalf("Intern(2024) failed: %v", err)
	}
	if id2 != 1 {
		t.Errorf("second ID = %d, want 1", id2)
	}

	id3, err := interner.Intern("data")
	if err != nil {
		t.Fatalf("Intern(data) second time failed: %v", err)
	}
	if id3 != id1 {
		t.Errorf("repeated Intern(data) = %d, want %d", id3, id1)
	}

	if interner.Count() != 2 {
		t.Errorf("Count = %d, want 2", interner.Count())
	}

	if err := interner.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestPrefixDictionaryRoundTrip(t *testing.T) {
	dir := t.TempDir()

	interner, err := NewPrefixSegmentInterner(dir, 1<<20, nil)
	if err != nil {
		t.Fatalf("NewPrefixSegmentInterner failed: %v", err)
	}

	segments := []string{"", "data", "2024", "01", "bucket", "日本語"}
	for _, seg := range segments {
		if _, err := interner.Intern(seg); err != nil {
			t.Fatalf("Intern(%q) failed: %v", seg, err)
		}
	}

	if err := interner.Close(); err != nil {
		t.Fatalf("interner.Close failed: %v", err)
	}

	dict, err := OpenPrefixDictionary(dir)
	if err != nil {
		t.Fatalf("OpenPrefixDictionary failed: %v", err)
	}
	defer dict.Close()

	if dict.Count() != uint64(len(segments)) {
		t.Errorf("dict.Count() = %d, want %d", dict.Count(), len(segments))
	}

	for i, expected := range segments {
		got, err := dict.GetSegment(uint32(i))
		if err != nil {
			t.Errorf("GetSegment(%d) failed: %v", i, err)

			continue
		}
		if got != expected {
			t.Errorf("GetSegment(%d) = %q, want %q", i, got, expected)
		}
	}
}

// TestSegmentsOutliveTheDictionary: GetSegment returns a copy, so a
// segment a caller kept stays valid after the dictionary is closed and
// its files are unmapped.
func TestSegmentsOutliveTheDictionary(t *testing.T) {
	dir := internSegments(t, "alpha", "", "βeta", strings.Repeat("g", 300))
	dict, err := OpenPrefixDictionary(dir)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"alpha", "", "βeta", strings.Repeat("g", 300)}
	kept := make([]string, len(want))
	for i := range want {
		if kept[i], err = dict.GetSegment(uint32(i)); err != nil {
			t.Fatal(err)
		}
	}
	if got, want := dict.Bytes(), uint64(5+0+5+300); got != want {
		t.Errorf("Bytes = %d, want %d", got, want)
	}
	if err := dict.Close(); err != nil {
		t.Fatal(err)
	}
	for i := range want {
		if kept[i] != want[i] {
			t.Errorf("segment %d after Close = %q, want %q", i, kept[i], want[i])
		}
	}
}

// TestPrefixDictionaryCorruptOffsets: a read whose offsets run backwards
// or past the blob, or whose ID is past the end, fails instead of
// reading outside the segment; an offsets file without its tail pad
// fails to open.
func TestPrefixDictionaryCorruptOffsets(t *testing.T) {
	tests := []struct {
		name   string
		offset int // the offset entry to overwrite, or -1
		value  byte
		id     uint32
	}{
		{"backwards", 1, 4, 1}, // "bb" then starts after it ends
		{"past the blob", 1, 200, 0},
		{"id past the end", -1, 0, 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := internSegments(t, "a", "bb", "ccc")
			if tt.offset >= 0 {
				rewriteFile(t, dir, PrefixDictOffsetsFile, func(data []byte) []byte {
					data[HeaderSize+tt.offset] = tt.value // one-byte offsets
					return data
				})
			}
			dict, err := OpenPrefixDictionary(dir)
			if err != nil {
				t.Fatal(err)
			}
			defer dict.Close()
			if got, err := dict.GetSegment(tt.id); !errors.Is(err, ErrBoundsCheck) {
				t.Errorf("GetSegment(%d) = %q, %v; want ErrBoundsCheck", tt.id, got, err)
			}
		})
	}

	t.Run("no tail pad", func(t *testing.T) {
		dir := internSegments(t, "a", "bb", "ccc")
		rewriteFile(t, dir, PrefixDictOffsetsFile, func(data []byte) []byte { return data[:HeaderSize+4] })
		dict, err := OpenPrefixDictionary(dir)
		if err == nil {
			dict.Close()
		}
		if !errors.Is(err, ErrBoundsCheck) {
			t.Errorf("open without tail pad: err = %v, want ErrBoundsCheck", err)
		}
	})
}

// rewriteFile replaces dir's file name with edit's result.
func rewriteFile(t *testing.T, dir, name string, edit func([]byte) []byte) {
	t.Helper()
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	data, err := root.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	if err := root.WriteFile(name, edit(data), FilePerm); err != nil {
		t.Fatal(err)
	}
}

// internSegments writes a dictionary of segs, in ID order, to a new
// directory and returns it.
func internSegments(t *testing.T, segs ...string) string {
	t.Helper()
	dir := t.TempDir()
	interner, err := NewPrefixSegmentInterner(dir, 1<<20, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, seg := range segs {
		if _, err := interner.Intern(seg); err != nil {
			t.Fatal(err)
		}
	}
	if err := interner.Close(); err != nil {
		t.Fatal(err)
	}

	return dir
}

// TestSegmentOffsetsAreNarrow: the segment offsets file takes the bytes
// the blob's size needs, not 8.
func TestSegmentOffsetsAreNarrow(t *testing.T) {
	dir := t.TempDir()
	interner, err := NewPrefixSegmentInterner(dir, 1<<20, nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := range 1000 { // 3890 blob bytes: 2-byte offsets
		if _, err := interner.Intern(fmt.Sprintf("seg%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := interner.Close(); err != nil {
		t.Fatal(err)
	}
	offsets, err := OpenArray(filepath.Join(dir, PrefixDictOffsetsFile))
	if err != nil {
		t.Fatal(err)
	}
	defer offsets.Close()
	if offsets.Width() != 2 || offsets.Count() != 1001 {
		t.Errorf("offsets width %d count %d, want 2, 1001", offsets.Width(), offsets.Count())
	}
}

// TestInternerAbortRemovesTheDictionary: a build that fails before the
// dictionary is finished leaves no part of it, and Abort after Close
// keeps it.
func TestInternerAbortRemovesTheDictionary(t *testing.T) {
	dir := t.TempDir()
	si, err := NewPrefixSegmentInterner(dir, 1<<20, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := si.Intern("data"); err != nil {
		t.Fatal(err)
	}
	if err := si.Abort(); err != nil {
		t.Fatal(err)
	}
	if err := si.Abort(); err != nil {
		t.Fatalf("second Abort: %v", err)
	}
	for _, f := range []string{PrefixDictBlobFile, PrefixDictOffsetsFile} {
		if _, err := os.Stat(filepath.Join(dir, f)); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("%s after Abort: %v", f, err)
		}
	}

	si, err = NewPrefixSegmentInterner(dir, 1<<20, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := si.Intern("data"); err != nil {
		t.Fatal(err)
	}
	if err := si.Close(); err != nil {
		t.Fatal(err)
	}
	if err := si.Abort(); err != nil {
		t.Fatal(err)
	}
	for _, f := range []string{PrefixDictBlobFile, PrefixDictOffsetsFile} {
		if _, err := os.Stat(filepath.Join(dir, f)); err != nil {
			t.Errorf("%s after Close then Abort: %v", f, err)
		}
	}
}
