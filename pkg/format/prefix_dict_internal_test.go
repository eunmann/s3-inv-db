package format

import (
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

	interner, err := NewPrefixSegmentInterner(dir)
	if err != nil {
		t.Fatalf("NewPrefixSegmentInterner failed: %v", err)
	}

	id1, err := interner.Intern([]byte("data"))
	if err != nil {
		t.Fatalf("Intern(data) failed: %v", err)
	}
	if id1 != 0 {
		t.Errorf("first ID = %d, want 0", id1)
	}

	id2, err := interner.Intern([]byte("2024"))
	if err != nil {
		t.Fatalf("Intern(2024) failed: %v", err)
	}
	if id2 != 1 {
		t.Errorf("second ID = %d, want 1", id2)
	}

	id3, err := interner.Intern([]byte("data"))
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

	interner, err := NewPrefixSegmentInterner(dir)
	if err != nil {
		t.Fatalf("NewPrefixSegmentInterner failed: %v", err)
	}

	segments := []string{"", "data", "2024", "01", "bucket"}
	for _, seg := range segments {
		if _, err := interner.Intern([]byte(seg)); err != nil {
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

func TestPreloadedPrefixCache(t *testing.T) {
	dir := t.TempDir()

	interner, err := NewPrefixSegmentInterner(dir)
	if err != nil {
		t.Fatalf("NewPrefixSegmentInterner failed: %v", err)
	}

	segments := []string{"", "data", "2024", "01", "bucket", "日本語"}
	for _, seg := range segments {
		if _, err := interner.Intern([]byte(seg)); err != nil {
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

	cache, err := dict.PreloadSegments()
	if err != nil {
		t.Fatalf("PreloadSegments failed: %v", err)
	}

	if cache.Count() != len(segments) {
		t.Errorf("cache.Count() = %d, want %d", cache.Count(), len(segments))
	}

	for i, expected := range segments {
		got := cache.Get(uint32(i))
		if got != expected {
			t.Errorf("cache.Get(%d) = %q, want %q", i, got, expected)
		}
	}
}
