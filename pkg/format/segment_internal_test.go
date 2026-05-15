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

func TestSegmentInternerBasic(t *testing.T) {
	dir := t.TempDir()

	interner, err := NewSegmentInterner(dir)
	if err != nil {
		t.Fatalf("NewSegmentInterner failed: %v", err)
	}

	// Intern some segments
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

	// Intern same segment again - should return same ID
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

func TestSegmentDictionaryRoundTrip(t *testing.T) {
	dir := t.TempDir()

	// Create and populate interner
	interner, err := NewSegmentInterner(dir)
	if err != nil {
		t.Fatalf("NewSegmentInterner failed: %v", err)
	}

	segments := []string{"", "data", "2024", "01", "bucket"}
	for _, seg := range segments {
		if _, err := interner.Intern(seg); err != nil {
			t.Fatalf("Intern(%q) failed: %v", seg, err)
		}
	}

	if err := interner.Close(); err != nil {
		t.Fatalf("interner.Close failed: %v", err)
	}

	// Open dictionary and verify
	dict, err := OpenSegmentDictionary(dir)
	if err != nil {
		t.Fatalf("OpenSegmentDictionary failed: %v", err)
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

func TestSegmentedPrefixWriterReader(t *testing.T) {
	dir := t.TempDir()

	// Create writer and write some prefixes
	writer, err := NewSegmentedPrefixWriter(dir)
	if err != nil {
		t.Fatalf("NewSegmentedPrefixWriter failed: %v", err)
	}

	prefixes := []string{
		"",
		"data/",
		"data/2024/",
		"data/2025/",
		"logs/",
		"logs/2024/",
	}

	for _, p := range prefixes {
		if err := writer.WritePrefix(p); err != nil {
			t.Fatalf("WritePrefix(%q) failed: %v", p, err)
		}
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("writer.Close failed: %v", err)
	}

	// Open reader and verify all prefixes
	reader, err := OpenSegmentedPrefixReader(dir)
	if err != nil {
		t.Fatalf("OpenSegmentedPrefixReader failed: %v", err)
	}
	defer reader.Close()

	if reader.Count() != uint64(len(prefixes)) {
		t.Errorf("reader.Count() = %d, want %d", reader.Count(), len(prefixes))
	}

	for i, expected := range prefixes {
		got, err := reader.GetPrefix(uint64(i))
		if err != nil {
			t.Errorf("GetPrefix(%d) failed: %v", i, err)

			continue
		}
		if got != expected {
			t.Errorf("GetPrefix(%d) = %q, want %q", i, got, expected)
		}
	}
}

func TestSegmentedPrefixUnsafeGet(t *testing.T) {
	dir := t.TempDir()

	writer, err := NewSegmentedPrefixWriter(dir)
	if err != nil {
		t.Fatalf("NewSegmentedPrefixWriter failed: %v", err)
	}

	prefixes := []string{"a/b/c/", "x/y/", "test/"}
	for _, p := range prefixes {
		if err := writer.WritePrefix(p); err != nil {
			t.Fatalf("WritePrefix(%q) failed: %v", p, err)
		}
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("writer.Close failed: %v", err)
	}

	reader, err := OpenSegmentedPrefixReader(dir)
	if err != nil {
		t.Fatalf("OpenSegmentedPrefixReader failed: %v", err)
	}
	defer reader.Close()

	// Test UnsafeGetPrefix
	for i, expected := range prefixes {
		got := reader.UnsafeGetPrefix(uint64(i))
		if got != expected {
			t.Errorf("UnsafeGetPrefix(%d) = %q, want %q", i, got, expected)
		}
	}
}

func TestSegmentedPrefixEmpty(t *testing.T) {
	dir := t.TempDir()

	writer, err := NewSegmentedPrefixWriter(dir)
	if err != nil {
		t.Fatalf("NewSegmentedPrefixWriter failed: %v", err)
	}

	// Close without writing any prefixes
	if err := writer.Close(); err != nil {
		t.Fatalf("writer.Close failed: %v", err)
	}

	reader, err := OpenSegmentedPrefixReader(dir)
	if err != nil {
		t.Fatalf("OpenSegmentedPrefixReader failed: %v", err)
	}
	defer reader.Close()

	if reader.Count() != 0 {
		t.Errorf("reader.Count() = %d, want 0", reader.Count())
	}
}

func TestSegmentedPrefixUnicode(t *testing.T) {
	dir := t.TempDir()

	writer, err := NewSegmentedPrefixWriter(dir)
	if err != nil {
		t.Fatalf("NewSegmentedPrefixWriter failed: %v", err)
	}

	prefixes := []string{
		"日本語/パス/",
		"한국어/경로/",
		"emoji/🎉/🚀/",
		"mixed/日本/english/",
	}

	for _, p := range prefixes {
		if err := writer.WritePrefix(p); err != nil {
			t.Fatalf("WritePrefix(%q) failed: %v", p, err)
		}
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("writer.Close failed: %v", err)
	}

	reader, err := OpenSegmentedPrefixReader(dir)
	if err != nil {
		t.Fatalf("OpenSegmentedPrefixReader failed: %v", err)
	}
	defer reader.Close()

	for i, expected := range prefixes {
		got, err := reader.GetPrefix(uint64(i))
		if err != nil {
			t.Errorf("GetPrefix(%d) failed: %v", i, err)

			continue
		}
		if got != expected {
			t.Errorf("GetPrefix(%d) = %q, want %q", i, got, expected)
		}
	}
}

func TestSegmentedPrefixDeduplication(t *testing.T) {
	dir := t.TempDir()

	writer, err := NewSegmentedPrefixWriter(dir)
	if err != nil {
		t.Fatalf("NewSegmentedPrefixWriter failed: %v", err)
	}

	// These prefixes share many common segments
	prefixes := []string{
		"bucket/year/month/day/",
		"bucket/year/month/",
		"bucket/year/",
		"bucket/",
		"bucket/year/month/day/hour/",
	}

	for _, p := range prefixes {
		if err := writer.WritePrefix(p); err != nil {
			t.Fatalf("WritePrefix(%q) failed: %v", p, err)
		}
	}

	// Check segment count - should be much less than total segments
	// Expected unique segments: bucket, year, month, day, hour, "" (empty for trailing slash)
	expectedSegments := uint32(6)
	if writer.SegmentCount() != expectedSegments {
		t.Errorf("SegmentCount() = %d, want %d", writer.SegmentCount(), expectedSegments)
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("writer.Close failed: %v", err)
	}

	reader, err := OpenSegmentedPrefixReader(dir)
	if err != nil {
		t.Fatalf("OpenSegmentedPrefixReader failed: %v", err)
	}
	defer reader.Close()

	// Verify all prefixes can be reconstructed
	for i, expected := range prefixes {
		got, err := reader.GetPrefix(uint64(i))
		if err != nil {
			t.Errorf("GetPrefix(%d) failed: %v", i, err)

			continue
		}
		if got != expected {
			t.Errorf("GetPrefix(%d) = %q, want %q", i, got, expected)
		}
	}
}

func TestStreamingMPHFWithSegmentedEncoding(t *testing.T) {
	dir := t.TempDir()

	builder, err := NewStreamingMPHFBuilder(dir, WithPrefixEncoding(PrefixEncodingSegDict))
	if err != nil {
		t.Fatalf("NewStreamingMPHFBuilder failed: %v", err)
	}

	prefixes := []string{
		"",
		"data/",
		"data/2024/",
		"data/2024/01/",
		"logs/",
		"logs/2024/",
	}

	for i, p := range prefixes {
		if err := builder.Add(p, uint64(i)); err != nil {
			t.Fatalf("Add(%q) failed: %v", p, err)
		}
	}

	if err := builder.Build(dir); err != nil {
		t.Fatalf("Build failed: %v", err)
	}
	builder.Close()

	// Open and verify
	m, err := OpenMPHF(dir)
	if err != nil {
		t.Fatalf("OpenMPHF failed: %v", err)
	}
	defer m.Close()

	if !m.useSegments {
		t.Error("expected useSegments to be true")
	}

	if m.Count() != uint64(len(prefixes)) {
		t.Errorf("Count = %d, want %d", m.Count(), len(prefixes))
	}

	// Verify lookups and GetPrefix
	for i, p := range prefixes {
		pos, ok := m.Lookup(p)
		if !ok {
			t.Errorf("Lookup(%q) failed", p)

			continue
		}
		if pos != uint64(i) {
			t.Errorf("Lookup(%q) = %d, want %d", p, pos, i)
		}

		stored, err := m.GetPrefix(pos)
		if err != nil {
			t.Errorf("GetPrefix(%d) failed: %v", pos, err)

			continue
		}
		if stored != p {
			t.Errorf("GetPrefix(%d) = %q, want %q", pos, stored, p)
		}
	}

	// Verify with VerifyMPHF
	if err := VerifyMPHF(m); err != nil {
		t.Errorf("VerifyMPHF failed: %v", err)
	}
}

func TestStreamingMPHFSegmentedEmpty(t *testing.T) {
	dir := t.TempDir()

	builder, err := NewStreamingMPHFBuilder(dir, WithPrefixEncoding(PrefixEncodingSegDict))
	if err != nil {
		t.Fatalf("NewStreamingMPHFBuilder failed: %v", err)
	}

	if err := builder.Build(dir); err != nil {
		t.Fatalf("Build failed: %v", err)
	}
	builder.Close()

	m, err := OpenMPHF(dir)
	if err != nil {
		t.Fatalf("OpenMPHF failed: %v", err)
	}
	defer m.Close()

	if m.Count() != 0 {
		t.Errorf("Count = %d, want 0", m.Count())
	}
}

func TestStreamingMPHFSegmentedLarge(t *testing.T) {
	dir := t.TempDir()

	builder, err := NewStreamingMPHFBuilder(dir, WithPrefixEncoding(PrefixEncodingSegDict))
	if err != nil {
		t.Fatalf("NewStreamingMPHFBuilder failed: %v", err)
	}

	// Create 1000 prefixes with shared segments
	prefixes := make([]string, 1000)
	for i := range 1000 {
		prefixes[i] = prefixFromInt(i)
	}

	for i, p := range prefixes {
		if err := builder.Add(p, uint64(i)); err != nil {
			t.Fatalf("Add(%q) failed: %v", p, err)
		}
	}

	if err := builder.Build(dir); err != nil {
		t.Fatalf("Build failed: %v", err)
	}
	builder.Close()

	m, err := OpenMPHF(dir)
	if err != nil {
		t.Fatalf("OpenMPHF failed: %v", err)
	}
	defer m.Close()

	if m.Count() != 1000 {
		t.Errorf("Count = %d, want 1000", m.Count())
	}

	if err := VerifyMPHF(m); err != nil {
		t.Errorf("VerifyMPHF failed: %v", err)
	}
}

func TestStreamingMPHFSegmentedUnicode(t *testing.T) {
	dir := t.TempDir()

	builder, err := NewStreamingMPHFBuilder(dir, WithPrefixEncoding(PrefixEncodingSegDict))
	if err != nil {
		t.Fatalf("NewStreamingMPHFBuilder failed: %v", err)
	}

	prefixes := []string{"", "日本語/", "한국어/", "emoji/🎉/"}
	for i, p := range prefixes {
		if err := builder.Add(p, uint64(i)); err != nil {
			t.Fatalf("Add(%q) failed: %v", p, err)
		}
	}

	if err := builder.Build(dir); err != nil {
		t.Fatalf("Build failed: %v", err)
	}
	builder.Close()

	m, err := OpenMPHF(dir)
	if err != nil {
		t.Fatalf("OpenMPHF failed: %v", err)
	}
	defer m.Close()

	for _, p := range prefixes {
		pos, ok := m.Lookup(p)
		if !ok {
			t.Errorf("Lookup(%q) failed", p)

			continue
		}
		stored, _ := m.GetPrefix(pos)
		if stored != p {
			t.Errorf("GetPrefix returned %q, want %q", stored, p)
		}
	}
}

func TestMPHFLookupWithVerifySegmented(t *testing.T) {
	dir := t.TempDir()

	builder, err := NewStreamingMPHFBuilder(dir, WithPrefixEncoding(PrefixEncodingSegDict))
	if err != nil {
		t.Fatalf("NewStreamingMPHFBuilder failed: %v", err)
	}

	prefixes := []string{"", "x/", "y/", "z/"}
	for i, p := range prefixes {
		if err := builder.Add(p, uint64(i)); err != nil {
			t.Fatalf("Add(%q) failed: %v", p, err)
		}
	}

	if err := builder.Build(dir); err != nil {
		t.Fatalf("Build failed: %v", err)
	}
	builder.Close()

	m, err := OpenMPHF(dir)
	if err != nil {
		t.Fatalf("OpenMPHF failed: %v", err)
	}
	defer m.Close()

	// LookupWithVerify should work for existing prefixes
	for _, p := range prefixes {
		pos, ok := m.LookupWithVerify(p)
		if !ok {
			t.Errorf("LookupWithVerify(%q) failed", p)
		}
		_ = pos
	}

	// LookupWithVerify should fail for non-existent prefix
	_, ok := m.LookupWithVerify("missing/")
	if ok {
		t.Error("LookupWithVerify(missing) should return false")
	}
}

func TestPreloadedSegmentCache(t *testing.T) {
	dir := t.TempDir()

	// Create segment dictionary
	interner, err := NewSegmentInterner(dir)
	if err != nil {
		t.Fatalf("NewSegmentInterner failed: %v", err)
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

	// Open dictionary and preload
	dict, err := OpenSegmentDictionary(dir)
	if err != nil {
		t.Fatalf("OpenSegmentDictionary failed: %v", err)
	}
	defer dict.Close()

	cache, err := dict.PreloadSegments()
	if err != nil {
		t.Fatalf("PreloadSegments failed: %v", err)
	}

	// Verify cache count
	if cache.Count() != len(segments) {
		t.Errorf("cache.Count() = %d, want %d", cache.Count(), len(segments))
	}

	// Verify all segments can be retrieved
	for i, expected := range segments {
		got := cache.Get(uint32(i))
		if got != expected {
			t.Errorf("cache.Get(%d) = %q, want %q", i, got, expected)
		}
	}
}
