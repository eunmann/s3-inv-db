package format

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/relab/bbhash"
)

func TestMPHFBuilderEmpty(t *testing.T) {
	dir := t.TempDir()
	b := newTestMPHFBuilder(t)

	if err := b.Build(dir); err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	m, err := OpenMPHF(dir)
	if err != nil {
		t.Fatalf("OpenMPHF failed: %v", err)
	}
	defer m.Close()

	if m.Count() != 0 {
		t.Errorf("Count = %d, want 0", m.Count())
	}

	_, ok := m.Lookup("test")
	if ok {
		t.Error("expected Lookup to return false for empty MPHF")
	}
}

func TestMPHFBuilderSimple(t *testing.T) {
	dir := t.TempDir()
	b := newTestMPHFBuilder(t)

	prefixes := []string{"", "a/", "a/b/", "b/", "c/"}
	for i, p := range prefixes {
		b.Add(p, uint64(i))
	}

	// Build writes the prefix blob in preorder
	if err := b.Build(dir); err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	m, err := OpenMPHF(dir)
	if err != nil {
		t.Fatalf("OpenMPHF failed: %v", err)
	}
	defer m.Close()

	if m.Count() != uint64(len(prefixes)) {
		t.Errorf("Count = %d, want %d", m.Count(), len(prefixes))
	}

	// Test lookup of all prefixes
	for _, p := range prefixes {
		pos, ok := m.Lookup(p)
		if !ok {
			t.Errorf("Lookup(%q) failed", p)

			continue
		}
		// Verify we can get the prefix back
		stored, err := m.Prefix(pos)
		if err != nil {
			t.Errorf("GetPrefix(%d) failed: %v", pos, err)
		}
		if stored != p {
			t.Errorf("GetPrefix(%d) = %q, want %q", pos, stored, p)
		}
	}

	// Test lookup of non-existent prefix
	_, ok := m.Lookup("nonexistent/")
	if ok {
		t.Error("Lookup(nonexistent) should return false")
	}
}

func TestMPHFLarge(t *testing.T) {
	dir := t.TempDir()
	b := newTestMPHFBuilder(t)

	// Create 1000 unique prefixes
	prefixes := make([]string, 1000)
	for i := range 1000 {
		prefixes[i] = prefixFromInt(i)
	}

	for i, p := range prefixes {
		b.Add(p, uint64(i))
	}

	if err := b.Build(dir); err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	m, err := OpenMPHF(dir)
	if err != nil {
		t.Fatalf("OpenMPHF failed: %v", err)
	}
	defer m.Close()

	if m.Count() != 1000 {
		t.Errorf("Count = %d, want 1000", m.Count())
	}

	// Verify all lookups round-trip to their build positions.
	for i := range uint64(1000) {
		p := prefixFromInt(int(i))
		pos, ok := m.Lookup(p)
		if !ok {
			t.Errorf("Lookup(%q) failed", p)
		}
		if pos != i {
			t.Errorf("Lookup(%q) = %d, want %d", p, pos, i)
		}
	}

	// Test some random lookups
	for i := range 100 {
		p := prefixFromInt(i * 10)
		pos, ok := m.Lookup(p)
		if !ok {
			t.Errorf("Lookup(%q) failed", p)
		}
		_ = pos
	}
}

func TestMPHFNoFalsePositives(t *testing.T) {
	dir := t.TempDir()
	b := newTestMPHFBuilder(t)

	prefixes := []string{"alpha/", "beta/", "gamma/"}
	for i, p := range prefixes {
		b.Add(p, uint64(i))
	}

	if err := b.Build(dir); err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	m, err := OpenMPHF(dir)
	if err != nil {
		t.Fatalf("OpenMPHF failed: %v", err)
	}
	defer m.Close()

	// These should all fail (not in the set)
	nonExistent := []string{
		"delta/",
		"epsilon/",
		"alpha", // missing trailing slash
		"/alpha",
		"ALPHA/", // case sensitive
		"",
	}

	for _, p := range nonExistent {
		_, ok := m.Lookup(p)
		if ok {
			t.Errorf("Lookup(%q) should return false", p)
		}
	}
}

func TestMPHFUnicode(t *testing.T) {
	dir := t.TempDir()
	b := newTestMPHFBuilder(t)

	prefixes := []string{"", "日本語/", "한국어/", "emoji/🎉/"}
	for i, p := range prefixes {
		b.Add(p, uint64(i))
	}

	if err := b.Build(dir); err != nil {
		t.Fatalf("Build failed: %v", err)
	}

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
		stored, _ := m.Prefix(pos)
		if stored != p {
			t.Errorf("GetPrefix returned %q, want %q", stored, p)
		}
	}
}

// TestEmptyMPHF_GetPrefixReturnsError asserts that GetPrefix on an
// empty MPHF (the size-0 mph.bin fast path) returns ErrNoPrefixStorage
// instead of nil-dereferencing the unset dictPrefixes field.
func TestEmptyMPHF_GetPrefixReturnsError(t *testing.T) {
	dir := t.TempDir()
	b := newTestMPHFBuilder(t)
	if err := b.Build(dir); err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	m, err := OpenMPHF(dir)
	if err != nil {
		t.Fatalf("OpenMPHF: %v", err)
	}
	defer m.Close()

	if _, err := m.GetPrefix(0); !errors.Is(err, ErrNoPrefixStorage) {
		t.Errorf("GetPrefix on empty MPHF = %v, want ErrNoPrefixStorage", err)
	}
}

// TestOpenMPHF_MissingDictFiles asserts OpenMPHF surfaces an error
// (not a panic) when the prefix dictionary files are absent. The old
// raw-blob fallback is gone, so the dict files are the only prefix
// storage; their absence must fail cleanly.
func TestOpenMPHF_MissingDictFiles(t *testing.T) {
	dir := t.TempDir()
	b := newTestMPHFBuilder(t)
	b.Add("a/", 0)
	b.Add("b/", 1)
	if err := b.Build(dir); err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	for _, name := range []string{
		PrefixDictBlobFile,
		PrefixDictOffsetsFile,
		PrefixDictIDsFile,
		PrefixDictOffsetsPerPrefixFile,
	} {
		if err := os.Remove(filepath.Join(dir, name)); err != nil {
			t.Fatalf("remove %s: %v", name, err)
		}
	}

	m, err := OpenMPHF(dir)
	if err == nil {
		m.Close()
		t.Fatal("OpenMPHF returned nil error when prefix dict files are missing")
	}
}

func TestComputeFingerprint(t *testing.T) {
	// Same input should give same fingerprint
	fp1 := computeFingerprint("test")
	fp2 := computeFingerprint("test")
	if fp1 != fp2 {
		t.Error("fingerprint not deterministic")
	}

	// Different input should give different fingerprint
	fp3 := computeFingerprint("other")
	if fp1 == fp3 {
		t.Error("different inputs gave same fingerprint")
	}
}

// testMPHFBuilder wraps StreamingMPHFBuilder with the no-arg shape
// the legacy MPHFBuilder API offered. Internal to this test file
// so the production builder stays the single non-test code path.
type testMPHFBuilder struct {
	tb testing.TB
	sb *StreamingMPHFBuilder
}

func newTestMPHFBuilder(tb testing.TB) *testMPHFBuilder {
	tb.Helper()
	sb, err := NewStreamingMPHFBuilder(tb.TempDir())
	if err != nil {
		tb.Fatalf("NewStreamingMPHFBuilder: %v", err)
	}

	return &testMPHFBuilder{tb: tb, sb: sb}
}

func (b *testMPHFBuilder) Add(prefix string, pos uint64) {
	b.tb.Helper()
	if err := b.sb.Add(prefix, pos); err != nil {
		b.tb.Fatalf("Add(%q, %d): %v", prefix, pos, err)
	}
}

func (b *testMPHFBuilder) Build(dir string) error { return b.sb.Build(dir) }

// Helper function to create unique prefix strings.
func prefixFromInt(i int) string {
	// Create a path like "a/b/c/" based on integer
	result := ""
	for i > 0 {
		c := 'a' + rune(i%26)
		result = string(c) + "/" + result
		i /= 26
	}
	if result == "" {
		result = "root/"
	}

	return result
}

// buildTestMPHF builds an MPHF over prefixes (position = slice index)
// into a fresh directory and returns it.
func buildTestMPHF(t *testing.T, prefixes []string) string {
	t.Helper()
	dir := t.TempDir()
	b := newTestMPHFBuilder(t)
	for i, p := range prefixes {
		b.Add(p, uint64(i))
	}
	if err := b.Build(dir); err != nil {
		t.Fatalf("Build: %v", err)
	}

	return dir
}

// TestMPHFLookupRejectsFingerprintFalsePositive forges the stored
// fingerprint of the slot a non-member query hashes to, so the
// fingerprint pre-filter passes; Lookup must still report not-found
// because the stored prefix differs from the query.
func TestMPHFLookupRejectsFingerprintFalsePositive(t *testing.T) {
	prefixes := make([]string, 1000)
	for i := range prefixes {
		prefixes[i] = prefixFromInt(i + 1)
	}
	dir := buildTestMPHF(t, prefixes)

	mphData, err := os.ReadFile(filepath.Join(dir, MPHFile))
	if err != nil {
		t.Fatalf("read mph: %v", err)
	}
	mph := &bbhash.BBHash2{}
	if err := mph.UnmarshalBinary(mphData); err != nil {
		t.Fatalf("unmarshal mph: %v", err)
	}
	query, slot := "", uint64(0)
	for i := range 1_000_000 {
		q := fmt.Sprintf("absent/%d/", i)
		if v := mph.Find(hashString(q)); v != 0 && v <= uint64(len(prefixes)) {
			query, slot = q, v-1

			break
		}
	}
	if query == "" {
		t.Fatal("no non-member query maps to a slot")
	}

	f, err := os.OpenFile(filepath.Join(dir, CombinedMPHFArrayFile), os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("open fp/pos: %v", err)
	}
	var fp [8]byte
	binary.LittleEndian.PutUint64(fp[:], computeFingerprint(query))
	_, werr := f.WriteAt(fp[:], int64(HeaderSize+16*slot))
	if err := errors.Join(werr, f.Close()); err != nil {
		t.Fatalf("forge fingerprint: %v", err)
	}

	m, err := OpenMPHF(dir)
	if err != nil {
		t.Fatalf("OpenMPHF: %v", err)
	}
	defer m.Close()

	if pos, ok := m.Lookup(query); ok {
		t.Fatalf("Lookup(%q) = %d, true; want not found", query, pos)
	}
}

// TestMPHFLookupRoundTripsEdgePrefixes checks every stored prefix
// looks up to its own position, including empty segments and
// trailing-slash forms that the segment-wise verification must
// reassemble exactly.
func TestMPHFLookupRoundTripsEdgePrefixes(t *testing.T) {
	prefixes := []string{"", "/", "//", "a", "a/", "a//", "a/b", "a/b/", "ab/", "/a", "日本語/x/", "emoji/🎉/"}
	dir := buildTestMPHF(t, prefixes)

	m, err := OpenMPHF(dir)
	if err != nil {
		t.Fatalf("OpenMPHF: %v", err)
	}
	defer m.Close()

	for i, p := range prefixes {
		pos, ok := m.Lookup(p)
		if !ok || pos != uint64(i) {
			t.Errorf("Lookup(%q) = %d, %v; want %d, true", p, pos, ok, i)
		}
	}
	if err := VerifyMPHF(m); err != nil {
		t.Errorf("VerifyMPHF: %v", err)
	}
	if allocs := testing.AllocsPerRun(100, func() { _, _ = m.Lookup("a/b/") }); allocs != 0 {
		t.Errorf("Lookup allocs = %v, want 0", allocs)
	}
}

// TestDictPrefixReaderPrefixEquals compares every stored prefix
// against every candidate, including near misses that differ only in
// a separator, a trailing slash or one segment's length.
func TestDictPrefixReaderPrefixEquals(t *testing.T) {
	stored := []string{"", "/", "a", "a/", "a//", "a/b", "a/b/", "ab/", "data/2024/01/"}
	candidates := append([]string{
		"b", "a/c", "a/bb", "a/b//", "ab", "a/b/c", "data/2024/", "data/2024/01", "data/2024/011/", "data/2024-01/",
	}, stored...)
	dir := buildTestMPHF(t, stored)

	r, err := OpenDictPrefixReader(dir)
	if err != nil {
		t.Fatalf("OpenDictPrefixReader: %v", err)
	}
	defer r.Close()

	for pos, want := range stored {
		for _, c := range candidates {
			if got := r.prefixEquals(uint64(pos), c); got != (c == want) {
				t.Errorf("prefixEquals(%d [%q], %q) = %v", pos, want, c, got)
			}
		}
	}
	if r.prefixEquals(uint64(len(stored)), "") {
		t.Error("prefixEquals past the last position = true")
	}
}
