package format

import (
	"errors"
	"hash/fnv"
	"os"
	"path/filepath"
	"strings"
	"testing"
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

	// Every prefix looks up to the position it was added at.
	for i, p := range prefixes {
		pos, ok := m.Lookup(p)
		if !ok || pos != uint64(i) {
			t.Errorf("Lookup(%q) = %d, %v; want %d, true", p, pos, ok, i)
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

	for i, p := range prefixes {
		if pos, ok := m.Lookup(p); !ok || pos != uint64(i) {
			t.Errorf("Lookup(%q) = %d, %v; want %d, true", p, pos, ok, i)
		}
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
	if allocs := testing.AllocsPerRun(100, func() { _, _ = m.Lookup("a/b/") }); allocs != 0 {
		t.Errorf("Lookup allocs = %v, want 0", allocs)
	}
}

// TestHashBytesSeedZeroIsFNV1a pins seed 0 to plain FNV-1a, the key
// hash every pre-seed index was built with.
func TestHashBytesSeedZeroIsFNV1a(t *testing.T) {
	for _, s := range []string{"", "a", "data/2024/", "日本語/", strings.Repeat("x/", 100)} {
		h := fnv.New64a()
		_, _ = h.Write([]byte(s))
		if got, want := hashString(s, 0), h.Sum64(); got != want {
			t.Errorf("hashString(%q, 0) = %#x, want FNV-1a %#x", s, got, want)
		}
		if hashString(s, mphfSeed(1)) == hashString(s, 0) {
			t.Errorf("hashString(%q) ignores the seed", s)
		}
	}
}

// collidingKeyHash makes a and b share a key hash under seed 0 only.
func collidingKeyHash(a, b string) func(string, uint64) uint64 {
	return func(s string, seed uint64) uint64 {
		if seed == 0 && (s == a || s == b) {
			return hashString(a, 0)
		}

		return hashString(s, seed)
	}
}

// readCombined returns the raw mph_fp_pos.u64 bytes and its header.
func readCombined(t *testing.T, dir string) ([]byte, Header) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dir, CombinedMPHFArrayFile))
	if err != nil {
		t.Fatalf("read fp/pos: %v", err)
	}
	h, err := DecodeHeader(data)
	if err != nil {
		t.Fatalf("decode fp/pos header: %v", err)
	}

	return data, h
}

func TestStreamingMPHFBuildSurvivesKeyHashCollision(t *testing.T) {
	prefixes := make([]string, 2000)
	for i := range prefixes {
		prefixes[i] = prefixFromInt(i + 1)
	}
	dir := t.TempDir()
	b := newTestMPHFBuilder(t)
	b.sb.keyHash = collidingKeyHash(prefixes[3], prefixes[1500])
	for i, p := range prefixes {
		b.Add(p, uint64(i))
	}
	if err := b.Build(dir); err != nil {
		t.Fatalf("Build with a seed-0 key collision: %v", err)
	}

	data, h := readCombined(t, dir)
	if h.Version != Version {
		t.Errorf("fp/pos Version = %d, want %d", h.Version, Version)
	}
	if want := (HeaderSize + 16*len(prefixes) + mphfSeedBytes); len(data) != want {
		t.Errorf("fp/pos size = %d, want %d", len(data), want)
	}

	m, err := OpenMPHF(dir)
	if err != nil {
		t.Fatalf("OpenMPHF: %v", err)
	}
	defer m.Close()
	if m.seed == 0 {
		t.Fatal("seed = 0 after a seed-0 collision")
	}
	for i, p := range prefixes {
		if pos, ok := m.Lookup(p); !ok || pos != uint64(i) {
			t.Errorf("Lookup(%q) = %d, %v; want %d, true", p, pos, ok, i)
		}
	}
}

func TestStreamingMPHFBuildPersistentKeyCollisionFails(t *testing.T) {
	b := newTestMPHFBuilder(t)
	b.sb.keyHash = func(s string, seed uint64) uint64 {
		if s == "a/" || s == "b/" {
			return seed
		}

		return hashString(s, seed)
	}
	for i, p := range []string{"", "a/", "b/"} {
		b.Add(p, uint64(i))
	}
	if err := b.Build(t.TempDir()); !errors.Is(err, ErrMPHFKeyCollision) {
		t.Fatalf("Build = %v, want ErrMPHFKeyCollision", err)
	}
}

func TestOpenMPHFMissingSeedWord(t *testing.T) {
	dir := t.TempDir()
	b := newTestMPHFBuilder(t)
	b.sb.keyHash = collidingKeyHash("a/", "b/")
	for i, p := range []string{"", "a/", "b/"} {
		b.Add(p, uint64(i))
	}
	if err := b.Build(dir); err != nil {
		t.Fatalf("Build: %v", err)
	}
	path := filepath.Join(dir, CombinedMPHFArrayFile)
	if err := os.Truncate(path, HeaderSize+16*3); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	if m, err := OpenMPHF(dir); !errors.Is(err, ErrFileTooSmall) {
		if err == nil {
			m.Close()
		}
		t.Fatalf("OpenMPHF = %v, want ErrFileTooSmall", err)
	}
}

// The written-out fingerprint must stay hash/fnv's FNV-1: indexes store it.
func TestComputeFingerprintMatchesFNV1(t *testing.T) {
	for _, s := range []string{"", "a", "dir/sub/", "données/été/"} {
		h := fnv.New64()
		_, _ = h.Write([]byte(s))
		if got, want := computeFingerprint(s), h.Sum64(); got != want {
			t.Errorf("computeFingerprint(%q) = %#x, want %#x", s, got, want)
		}
	}
}
