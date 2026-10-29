package format

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
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

// TestMPHFPositionsPastOneByte pins that the slot position width fits
// the largest position: 300 prefixes put positions past 255, which a
// one-byte width would truncate.
func TestMPHFPositionsPastOneByte(t *testing.T) {
	dir := t.TempDir()
	b := newTestMPHFBuilder(t)

	const n = 300
	posOf := func(i int) uint64 { return uint64(i) }
	for i := range n {
		b.Add(prefixFromInt(i+1), posOf(i))
	}
	if err := b.Build(dir); err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	m, err := OpenMPHF(dir)
	if err != nil {
		t.Fatalf("OpenMPHF failed: %v", err)
	}
	defer m.Close()

	for i := range n {
		p := prefixFromInt(i + 1)
		if pos, ok := m.Lookup(p); !ok || pos != posOf(i) {
			t.Errorf("Lookup(%q) = %d, %v; want %d, true", p, pos, ok, posOf(i))
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

// testMPHFBuilder wraps StreamingMPHFBuilder with a test's failure
// handling and checks each prefix's position.
type testMPHFBuilder struct {
	tb testing.TB
	sb *StreamingMPHFBuilder
}

func newTestMPHFBuilder(tb testing.TB) *testMPHFBuilder {
	tb.Helper()
	sb := NewStreamingMPHFBuilder(tb.TempDir())
	tb.Cleanup(func() { sb.Close() })

	return &testMPHFBuilder{tb: tb, sb: sb}
}

// Add adds prefix, which must be at position pos: the builder numbers
// prefixes in the order they are added.
func (b *testMPHFBuilder) Add(prefix string, pos uint64) {
	b.tb.Helper()
	if pos != b.sb.Count() {
		b.tb.Fatalf("Add(%q, %d): the builder's next position is %d", prefix, pos, b.sb.Count())
	}
	if err := b.sb.Add(prefix); err != nil {
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

// TestKeyHashDiffersFromFingerprint: the key hash and the fingerprint
// use different seeds, so keys that share a key hash still differ in
// the fingerprint.
func TestKeyHashDiffersFromFingerprint(t *testing.T) {
	for _, s := range []string{"", "a", "data/2024/", "日本語/", strings.Repeat("x/", 100)} {
		if hashString(s) == computeFingerprint(s) {
			t.Errorf("key hash and fingerprint of %q are equal", s)
		}
	}
}

// collidingKeyHash gives every prefix in group the real key hash of
// group[0], so Lookup(group[0]) still reaches it.
func collidingKeyHash(group ...string) func(string) uint64 {
	return func(s string) uint64 {
		if slices.Contains(group, s) {
			return hashString(group[0])
		}

		return hashString(s)
	}
}

// lookupAs is Lookup with keyHash standing in for hashString, to reach
// prefixes whose key hash a test replaced.
func lookupAs(m *MPHF, keyHash func(string) uint64, prefix string) (uint64, bool) {
	if m.count == 0 {
		return 0, false
	}
	key := keyHash(prefix)
	if slot, ok := m.mph.find(key); ok {
		return m.slotPosition(slot, computeFingerprint(prefix))
	}

	return m.lookupFallback(prefix, key)
}

// readSlots returns the raw mph_slots.bin bytes and its header.
func readSlots(t *testing.T, dir string) ([]byte, Header) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dir, MPHFSlotsFile))
	if err != nil {
		t.Fatalf("read slots: %v", err)
	}
	h, err := DecodeHeader(data)
	if err != nil {
		t.Fatalf("decode slots header: %v", err)
	}

	return data, h
}

// buildWithKeyHash builds an MPHF in dir over prefixes (position =
// slice index) whose key hashes come from keyHash, and opens it.
func buildWithKeyHash(t *testing.T, dir string, prefixes []string, keyHash func(string) uint64) *MPHF {
	t.Helper()
	b := newTestMPHFBuilder(t)
	b.sb.keyHash = keyHash
	for i, p := range prefixes {
		b.Add(p, uint64(i))
	}
	if err := b.Build(dir); err != nil {
		t.Fatalf("Build: %v", err)
	}
	m, err := OpenMPHF(dir)
	if err != nil {
		t.Fatalf("OpenMPHF: %v", err)
	}
	t.Cleanup(func() { m.Close() })

	return m
}

// assertLookups checks every prefix resolves to its index under keyHash
// and that non-members miss, through the fallback as well as the levels.
func assertLookups(t *testing.T, m *MPHF, keyHash func(string) uint64, prefixes []string) {
	t.Helper()
	for i, p := range prefixes {
		if pos, ok := lookupAs(m, keyHash, p); !ok || pos != uint64(i) {
			t.Errorf("lookup(%q) = %d, %v; want %d, true", p, pos, ok, i)
		}
	}
	for _, p := range []string{"absent/", "zz/zz/zz/", prefixes[0] + "x"} {
		if pos, ok := m.Lookup(p); ok {
			t.Errorf("Lookup(%q) = %d for a non-member", p, pos)
		}
		// A non-member that shares the fallback keys' hash must still
		// miss on the fingerprint.
		if len(m.mph.fallback) > 0 {
			if pos, ok := m.lookupFallback(p, m.mph.fallback[0].hash); ok {
				t.Errorf("fallback lookup(%q) = %d for a non-member", p, pos)
			}
		}
	}
}

// Distinct prefixes that share one 64-bit key hash all go to the
// fallback table, and the levels place the rest.
func TestStreamingMPHFBuildKeyHashCollisionMixed(t *testing.T) {
	prefixes := make([]string, 2000)
	for i := range prefixes {
		prefixes[i] = prefixFromInt(i + 1)
	}
	group := []string{prefixes[3], prefixes[700], prefixes[1500], prefixes[1999]}
	keyHash := collidingKeyHash(group...)
	dir := t.TempDir()
	m := buildWithKeyHash(t, dir, prefixes, keyHash)

	if got := len(m.mph.fallback); got != len(group) {
		t.Fatalf("fallback holds %d keys, want %d", got, len(group))
	}
	if m.mph.placed != uint64(len(prefixes)-len(group)) || m.Count() != uint64(len(prefixes)) {
		t.Fatalf("placed = %d, count = %d; want %d, %d", m.mph.placed, m.Count(), len(prefixes)-len(group), len(prefixes))
	}
	// Group[0]'s stand-in hash is its real one, so the public Lookup
	// reaches it through the fallback.
	if pos, ok := m.Lookup(group[0]); !ok || pos != 3 {
		t.Errorf("Lookup(%q) = %d, %v; want 3, true", group[0], pos, ok)
	}
	assertLookups(t, m, keyHash, prefixes)

	data, h := readSlots(t, dir)
	if h.Version != Version {
		t.Errorf("slots Version = %d, want %d", h.Version, Version)
	}
	// 2000 positions need 2 bytes.
	slotW := mphfFingerprintBytes + 2
	if int(h.Width) != slotW {
		t.Errorf("slot width = %d, want %d", h.Width, slotW)
	}
	if want := mphfSlotsDataOff + slotW*len(prefixes) + mphfSlotsTailPad; len(data) != want {
		t.Errorf("slots size = %d, want %d", len(data), want)
	}
}

// When every prefix shares one key hash, the levels hold nothing and
// the whole MPHF is the fallback table.
func TestStreamingMPHFBuildAllKeysCollide(t *testing.T) {
	prefixes := []string{"", "a/", "a/b/", "b/", "c/d/"}
	keyHash := func(string) uint64 { return hashString(prefixes[0]) }
	m := buildWithKeyHash(t, t.TempDir(), prefixes, keyHash)
	if len(m.mph.levels) != 0 || len(m.mph.fallback) != len(prefixes) {
		t.Fatalf("levels = %d, fallback = %d; want 0, %d", len(m.mph.levels), len(m.mph.fallback), len(prefixes))
	}
	assertLookups(t, m, keyHash, prefixes)
}

// Two prefixes equal in both the key hash and the fingerprint cannot
// be told apart, so the build fails rather than drop one.
func TestStreamingMPHFBuildFullCollisionFails(t *testing.T) {
	b := newTestMPHFBuilder(t)
	b.sb.keyHash = collidingKeyHash("a/", "b/")
	b.sb.fingerprint = func(s string) uint64 {
		if s == "b/" {
			return computeFingerprint("a/")
		}

		return computeFingerprint(s)
	}
	for i, p := range []string{"", "a/", "b/", "c/"} {
		b.Add(p, uint64(i))
	}
	if err := b.Build(t.TempDir()); !errors.Is(err, ErrMPHFKeyCollision) {
		t.Fatalf("Build = %v, want ErrMPHFKeyCollision", err)
	}
}

// A large build with no forced collision leaves the fallback empty.
func TestStreamingMPHFBuildLargeNoFallback(t *testing.T) {
	const n = 200_000
	prefixes := make([]string, n)
	for i := range prefixes {
		prefixes[i] = prefixFromInt(i + 1)
	}
	m := buildWithKeyHash(t, t.TempDir(), prefixes, hashString)
	if len(m.mph.fallback) != 0 || m.mph.placed != n {
		t.Fatalf("fallback = %d, placed = %d; want 0, %d", len(m.mph.fallback), m.mph.placed, n)
	}
	for i, p := range prefixes {
		if pos, ok := m.Lookup(p); !ok || pos != uint64(i) {
			t.Fatalf("Lookup(%q) = %d, %v; want %d, true", p, pos, ok, i)
		}
	}
}

func TestOpenMPHFTruncatedSlots(t *testing.T) {
	dir := buildTestMPHF(t, []string{"", "a/", "b/"})
	path := filepath.Join(dir, MPHFSlotsFile)
	_, h := readSlots(t, dir)
	if err := os.Truncate(path, int64(mphfSlotsDataOff)+int64(h.Width)*3); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	if m, err := OpenMPHF(dir); !errors.Is(err, ErrFileTooSmall) {
		if err == nil {
			m.Close()
		}
		t.Fatalf("OpenMPHF = %v, want ErrFileTooSmall", err)
	}
}

// mph.bin and mph_slots.bin must agree on the key count.
func TestOpenMPHFCountMismatch(t *testing.T) {
	dir := buildTestMPHF(t, []string{"", "a/", "b/"})
	keys := randomKeys(4, 1)
	parts, err := testPartitions(t, keys, testFingerprints(keys), 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, MPHFile), encodeTable(t, 0, parts), 0o600); err != nil {
		t.Fatal(err)
	}
	if m, err := OpenMPHF(dir); !errors.Is(err, ErrSizeMismatch) {
		if err == nil {
			m.Close()
		}
		t.Fatalf("OpenMPHF = %v, want ErrSizeMismatch", err)
	}
}
