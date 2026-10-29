package format

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
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
	if err := mustBBHash(t, randomKeys(4, 1)).writeFile(filepath.Join(dir, MPHFile)); err != nil {
		t.Fatal(err)
	}
	if m, err := OpenMPHF(dir); !errors.Is(err, ErrSizeMismatch) {
		if err == nil {
			m.Close()
		}
		t.Fatalf("OpenMPHF = %v, want ErrSizeMismatch", err)
	}
}

// TestSlotBucketsManyBuckets: a build whose slots span several buckets,
// the last one partial, looks up every prefix exactly and leaves no
// scratch file behind.
func TestSlotBucketsManyBuckets(t *testing.T) {
	tmp, dir := t.TempDir(), t.TempDir()
	b, err := NewStreamingMPHFBuilder(tmp)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	const n = 300_000 // ~4.6 buckets of 2^16 11-byte slots, more than slotWriteWorkers
	for i := range n {
		if err := b.Add(prefixFromInt(i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.Build(dir); err != nil {
		t.Fatalf("Build: %v", err)
	}
	m, err := OpenMPHF(dir)
	if err != nil {
		t.Fatalf("OpenMPHF: %v", err)
	}
	defer m.Close()
	for i := range n {
		if pos, ok := m.Lookup(prefixFromInt(i)); !ok || pos != uint64(i) {
			t.Fatalf("Lookup(%q) = %d, %t; want %d", prefixFromInt(i), pos, ok, i)
		}
	}
	matches, err := filepath.Glob(filepath.Join(tmp, "mphf_slot_buckets_*"))
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 0 {
		t.Errorf("slot bucket scratch left in the temp dir: %v", matches)
	}
}

// TestSlotBucketsSizing: a bucket's slot count is a power of two, with
// at most maxSlotBuckets buckets; a bucket's bytes, the write workers'
// layout buffer, are over half the target (slotBucketBytes or a
// maxSlotBuckets'th of the slot file) and at most it, or at most twice
// it where halving the bucket would leave too many; and a slot's bucket
// and place by shift and mask are its quotient and remainder.
func TestSlotBucketsSizing(t *testing.T) {
	for _, slotW := range []int{9, 12, 16} {
		// 89_000_000 slots of 12 bytes are under maxSlotBuckets ×
		// slotBucketBytes, but rounding down leaves too many buckets.
		for _, n := range []int{1, 1000, 300_000, 1 << 27, 89_000_000, 3<<30 + 7, 1 << 40} {
			t.Run(fmt.Sprintf("slotW=%d/n=%d", slotW, n), func(t *testing.T) {
				s, err := newSlotBuckets(t.TempDir(), n, slotW)
				if err != nil {
					t.Fatal(err)
				}
				defer s.close()
				if s.perBucket != 1<<s.shift {
					t.Errorf("%d slots per bucket, shift %d; want a power of two", s.perBucket, s.shift)
				}
				target := max(slotBucketBytes, (n*slotW+maxSlotBuckets-1)/maxSlotBuckets)
				bytes := s.perBucket * slotW
				halved := (n + s.perBucket/2 - 1) / (s.perBucket / 2)
				if 2*bytes <= target || bytes > 2*target || bytes > target && halved <= maxSlotBuckets {
					t.Errorf("%d slots of %d bytes per bucket: %d bytes, want over half of %d and at most it, or twice it where halving leaves %d buckets", s.perBucket, slotW, bytes, target, halved)
				}
				if len(s.next) > maxSlotBuckets || len(s.next) != (n+s.perBucket-1)/s.perBucket {
					t.Errorf("%d buckets of %d slots for %d slots, want at most %d covering them", len(s.next), s.perBucket, n, maxSlotBuckets)
				}
				for _, slot := range []int{0, s.perBucket - 1, s.perBucket, n / 3, n - 1} {
					if slot < 0 || slot >= n {
						continue
					}
					if b, at := slot>>s.shift, slot&(s.perBucket-1); b != slot/s.perBucket || at != slot%s.perBucket {
						t.Errorf("slot %d: bucket %d at %d, want %d at %d", slot, b, at, slot/s.perBucket, slot%s.perBucket)
					}
				}
			})
		}
	}
	if _, err := newSlotBuckets(t.TempDir(), 1<<43, 16); !errors.Is(err, errTooManySlots) {
		t.Errorf("2^43 slots = %v, want errTooManySlots", err)
	}
}

// testSlotFile returns a slot file sized for n slots of slotW bytes.
func testSlotFile(t *testing.T, n, slotW int) *os.File {
	t.Helper()
	f, err := os.Create(filepath.Join(t.TempDir(), MPHFSlotsFile))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.Close() })
	if err := f.Truncate(int64(mphfSlotsDataOff + n*slotW + mphfSlotsTailPad)); err != nil {
		t.Fatal(err)
	}

	return f
}

// TestSlotBucketsWidePositions: records with 8-byte positions, as past
// 2^32 prefixes, lay out each slot's fingerprint and position.
func TestSlotBucketsWidePositions(t *testing.T) {
	const n, posW = 10, 5
	slotW := mphfFingerprintBytes + posW
	s, err := newSlotBuckets(t.TempDir(), n, slotW)
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	s.posBytes = 8
	s.recW = 4 + mphfFingerprintBytes + 8
	var buf []byte
	for slot := range n {
		buf = s.appendRecord(buf, slot, uint64(1000+slot), uint64(1)<<36+uint64(slot))
	}
	if err := s.appendRecords(0, buf); err != nil {
		t.Fatal(err)
	}
	f := testSlotFile(t, n, slotW)
	if err := s.write(f, posW, nil); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, n*slotW)
	if _, err := f.ReadAt(got, mphfSlotsDataOff); err != nil {
		t.Fatal(err)
	}
	for slot := range n {
		fp := binary.LittleEndian.Uint64(got[slot*slotW:])
		var pos [8]byte
		copy(pos[:], got[slot*slotW+mphfFingerprintBytes:(slot+1)*slotW])
		if fp != uint64(1000+slot) || binary.LittleEndian.Uint64(pos[:]) != uint64(1)<<36+uint64(slot) {
			t.Errorf("slot %d = fp %d pos %#x", slot, fp, binary.LittleEndian.Uint64(pos[:]))
		}
	}
}

// TestSlotBucketsLayoutEveryWidth: at every position width, records
// arriving in random slot order lay out each slot's fingerprint and
// position bytes exactly, without touching a neighbour's, across
// several buckets and a partial last one.
func TestSlotBucketsLayoutEveryWidth(t *testing.T) {
	const n = 200_003
	r := rand.New(rand.NewPCG(1, 2))
	order := r.Perm(n)
	for posW := 1; posW <= 8; posW++ {
		t.Run(fmt.Sprintf("posW=%d", posW), func(t *testing.T) {
			slotW := mphfFingerprintBytes + posW
			s, err := newSlotBuckets(t.TempDir(), n, slotW)
			if err != nil {
				t.Fatal(err)
			}
			defer s.close()
			if posW > 4 {
				s.posBytes = 8
				s.recW = 4 + mphfFingerprintBytes + 8
			}
			want := make([]byte, n*slotW)
			bufs := make([][]byte, len(s.next))
			for _, slot := range order {
				fp, pos := r.Uint64(), r.Uint64()&widthMask[posW]
				binary.LittleEndian.PutUint64(want[slot*slotW:], fp)
				var p [8]byte
				binary.LittleEndian.PutUint64(p[:], pos)
				copy(want[slot*slotW+mphfFingerprintBytes:(slot+1)*slotW], p[:posW])
				b := slot / s.perBucket
				bufs[b] = s.appendRecord(bufs[b], slot%s.perBucket, fp, pos)
			}
			for b, buf := range bufs {
				if err := s.appendRecords(b, buf); err != nil {
					t.Fatal(err)
				}
			}
			f := testSlotFile(t, n, slotW)
			if err := s.write(f, posW, nil); err != nil {
				t.Fatal(err)
			}
			got := make([]byte, n*slotW)
			if _, err := f.ReadAt(got, mphfSlotsDataOff); err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(got, want) {
				for j := range got {
					if got[j] != want[j] {
						t.Fatalf("slot %d byte %d = %#x, want %#x", j/slotW, j%slotW, got[j], want[j])
					}
				}
			}
		})
	}
}

// TestSlotBucketsRejectMiscounts: a bucket that receives more records
// than it has slots, or fewer, fails the build rather than write a slot
// file with stale or missing slots.
func TestSlotBucketsRejectMiscounts(t *testing.T) {
	const n = 4
	s, err := newSlotBuckets(t.TempDir(), n, 12)
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	var buf []byte
	for slot := range n - 1 {
		buf = s.appendRecord(buf, slot, 1, uint64(slot))
	}
	if err := s.appendRecords(0, buf); err != nil {
		t.Fatal(err)
	}
	if err := s.write(testSlotFile(t, n, 12), 4, nil); !errors.Is(err, ErrMPHFLookupFailed) {
		t.Errorf("write with a short bucket = %v, want ErrMPHFLookupFailed", err)
	}
	if err := s.appendRecords(0, buf); !errors.Is(err, ErrMPHFLookupFailed) {
		t.Errorf("overfilling a bucket = %v, want ErrMPHFLookupFailed", err)
	}
}

// TestSlotBucketsWriteHeapFixed: laying out the slot file allocates one
// layout and one read buffer per write worker, slotWriteWorkers of them
// whatever the CPU count, and fills every slot.
func TestSlotBucketsWriteHeapFixed(t *testing.T) {
	const n, posW = 600_000, 4
	slotW := mphfFingerprintBytes + posW
	s, err := newSlotBuckets(t.TempDir(), n, slotW)
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	if len(s.next) <= slotWriteWorkers {
		t.Fatalf("%d buckets, want more than %d write workers", len(s.next), slotWriteWorkers)
	}
	for b := range s.next {
		var buf []byte
		for j := range s.slots(b) {
			slot := b*s.perBucket + j
			buf = s.appendRecord(buf, j, uint64(1000+slot), uint64(slot))
		}
		if err := s.appendRecords(b, buf); err != nil {
			t.Fatal(err)
		}
	}
	f := testSlotFile(t, n, slotW)

	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	if err := s.write(f, posW, nil); err != nil {
		t.Fatal(err)
	}
	runtime.ReadMemStats(&after)
	perWorker := s.perBucket*slotW + slotReadBuffer
	if got, limit := after.TotalAlloc-before.TotalAlloc, uint64(slotWriteWorkers*perWorker+64<<10); got > limit {
		t.Errorf("write allocated %d bytes, want at most %d (%d workers × %d)", got, limit, slotWriteWorkers, perWorker)
	}

	got := make([]byte, n*slotW)
	if _, err := f.ReadAt(got, mphfSlotsDataOff); err != nil {
		t.Fatal(err)
	}
	for slot := range n {
		fp := binary.LittleEndian.Uint64(got[slot*slotW:])
		pos := binary.LittleEndian.Uint32(got[slot*slotW+mphfFingerprintBytes:])
		if fp != uint64(1000+slot) || pos != uint32(slot) {
			t.Fatalf("slot %d = fp %d pos %d", slot, fp, pos)
		}
	}
}

// TestMapScratchFileReservesBlocks: the scratch map's disk blocks are
// allocated before it is mapped, so a full temp volume fails the build
// with an error rather than a SIGBUS on the first write to a page.
func TestMapScratchFileReservesBlocks(t *testing.T) {
	const size = 1 << 20
	m, err := mapScratchFile(t.TempDir(), "scratch_*.tmp", size)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := unix.Munmap(m.data); err != nil {
			t.Error(err)
		}
	}()
	if len(m.data) != size {
		t.Fatalf("mapped %d bytes, want %d", len(m.data), size)
	}
	var st unix.Stat_t
	if err := unix.Stat(m.path, &st); err != nil {
		t.Fatal(err)
	}
	if st.Size != size {
		t.Errorf("file size = %d, want %d", st.Size, size)
	}
	if allocated := st.Blocks * 512; allocated < size {
		t.Errorf("allocated %d bytes on disk, want at least %d", allocated, size)
	}
}

// TestWriteZerosFillsFile: the fallback for filesystems with no block
// reservation call writes the whole range, including a partial last
// chunk.
func TestWriteZerosFillsFile(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "zeros_*.tmp")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	const size = preallocateChunk + 4097
	if err := writeZeros(f, 0, size); err != nil {
		t.Fatal(err)
	}
	var st unix.Stat_t
	if err := unix.Fstat(int(f.Fd()), &st); err != nil {
		t.Fatal(err)
	}
	if st.Size != size {
		t.Errorf("file size = %d, want %d", st.Size, size)
	}
	if allocated := st.Blocks * 512; allocated < size {
		t.Errorf("allocated %d bytes on disk, want at least %d", allocated, size)
	}
}

// TestMPHFLevelKeysInScratch: a build large enough that its first
// level's colliding keys go to a scratch file looks up every prefix
// exactly, and leaves no scratch file behind.
func TestMPHFLevelKeysInScratch(t *testing.T) {
	tmp, dir := t.TempDir(), t.TempDir()
	b, err := NewStreamingMPHFBuilder(tmp)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	const n = 400_000 // ~39% collide on level 0: ~1.2 MiB of keys
	for i := range n {
		if err := b.Add(prefixFromInt(i + 1)); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.Build(dir); err != nil {
		t.Fatal(err)
	}
	if entries, err := os.ReadDir(tmp); err != nil || len(entries) != 0 {
		t.Fatalf("temp dir after Build: %v (%v)", entries, err)
	}
	m, err := OpenMPHF(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	for i := range n {
		if pos, ok := m.Lookup(prefixFromInt(i + 1)); !ok || pos != uint64(i) {
			t.Fatalf("prefix %d: position %d (found %v)", i, pos, ok)
		}
	}
}

func TestMPHFWorkers(t *testing.T) {
	for _, c := range []struct{ procs, n, want int }{
		{8, 0, 1},
		{8, 4095, 1},
		{8, 3 * 4096, 3},
		{8, 1 << 30, 8},
		{1, 1 << 30, 1},
	} {
		if got := mphfWorkers(c.procs, c.n); got != c.want {
			t.Errorf("mphfWorkers(%d, %d) = %d, want %d", c.procs, c.n, got, c.want)
		}
	}
}

// TestMPHFWorkerCountFollowsGOMAXPROCS: a container CPU quota lowers
// GOMAXPROCS but not NumCPU. Setting it above NumCPU makes the two
// differ on any host.
func TestMPHFWorkerCountFollowsGOMAXPROCS(t *testing.T) {
	procs := runtime.NumCPU() + 3
	prev := runtime.GOMAXPROCS(procs)
	t.Cleanup(func() { runtime.GOMAXPROCS(prev) })

	if got := mphfWorkerCount(1 << 30); got != procs {
		t.Errorf("mphfWorkerCount = %d with GOMAXPROCS %d, want %d", got, procs, procs)
	}
}
