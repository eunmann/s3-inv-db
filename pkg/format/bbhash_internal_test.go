package format

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/eunmann/s3-inv-db/internal/benchutil"
)

func randomKeys(n int, seed uint64) []uint64 {
	r := rand.New(rand.NewPCG(seed, seed^0x5eed))
	seen := make(map[uint64]struct{}, n)
	keys := make([]uint64, 0, n)
	for len(keys) < n {
		k := r.Uint64()
		if _, dup := seen[k]; !dup {
			seen[k] = struct{}{}
			keys = append(keys, k)
		}
	}

	return keys
}

var errUnexpectedLeftovers = errors.New("distinct keys left unplaced")

// noFallback is the fallback for distinct keys, which the levels must
// all place.
func noFallback(leftovers []uint64) ([]bbFallback, error) {
	return nil, fmt.Errorf("%w: %d", errUnexpectedLeftovers, len(leftovers))
}

// mustBBHash builds the table over distinct keys.
func mustBBHash(tb testing.TB, keys []uint64) *bbhash {
	tb.Helper()
	h, err := newBBHash(keys, noFallback)
	if err != nil {
		tb.Fatalf("newBBHash: %v", err)
	}

	return h
}

// keepLeftovers is a fallback that stores the keys the levels left
// unplaced in dst and adds no fallback entries.
func keepLeftovers(dst *[]uint64) func([]uint64) ([]bbFallback, error) {
	return func(leftovers []uint64) ([]bbFallback, error) {
		*dst = leftovers

		return nil, nil
	}
}

// assertMinimalPerfect checks that the keys map one-to-one onto [0, n).
func assertMinimalPerfect(t *testing.T, h *bbhash, keys []uint64) {
	t.Helper()
	if h.count != uint64(len(keys)) {
		t.Fatalf("count = %d, want %d", h.count, len(keys))
	}
	owner := make([]int, len(keys))
	for i := range owner {
		owner[i] = -1
	}
	for i, k := range keys {
		slot, ok := h.find(k)
		if !ok || slot >= uint64(len(keys)) {
			t.Fatalf("key %d: slot %d, found %t", i, slot, ok)
		}
		if owner[slot] >= 0 {
			t.Fatalf("keys %d and %d share slot %d", owner[slot], i, slot)
		}
		owner[slot] = i
	}
}

func TestBBHashIsMinimalPerfect(t *testing.T) {
	for _, n := range []int{1, 2, 63, 64, 65, 1000, 100_000} {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			keys := randomKeys(n, uint64(n))
			assertMinimalPerfect(t, mustBBHash(t, keys), keys)
		})
	}
}

// Low-entropy keys (sequential integers, like a weak key hash) must
// still spread: the level hash mixes before reducing.
func TestBBHashSequentialKeys(t *testing.T) {
	keys := make([]uint64, 50_000)
	for i := range keys {
		keys[i] = uint64(i) << 20
	}
	assertMinimalPerfect(t, mustBBHash(t, keys), keys)
}

// Equal keys collide on every level: the build stops at the first level
// that places nothing, without appending it, and returns them; every
// other key is placed on its own slot.
func TestBBHashLeavesEqualKeysOver(t *testing.T) {
	distinct := randomKeys(10_000, 7)
	keys := append(slices.Clone(distinct), 42, 42, 42)
	var leftovers []uint64
	h, err := newBBHash(keys, keepLeftovers(&leftovers))
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(leftovers, []uint64{42, 42, 42}) {
		t.Fatalf("leftovers = %v, want three 42s", leftovers)
	}
	if h.placed != uint64(len(distinct)) || h.count != h.placed {
		t.Fatalf("placed = %d, count = %d; want %d", h.placed, h.count, len(distinct))
	}
	if len(h.levels) >= maxBBHashLevels {
		t.Errorf("built %d levels; the build must stop once a level places nothing", len(h.levels))
	}
	last := h.levels[len(h.levels)-1]
	if !slices.ContainsFunc(h.bits[last.offset:last.offset+last.words], func(w uint64) bool { return w != 0 }) {
		t.Error("the last level places no key")
	}
	if _, ok := h.find(42); ok {
		t.Error("an unplaced key hits a level")
	}
	assertMinimalPerfect(t, h, distinct)
}

// Keys that all share one hash leave no level at all: the whole table is
// the fallback.
func TestBBHashAllKeysEqual(t *testing.T) {
	var leftovers []uint64
	h, err := newBBHash([]uint64{7, 7}, keepLeftovers(&leftovers))
	if err != nil {
		t.Fatal(err)
	}
	if len(h.levels) != 0 || h.placed != 0 || len(leftovers) != 2 {
		t.Fatalf("levels = %d, placed = %d, leftovers = %v; want 0, 0, two keys", len(h.levels), h.placed, leftovers)
	}
}

// The parallel build must not depend on worker scheduling.
func TestBBHashBuildIsDeterministic(t *testing.T) {
	keys := randomKeys(200_000, 3)
	a, b := mustBBHash(t, keys), mustBBHash(t, keys)
	if !slices.Equal(a.bits, b.bits) || !slices.Equal(a.levels, b.levels) {
		t.Fatal("two builds over the same keys differ")
	}
}

func TestBBHashFileRoundTrip(t *testing.T) {
	keys := randomKeys(30_000, 11)
	h := mustBBHash(t, keys)
	got := roundTripBBHash(t, h)
	for i, k := range keys {
		want, _ := h.find(k)
		if slot, ok := got.find(k); !ok || slot != want {
			t.Fatalf("key %d: loaded slot %d (found %t), built %d", i, slot, ok, want)
		}
	}
	if len(got.fallback) != 0 || got.count != uint64(len(keys)) {
		t.Fatalf("loaded %d fallback entries, count %d; want 0, %d", len(got.fallback), got.count, len(keys))
	}
}

// roundTripBBHash writes h as mph.bin and reads it back.
func roundTripBBHash(t *testing.T, h *bbhash) *bbhash {
	t.Helper()
	path := filepath.Join(t.TempDir(), MPHFile)
	if err := h.writeFile(path); err != nil {
		t.Fatalf("writeFile: %v", err)
	}
	got, err := readBBHash(path)
	if err != nil {
		t.Fatalf("readBBHash: %v", err)
	}

	return got
}

// withFallback builds a table over distinct keys plus the fallback
// entries, as the MPHF builder does for keys that share a hash.
func withFallback(t *testing.T, keys []uint64, entries []bbFallback) *bbhash {
	t.Helper()
	all := slices.Clone(keys)
	for _, e := range entries {
		all = append(all, e.hash)
	}
	h, err := newBBHash(all, func(leftovers []uint64) ([]bbFallback, error) {
		if len(leftovers) != len(entries) {
			t.Fatalf("leftovers = %d, want %d", len(leftovers), len(entries))
		}
		slices.SortFunc(entries, compareFallback)

		return entries, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	return h
}

// The fallback round-trips through mph.bin, and its entries take the
// slots after the levels', each its own.
func TestBBHashFallbackRoundTrip(t *testing.T) {
	keys := randomKeys(5000, 13)
	entries := []bbFallback{{hash: 9, fp: 3}, {hash: 9, fp: 1}, {hash: 9, fp: 2}, {hash: 5, fp: 8}, {hash: 5, fp: 4}}
	h := withFallback(t, keys, slices.Clone(entries))
	got := roundTripBBHash(t, h)
	if got.count != uint64(len(keys)+len(entries)) || got.placed != uint64(len(keys)) {
		t.Fatalf("count = %d, placed = %d; want %d, %d", got.count, got.placed, len(keys)+len(entries), len(keys))
	}
	seen := make(map[uint64]bool)
	for _, e := range entries {
		slot, ok := got.findFallback(e.hash, e.fp)
		if !ok || slot < got.placed || slot >= got.count || seen[slot] {
			t.Fatalf("fallback %+v: slot %d (found %t), want a free slot in [%d, %d)", e, slot, ok, got.placed, got.count)
		}
		seen[slot] = true
	}
	for _, miss := range []bbFallback{{hash: 9, fp: 4}, {hash: 6, fp: 1}, {hash: 0, fp: 0}} {
		if slot, ok := got.findFallback(miss.hash, miss.fp); ok {
			t.Errorf("non-member %+v found slot %d", miss, slot)
		}
	}
}

var errDiskFull = errors.New("disk full")

type fullWriter struct{}

func (fullWriter) Write([]byte) (int, error) { return 0, errDiskFull }

// errDepth is how deeply err nests wrapped and joined errors.
func errDepth(err error) int {
	{
		var e interface{ Unwrap() []error }
		var e1 interface{ Unwrap() error }
		switch {
		case errors.As(err, &e):
			deepest := 0
			for _, inner := range e.Unwrap() {
				deepest = max(deepest, errDepth(inner))
			}
			return 1 + deepest
		case errors.As(err, &e1):
			return 1 + errDepth(e1.Unwrap())
		default:
			return 1
		}
	}
}

// A failed write must surface as one error, not one per remaining
// word: a full disk mid-file must fail the build, not hang it.
func TestBBHashWriteFailureReturnsOneError(t *testing.T) {
	h := &bbhash{bits: make([]uint64, bbhashIOBuffer/8+1000), levels: []bbLevel{{words: 1}}}
	err := h.writeTo(fullWriter{})
	if !errors.Is(err, errDiskFull) {
		t.Fatalf("writeTo err = %v, want %v", err, errDiskFull)
	}
	if d := errDepth(err); d > 3 {
		t.Errorf("writeTo error nests %d deep, want at most 3", d)
	}
}

// encodeBBHash returns h's mph.bin bytes.
func encodeBBHash(t *testing.T, h *bbhash) []byte {
	t.Helper()
	path := filepath.Join(t.TempDir(), MPHFile)
	if err := h.writeFile(path); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	return data
}

func TestReadBBHashRejectsCorruptFiles(t *testing.T) {
	data := encodeBBHash(t, mustBBHash(t, randomKeys(1000, 5)))
	zeroLevels := slices.Clone(data)
	clear(zeroLevels[:8])
	hugeLevel := slices.Clone(data)
	hugeLevel[8+7] = 0xff // first level's word count

	fb := encodeBBHash(t, withFallback(t, randomKeys(1000, 6), []bbFallback{{hash: 3, fp: 1}, {hash: 3, fp: 2}}))
	unsorted := slices.Clone(fb)
	// Swap the low bytes of the two entries' fingerprints.
	unsorted[len(fb)-24], unsorted[len(fb)-8] = unsorted[len(fb)-8], unsorted[len(fb)-24]
	hugeFallback := slices.Clone(fb)
	hugeFallback[len(fb)-32-1] = 0xff // the fallback entry count's top byte
	noKeys := make([]byte, 16)        // zero levels, zero fallback entries

	cases := map[string][]byte{
		"empty_header":       data[:4],
		"truncated_bits":     data[:len(data)-16],
		"no_fallback_count":  data[:len(data)-8],
		"trailing_bytes":     append(slices.Clone(data), 0, 0, 0, 0, 0, 0, 0, 0),
		"zero_levels":        zeroLevels,
		"huge_level":         hugeLevel,
		"truncated_fallback": fb[:len(fb)-8],
		"half_entry":         fb[:len(fb)-16],
		"unsorted_fallback":  unsorted,
		"huge_fallback":      hugeFallback,
		"no_keys":            noKeys,
	}
	dir := t.TempDir()
	for name, content := range cases {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(dir, name)
			if err := os.WriteFile(path, content, 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := readBBHash(path); !errors.Is(err, errBBHashCorrupt) {
				t.Fatalf("readBBHash = %v, want errBBHashCorrupt", err)
			}
		})
	}
}

// rank counts the set bits before every bit, over built and loaded
// tables whose last rank block is partial.
func TestBBHashRankCountsEveryBit(t *testing.T) {
	for _, n := range []int{1, 100, 1000, 30_011} {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			built := mustBBHash(t, randomKeys(n, uint64(n)))
			for name, h := range map[string]*bbhash{"built": built, "loaded": roundTripBBHash(t, built)} {
				var want uint64
				for i := range uint64(len(h.bits)) * 64 {
					if got := h.rank(i); got != want {
						t.Fatalf("%s: rank(%d) = %d, want %d", name, i, got, want)
					}
					want += h.bits[i/64] >> (i % 64) & 1
				}
				if want != h.placed {
					t.Fatalf("%s: %d set bits, placed %d", name, want, h.placed)
				}
			}
		})
	}
}

// Keys outside the build set must miss or land in range, never panic.
func TestBBHashForeignKeysStayInRange(t *testing.T) {
	keys := randomKeys(5000, 9)
	h := mustBBHash(t, keys)
	for _, k := range randomKeys(50_000, 10) {
		if slot, ok := h.find(k); ok && slot >= uint64(len(keys)) {
			t.Fatalf("foreign key %#x: slot %d out of range", k, slot)
		}
	}
}

// BenchmarkBBHash times the table's build and lookup. A lookup is a
// few hashes and popcounts, so find's ns/op should stay flat as n grows.
func BenchmarkBBHash(b *testing.B) {
	sizes := []int{1_000_000, 10_000_000}
	if benchutil.LongBenchEnabled() {
		sizes = append(sizes, 50_000_000)
	}
	for _, n := range sizes {
		keys := randomKeys(n, uint64(n))
		b.Run(fmt.Sprintf("op=build/n=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			var h *bbhash
			for range b.N {
				h = mustBBHash(b, keys)
			}
			// Table size including the rank directory.
			b.ReportMetric(float64(len(h.bits)+len(h.ranks))*64/float64(n), "bits/key")
		})
		b.Run(fmt.Sprintf("op=find/n=%d", n), func(b *testing.B) {
			h := mustBBHash(b, keys)
			b.ResetTimer()
			var sink uint64
			for i := range b.N {
				slot, _ := h.find(keys[(i*7919)%n])
				sink += slot
			}
			_ = sink
		})
	}
}
