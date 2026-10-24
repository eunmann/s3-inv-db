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
			h, err := newBBHash(keys)
			if err != nil {
				t.Fatalf("newBBHash: %v", err)
			}
			assertMinimalPerfect(t, h, keys)
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
	h, err := newBBHash(keys)
	if err != nil {
		t.Fatalf("newBBHash: %v", err)
	}
	assertMinimalPerfect(t, h, keys)
}

func TestBBHashRejectsDuplicateKeys(t *testing.T) {
	keys := append(randomKeys(10_000, 7), 42, 42)
	if _, err := newBBHash(keys); !errors.Is(err, errBBHashUnplaced) {
		t.Fatalf("err = %v, want errBBHashUnplaced", err)
	}
}

// The parallel build must not depend on worker scheduling.
func TestBBHashBuildIsDeterministic(t *testing.T) {
	keys := randomKeys(200_000, 3)
	a, err := newBBHash(keys)
	if err != nil {
		t.Fatal(err)
	}
	b, err := newBBHash(keys)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(a.bits, b.bits) || !slices.Equal(a.levels, b.levels) {
		t.Fatal("two builds over the same keys differ")
	}
}

func TestBBHashFileRoundTrip(t *testing.T) {
	keys := randomKeys(30_000, 11)
	h, err := newBBHash(keys)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), MPHFile)
	if err := h.writeFile(path); err != nil {
		t.Fatalf("writeFile: %v", err)
	}
	got, err := readBBHash(path)
	if err != nil {
		t.Fatalf("readBBHash: %v", err)
	}
	for i, k := range keys {
		want, _ := h.find(k)
		if slot, ok := got.find(k); !ok || slot != want {
			t.Fatalf("key %d: loaded slot %d (found %t), built %d", i, slot, ok, want)
		}
	}
}

func TestReadBBHashRejectsCorruptFiles(t *testing.T) {
	h, err := newBBHash(randomKeys(1000, 5))
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	good := filepath.Join(dir, "good")
	if err := h.writeFile(good); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(good)
	if err != nil {
		t.Fatal(err)
	}
	zeroLevels := slices.Clone(data)
	clear(zeroLevels[:8])
	hugeLevel := slices.Clone(data)
	hugeLevel[8+7] = 0xff // first level's word count

	cases := map[string][]byte{
		"empty_header":   data[:4],
		"truncated_bits": data[:len(data)-8],
		"trailing_bytes": append(slices.Clone(data), 0, 0, 0, 0, 0, 0, 0, 0),
		"zero_levels":    zeroLevels,
		"huge_level":     hugeLevel,
	}
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

// Keys outside the build set must miss or land in range, never panic.
func TestBBHashForeignKeysStayInRange(t *testing.T) {
	keys := randomKeys(5000, 9)
	h, err := newBBHash(keys)
	if err != nil {
		t.Fatal(err)
	}
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
				var err error
				if h, err = newBBHash(keys); err != nil {
					b.Fatal(err)
				}
			}
			// Table size including the rank directory.
			b.ReportMetric(float64(len(h.bits)+len(h.ranks))*64/float64(n), "bits/key")
		})
		b.Run(fmt.Sprintf("op=find/n=%d", n), func(b *testing.B) {
			h, err := newBBHash(keys)
			if err != nil {
				b.Fatal(err)
			}
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
