package format

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math/bits"
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

// testFingerprints gives key i a fingerprint of its own.
func testFingerprints(keys []uint64) []uint64 {
	fps := make([]uint64, len(keys))
	for i, k := range keys {
		fps[i] = k ^ uint64(i)*0x9e3779b97f4a7c15
	}

	return fps
}

// testPartitions builds the 2^partBits partitions of keys, whose
// fingerprints are fps, as the build would.
func testPartitions(tb testing.TB, keys, fps []uint64, partBits uint) ([]bbPartition, error) {
	tb.Helper()
	hashes := make([][]uint64, 1<<partBits)
	pfps := make([][]uint64, 1<<partBits)
	for i, k := range keys {
		p := k >> (64 - partBits)
		hashes[p] = append(hashes[p], k)
		pfps[p] = append(pfps[p], fps[i])
	}
	parts := make([]bbPartition, 1<<partBits)
	var s partScratch
	for p := range parts {
		if err := buildPartition(hashes[p], pfps[p], &s, &parts[p]); err != nil {
			return nil, err
		}
	}

	return parts, nil
}

// assembleTable loads partitions into a table as readBBHash does.
func assembleTable(tb testing.TB, partBits uint, parts []bbPartition) *bbhash {
	tb.Helper()
	h := newBBHashTable(partBits, 0)
	for i := range parts {
		p := &parts[i]
		levelsFrom, bitsFrom, fbFrom := len(h.levels), len(h.bits), len(h.fallback)
		for _, lv := range p.levels {
			h.levels = append(h.levels, bbLevel{offset: uint64(bitsFrom) + lv.offset, words: lv.words})
		}
		h.bits = append(h.bits, p.bits...)
		h.fallback = append(h.fallback, p.fallback...)
		if err := h.addPart(p.n, levelsFrom, bitsFrom, fbFrom); err != nil {
			tb.Fatalf("addPart %d: %v", i, err)
		}
	}
	h.finish()

	return h
}

// mustTable builds the table over distinct keys at partBits.
func mustTable(tb testing.TB, keys []uint64, partBits uint) *bbhash {
	tb.Helper()
	parts, err := testPartitions(tb, keys, testFingerprints(keys), partBits)
	if err != nil {
		tb.Fatalf("build partitions: %v", err)
	}

	return assembleTable(tb, partBits, parts)
}

// encodeTable returns the mph.bin bytes of partitions.
func encodeTable(tb testing.TB, partBits uint, parts []bbPartition) []byte {
	tb.Helper()
	var buf bytes.Buffer
	w := bufio.NewWriter(&buf)
	if err := putU64(w, uint64(partBits)); err != nil {
		tb.Fatal(err)
	}
	for i := range parts {
		if err := writePartition(w, &parts[i]); err != nil {
			tb.Fatal(err)
		}
	}
	if err := w.Flush(); err != nil {
		tb.Fatal(err)
	}

	return buf.Bytes()
}

// readTable writes data as mph.bin and reads it back.
func readTable(tb testing.TB, data []byte) (*bbhash, error) {
	tb.Helper()
	path := filepath.Join(tb.TempDir(), MPHFile)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		tb.Fatal(err)
	}

	return readBBHash(path)
}

// assertMinimalPerfect checks that the keys map one-to-one onto [0, n),
// through the levels or the fallback.
func assertMinimalPerfect(t *testing.T, h *bbhash, keys, fps []uint64) {
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
		if !ok {
			slot, ok = h.findFallback(k, fps[i])
		}
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
	for _, partBits := range []uint{0, 3, 8} {
		for _, n := range []int{1, 2, 63, 64, 65, 1000, 100_000} {
			t.Run(fmt.Sprintf("partBits=%d/n=%d", partBits, n), func(t *testing.T) {
				keys := randomKeys(n, uint64(n))
				assertMinimalPerfect(t, mustTable(t, keys, partBits), keys, testFingerprints(keys))
			})
		}
	}
}

// Low-entropy keys (sequential integers, like a weak key hash) must
// still spread: the level hash mixes before reducing.
func TestBBHashSequentialKeys(t *testing.T) {
	keys := make([]uint64, 50_000)
	for i := range keys {
		keys[i] = uint64(i) << 20
	}
	assertMinimalPerfect(t, mustTable(t, keys, 0), keys, testFingerprints(keys))
}

// Equal keys collide on every level: the build stops at the first level
// that places no key, without appending it, and they become fallback
// entries; every other key is placed on its own slot.
func TestBuildPartitionLeavesEqualKeysOver(t *testing.T) {
	keys := append(randomKeys(10_000, 7), 42, 42, 42)
	fps := testFingerprints(keys)
	var p bbPartition
	if err := buildPartition(keys, fps, &partScratch{}, &p); err != nil {
		t.Fatal(err)
	}
	want := []bbFallback{{42, fps[10_000]}, {42, fps[10_001]}, {42, fps[10_002]}}
	slices.SortFunc(want, compareFallback)
	if !slices.Equal(p.fallback, want) {
		t.Fatalf("fallback = %v, want %v", p.fallback, want)
	}
	if len(p.levels) >= maxBBHashLevels {
		t.Errorf("built %d levels; the build must stop once a level places nothing", len(p.levels))
	}
	last := p.levels[len(p.levels)-1]
	if !slices.ContainsFunc(p.bits[last.offset:last.offset+last.words], func(w uint64) bool { return w != 0 }) {
		t.Error("the last level places no key")
	}
	h := assembleTable(t, 0, []bbPartition{p})
	if h.placed != 10_000 || h.count != uint64(len(keys)) {
		t.Fatalf("placed = %d, count = %d; want 10000, %d", h.placed, h.count, len(keys))
	}
	if _, ok := h.find(42); ok {
		t.Error("an unplaced key hits a level")
	}
	assertMinimalPerfect(t, h, keys, fps)
}

// Keys that all share one hash leave no level at all: the whole table is
// the fallback.
func TestBuildPartitionAllKeysEqual(t *testing.T) {
	var p bbPartition
	if err := buildPartition([]uint64{7, 7}, []uint64{2, 1}, &partScratch{}, &p); err != nil {
		t.Fatal(err)
	}
	if len(p.levels) != 0 || len(p.bits) != 0 || !slices.Equal(p.fallback, []bbFallback{{7, 1}, {7, 2}}) {
		t.Fatalf("levels = %d, bits = %d, fallback = %v; want none, both keys", len(p.levels), len(p.bits), p.fallback)
	}
}

// Two keys equal in both hash and fingerprint cannot be told apart.
func TestBuildPartitionFullCollisionFails(t *testing.T) {
	keys := append(randomKeys(100, 3), 9, 9)
	fps := testFingerprints(keys)
	fps[len(fps)-1] = fps[len(fps)-2]
	if err := buildPartition(keys, fps, &partScratch{}, &bbPartition{}); !errors.Is(err, ErrMPHFKeyCollision) {
		t.Fatalf("buildPartition = %v, want ErrMPHFKeyCollision", err)
	}
}

// A worker builds partition after partition in the same buffers: once
// they have grown to a partition's size, building another costs no
// allocation.
func TestBuildPartitionReusesScratch(t *testing.T) {
	keys := randomKeys(50_000, 4)
	fps := testFingerprints(keys)
	var s partScratch
	var p bbPartition
	if err := buildPartition(keys, fps, &s, &p); err != nil {
		t.Fatal(err)
	}
	allocs := testing.AllocsPerRun(5, func() {
		if err := buildPartition(keys, fps, &s, &p); err != nil {
			t.Fatal(err)
		}
	})
	if allocs != 0 {
		t.Errorf("buildPartition allocates %v times on reused buffers", allocs)
	}
}

// The build must not depend on the order of a partition's keys.
func TestBuildPartitionIgnoresKeyOrder(t *testing.T) {
	keys := randomKeys(200_000, 3)
	fps := testFingerprints(keys)
	var a, b bbPartition
	if err := buildPartition(keys, fps, &partScratch{}, &a); err != nil {
		t.Fatal(err)
	}
	perm := rand.New(rand.NewPCG(1, 1)).Perm(len(keys))
	shuffled, shuffledFps := make([]uint64, len(keys)), make([]uint64, len(keys))
	for i, j := range perm {
		shuffled[i], shuffledFps[i] = keys[j], fps[j]
	}
	if err := buildPartition(shuffled, shuffledFps, &partScratch{}, &b); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(a.bits, b.bits) || !slices.Equal(a.levels, b.levels) {
		t.Fatal("two builds over the same keys in different orders differ")
	}
}

// withFallback builds partitions over distinct keys plus keys that
// share a hash, so the table has fallback entries, and returns them
// with the keys and their fingerprints.
//
//nolint:gocritic // gocritic wants named returns, nonamedreturns forbids them
func withFallback(t *testing.T, partBits uint) ([]bbPartition, []uint64, []uint64) {
	t.Helper()
	keys := randomKeys(5000, 13)
	shared := []uint64{keys[1], keys[2], keys[3]}
	keys = append(keys, shared...)
	fps := testFingerprints(keys)
	parts, err := testPartitions(t, keys, fps, partBits)
	if err != nil {
		t.Fatal(err)
	}

	return parts, keys, fps
}

// The table round-trips through mph.bin, fallback entries included,
// each partition's taking the slots after its levels'.
func TestBBHashFileRoundTrip(t *testing.T) {
	for _, partBits := range []uint{0, 2, 5} {
		t.Run(fmt.Sprintf("partBits=%d", partBits), func(t *testing.T) {
			parts, keys, fps := withFallback(t, partBits)
			built := assembleTable(t, partBits, parts)
			got, err := readTable(t, encodeTable(t, partBits, parts))
			if err != nil {
				t.Fatal(err)
			}
			if len(got.fallback) != 6 || got.count != uint64(len(keys)) || got.placed != built.placed {
				t.Fatalf("loaded %d fallback entries, count %d, placed %d; want 6, %d, %d",
					len(got.fallback), got.count, got.placed, len(keys), built.placed)
			}
			for i, k := range keys {
				want, wantOK := built.find(k)
				if slot, ok := got.find(k); ok != wantOK || slot != want {
					t.Fatalf("key %d: loaded slot %d (found %t), built %d (%t)", i, slot, ok, want, wantOK)
				}
			}
			assertMinimalPerfect(t, got, keys, fps)
			for _, miss := range []bbFallback{{hash: keys[1], fp: 4}, {hash: 6, fp: 1}} {
				if slot, ok := got.findFallback(miss.hash, miss.fp); ok {
					t.Errorf("non-member %+v found slot %d", miss, slot)
				}
			}
		})
	}
}

var errDiskFull = errors.New("disk full")

type fullWriter struct{}

func (fullWriter) Write([]byte) (int, error) { return 0, errDiskFull }

// errDepth is how deeply err nests wrapped and joined errors.
func errDepth(err error) int {
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

// A failed write must surface as one error, not one per remaining
// word: a full disk mid-file must fail the build, not hang it.
func TestWritePartitionFailureReturnsOneError(t *testing.T) {
	p := &bbPartition{n: 1, bits: make([]uint64, bbhashIOBuffer/8+1000), levels: []bbLevel{{words: 1}}}
	w := bufio.NewWriterSize(fullWriter{}, bbhashIOBuffer)
	err := writePartition(w, p)
	if err == nil {
		err = w.Flush()
	}
	if !errors.Is(err, errDiskFull) {
		t.Fatalf("writePartition err = %v, want %v", err, errDiskFull)
	}
	if d := errDepth(err); d > 3 {
		t.Errorf("writePartition error nests %d deep, want at most 3", d)
	}
}

// corrupt returns a copy of data with the uint64 at word index i set
// to v.
func corrupt(data []byte, i int, v uint64) []byte {
	out := slices.Clone(data)
	binary.LittleEndian.PutUint64(out[8*i:], v)

	return out
}

func TestReadBBHashRejectsCorruptFiles(t *testing.T) {
	parts, _, _ := withFallback(t, 1)
	data := encodeTable(t, 1, parts)
	// Words: [0] partition bits, [1] partition 0's key count, [2] its
	// level count, [3] its first level's words.
	first := &parts[0]
	firstEnd := 1 + 3 + len(first.levels) + len(first.bits) + 2*len(first.fallback)
	fbCountAt := 1 + 2 + len(first.levels) + len(first.bits)

	// A partition whose fallback entry belongs to the other partition.
	misplaced := slices.Clone(parts)
	misplaced[0].fallback = []bbFallback{{hash: 1 << 63, fp: 1}}
	misplaced[0].n = 1
	for _, w := range first.bits {
		misplaced[0].n += uint64(bits.OnesCount64(w))
	}

	// The last two fallback entries swapped.
	unsortedParts, _, _ := withFallback(t, 0)
	fb := unsortedParts[0].fallback
	end := len(encodeTable(t, 0, unsortedParts)) / 8
	unsorted := encodeTable(t, 0, unsortedParts)
	for i, v := range []uint64{fb[len(fb)-1].hash, fb[len(fb)-1].fp, fb[len(fb)-2].hash, fb[len(fb)-2].fp} {
		unsorted = corrupt(unsorted, end-4+i, v)
	}

	manyLevels := encodeTable(t, 0, []bbPartition{{n: 0, levels: make([]bbLevel, maxBBHashLevels+1)}})

	cases := map[string][]byte{
		"empty":               nil,
		"short_header":        data[:4],
		"huge_partition_bits": corrupt(data, 0, maxPartBits+1),
		"more_partitions":     corrupt(data, 0, 16),
		"truncated_bits":      data[:len(data)-16],
		"truncated_partition": data[:8*firstEnd],
		"trailing_bytes":      append(slices.Clone(data), 0, 0, 0, 0, 0, 0, 0, 0),
		"key_count":           corrupt(data, 1, first.n+1),
		"zero_word_level":     corrupt(data, 3, 0),
		"huge_level":          corrupt(data, 3, 1<<60),
		"too_many_levels":     manyLevels,
		"huge_fallback":       corrupt(data, fbCountAt, 1<<60),
		"misplaced_fallback":  encodeTable(t, 1, misplaced),
		"unsorted_fallback":   unsorted,
		"no_keys":             encodeTable(t, 0, []bbPartition{{}}),
	}
	for name, content := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := readTable(t, content); !errors.Is(err, errBBHashCorrupt) {
				t.Fatalf("readBBHash = %v, want errBBHashCorrupt", err)
			}
		})
	}
}

// rankOf counts the set bits before every bit, over built and loaded
// tables whose last rank block is partial.
func TestBBHashRankCountsEveryBit(t *testing.T) {
	for _, n := range []int{1, 100, 1000, 30_011} {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			keys := randomKeys(n, uint64(n))
			parts, err := testPartitions(t, keys, testFingerprints(keys), 2)
			if err != nil {
				t.Fatal(err)
			}
			loaded, err := readTable(t, encodeTable(t, 2, parts))
			if err != nil {
				t.Fatal(err)
			}
			for name, h := range map[string]*bbhash{"built": assembleTable(t, 2, parts), "loaded": loaded} {
				var want uint64
				for i := range uint64(len(h.bits)) * 64 {
					if got := rankOf(h.bits, h.ranks, i); got != want {
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
	h := mustTable(t, keys, 3)
	for _, k := range randomKeys(50_000, 10) {
		if slot, ok := h.find(k); ok && slot >= uint64(len(keys)) {
			t.Fatalf("foreign key %#x: slot %d out of range", k, slot)
		}
		if slot, ok := h.findFallback(k, 0); ok {
			t.Fatalf("foreign key %#x: fallback slot %d", k, slot)
		}
	}
}

// BenchmarkBBHash times one partition's serial build and the table's
// lookup. A lookup is a few hashes and popcounts, so find's ns/op
// should stay flat as n grows.
func BenchmarkBBHash(b *testing.B) {
	sizes := []int{1 << 16, 1_000_000}
	if benchutil.LongBenchEnabled() {
		sizes = append(sizes, 10_000_000)
	}
	for _, n := range sizes {
		keys := randomKeys(n, uint64(n))
		fps := testFingerprints(keys)
		b.Run(fmt.Sprintf("op=build/n=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			var s partScratch
			var p bbPartition
			for range b.N {
				if err := buildPartition(keys, fps, &s, &p); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(len(p.bits))*64/float64(n), "bits/key")
		})
		b.Run(fmt.Sprintf("op=find/n=%d", n), func(b *testing.B) {
			h := mustTable(b, keys, 8)
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
