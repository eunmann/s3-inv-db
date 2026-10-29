package format

import (
	"fmt"
	"runtime"
	"strings"
	"testing"
)

// interned is the IDs a sequence of segments got and the dictionary
// they were written to.
type interned struct {
	ids  []uint32
	segs []string
}

// internSequence interns names in order with an interner of budget
// bytes in a fresh directory, checking after each that its tables hold
// at most budget bytes.
func internSequence(t *testing.T, budget int64, names []string) interned {
	t.Helper()
	dir := t.TempDir()
	si, err := NewPrefixSegmentInterner(dir, budget, nil)
	if err != nil {
		t.Fatal(err)
	}
	ids := make([]uint32, len(names))
	for i, name := range names {
		if ids[i], err = si.Intern(name); err != nil {
			t.Fatal(err)
		}
		if got := si.heapBytes(); int64(got) > budget {
			t.Fatalf("after %d segments the tables hold %d bytes, over the %d budget", i+1, got, budget)
		}
	}
	if err := si.Close(); err != nil {
		t.Fatal(err)
	}
	dict, err := OpenPrefixDictionary(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer dict.Close()
	cache, err := dict.PreloadSegments()
	if err != nil {
		t.Fatal(err)
	}
	if uint32(cache.Count()) != si.Count() {
		t.Fatalf("dictionary has %d segments, interner wrote %d", cache.Count(), si.Count())
	}
	segs := make([]string, cache.Count())
	for i := range segs {
		segs[i] = cache.Get(uint32(i))
	}
	for i, name := range names {
		if segs[ids[i]] != name {
			t.Fatalf("segment %d (%q) got ID %d, which names %q", i, name, ids[i], segs[ids[i]])
		}
	}

	return interned{ids: ids, segs: segs}
}

// adversarialSegments are names that differ only in their last byte,
// only in length, are empty, contain bytes a hash or compare could trip
// on, or are long.
func adversarialSegments() []string {
	return []string{
		"", "a", "a ", "aa", "a\x00", "\x00", "\x00\x00", "\xff\xfe",
		"日本語", "日本", strings.Repeat("x", 1023), strings.Repeat("x", 1024),
		strings.Repeat("ab", 600), "2024", "2024-", "data", "Data", "data.",
	}
}

// TestInternerUnderBudgetIsDistinctInFirstSeenOrder: while every
// distinct segment fits the budget, the dictionary is each of them
// once, numbered in the order first seen.
func TestInternerUnderBudgetIsDistinctInFirstSeenOrder(t *testing.T) {
	names := make([]string, 0, 3*(len(adversarialSegments())+50))
	for round := range 3 {
		names = append(names, adversarialSegments()...)
		for i := range 50 {
			names = append(names, fmt.Sprintf("seg-%d-%d", i, round%2))
		}
	}
	segs := internSequence(t, 1<<20, names).segs
	var want []string
	seen := map[string]bool{}
	for _, n := range names {
		if !seen[n] {
			seen[n] = true
			want = append(want, n)
		}
	}
	if len(segs) != len(want) {
		t.Fatalf("%d segments, want %d", len(segs), len(want))
	}
	for i := range want {
		if segs[i] != want[i] {
			t.Fatalf("segment %d = %q, want %q", i, segs[i], want[i])
		}
	}
}

// TestInternerSpillsWithinBudget: with many more distinct segments than
// the budget holds, the tables stay within it, every ID still names its
// segment, a segment named throughout keeps one ID, and one that comes
// back only after the generations turned over gets a new entry.
func TestInternerSpillsWithinBudget(t *testing.T) {
	const budget = 64 << 10
	names := []string{"cold", "hot"}
	for i := range 40_000 {
		names = append(names, fmt.Sprintf("%08x-%04x-unique-dir", i*2654435761, i))
		if i%100 == 0 {
			names = append(names, "hot")
		}
	}
	names = append(names, "cold", "hot")
	names = append(names, adversarialSegments()...)
	got := internSequence(t, budget, names)
	ids, segs := got.ids, got.segs

	var hot, cold []uint32
	for i, n := range names {
		switch n {
		case "hot":
			hot = append(hot, ids[i])
		case "cold":
			cold = append(cold, ids[i])
		}
	}
	for _, id := range hot {
		if id != hot[0] {
			t.Fatalf("hot segment got IDs %d and %d", hot[0], id)
		}
	}
	if cold[0] == cold[1] {
		t.Errorf("cold segment kept ID %d across %d new segments in a %d-byte budget", cold[0], len(names), budget)
	}
	if want := 40_000 + 2 + 1 + len(adversarialSegments()); len(segs) != want {
		t.Errorf("%d dictionary entries, want %d: each unique segment and the cold one twice", len(segs), want)
	}
}

// TestInternerHeapStaysWithinBudget: the heap an interner holds after a
// GC is its budget at most, however many distinct segments it has seen.
func TestInternerHeapStaysWithinBudget(t *testing.T) {
	const budget = 1 << 20
	si, err := NewPrefixSegmentInterner(t.TempDir(), budget, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = si.Abort() })
	before := liveHeap()
	name := make([]byte, 0, 64)
	for i := range 200_000 {
		name = fmt.Appendf(name[:0], "uploads-%d-%x", i, uint64(i)*0x9e3779b97f4a7c15)
		if _, err := si.Intern(string(name)); err != nil {
			t.Fatal(err)
		}
	}
	held := int64(liveHeap()) - int64(before)
	runtime.KeepAlive(si)
	if held > budget {
		t.Errorf("interner holds %d heap bytes after 200K distinct segments, budget %d", held, budget)
	}
	if si.Count() != 200_000 {
		t.Errorf("Count = %d, want 200000", si.Count())
	}
}

func liveHeap() uint64 {
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	return m.HeapAlloc
}

// TestInternerIDsDoNotDependOnTheHash: the generations turn over by
// size alone, so two interners (each with its own hash seed) give the
// same IDs.
func TestInternerIDsDoNotDependOnTheHash(t *testing.T) {
	names := make([]string, 0, 20_000)
	for i := range 20_000 {
		names = append(names, fmt.Sprintf("n%d", i%7919*(i%3+1)))
	}
	a := internSequence(t, 32<<10, names).ids
	b := internSequence(t, 32<<10, names).ids
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("segment %d (%q): IDs %d and %d", i, names[i], a[i], b[i])
		}
	}
}

// TestInternerSegmentLongerThanABudget: a segment too long to remember
// is written each time it comes, each ID naming it.
func TestInternerSegmentLongerThanABudget(t *testing.T) {
	long := strings.Repeat("L", 40<<10)
	got := internSequence(t, 32<<10, []string{"a", long, "a", long})
	ids, segs := got.ids, got.segs
	if ids[0] != ids[2] || ids[1] == ids[3] || len(segs) != 3 {
		t.Errorf("IDs %v over %d entries, want a's repeated and the long one's two", ids, len(segs))
	}
}

// TestInternerZeroBudget: an interner that may remember nothing writes
// every segment it is given, still exactly.
func TestInternerZeroBudget(t *testing.T) {
	names := append(adversarialSegments(), adversarialSegments()...)
	segs := internSequence(t, 0, names).segs
	if len(segs) != len(names) {
		t.Errorf("%d entries, want %d", len(segs), len(names))
	}
}
