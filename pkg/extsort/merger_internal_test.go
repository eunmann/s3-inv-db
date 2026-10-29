package extsort

import (
	"errors"
	"fmt"
	"io"
	"maps"
	"math/rand/v2"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// mergeTestTokens are the bytes keys are built from: both ends of the
// byte range, the bytes either side of ASCII's top, '/', and multi-byte
// UTF-8, so keys are often prefixes of each other and differ at a
// byte's extremes.
var mergeTestTokens = []string{"\x00", "\x01", "a", "b", "/", "\x7f", "\x80", "\xfe", "\xff", "é", "€", "😀"}

// mergeTestStems give keys shared starts longer than a word, so
// comparisons run past the first eight bytes.
var mergeTestStems = []string{"", "", "bucket/", "bucket/logs/2024/", "xxxxxxxxxxxxxxxxxxxx"}

// mergeTestRow returns a row for prefix: its depth depends on the prefix
// alone, as in a real run, and its stats on seed.
func mergeTestRow(prefix string, seed uint64) PrefixRow {
	row := PrefixRow{Prefix: prefix, Depth: uint16(len(prefix) % 9)}
	t := int(seed % uint64(MaxTiers))
	row.TierCounts[t], row.TierBytes[t] = seed%1000+1, seed*4099
	row.Count, row.TotalBytes = row.TierCounts[t], row.TierBytes[t]

	return row
}

// sortedTestRun returns rows for the distinct keys, sorted.
func sortedTestRun(keys []string, seed uint64) []PrefixRow {
	keys = slices.Clone(keys)
	slices.Sort(keys)
	keys = slices.Compact(keys)
	rows := make([]PrefixRow, len(keys))
	for i, k := range keys {
		rows[i] = mergeTestRow(k, seed+uint64(i))
	}

	return rows
}

// randomTestKey returns a stem followed by up to five tokens.
func randomTestKey(rng *rand.Rand) string {
	var sb strings.Builder
	sb.WriteString(mergeTestStems[rng.IntN(len(mergeTestStems))])
	for range rng.IntN(6) {
		sb.WriteString(mergeTestTokens[rng.IntN(len(mergeTestTokens))])
	}

	return sb.String()
}

// scanMerge is what a merge of runs must give: every prefix once, its
// rows summed, in byte order.
func scanMerge(runs [][]PrefixRow) []PrefixRow {
	sums := map[string]PrefixRow{}
	for _, run := range runs {
		for i := range run {
			if sum, ok := sums[run[i].Prefix]; ok {
				sum.Merge(&run[i])
				sums[run[i].Prefix] = sum
			} else {
				sums[run[i].Prefix] = run[i]
			}
		}
	}
	out := make([]PrefixRow, 0, len(sums))
	for _, k := range slices.Sorted(maps.Keys(sums)) {
		out = append(out, sums[k])
	}

	return out
}

// heapMerge merges runs through the binary heap MergeIterator used
// before its tree of losers, ordered by prefix, each row merged into the
// last one out when their prefixes are equal.
func heapMerge(runs [][]PrefixRow) []PrefixRow {
	var h typedMergeHeap
	next := make([]int, len(runs))
	for i, run := range runs {
		if len(run) > 0 {
			h.push(mergeItem{row: &run[0], readerIdx: i})
			next[i] = 1
		}
	}
	var out []PrefixRow
	for h.Len() > 0 {
		top := h.pop()
		if run := runs[top.readerIdx]; next[top.readerIdx] < len(run) {
			h.push(mergeItem{row: &run[next[top.readerIdx]], readerIdx: top.readerIdx})
			next[top.readerIdx]++
		}
		if n := len(out); n > 0 && out[n-1].Prefix == top.row.Prefix {
			out[n-1].Merge(top.row)
		} else {
			out = append(out, *top.row)
		}
	}

	return out
}

// memSources returns runs as in-memory merge sources, over copies.
func memSources(runs [][]PrefixRow) []runSource {
	out := make([]runSource, len(runs))
	for i, run := range runs {
		rows := make([]*PrefixRow, len(run))
		for j := range run {
			row := run[j]
			rows[j] = &row
		}
		out[i] = newMemRun(rows)
	}

	return out
}

// writeTestRunFile writes rows to a run file, each record front-coded
// with a shared length drawn from 0 up to the most it could be: a
// writer may share less than all it could, and the merge must not
// depend on it sharing all.
func writeTestRunFile(t *testing.T, path string, rows []PrefixRow, rng *rand.Rand) {
	t.Helper()
	w, err := NewCompressedRunWriter(path, CompressedRunWriterOptions{BufferSize: 4096, CompressionLevel: CompressionFastest})
	if err != nil {
		t.Fatal(err)
	}
	prev := ""
	for i := range rows {
		p := rows[i].Prefix
		shared := rng.IntN(sharedPrefixLen(prev, p) + 1)
		if err := w.WriteEncoded(shared, p[shared:], &rows[i]); err != nil {
			t.Fatal(err)
		}
		prev = p
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

// mixedSources returns runs as merge sources: every other one, starting
// with the first when filesFirst, from a run file in dir, the rest in
// memory.
func mixedSources(t *testing.T, dir string, runs [][]PrefixRow, filesFirst bool, rng *rand.Rand) []runSource {
	t.Helper()
	out := memSources(runs)
	for i, run := range runs {
		if (i%2 == 0) != filesFirst {
			continue
		}
		path := filepath.Join(dir, fmt.Sprintf("run_%04d.crun", i))
		writeTestRunFile(t, path, run, rng)
		r, err := OpenCompressedRunFile(path, 4096)
		if err != nil {
			t.Fatal(err)
		}
		out[i] = r
	}

	return out
}

// drainMerge merges sources and returns every row, or the first error.
func drainMerge(sources []runSource) ([]PrefixRow, error) {
	it, err := newMergeIterator(sources)
	if err != nil {
		return nil, err
	}
	defer it.Close()
	var out []PrefixRow
	for {
		row, err := it.Next()
		if errors.Is(err, io.EOF) {
			return out, nil
		}
		if err != nil {
			return out, err
		}
		out = append(out, *row)
	}
}

// checkMerge merges runs from memory, and with every other run (odd or
// even) read from a file, and checks each against a scan and against
// the heap merge.
func checkMerge(t *testing.T, runs [][]PrefixRow, rng *rand.Rand) {
	t.Helper()
	want := scanMerge(runs)
	if got := heapMerge(runs); !slices.Equal(got, want) {
		t.Fatalf("heap merge differs from the scan: %s", firstRowDiff(got, want))
	}
	for _, src := range []struct {
		name    string
		sources []runSource
	}{
		{"memory", memSources(runs)},
		{"files_even", mixedSources(t, t.TempDir(), runs, true, rng)},
		{"files_odd", mixedSources(t, t.TempDir(), runs, false, rng)},
	} {
		got, err := drainMerge(src.sources)
		if err != nil {
			t.Fatalf("%s: merge: %v", src.name, err)
		}
		if !slices.Equal(got, want) {
			t.Fatalf("%s: merge differs from the scan and the heap merge: %s", src.name, firstRowDiff(got, want))
		}
	}
}

// firstRowDiff describes where got and want first differ.
func firstRowDiff(got, want []PrefixRow) string {
	for i := range min(len(got), len(want)) {
		if got[i] != want[i] {
			return fmt.Sprintf("row %d: got %q %+v, want %q %+v", i, got[i].Prefix, got[i], want[i].Prefix, want[i])
		}
	}

	return fmt.Sprintf("%d rows, want %d", len(got), len(want))
}

// TestMergeIteratorExhaustive merges every way of drawing three runs
// from five keys, and four runs from four, and checks each against a
// scan: every overlap, duplicate, empty run and order of exhaustion
// those allow. The keys are prefixes of each other and hold 0x00, 0xff
// and multi-byte UTF-8.
func TestMergeIteratorExhaustive(t *testing.T) {
	for _, c := range []struct {
		keys []string
		runs int
	}{
		{keys: []string{"", "\x00", "a", "a\x00", "\xff"}, runs: 3},
		{keys: []string{"\x00\xff", "é", "é\x00", "€"}, runs: 4},
	} {
		n := len(c.keys)
		for combo := range 1 << (n * c.runs) {
			runs := make([][]PrefixRow, c.runs)
			for r := range runs {
				var keys []string
				for i, k := range c.keys {
					if combo>>(r*n+i)&1 != 0 {
						keys = append(keys, k)
					}
				}
				runs[r] = sortedTestRun(keys, uint64(r*n+1))
			}
			want := scanMerge(runs)
			got, err := drainMerge(memSources(runs))
			if err != nil {
				t.Fatalf("keys %q, combination %#x: %v", c.keys, combo, err)
			}
			if !slices.Equal(got, want) {
				t.Fatalf("keys %q, combination %#x: %s", c.keys, combo, firstRowDiff(got, want))
			}
		}
	}
}

// TestMergeIteratorRandom merges random runs of every size of tree,
// with keys that are prefixes of each other, repeat across runs and
// hold the byte range's edges, from memory and from run files that
// share fewer prefix bytes than they could.
func TestMergeIteratorRandom(t *testing.T) {
	rng := rand.New(rand.NewPCG(201, 1))
	for _, k := range []int{1, 2, 3, 4, 5, 7, 8, 9, 16, 17, 33, 64} {
		for trial := range 6 {
			t.Run(fmt.Sprintf("k=%d/trial=%d", k, trial), func(t *testing.T) {
				runs := make([][]PrefixRow, k)
				for r := range runs {
					keys := make([]string, rng.IntN(60))
					for i := range keys {
						keys[i] = randomTestKey(rng)
					}
					runs[r] = sortedTestRun(keys, rng.Uint64())
				}
				checkMerge(t, runs, rng)
			})
		}
	}
}

// TestMergeIteratorLayouts merges the run layouts a merge meets: runs
// that each hold one range, so one wins row after row; one run winning
// a long streak then handing over; runs interleaved row by row; every
// run holding the same keys; and runs that are empty or end early.
func TestMergeIteratorLayouts(t *testing.T) {
	rng := rand.New(rand.NewPCG(201, 2))
	key := func(i int) string { return fmt.Sprintf("bucket/p%03d/k%06d/", i/100, i) }
	layouts := map[string]func(k int) [][]string{
		"disjoint": func(k int) [][]string {
			runs := make([][]string, k)
			for r := range runs {
				for j := range 50 {
					runs[r] = append(runs[r], key(r*50+j))
				}
			}

			return runs
		},
		"disjoint_reversed": func(k int) [][]string {
			runs := make([][]string, k)
			for r := range runs {
				for j := range 50 {
					runs[r] = append(runs[r], key((k-1-r)*50+j))
				}
			}

			return runs
		},
		"one_run_streaks": func(k int) [][]string {
			runs := make([][]string, k)
			for i := range 40 * k {
				r := 0
				if i%37 == 36 {
					r = i % k
				}
				runs[r] = append(runs[r], key(i))
			}

			return runs
		},
		"interleaved": func(k int) [][]string {
			runs := make([][]string, k)
			for i := range 40 * k {
				runs[i%k] = append(runs[i%k], key(i))
			}

			return runs
		},
		"all_equal": func(k int) [][]string {
			runs := make([][]string, k)
			for r := range runs {
				for j := range 30 {
					runs[r] = append(runs[r], key(j))
				}
			}

			return runs
		},
		"empty_and_short": func(k int) [][]string {
			runs := make([][]string, k)
			for r := range runs {
				for j := range r % 4 {
					runs[r] = append(runs[r], key(r*3+j))
				}
			}

			return runs
		},
		"all_empty": func(k int) [][]string { return make([][]string, k) },
		"nested": func(k int) [][]string {
			runs := make([][]string, k)
			for r := range runs {
				p := ""
				for j := range 20 {
					p += mergeTestTokens[(r+j)%len(mergeTestTokens)]
					runs[r] = append(runs[r], p)
				}
			}

			return runs
		},
	}
	for _, name := range slices.Sorted(maps.Keys(layouts)) {
		for _, k := range []int{1, 2, 3, 6, 8, 13, 32} {
			t.Run(fmt.Sprintf("%s/k=%d", name, k), func(t *testing.T) {
				keys := layouts[name](k)
				runs := make([][]PrefixRow, k)
				for r := range runs {
					runs[r] = sortedTestRun(keys[r], uint64(r)*1000)
				}
				checkMerge(t, runs, rng)
			})
		}
	}
}

// TestMergeIteratorNoSources: a merge of nothing ends at once.
func TestMergeIteratorNoSources(t *testing.T) {
	it, err := newMergeIterator(nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := it.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("Next = %v, want EOF", err)
	}
}

// TestMergeIteratorAllocatesOnlyAtStart: a merge makes the same few
// allocations, all at the start, whatever its fan-in and length.
func TestMergeIteratorAllocatesOnlyAtStart(t *testing.T) {
	const merges = 20
	allocs := map[int]float64{}
	for _, k := range []int{1, 8, 64} {
		runs := mergeBenchRuns(true, k, 50)
		sets := make([][]runSource, merges+1)
		for s := range sets {
			sets[s] = make([]runSource, k)
			for i, run := range runs {
				sets[s][i] = newMemRun(run)
			}
		}
		next := 0
		allocs[k] = testing.AllocsPerRun(merges, func() {
			if _, err := drainMergeDiscard(sets[next]); err != nil {
				t.Fatal(err)
			}
			next++
		})
	}
	if allocs[1] != allocs[64] || allocs[8] != allocs[64] || allocs[64] > 5 {
		t.Fatalf("allocations per merge by fan-in: %v, want the same at every fan-in, at most 5", allocs)
	}
}

// drainMergeDiscard merges sources and returns how many rows came out.
func drainMergeDiscard(sources []runSource) (int, error) {
	it, err := newMergeIterator(sources)
	if err != nil {
		return 0, err
	}
	n := 0
	for {
		if _, err := it.Next(); errors.Is(err, io.EOF) {
			return n, nil
		} else if err != nil {
			return n, err
		}
		n++
	}
}

// TestMergeIteratorRejectsUnsortedRun:a run whose prefix goes backwards,
// at a byte or by ending early, fails the merge rather than corrupting
// its order, from memory or from a file.
func TestMergeIteratorRejectsUnsortedRun(t *testing.T) {
	rng := rand.New(rand.NewPCG(201, 3))
	for name, keys := range map[string][]string{
		"smaller_byte":     {"a/", "b/x/", "b/b/"},
		"prefix_of_prev":   {"a/", "a/b/", "a/"},
		"shorter_and_0xff": {"\xff\xff", "\xff"},
	} {
		t.Run(name, func(t *testing.T) {
			run := make([]PrefixRow, len(keys))
			for i, k := range keys {
				run[i] = mergeTestRow(k, uint64(i))
			}
			other := sortedTestRun([]string{"0/", "c/"}, 9)
			for _, sources := range [][]runSource{
				memSources([][]PrefixRow{run, other}),
				mixedSources(t, t.TempDir(), [][]PrefixRow{run, other}, true, rng),
			} {
				if _, err := drainMerge(sources); !errors.Is(err, ErrUnsortedInput) {
					t.Fatalf("merge = %v, want ErrUnsortedInput", err)
				}
			}
		})
	}
}

// TestOffsetValueCodes: codes against a common base order keys as the
// keys sort, equal codes mean keys that agree one byte past the offset,
// and the key that sorts later keeps its code against the earlier one.
func TestOffsetValueCodes(t *testing.T) {
	rng := rand.New(rand.NewPCG(201, 4))
	for range 20000 {
		keys := []string{randomTestKey(rng), randomTestKey(rng), randomTestKey(rng)}
		slices.Sort(keys)
		base, x, y := keys[0], keys[1], keys[2]
		if rng.IntN(2) == 0 {
			x, y = y, x
		}
		cx, okx := ovcAfter(base, x, 0)
		cy, oky := ovcAfter(base, y, 0)
		if !okx || !oky {
			t.Fatalf("%q or %q sorts before %q", x, y, base)
		}
		switch {
		case (cx < cy && x > y) || (cx > cy && x < y):
			t.Fatalf("codes %#x, %#x of %q, %q against %q disagree with the keys' order", cx, cy, x, y, base)
		case cx == cy:
			if off := ovcOffset(cx); cx&(1<<ovcValueBits-1) != 0 && x[:off+1] != y[:off+1] {
				t.Fatalf("%q and %q share code %#x against %q but differ by offset %d", x, y, cx, base, off)
			}
		default:
			first, second, secondCode := x, y, cy
			if cy < cx {
				first, second, secondCode = y, x, cx
			}
			if c, _ := ovcAfter(first, second, 0); c != secondCode {
				t.Fatalf("%q against %q codes %#x, against %q %#x", second, first, c, base, secondCode)
			}
		}
	}
}
