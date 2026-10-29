package extsort

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/tiers"
	gocmp "github.com/google/go-cmp/cmp"
)

// adversarialRunKeys draws keys whose sibling segments share long runs
// of bytes (across the eight-byte words sharedPrefixLen compares) and
// then differ by a byte below '/', a byte above it, or by one being the
// other without its tail, at every depth, so a shared length one off
// anywhere changes the bytes written.
func adversarialRunKeys(rng *rand.Rand, n int) []string {
	const stem = "0123456789abcdefghij"
	cuts := []int{0, 1, 7, 8, 9, 15, 16, 17, 20}
	tails := []string{"", "-", ".", "\x00", "a", "\xff", "a-b"}
	segs := make([]string, 0, len(cuts)*len(tails))
	for _, cut := range cuts {
		for _, tail := range tails {
			segs = append(segs, stem[:cut]+tail)
		}
	}
	keys := make([]string, 0, n)
	for range n {
		var sb strings.Builder
		for range rng.IntN(9) {
			sb.WriteString(segs[rng.IntN(len(segs))])
			sb.WriteByte('/')
		}
		if rng.IntN(3) > 0 {
			sb.WriteString("f")
		}
		keys = append(keys, sb.String())
	}

	return keys
}

// writeTestRun writes a run file at path through write and returns its
// bytes.
func writeTestRun(t *testing.T, path string, write func(*CompressedRunWriter) error) []byte {
	t.Helper()
	w, err := NewCompressedRunWriter(path, CompressedRunWriterOptions{BufferSize: 4 << 10, CompressionLevel: CompressionFastest})
	if err != nil {
		t.Fatalf("NewCompressedRunWriter: %v", err)
	}
	if err := writeAndCloseRun(w, path, func() error { return write(w) }); err != nil {
		t.Fatalf("write run: %v", err)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read run: %v", err)
	}

	return b
}

// readTestRun decodes every row of the run file at path.
func readTestRun(t *testing.T, path string) []PrefixRow {
	t.Helper()
	r, err := OpenCompressedRunFile(path, 64<<10)
	if err != nil {
		t.Fatalf("OpenCompressedRunFile: %v", err)
	}
	defer r.Close()
	var rows []PrefixRow
	for {
		var row PrefixRow
		err := r.ReadInto(&row)
		if errors.Is(err, io.EOF) {
			return rows
		}
		if err != nil {
			t.Fatalf("ReadInto: %v", err)
		}
		rows = append(rows, row)
	}
}

// TestAggregatorWriteRunMatchesDrain: for random, sorted, per-chunk
// sorted and adversarial keys, with and without segments, at several
// maxDepths and spilling mid-stream, every WriteRun file is byte for
// byte the file w.WriteAll(Drain()) writes for the same keys, and
// reads back to Drain's rows. Spills from the main tree, from one
// ordered segment and from several runs all occur.
func TestAggregatorWriteRunMatchesDrain(t *testing.T) {
	dir := t.TempDir()
	runs := map[string]int{}
	for seed := range uint64(3) {
		rng := rand.New(rand.NewPCG(seed, 13))
		keys := propertyTestKeys(rng, 1200)
		sorted := slices.Clone(keys)
		slices.Sort(sorted)
		chunkSorted := slices.Clone(keys)
		for i := 0; i < len(chunkSorted); i += 200 {
			slices.Sort(chunkSorted[i:min(i+200, len(chunkSorted))])
		}
		adversarial := adversarialRunKeys(rng, 1200)
		adversarialSorted := slices.Clone(adversarial)
		slices.Sort(adversarialSorted)
		orders := map[string][]string{
			"random": keys, "sorted": sorted, "chunk_sorted": chunkSorted,
			"adversarial": adversarial, "adversarial_sorted": adversarialSorted,
		}
		for name, order := range orders {
			for _, segmentEvery := range []int{0, 200, 37} {
				for _, maxDepth := range []int{0, 3} {
					for _, spillEvery := range []int{0, 97, 701} {
						direct, drained := NewAggregator(0, maxDepth), NewAggregator(0, maxDepth)
						spill := func() {
							t.Helper()
							if direct.PrefixCount() != drained.PrefixCount() {
								t.Fatalf("aggregators diverged: %d and %d rows", direct.PrefixCount(), drained.PrefixCount())
							}
							if direct.PrefixCount() == 0 {
								return
							}
							switch which, one := direct.soleRun(); {
							case !one:
								runs["several"]++
							case which < 0:
								runs["tree"]++
							default:
								runs["segment"]++
							}
							var n int
							got := writeTestRun(t, filepath.Join(dir, "direct.crun"), func(w *CompressedRunWriter) error {
								var err error
								n, err = direct.WriteRun(w)

								return err
							})
							rows := drained.Drain()
							want := writeTestRun(t, filepath.Join(dir, "drained.crun"), func(w *CompressedRunWriter) error {
								return w.WriteAll(rows)
							})
							where := fmt.Sprintf("seed=%d %s segmentEvery=%d maxDepth=%d spillEvery=%d", seed, name, segmentEvery, maxDepth, spillEvery)
							if n != len(rows) {
								t.Fatalf("%s: WriteRun wrote %d rows, Drain has %d", where, n, len(rows))
							}
							if !bytes.Equal(got, want) {
								t.Fatalf("%s: WriteRun's run file (%d B) differs from Drain+WriteAll's (%d B)", where, len(got), len(want))
							}
							wantRows := make([]PrefixRow, len(rows))
							for i, r := range rows {
								wantRows[i] = *r
							}
							if read := readTestRun(t, filepath.Join(dir, "direct.crun")); !slices.Equal(wantRows, read) {
								t.Fatalf("%s: WriteRun's rows read back (-Drain +read):\n%s", where, gocmp.Diff(wantRows, read))
							}
						}
						for i, k := range order {
							if segmentEvery > 0 && i%segmentEvery == 0 {
								direct.StartSegment()
								drained.StartSegment()
							}
							direct.AddObject(k, uint64(len(k)+i), tiers.ID(i%3))
							drained.AddObject(k, uint64(len(k)+i), tiers.ID(i%3))
							if spillEvery > 0 && i%spillEvery == spillEvery-1 {
								spill()
							}
						}
						spill()
					}
				}
			}
		}
	}
	for _, kind := range []string{"tree", "segment", "several"} {
		if runs[kind] == 0 {
			t.Errorf("no spill came from %s runs; got %v", kind, runs)
		}
	}
}

// WriteRun codes its first row against no prefix and leaves the writer
// to front-code a later row against none, so spills and rows written
// one after another through the same writer read back as written.
func TestAggregatorWriteRunAfterOtherRows(t *testing.T) {
	path := filepath.Join(t.TempDir(), "run.crun")
	agg := NewAggregator(0, 0)
	var want []PrefixRow
	collect := func(keys ...string) {
		ref := NewAggregator(0, 0)
		for _, k := range keys {
			agg.AddObject(k, 3, tiers.Standard)
			ref.AddObject(k, 3, tiers.Standard)
		}
		for _, r := range ref.Drain() {
			want = append(want, *r)
		}
	}
	last := PrefixRow{Prefix: "zz/top/", Depth: 2, Count: 1, TotalBytes: 9}
	last.TierCounts[tiers.Standard], last.TierBytes[tiers.Standard] = 1, 9
	writeTestRun(t, path, func(w *CompressedRunWriter) error {
		lead := PrefixRow{Prefix: "data/lead/", Depth: 2}
		if err := w.Write(&lead); err != nil {
			return err
		}
		want = append(want, lead)
		collect("data/a/x", "data/b/y")
		if _, err := agg.WriteRun(w); err != nil {
			return err
		}
		collect("data/a/z", "data/c/q/r")
		if _, err := agg.WriteRun(w); err != nil {
			return err
		}
		want = append(want, last)

		return w.Write(&last)
	})
	if diff := gocmp.Diff(want, readTestRun(t, path)); diff != "" {
		t.Fatalf("rows read back (-written +read):\n%s", diff)
	}
}

// A spill no larger than the last allocates nothing: WriteRun never
// builds the whole prefixes Drain writes into a block it allocates.
func TestAggregatorWriteRunAllocatesNothing(t *testing.T) {
	agg := NewAggregator(0, 0)
	keys := make([]string, 0, 600)
	for i := range 300 {
		keys = append(keys, fmt.Sprintf("p%03d/q%d/x", (i*7)%300, i%5), fmt.Sprintf("r/%03d/y", 299-i))
	}
	w, err := NewCompressedRunWriter(filepath.Join(t.TempDir(), "run.crun"), CompressedRunWriterOptions{})
	if err != nil {
		t.Fatalf("NewCompressedRunWriter: %v", err)
	}
	defer w.Close()
	cycle := func() {
		agg.AddObject("p000/q0/x", 1, tiers.Standard)
		agg.StartSegment()
		agg.AddObject("p000/q0/a", 1, tiers.Standard)
		agg.AddObject("p001/q1/a", 1, tiers.Standard)
		for _, k := range keys {
			agg.AddObject(k, 1, tiers.Standard)
		}
		if _, err := agg.WriteRun(w); err != nil {
			t.Fatalf("WriteRun: %v", err)
		}
	}
	cycle()
	if allocs := testing.AllocsPerRun(10, cycle); allocs != 0 {
		t.Errorf("a spill no larger than the last = %v allocs, want 0", allocs)
	}
}
