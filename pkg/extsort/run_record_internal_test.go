package extsort

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"testing"
	"testing/iotest"

	"github.com/eunmann/s3-inv-db/pkg/tiers"
	"github.com/google/go-cmp/cmp"
)

// randomSortedRows returns n rows with distinct prefixes in order, many
// sharing long prefixes, tier mixes from none to all tiers, and values
// up to the full uint64 range.
func randomSortedRows(rng *rand.Rand, n int) []PrefixRow {
	segs := []string{"a", "ab", "b", "data", "2024", "x-y", "émoji", "", "very-long-segment-name-0123456789"}
	seen := map[string]bool{}
	var prefixes []string
	for len(prefixes) < n {
		var sb strings.Builder
		for range rng.IntN(6) {
			sb.WriteString(segs[rng.IntN(len(segs))])
			sb.WriteByte('/')
		}
		if p := sb.String(); !seen[p] {
			seen[p] = true
			prefixes = append(prefixes, p)
		}
	}
	slices.Sort(prefixes)
	rows := make([]PrefixRow, n)
	for i, p := range prefixes {
		r := &rows[i]
		r.Prefix, r.Depth = p, uint16(strings.Count(p, "/"))
		for t := range MaxTiers {
			if rng.IntN(3) != 0 {
				continue
			}
			switch rng.IntN(4) {
			case 0:
				r.TierCounts[t], r.TierBytes[t] = 1, 0 // zero-byte objects
			case 1:
				r.TierCounts[t], r.TierBytes[t] = math.MaxUint64/32, math.MaxUint64/32
			default:
				r.TierCounts[t], r.TierBytes[t] = rng.Uint64N(1000)+1, rng.Uint64N(1<<40)
			}
			r.Count += r.TierCounts[t]
			r.TotalBytes += r.TierBytes[t]
		}
	}

	return rows
}

// TestSharedPrefixLen: the word-at-a-time common prefix matches a byte
// loop for a difference at every position of the first and later
// words, either string shorter, and equal strings.
func TestSharedPrefixLen(t *testing.T) {
	naive := func(a, b string) int {
		i := 0
		for i < min(len(a), len(b)) && a[i] == b[i] {
			i++
		}

		return i
	}
	base := "data/2024/01/02/very-long-segment-name-0123456789/"
	for n := range len(base) + 1 {
		a := base[:n]
		for _, b := range []string{base, base[:n], base[:n/2]} {
			if got, want := sharedPrefixLen(a, b), naive(a, b); got != want {
				t.Errorf("sharedPrefixLen(%q, %q) = %d, want %d", a, b, got, want)
			}
		}
		for i := range n {
			b := []byte(a)
			b[i] ^= 0x80
			if got := sharedPrefixLen(a, string(b)); got != i {
				t.Errorf("difference at %d of %q: sharedPrefixLen = %d", i, a, got)
			}
		}
	}
}

// TestRunRecordsRoundTrip: every row written to a run file reads back
// equal, through the compressed writer and reader.
func TestRunRecordsRoundTrip(t *testing.T) {
	rows := randomSortedRows(rand.New(rand.NewPCG(1, 2)), 5000)
	path := t.TempDir() + "/run.bin"
	w, err := NewCompressedRunWriter(path, CompressedRunWriterOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for i := range rows {
		if err := w.Write(&rows[i]); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	r, err := OpenCompressedRunFile(path, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	var got PrefixRow
	for i := range rows {
		if err := r.ReadInto(&got); err != nil {
			t.Fatalf("row %d: %v", i, err)
		}
		if diff := cmp.Diff(rows[i], got); diff != "" {
			t.Fatalf("row %d (-written +read):\n%s", i, diff)
		}
	}
	if err := r.ReadInto(&got); !errors.Is(err, io.EOF) {
		t.Fatalf("after the last row: %v, want EOF", err)
	}
}

// TestRunRecordKeepsInconsistentTotals: a row whose totals differ from
// its tier sums stores them, and reads back exactly.
func TestRunRecordKeepsInconsistentTotals(t *testing.T) {
	for _, row := range []PrefixRow{
		{Prefix: "a/", Count: 2, TotalBytes: 5, TierCounts: [MaxTiers]uint64{1}, TierBytes: [MaxTiers]uint64{5}},
		{Prefix: "b/", Count: 7, TotalBytes: 700},
	} {
		data := encodeRunRecord(nil, "", &row)
		var got PrefixRow
		d := runRecordDecoder{in: readWindow{r: bufio.NewReader(bytes.NewReader(data))}}
		if _, err := d.next(&got); err != nil {
			t.Fatal(err)
		}
		if diff := cmp.Diff(row, got); diff != "" {
			t.Fatalf("(-written +read):\n%s", diff)
		}
	}
}

// TestRunRecordRejectsCorruption: truncated records and impossible
// fields are errors, never rows.
func TestRunRecordRejectsCorruption(t *testing.T) {
	rows := randomSortedRows(rand.New(rand.NewPCG(3, 4)), 2)
	first := encodeRunRecord(nil, "", &rows[0])
	cases := map[string][]byte{
		"truncated":      first[:len(first)-1],
		"shared_too_far": {5, 0, 0, 0},
		"mask_too_wide":  {0, 0, 0, 0xff, 0xff, 0x01},
		"depth_too_deep": {0, 0, 0x80, 0x80, 0x04, 0},
	}
	for name, data := range cases {
		t.Run(name, func(t *testing.T) {
			var row PrefixRow
			d := runRecordDecoder{in: readWindow{r: bufio.NewReader(bytes.NewReader(data))}}
			_, err := d.next(&row)
			if err == nil || errors.Is(err, io.EOF) {
				t.Fatalf("decode = %v, want a corruption error", err)
			}
			if !errors.Is(err, errRunRecordCorrupt) || strings.Contains(err.Error(), "%!") {
				t.Fatalf("decode = %q, want a well-formed corruption error", err)
			}
		})
	}
}

// TestRunRecordLongerThanWindow: a record longer than the in-place
// window decodes from the whole read buffer, and the next record,
// front-coded against its prefix, reads back too.
func TestRunRecordLongerThanWindow(t *testing.T) {
	long := strings.Repeat("segment/", runRecordWindow/4) // 2× the window
	rows := []PrefixRow{
		{Prefix: long, Depth: uint16(runRecordWindow / 4), Count: 1, TotalBytes: 2, TierCounts: [MaxTiers]uint64{1}, TierBytes: [MaxTiers]uint64{2}},
		{Prefix: long + "x/", Depth: uint16(runRecordWindow/4 + 1)},
	}
	data := encodeRunRecord(nil, "", &rows[0])
	data = encodeRunRecord(data, rows[0].Prefix, &rows[1])
	d := runRecordDecoder{in: readWindow{r: bufio.NewReaderSize(bytes.NewReader(data), 4*runRecordWindow)}}
	for i := range rows {
		var got PrefixRow
		if _, err := d.next(&got); err != nil {
			t.Fatalf("row %d: %v", i, err)
		}
		if diff := cmp.Diff(rows[i], got); diff != "" {
			t.Fatalf("row %d (-written +read):\n%s", i, diff)
		}
	}
	var row PrefixRow
	if _, err := d.next(&row); !errors.Is(err, io.EOF) {
		t.Fatalf("after the last row: %v, want EOF", err)
	}
}

// TestCompressedRunReaderShortOfCount: a run whose stream ends before
// the header's row count is corrupt, not a shorter run: a merge that
// stopped there would drop rows.
func TestCompressedRunReaderShortOfCount(t *testing.T) {
	rows := randomSortedRows(rand.New(rand.NewPCG(5, 6)), 3)
	path := filepath.Join(t.TempDir(), "run.crun")
	w, err := NewCompressedRunWriter(path, CompressedRunWriterOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for i := range rows {
		if err := w.Write(&rows[i]); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt(binary.LittleEndian.AppendUint64(nil, uint64(len(rows)+1)), compressedRunCountOffset); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	r, err := OpenCompressedRunFile(path, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	var got PrefixRow
	for i := range rows {
		if err := r.ReadInto(&got); err != nil {
			t.Fatalf("row %d: %v", i, err)
		}
	}
	if err := r.ReadInto(&got); !errors.Is(err, errRunRecordCorrupt) {
		t.Fatalf("past the stream's end: %v, want errRunRecordCorrupt", err)
	}
}

// runRecordStream is rows' records, each front-coded against the one
// before, and where each record ends.
type runRecordStream struct {
	data []byte
	ends []int
}

func encodeRunRecords(rows []PrefixRow) runRecordStream {
	var s runRecordStream
	prev := ""
	for i := range rows {
		s.data = encodeRunRecord(s.data, prev, &rows[i])
		s.ends, prev = append(s.ends, len(s.data)), rows[i].Prefix
	}

	return s
}

// diffRows is cmp.Diff for long row lists, computed only when they
// differ.
func diffRows(want, got []PrefixRow) string {
	if slices.Equal(want, got) {
		return ""
	}

	return cmp.Diff(want, got)
}

// decodeRunRecords reads src's records through a size-byte buffer until
// the stream ends or a record fails, keeping every row: rows decoded
// early must still read back after the buffer was refilled. Each record
// must report the bytes it shares with the one before, all of them, as
// encodeRunRecord codes it.
func decodeRunRecords(src io.Reader, size int) ([]PrefixRow, error) {
	d := runRecordDecoder{in: readWindow{r: bufio.NewReaderSize(src, size)}}
	var rows []PrefixRow
	prev := ""
	for {
		var row PrefixRow
		shared, err := d.next(&row)
		if err != nil {
			return rows, err
		}
		if want := sharedPrefixLen(prev, row.Prefix); shared != want {
			return rows, fmt.Errorf("%w: row %d reports %d bytes, want %d", errWrongShared, len(rows), shared, want)
		}
		rows, prev = append(rows, row), row.Prefix
	}
}

// errWrongShared means a record reported a shared length other than the
// one it was coded with.
var errWrongShared = errors.New("record reports the wrong shared length")

// runRecordReaders wrap a stream as the decoder may meet it: read in
// full, half a buffer per read, and a byte per read, so refills stop at
// every possible byte.
var runRecordReaders = map[string]func([]byte) io.Reader{
	"whole":    func(b []byte) io.Reader { return bytes.NewReader(b) },
	"half":     func(b []byte) io.Reader { return iotest.HalfReader(bytes.NewReader(b)) },
	"one_byte": func(b []byte) io.Reader { return iotest.OneByteReader(bytes.NewReader(b)) },
}

// TestRunRecordsAcrossRefills: records that straddle the end of what was
// buffered, in buffers from a few records to many windows, read back
// equal and in order, and the stream ends with io.EOF.
func TestRunRecordsAcrossRefills(t *testing.T) {
	rows := randomSortedRows(rand.New(rand.NewPCG(7, 8)), 3000)
	s := encodeRunRecords(rows)
	for _, size := range []int{512, 777, runRecordWindow - 1, runRecordWindow, runRecordWindow + 1, mergeReaderBufferSize} {
		for name, wrap := range runRecordReaders {
			t.Run(fmt.Sprintf("buffer=%d/%s", size, name), func(t *testing.T) {
				got, err := decodeRunRecords(wrap(s.data), size)
				if !errors.Is(err, io.EOF) {
					t.Fatalf("after %d rows: %v, want EOF", len(got), err)
				}
				if diff := diffRows(rows, got); diff != "" {
					t.Fatalf("(-written +read):\n%s", diff)
				}
			})
		}
	}
}

// TestRunRecordsManyTiny: records of a few bytes each, many to a
// window, through the smallest buffer bufio allows and larger ones.
func TestRunRecordsManyTiny(t *testing.T) {
	rows := make([]PrefixRow, 20_000)
	for i := range rows {
		rows[i] = PrefixRow{Prefix: fmt.Sprintf("%05d/", i), Depth: 1}
		if i%3 == 0 {
			rows[i].Count, rows[i].TierCounts[0] = 1, 1
		}
	}
	s := encodeRunRecords(rows)
	if avg := len(s.data) / len(rows); avg > 8 {
		t.Fatalf("records average %d bytes, want tiny ones", avg)
	}
	for _, size := range []int{16, runRecordWindow, mergeReaderBufferSize} {
		for name, wrap := range runRecordReaders {
			t.Run(fmt.Sprintf("buffer=%d/%s", size, name), func(t *testing.T) {
				got, err := decodeRunRecords(wrap(s.data), size)
				if !errors.Is(err, io.EOF) {
					t.Fatalf("after %d rows: %v, want EOF", len(got), err)
				}
				if diff := diffRows(rows, got); diff != "" {
					t.Fatalf("(-written +read):\n%s", diff)
				}
			})
		}
	}
}

// runRecordOfLen returns a row, its prefix starting with first, whose
// record front-coded against a prefix it shares nothing with is n
// bytes.
func runRecordOfLen(t *testing.T, first byte, n int) PrefixRow {
	t.Helper()
	row := PrefixRow{Depth: 1, Count: 3, TotalBytes: 300, TierCounts: [MaxTiers]uint64{3}, TierBytes: [MaxTiers]uint64{300}}
	for l := n; l > 0; l-- {
		row.Prefix = string(first) + strings.Repeat("k", l-1)
		if len(encodeRunRecord(nil, "", &row)) == n {
			return row
		}
	}
	t.Fatalf("no record is %d bytes", n)

	return row
}

// TestRunRecordFillsTheBuffer: a record exactly the window's or the
// read buffer's size decodes between small records; one a byte longer
// than the buffer is corrupt, never a row or the stream's end.
func TestRunRecordFillsTheBuffer(t *testing.T) {
	const size = 4 * runRecordWindow
	for _, n := range []int{runRecordWindow, size} {
		rows := []PrefixRow{{Prefix: "a/", Depth: 1}, runRecordOfLen(t, 'b', n), {Prefix: "c/", Depth: 1}}
		s := encodeRunRecords(rows)
		if got := s.ends[1] - s.ends[0]; got != n {
			t.Fatalf("the long record is %d bytes, want %d", got, n)
		}
		for name, wrap := range runRecordReaders {
			t.Run(fmt.Sprintf("record=%d/%s", n, name), func(t *testing.T) {
				got, err := decodeRunRecords(wrap(s.data), size)
				if !errors.Is(err, io.EOF) {
					t.Fatalf("after %d rows: %v, want EOF", len(got), err)
				}
				if diff := diffRows(rows, got); diff != "" {
					t.Fatalf("(-written +read):\n%s", diff)
				}
			})
		}
	}
	rows := []PrefixRow{{Prefix: "a/", Depth: 1}, runRecordOfLen(t, 'b', size+1)}
	s := encodeRunRecords(rows)
	for name, wrap := range runRecordReaders {
		t.Run("record=buffer+1/"+name, func(t *testing.T) {
			got, err := decodeRunRecords(wrap(s.data), size)
			if len(got) != 1 || !errors.Is(err, errRunRecordCorrupt) {
				t.Fatalf("%d rows, then %v; want 1 row, then errRunRecordCorrupt", len(got), err)
			}
		})
	}
}

// TestRunRecordsTruncated: a stream cut at every byte reads back the
// records before the cut, then ends with io.EOF if the cut falls between
// records and errRunRecordCorrupt if inside one.
func TestRunRecordsTruncated(t *testing.T) {
	rows := randomSortedRows(rand.New(rand.NewPCG(9, 10)), 40)
	s := encodeRunRecords(rows)
	for _, size := range []int{512, runRecordWindow} {
		for cut := range s.data {
			whole := sort.SearchInts(s.ends, cut+1)
			got, err := decodeRunRecords(bytes.NewReader(s.data[:cut]), size)
			if diff := diffRows(rows[:whole], got); diff != "" {
				t.Fatalf("buffer %d, cut at %d (-written +read):\n%s", size, cut, diff)
			}
			atBoundary := whole == 0 && cut == 0 || whole > 0 && s.ends[whole-1] == cut
			if atBoundary && !errors.Is(err, io.EOF) {
				t.Fatalf("buffer %d, cut at %d, between records: %v, want EOF", size, cut, err)
			}
			if !atBoundary && !errors.Is(err, errRunRecordCorrupt) {
				t.Fatalf("buffer %d, cut at %d, inside a record: %v, want errRunRecordCorrupt", size, cut, err)
			}
		}
	}
}

// FuzzAppendNodeStats: a node's stats, in one tier or in a tier block
// whose sums are its totals, encode byte for byte as the row holding the
// same counts does.
func FuzzAppendNodeStats(f *testing.F) {
	f.Add(uint16(0), uint8(0), uint64(1), uint64(0), []byte(nil))
	f.Add(uint16(3), uint8(MaxTiers-1), uint64(math.MaxUint64), uint64(math.MaxUint64), []byte(nil))
	f.Add(uint16(1), uint8(2), uint64(0), uint64(0), []byte(nil))
	f.Add(uint16(2), uint8(0), uint64(9), uint64(4), []byte{0, 1, 5, 1, 0, 0, 7, 3, 200, 15, 255, 255})
	f.Add(uint16(70), uint8(0), uint64(math.MaxUint64), uint64(63), []byte{3, 0, 0, 3, 1, 0, 3, 255, 255})
	f.Fuzz(func(t *testing.T, depth uint16, tier uint8, count, size uint64, spread []byte) {
		row := PrefixRow{Depth: depth}
		var block *tierBlock
		if len(spread) == 0 {
			tier %= uint8(MaxTiers)
			row.Count, row.TotalBytes = count, size
			row.TierCounts[tier], row.TierBytes[tier] = count, size
		} else {
			// Each triple sets a tier's count and bytes.
			block = &tierBlock{}
			for i := 0; i+2 < len(spread); i += 3 {
				tr := int(spread[i]) % MaxTiers
				block.counts[tr] = uint64(spread[i+1]) * count
				block.bytes[tr] = uint64(spread[i+2]) << (size % 57)
			}
			for tr := range MaxTiers {
				row.TierCounts[tr], row.TierBytes[tr] = block.counts[tr], block.bytes[tr]
				row.Count += block.counts[tr]
				row.TotalBytes += block.bytes[tr]
			}
		}
		want := appendRunRecordStats(nil, &row)
		got := appendNodeStats(nil, depth, row.Count, row.TotalBytes, tiers.ID(tier), block)
		if !bytes.Equal(got, want) {
			t.Fatalf("node stats %x, row stats %x for %+v", got, want, row)
		}
	})
}
