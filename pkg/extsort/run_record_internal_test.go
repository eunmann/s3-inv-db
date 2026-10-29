package extsort

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

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
		d := runRecordDecoder{in: bufio.NewReader(bytes.NewReader(data))}
		if err := d.next(&got); err != nil {
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
			d := runRecordDecoder{in: bufio.NewReader(bytes.NewReader(data))}
			err := d.next(&row)
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
	d := runRecordDecoder{in: bufio.NewReaderSize(bytes.NewReader(data), 4*runRecordWindow)}
	for i := range rows {
		var got PrefixRow
		if err := d.next(&got); err != nil {
			t.Fatalf("row %d: %v", i, err)
		}
		if diff := cmp.Diff(rows[i], got); diff != "" {
			t.Fatalf("row %d (-written +read):\n%s", i, diff)
		}
	}
	var row PrefixRow
	if err := d.next(&row); !errors.Is(err, io.EOF) {
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
