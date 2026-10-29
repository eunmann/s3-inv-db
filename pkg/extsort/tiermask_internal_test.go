package extsort

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"path/filepath"
	"slices"
	"testing"
	"unsafe"

	"github.com/eunmann/s3-inv-db/pkg/tiers"
	"github.com/google/go-cmp/cmp"
)

// rowCmp compares PrefixRows field by field, tiers and mask included.
var rowCmp = cmp.AllowUnexported(PrefixRow{})

// tierRow returns row with one tier set.
func tierRow(row PrefixRow, id tiers.ID, count, bytes uint64) PrefixRow {
	row.SetTier(id, count, bytes)

	return row
}

// fullTierMask is the mask a scan of all the tiers gives.
func fullTierMask(counts, bytes *[MaxTiers]uint64) uint16 {
	var m uint16
	for t := range MaxTiers {
		if counts[t]|bytes[t] != 0 {
			m |= 1 << t
		}
	}

	return m
}

// checkTierMask fails unless row's mask has exactly the bits of its
// nonzero tiers.
func checkTierMask(t *testing.T, where string, row *PrefixRow) {
	t.Helper()
	if want := fullTierMask(&row.tierCounts, &row.tierBytes); row.tierMask != want {
		t.Fatalf("%s: row %q has tier mask %013b, its tiers say %013b", where, row.Prefix, row.tierMask, want)
	}
}

// fullLoopEncode is encodeRunRecord as it was before the tier mask:
// every tier scanned for the record's mask and sums.
func fullLoopEncode(dst []byte, prev string, row *PrefixRow) []byte {
	var count, bytes, mask uint64
	for t := range MaxTiers {
		count += row.tierCounts[t]
		bytes += row.tierBytes[t]
		if row.tierCounts[t]|row.tierBytes[t] != 0 {
			mask |= 1 << t
		}
	}
	explicit := count != row.Count || bytes != row.TotalBytes
	if explicit {
		mask |= runRecordTotals
	}
	shared := sharedPrefixLen(prev, row.Prefix)
	dst = binary.AppendUvarint(dst, uint64(shared))
	dst = binary.AppendUvarint(dst, uint64(len(row.Prefix)-shared))
	dst = append(dst, row.Prefix[shared:]...)
	dst = binary.AppendUvarint(dst, uint64(row.Depth))
	dst = binary.AppendUvarint(dst, mask)
	for t := range MaxTiers {
		if mask&(1<<t) != 0 {
			dst = binary.AppendUvarint(dst, row.tierCounts[t])
			dst = binary.AppendUvarint(dst, row.tierBytes[t])
		}
	}
	if explicit {
		dst = binary.AppendUvarint(dst, row.Count)
		dst = binary.AppendUvarint(dst, row.TotalBytes)
	}

	return dst
}

// maskTestRows is randomSortedRows with every fifth row's totals off its
// tier sums, so the explicit-totals records are covered too.
func maskTestRows(seed uint64, n int) []PrefixRow {
	rows := randomSortedRows(rand.New(rand.NewPCG(seed, seed+1)), n)
	for i := 0; i < n; i += 5 {
		rows[i].Count += 3
	}

	return rows
}

func TestPrefixRowSizeUnchanged(t *testing.T) {
	if got := unsafe.Sizeof(PrefixRow{}); got != 248 {
		t.Fatalf("PrefixRow is %d bytes, want 248: the mask should sit in Depth's padding", got)
	}
}

// TestTierMaskMatchesFullLoop: for random rows, encoding, decoding into
// a reused row, Merge, copyFrom, Reset and the level spool give what the
// loops over all thirteen tiers gave, byte for byte.
func TestTierMaskMatchesFullLoop(t *testing.T) {
	rows := maskTestRows(11, 4000)

	var got, last []byte
	prev := ""
	for i := range rows {
		checkTierMask(t, "random row", &rows[i])
		got = encodeRunRecord(got, prev, &rows[i])
		last = fullLoopEncode(last, prev, &rows[i])
		prev = rows[i].Prefix
	}
	if !slices.Equal(got, last) {
		t.Fatal("encodeRunRecord differs from the full tier loop")
	}

	// One row decoded into over and over, as the readers do.
	var arena prefixArena
	var reused PrefixRow
	prev = ""
	for i := range rows {
		n, err := decodeRunRecord(got, prev, &arena, &reused)
		if err != nil {
			t.Fatalf("row %d: %v", i, err)
		}
		got, prev = got[n:], reused.Prefix
		checkTierMask(t, "decoded", &reused)
		if diff := cmp.Diff(rows[i], reused, rowCmp); diff != "" {
			t.Fatalf("row %d (-written +decoded into a reused row):\n%s", i, diff)
		}
	}

	for i := 1; i < len(rows); i++ {
		a, b := rows[i-1], rows[i]
		want := a
		want.Count += b.Count
		want.TotalBytes += b.TotalBytes
		for t := range MaxTiers {
			want.tierCounts[t] += b.tierCounts[t]
			want.tierBytes[t] += b.tierBytes[t]
		}
		want.tierMask = fullTierMask(&want.tierCounts, &want.tierBytes)
		a.Merge(&b)
		checkTierMask(t, "merged", &a)
		if diff := cmp.Diff(want, a, rowCmp); diff != "" {
			t.Fatalf("Merge of rows %d and %d (-full loop +masked):\n%s", i-1, i, diff)
		}

		dst := rows[i-1]
		dst.copyFrom(&rows[i])
		if diff := cmp.Diff(rows[i], dst, rowCmp); diff != "" {
			t.Fatalf("copyFrom row %d into row %d (-source +copy):\n%s", i, i-1, diff)
		}

		dst.Reset()
		if diff := cmp.Diff(PrefixRow{}, dst, rowCmp); diff != "" {
			t.Fatalf("Reset row %d (-zero +reset):\n%s", i, diff)
		}
	}

	checkSpoolMatchesFullLoop(t, rows)
}

// checkSpoolMatchesFullLoop runs rows through reused spool entries and a
// reused read record: each reads back with every tier and the mask a
// scan of its tiers gives, as the spool wrote before the mask.
func checkSpoolMatchesFullLoop(t *testing.T, rows []PrefixRow) {
	t.Helper()
	s := newLevelSpool(t.TempDir())
	t.Cleanup(func() { _ = s.close() })
	for i := range rows {
		e := s.next() // batches are reused past spoolBatches*spoolBatchRows rows
		e.depth = 0
		e.rec.count, e.rec.bytes = rows[i].Count, rows[i].TotalBytes
		e.rec.setTiers(&rows[i])
		if e.rec.tierCounts != rows[i].tierCounts || e.rec.tierBytes != rows[i].tierBytes {
			t.Fatalf("row %d: setTiers into a reused entry kept a stale tier", i)
		}
		if err := s.commit(); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.finish(); err != nil {
		t.Fatal(err)
	}
	r, err := s.open(0)
	if err != nil {
		t.Fatal(err)
	}
	defer r.close()
	var rec spoolRecord
	for i := range rows {
		if err := r.next(&rec); err != nil {
			t.Fatalf("spool record %d: %v", i, err)
		}
		if rec.tierCounts != rows[i].tierCounts || rec.tierBytes != rows[i].tierBytes {
			t.Fatalf("spool record %d: tiers read back %v/%v, want %v/%v",
				i, rec.tierCounts, rec.tierBytes, rows[i].tierCounts, rows[i].tierBytes)
		}
		if want := fullTierMask(&rows[i].tierCounts, &rows[i].tierBytes); rec.tierMask != want {
			t.Fatalf("spool record %d: mask %013b, want %013b", i, rec.tierMask, want)
		}
	}
	if err := r.next(&rec); !errors.Is(err, io.EOF) {
		t.Fatalf("after the last spool record: %v, want EOF", err)
	}
}

// TestSetTierKeepsTheMask: setting a tier to zero takes its bit out.
func TestSetTierKeepsTheMask(t *testing.T) {
	var row PrefixRow
	row.SetTier(tiers.GlacierIR, 0, 5) // bytes alone count
	row.SetTier(tiers.Standard, 2, 0)
	checkTierMask(t, "set", &row)
	row.SetTier(tiers.GlacierIR, 0, 0)
	checkTierMask(t, "cleared", &row)
	if row.tierMask != 1<<tiers.Standard {
		t.Fatalf("mask %013b, want only Standard", row.tierMask)
	}
}

// TestTierMaskEveryConstructor: rows from every path that builds or
// fills one (the aggregator, its segment merge, run files, memory runs,
// the merge iterator and the prefetcher) hold the mask invariant, and
// the merged rows equal a direct sum.
func TestTierMaskEveryConstructor(t *testing.T) {
	rng := rand.New(rand.NewPCG(21, 22))
	keys := pathTestKeys(rng, 4000)
	agg := NewAggregator(0, 0)
	want := map[string]PrefixRow{}
	var runs [][]*PrefixRow
	var paths []string
	dir := t.TempDir()
	for spill := range 6 {
		// Sorted segments (no map) and random ones (mapped, merged).
		chunk := slices.Clone(keys[spill*600 : (spill+1)*600])
		if spill%2 == 0 {
			slices.Sort(chunk)
		}
		for i, k := range chunk {
			if i%150 == 0 && spill >= 2 {
				agg.StartSegment()
			}
			id := tiers.ID(rng.IntN(MaxTiers))
			size := rng.Uint64N(3) * rng.Uint64N(1<<30) // some zero-byte objects
			agg.AddObject(k, size, id)
			addReference(want, k, size, id)
		}
		rows := agg.DrainSorted()
		for _, r := range rows {
			checkTierMask(t, fmt.Sprintf("spill %d aggregator row", spill), r)
		}
		if spill%2 == 0 {
			// Held as a memory run: copy them out, the aggregator recycles
			// its rows (Reset) at the next spill.
			held := make([]*PrefixRow, len(rows))
			for i, r := range rows {
				c := *r
				held[i] = &c
			}
			runs = append(runs, held)

			continue
		}
		path := filepath.Join(dir, fmt.Sprintf("run%d", spill))
		if err := writeAndCloseRun(mustRunWriter(t, path), rows, path); err != nil {
			t.Fatal(err)
		}
		paths = append(paths, path)
	}

	sources := make([]runSource, 0, len(runs)+len(paths))
	for _, rows := range runs {
		sources = append(sources, newMemRun(rows))
	}
	for _, p := range paths {
		r, err := OpenCompressedRunFile(p, 0)
		if err != nil {
			t.Fatal(err)
		}
		sources = append(sources, r)
	}
	merge, err := newMergeIterator(sources)
	if err != nil {
		t.Fatal(err)
	}
	defer merge.Close()
	pre := newPrefetchIterator(merge)
	defer pre.stop()
	got := map[string]PrefixRow{}
	for {
		row, err := pre.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		checkTierMask(t, "merged row", row)
		got[row.Prefix] = *row
	}
	if diff := cmp.Diff(want, got, rowCmp); diff != "" {
		t.Fatalf("(-direct sum +pipeline):\n%s", diff)
	}
}

// addReference adds an object to each of its prefixes in want with
// SetTier, apart from the aggregator's add.
func addReference(want map[string]PrefixRow, key string, size uint64, id tiers.ID) {
	add := func(prefix string, depth uint16) {
		r := want[prefix]
		r.Prefix, r.Depth = prefix, depth
		r.Count++
		r.TotalBytes += size
		r.SetTier(id, r.TierCount(id)+1, r.TierBytes(id)+size)
		want[prefix] = r
	}
	add("", 0)
	depth := uint16(1)
	for i := range len(key) {
		if key[i] == '/' {
			add(key[:i+1], depth)
			depth++
		}
	}
}

func mustRunWriter(t *testing.T, path string) *CompressedRunWriter {
	t.Helper()
	w, err := NewCompressedRunWriter(path, CompressedRunWriterOptions{})
	if err != nil {
		t.Fatal(err)
	}

	return w
}

// TestDecodeDropsEmptyTier: a record from elsewhere that lists a tier
// with neither objects nor bytes reads back without its bit.
func TestDecodeDropsEmptyTier(t *testing.T) {
	data := []byte{0, 2, 'a', '/', 1, 0b11, 0, 0, 1, 7} // tiers 0 (empty) and 1
	var row PrefixRow
	row.SetTier(5, 1, 1) // left from a previous row
	var arena prefixArena
	if _, err := decodeRunRecord(data, "", &arena, &row); err != nil {
		t.Fatal(err)
	}
	checkTierMask(t, "decoded", &row)
	want := tierRow(PrefixRow{Prefix: "a/", Depth: 1, Count: 1, TotalBytes: 7}, 1, 1, 7)
	if diff := cmp.Diff(want, row, rowCmp); diff != "" {
		t.Fatalf("(-want +decoded):\n%s", diff)
	}
}
