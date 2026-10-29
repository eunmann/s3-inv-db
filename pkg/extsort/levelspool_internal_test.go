package extsort

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/google/go-cmp/cmp"
)

// spoolTestRecord is the i-th record of the spool tests, at depth i%4,
// without the name, which is not spooled.
func spoolTestRecord(i int) spoolRecord {
	r := spoolRecord{
		count: uint64(i), bytes: uint64(i) << 20, childLocal: uint64(i / 2), parentLocal: uint64(i / 3),
		keyHash: uint64(i) * 0x9e3779b97f4a7c15, fp: ^uint64(i), segID: uint32(i) * 0x9e3779b9,
	}
	r.tierCounts[i%MaxTiers], r.tierBytes[i%MaxTiers] = uint64(i), uint64(i)<<20

	return r
}

// TestLevelSpoolWritesEveryRecordInOrder: records handed over across
// several batches, and a last partial one, read back per depth in the
// order they were added.
func TestLevelSpoolWritesEveryRecordInOrder(t *testing.T) {
	const n = 3*spoolBatchRows + 5
	s := newLevelSpool(t.TempDir())
	t.Cleanup(func() { _ = s.close() })
	for i := range n {
		e := s.next()
		e.depth, e.rec = i%4, spoolTestRecord(i)
		e.rec.name = fmt.Appendf(nil, "seg-%d", i)
		if err := s.commit(); err != nil {
			t.Fatalf("commit %d: %v", i, err)
		}
	}
	if err := s.finish(); err != nil {
		t.Fatal(err)
	}
	for depth := range 4 {
		r, err := s.open(depth)
		if err != nil {
			t.Fatal(err)
		}
		i := depth
		var got spoolRecord
		for ; ; i += 4 {
			if err := r.next(&got); errors.Is(err, io.EOF) {
				break
			} else if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(spoolTestRecord(i), got, cmp.AllowUnexported(spoolRecord{})); diff != "" {
				t.Fatalf("record %d (-added +read):\n%s", i, diff)
			}
		}
		if err := r.close(); err != nil {
			t.Fatal(err)
		}
		if i < n {
			t.Fatalf("depth %d read up to record %d of %d", depth, i, n)
		}
	}
}

// TestLevelSpoolWriterErrorSurfaces: a write the writer goroutine fails
// reaches the builder through commit, then finish, and nothing hangs.
func TestLevelSpoolWriterErrorSurfaces(t *testing.T) {
	s := newLevelSpool(filepath.Join(t.TempDir(), "missing"))
	var commitErr error
	for i := 0; commitErr == nil && i < 10*spoolBatchRows; i++ {
		e := s.next()
		e.depth, e.rec = 0, spoolTestRecord(i)
		commitErr = s.commit()
	}
	if commitErr == nil {
		t.Fatal("commit never returned the writer's error")
	}
	if err := s.finish(); !errors.Is(err, commitErr) {
		t.Fatalf("finish = %v, want the commit error %v", err, commitErr)
	}
	if err := s.close(); !errors.Is(err, commitErr) {
		t.Fatalf("close = %v, want the commit error %v", err, commitErr)
	}
}

// TestLevelSpoolCommitAfterFinish: a row added after finish is refused,
// not lost or a panic.
func TestLevelSpoolCommitAfterFinish(t *testing.T) {
	for _, rows := range []int{0, 1} {
		s := newLevelSpool(t.TempDir())
		for i := range rows {
			e := s.next()
			e.rec = spoolTestRecord(i)
			if err := s.commit(); err != nil {
				t.Fatal(err)
			}
		}
		if err := s.finish(); err != nil {
			t.Fatal(err)
		}
		s.next().rec = spoolTestRecord(rows)
		if err := s.commit(); !errors.Is(err, errSpoolStopped) {
			t.Fatalf("after %d rows: commit after finish = %v, want errSpoolStopped", rows, err)
		}
		if err := s.close(); err != nil {
			t.Fatal(err)
		}
	}
}

// spoolStream returns recs spooled at depth 0, as the bytes of the
// depth's file.
func spoolStream(t *testing.T, recs []spoolRecord) []byte {
	t.Helper()
	s := newLevelSpool(t.TempDir())
	t.Cleanup(func() { _ = s.close() })
	for i := range recs {
		s.next().rec = recs[i]
		if err := s.commit(); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.finish(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(s.depths[0].path)
	if err != nil {
		t.Fatal(err)
	}

	return data
}

// spoolEnds returns where each of a spool stream's records ends.
func spoolEnds(data []byte) []int {
	var ends []int
	for off := 0; off < len(data); {
		n, k := binary.Uvarint(data[off:])
		off += k + int(n)
		ends = append(ends, off)
	}

	return ends
}

// readSpool reads src's records through a size-byte buffer until the
// stream ends or a record fails.
func readSpool(src io.Reader, size int) ([]spoolRecord, error) {
	r := spoolReader{in: readWindow{r: bufio.NewReaderSize(src, size)}}
	var recs []spoolRecord
	for {
		var rec spoolRecord
		if err := r.next(&rec); err != nil {
			return recs, err
		}
		recs = append(recs, rec)
	}
}

func spoolTestRecords(n int) []spoolRecord {
	recs := make([]spoolRecord, n)
	for i := range recs {
		recs[i] = spoolTestRecord(i)
	}

	return recs
}

// TestSpoolReaderAcrossRefills: records that straddle the end of what
// was buffered, in buffers from about a record to the spool's own, read
// back equal and in order, and the stream ends with io.EOF.
func TestSpoolReaderAcrossRefills(t *testing.T) {
	want := spoolTestRecords(3000)
	data := spoolStream(t, want)
	for _, size := range []int{64, 100, 4096, levelSpoolBufSize} {
		for name, wrap := range runRecordReaders {
			t.Run(fmt.Sprintf("buffer=%d/%s", size, name), func(t *testing.T) {
				got, err := readSpool(wrap(data), size)
				if !errors.Is(err, io.EOF) {
					t.Fatalf("after %d records: %v, want EOF", len(got), err)
				}
				if diff := cmp.Diff(want, got, cmp.AllowUnexported(spoolRecord{})); diff != "" {
					t.Fatalf("(-added +read):\n%s", diff)
				}
			})
		}
	}
}

// TestSpoolReaderFillsTheBuffer: the largest record (every field at its
// widest) decodes between small ones from a read buffer exactly its
// size; from a buffer a byte shorter it is an error, never a record or
// the stream's end.
func TestSpoolReaderFillsTheBuffer(t *testing.T) {
	big := spoolRecord{
		count: math.MaxUint64, bytes: math.MaxUint64, childLocal: math.MaxUint64, parentLocal: math.MaxUint64,
		keyHash: math.MaxUint64, fp: math.MaxUint64, segID: math.MaxUint32,
	}
	for i := range big.tierCounts {
		big.tierCounts[i], big.tierBytes[i] = math.MaxUint64, math.MaxUint64
	}
	size := len(spoolStream(t, []spoolRecord{big}))
	want := []spoolRecord{spoolTestRecord(0), big, spoolTestRecord(2)}
	data := spoolStream(t, want)
	tooLong := spoolStream(t, []spoolRecord{spoolTestRecord(0), big})
	for name, wrap := range runRecordReaders {
		t.Run(name, func(t *testing.T) {
			got, err := readSpool(wrap(data), size)
			if !errors.Is(err, io.EOF) {
				t.Fatalf("after %d records: %v, want EOF", len(got), err)
			}
			if diff := cmp.Diff(want, got, cmp.AllowUnexported(spoolRecord{})); diff != "" {
				t.Fatalf("(-added +read):\n%s", diff)
			}
			got, err = readSpool(wrap(tooLong), size-1)
			if len(got) != 1 || !errors.Is(err, errSpoolTruncated) {
				t.Fatalf("a record past the buffer: %d records, then %v; want 1, then errSpoolTruncated", len(got), err)
			}
		})
	}
}

// TestSpoolReaderTruncated: a spool cut at every byte reads back the
// records before the cut, then ends with io.EOF if the cut falls between
// records and errSpoolTruncated if inside one.
func TestSpoolReaderTruncated(t *testing.T) {
	want := spoolTestRecords(30)
	data := spoolStream(t, want)
	ends := spoolEnds(data)
	for _, size := range []int{64, 4096} {
		for cut := range data {
			whole := sort.SearchInts(ends, cut+1)
			got, err := readSpool(bytes.NewReader(data[:cut]), size)
			if len(got) != whole {
				t.Fatalf("buffer %d, cut at %d: read %d records, want %d", size, cut, len(got), whole)
			}
			if diff := cmp.Diff(want[:whole], got, cmp.AllowUnexported(spoolRecord{})); whole > 0 && diff != "" {
				t.Fatalf("buffer %d, cut at %d (-added +read):\n%s", size, cut, diff)
			}
			atBoundary := whole == 0 && cut == 0 || whole > 0 && ends[whole-1] == cut
			if atBoundary && !errors.Is(err, io.EOF) {
				t.Fatalf("buffer %d, cut at %d, between records: %v, want EOF", size, cut, err)
			}
			if !atBoundary && !errors.Is(err, errSpoolTruncated) {
				t.Fatalf("buffer %d, cut at %d, inside a record: %v, want errSpoolTruncated", size, cut, err)
			}
		}
	}
}

// BenchmarkLevelSpoolRead reads back one depth's spooled records, as
// Finalize does for every depth.
func BenchmarkLevelSpoolRead(b *testing.B) {
	const n = 100_000
	s := newLevelSpool(b.TempDir())
	b.Cleanup(func() { _ = s.close() })
	for i := range n {
		e := s.next()
		e.rec = spoolTestRecord(i)
		if err := s.commit(); err != nil {
			b.Fatal(err)
		}
	}
	if err := s.finish(); err != nil {
		b.Fatal(err)
	}
	var rec spoolRecord
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		r, err := s.open(0)
		if err != nil {
			b.Fatal(err)
		}
		read := 0
		for ; ; read++ {
			if err := r.next(&rec); errors.Is(err, io.EOF) {
				break
			} else if err != nil {
				b.Fatal(err)
			}
		}
		if err := r.close(); err != nil {
			b.Fatal(err)
		}
		if read != n {
			b.Fatalf("read %d of %d records", read, n)
		}
	}
}
