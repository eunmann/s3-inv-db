package extsort

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"math/bits"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

// spoolRecordCmp compares records by what they hold: a record without
// tiers reads back with an empty slice.
var spoolRecordCmp = []cmp.Option{cmp.AllowUnexported(spoolRecord{}), cmpopts.EquateEmpty()}

// spoolTestRecord is the i-th record of the spool tests, at depth i%4,
// with no tier slot to three of them.
func spoolTestRecord(i int) spoolRecord {
	r := spoolRecord{
		count: uint64(i), bytes: uint64(i) << 20, childLocal: uint64(i / 2), parentLocal: uint64(i / 3),
		segID: uint32(i) * 0x9e3779b9,
	}
	for j := range i % 4 {
		r.tierMask |= 1 << ((i + 5*j) % MaxTiers)
	}
	for j := range bits.OnesCount16(r.tierMask) {
		r.tiers = append(r.tiers, uint64(i+j), uint64(i)<<20|uint64(j))
	}

	return r
}

// TestLevelSpoolWritesEveryRecordInOrder: records handed over across
// several batches, and a last partial one, read back per depth in the
// order they were added, without the name and MPHF key.
func TestLevelSpoolWritesEveryRecordInOrder(t *testing.T) {
	const n = 3*spoolBatchRows + 5
	s := newLevelSpool(t.TempDir())
	t.Cleanup(func() { _ = s.close() })
	for i := range n {
		e := s.next()
		e.depth, e.rec = i%4, spoolTestRecord(i)
		e.name = fmt.Appendf(nil, "seg-%d", i)
		e.keyHash, e.fp, e.local = uint64(i)*0x9e3779b97f4a7c15, ^uint64(i), uint64(i)
		if err := s.commit(); err != nil {
			t.Fatalf("commit %d: %v", i, err)
		}
	}
	if err := s.finish(); err != nil {
		t.Fatal(err)
	}
	checkSpoolTestDepths(t, s, n)
}

// checkSpoolTestDepths checks a finished spool holds spoolTestRecord(i)
// at depth i%4 for every i below n, each depth in order.
func checkSpoolTestDepths(t *testing.T, s *levelSpool, n int) {
	t.Helper()
	for depth := range 4 {
		r, err := s.open(depth)
		if err != nil {
			t.Fatal(err)
		}
		i := depth
		var got spoolRecord
		for ; ; i += 4 {
			if _, err := r.next(&got, nil); errors.Is(err, io.EOF) {
				break
			} else if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(spoolTestRecord(i), got, spoolRecordCmp...); diff != "" {
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

// TestLevelSpoolMarks: every markEvery rows of a depth a mark places the
// next record in the depth's file and counts the multi-tier rows and
// stored tier slots before it, and the records from one mark to the
// next, or to the file's end, read back on their own.
func TestLevelSpoolMarks(t *testing.T) {
	const every = 64
	for _, n := range []int{1, 4 * every, 4*3*every + 9} {
		s := newLevelSpool(t.TempDir())
		t.Cleanup(func() { _ = s.close() })
		s.markEvery = every
		for i := range n {
			e := s.next()
			e.depth, e.rec = i%4, spoolTestRecord(i)
			if err := s.commit(); err != nil {
				t.Fatalf("commit %d: %v", i, err)
			}
		}
		if err := s.finish(); err != nil {
			t.Fatal(err)
		}
		r := newSectionReader()
		for depth, sf := range s.depths {
			checkDepthMarks(t, r, sf, depth, every)
		}
	}
}

// checkDepthMarks reads sf, a depth of the spool TestLevelSpoolMarks
// filled, from each of its marks.
func checkDepthMarks(t *testing.T, r *spoolReader, sf *spoolFile, depth int, every uint64) {
	t.Helper()
	f, err := os.Open(sf.path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if info, err := f.Stat(); err != nil || uint64(info.Size()) != sf.size {
		t.Fatalf("depth %d: file %v (%v), %d bytes counted", depth, info.Size(), err, sf.size)
	}
	if want := (sf.rows + every - 1) / every; uint64(len(sf.marks)) != want {
		t.Fatalf("depth %d: %d marks for %d rows, want %d", depth, len(sf.marks), sf.rows, want)
	}
	var local, multi, stored uint64
	for j, m := range sf.marks {
		if m.multi != multi || m.stored != stored {
			t.Fatalf("depth %d mark %d: %d multi-tier rows, %d stored slots before it; want %d, %d", depth, j, m.multi, m.stored, multi, stored)
		}
		end := sf.size
		if j+1 < len(sf.marks) {
			end = sf.marks[j+1].off
		}
		r.section(f, depth, m.off, end)
		var got spoolRecord
		for {
			if _, err := r.next(&got, nil); errors.Is(err, io.EOF) {
				break
			} else if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(spoolTestRecord(depth+4*int(local)), got, spoolRecordCmp...); diff != "" {
				t.Fatalf("depth %d row %d from mark %d (-added +read):\n%s", depth, local, j, diff)
			}
			if k := uint64(bits.OnesCount16(got.tierMask)); k >= 2 {
				multi++
				stored += k - 1
			}
			local++
		}
		if want := min(uint64(j+1)*every, sf.rows); local != want {
			t.Fatalf("depth %d: read up to row %d from mark %d, want %d", depth, local, j, want)
		}
	}
	if local != sf.rows || multi != sf.multi || stored != sf.stored {
		t.Fatalf("depth %d: read %d rows, %d multi-tier, %d stored slots; counted %d, %d, %d",
			depth, local, multi, stored, sf.rows, sf.multi, sf.stored)
	}
}

// TestLevelSpoolStagesSeeEveryRowInOrder: plan and keys each see every
// row once, in Add order, and the tier slots plan appends to its batch's
// buffer are the ones each record is written with.
func TestLevelSpoolStagesSeeEveryRowInOrder(t *testing.T) {
	const n = 2*spoolBatches*spoolBatchRows + 7
	s := newLevelSpool(t.TempDir())
	t.Cleanup(func() { _ = s.close() })
	var planned, keyed []uint64
	s.plan = func(e *spoolEntry, slots []uint64) ([]uint64, error) {
		planned = append(planned, e.local)
		want := spoolTestRecord(int(e.local))
		start := len(slots)
		slots = append(slots, want.tiers...)
		e.rec.tierMask, e.rec.tiers = want.tierMask, slots[start:]

		return slots, nil
	}
	s.keys = func(e *spoolEntry) error {
		keyed = append(keyed, e.local)

		return nil
	}
	for i := range n {
		e := s.next()
		e.depth, e.local, e.rec = i%4, uint64(i), spoolTestRecord(i)
		e.rec.tierMask, e.rec.tiers = 0, nil
		if err := s.commit(); err != nil {
			t.Fatalf("commit %d: %v", i, err)
		}
	}
	if err := s.finish(); err != nil {
		t.Fatal(err)
	}
	checkSpoolTestDepths(t, s, n)
	for name, seen := range map[string][]uint64{"plan": planned, "keys": keyed} {
		if len(seen) != n {
			t.Fatalf("%s saw %d rows, want %d", name, len(seen), n)
		}
		for i, local := range seen {
			if local != uint64(i) {
				t.Fatalf("%s saw row %d as its row %d", name, local, i)
			}
		}
	}
	assertNoSpoolStages(t)
}

// TestLevelSpoolFinishWaitsForEveryStage: rows still at a stage when the
// spool finishes are all seen first, so the plans and the MPHF keys are
// complete once finish returns.
func TestLevelSpoolFinishWaitsForEveryStage(t *testing.T) {
	const n = 2*spoolBatchRows + 5
	s := newLevelSpool(t.TempDir())
	t.Cleanup(func() { _ = s.close() })
	gate := make(chan struct{})
	time.AfterFunc(20*time.Millisecond, func() { close(gate) })
	keyed := 0
	s.keys = func(*spoolEntry) error {
		<-gate
		keyed++

		return nil
	}
	for i := range n {
		e := s.next()
		e.depth, e.rec = i%4, spoolTestRecord(i)
		if err := s.commit(); err != nil {
			t.Fatalf("commit %d: %v", i, err)
		}
	}
	if err := s.finish(); err != nil {
		t.Fatal(err)
	}
	if keyed != n {
		t.Errorf("keys saw %d rows before finish returned, want %d", keyed, n)
	}
	checkSpoolTestDepths(t, s, n)
	assertNoSpoolStages(t)
}

var errInjectedStage = errors.New("injected stage error")

// TestLevelSpoolStageErrorStopsTheSpool: an error in any stage reaches
// the builder, through commit then finish, or through close when the
// spool stops first; the stages after plan never see the batch plan
// failed on; and no stage goroutine outlives the spool.
func TestLevelSpoolStageErrorStopsTheSpool(t *testing.T) {
	const failRow = spoolBatchRows + 3
	for _, stage := range []string{"plan", "keys", "write"} {
		for _, stopEarly := range []bool{false, true} {
			dir := t.TempDir()
			if stage == "write" {
				dir = filepath.Join(dir, "missing")
			}
			s := newLevelSpool(dir)
			keyed := 0
			s.plan = func(e *spoolEntry, slots []uint64) ([]uint64, error) {
				if stage == "plan" && e.local == failRow {
					return slots, errInjectedStage
				}

				return slots, nil
			}
			s.keys = func(e *spoolEntry) error {
				if stage == "keys" && e.local == failRow {
					return errInjectedStage
				}
				keyed++

				return nil
			}
			rows := 20 * spoolBatchRows
			if stopEarly {
				rows = failRow + 10
			}
			var commitErr error
			for i := 0; commitErr == nil && i < rows; i++ {
				e := s.next()
				e.local, e.rec = uint64(i), spoolTestRecord(i)
				commitErr = s.commit()
			}
			if stopEarly && commitErr != nil {
				t.Fatalf("%s: commit failed before the spool stopped: %v", stage, commitErr)
			}
			if !stopEarly {
				if commitErr == nil {
					t.Fatalf("%s: commit never returned the stage's error", stage)
				}
				if err := s.finish(); !errors.Is(err, commitErr) {
					t.Errorf("%s: finish = %v, want the commit error %v", stage, err, commitErr)
				}
			}
			closeErr := s.close()
			switch {
			case closeErr == nil:
				t.Errorf("%s, stop early %v: close = nil, want the stage's error", stage, stopEarly)
			case commitErr != nil && !errors.Is(closeErr, commitErr):
				t.Errorf("%s: close = %v, want the commit error %v", stage, closeErr, commitErr)
			case stage != "write" && !errors.Is(closeErr, errInjectedStage):
				t.Errorf("%s, stop early %v: close = %v, want the injected error", stage, stopEarly, closeErr)
			}
			if stage == "plan" && keyed > spoolBatchRows {
				t.Errorf("stop early %v: keys saw %d rows, plan failed on row %d", stopEarly, keyed, failRow)
			}
			assertNoSpoolStages(t)
		}
	}
}

// assertNoSpoolStages fails the test if a level spool's stage goroutine
// is still running: stop waits for every stage to return.
func assertNoSpoolStages(t *testing.T) {
	t.Helper()
	buf := make([]byte, 1<<20)
	if stacks := buf[:runtime.Stack(buf, true)]; bytes.Contains(stacks, []byte("extsort.(*levelSpool)")) {
		t.Errorf("a level spool stage outlived its spool:\n%s", stacks)
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
		if _, err := r.next(&rec, nil); err != nil {
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
				if diff := cmp.Diff(want, got, spoolRecordCmp...); diff != "" {
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
		segID: math.MaxUint32, tierMask: math.MaxUint16,
	}
	for range 2 * MaxTiers {
		big.tiers = append(big.tiers, math.MaxUint64)
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
			if diff := cmp.Diff(want, got, spoolRecordCmp...); diff != "" {
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
			if diff := cmp.Diff(want[:whole], got, spoolRecordCmp...); whole > 0 && diff != "" {
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
	var vals []uint64
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		r, err := s.open(0)
		if err != nil {
			b.Fatal(err)
		}
		read := 0
		for ; ; read++ {
			if vals, err = r.next(&rec, vals[:0]); errors.Is(err, io.EOF) {
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
