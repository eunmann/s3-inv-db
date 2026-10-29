package extsort

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"testing"
	"time"
	"unsafe"
)

var errInjected = errors.New("injected read failure")

// failingSource yields its rows, then fails with errInjected.
type failingSource struct {
	rows   []PrefixRow
	next   int
	closed bool
}

func (f *failingSource) readNext(row *PrefixRow) (int, error) {
	if f.next == len(f.rows) {
		return 0, errInjected
	}
	*row = f.rows[f.next]
	f.next++

	return 0, nil
}

func (f *failingSource) Count() uint64     { return uint64(len(f.rows)) + 1 }
func (f *failingSource) ReadCount() uint64 { return uint64(f.next) }
func (f *failingSource) Close() error {
	f.closed = true

	return nil
}

// checkNoGoroutineLeak fails t unless the goroutine count returns to
// before. Close waits for the decoders, but one that has signalled its
// WaitGroup may not have exited yet.
func checkNoGoroutineLeak(t *testing.T, before int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for runtime.NumGoroutine() > before {
		if time.Now().After(deadline) {
			t.Fatalf("%d goroutines, %d before the merge", runtime.NumGoroutine(), before)
		}
		time.Sleep(time.Millisecond)
	}
}

// aheadTestRuns returns k interleaved runs of perRun keys each.
func aheadTestRuns(k, perRun int) [][]PrefixRow {
	runs := make([][]PrefixRow, k)
	for r := range runs {
		keys := make([]string, perRun)
		for j := range keys {
			keys[j] = fmt.Sprintf("bucket/p%03d/k%06d/", j%7, j*k+r)
		}
		runs[r] = sortedTestRun(keys, uint64(r)*1000)
	}

	return runs
}

// TestAheadMergeSurfacesReadError: a run that fails partway fails the
// merge at the row the iterator fails at, after the same rows, and
// closing stops every decoder.
func TestAheadMergeSurfacesReadError(t *testing.T) {
	runs := aheadTestRuns(5, 40)
	sources := func() []runSource {
		s := memSources(runs)
		s[2] = &failingSource{rows: runs[2][:17]}

		return s
	}
	want, wantErr := drainMerge(sources())
	if !errors.Is(wantErr, errInjected) {
		t.Fatalf("iterator: err = %v, want errInjected", wantErr)
	}
	for _, batchRows := range []int{1, 4, aheadBatchRows} {
		t.Run(fmt.Sprintf("batch=%d", batchRows), func(t *testing.T) {
			before := runtime.NumGoroutine()
			got, err := drainAhead(sources(), batchRows)
			if !errors.Is(err, errInjected) {
				t.Fatalf("err = %v, want errInjected", err)
			}
			if !slices.Equal(got, want) {
				t.Fatalf("rows before the error: %s", firstRowDiff(got, want))
			}
			checkNoGoroutineLeak(t, before)
		})
	}
}

// TestAheadMergeSeedError: a run that fails on its first row fails the
// merge's start, which closes every source and stops every decoder.
func TestAheadMergeSeedError(t *testing.T) {
	before := runtime.NumGoroutine()
	sources := memSources(aheadTestRuns(4, 10))
	failing := &failingSource{}
	sources[3] = failing
	if _, err := newAheadMerge(sources, 2); !errors.Is(err, errInjected) {
		t.Fatalf("err = %v, want errInjected", err)
	}
	if !failing.closed {
		t.Fatal("the failing source was not closed")
	}
	for i, s := range sources[:3] {
		if run, ok := s.(*memRun); !ok || run.chunks != nil {
			t.Errorf("source %d was not closed", i)
		}
	}
	checkNoGoroutineLeak(t, before)
}

// TestAheadMergeCorruptRunFile: a run file whose records end before its
// header's count fails the merge rather than ending it early.
func TestAheadMergeCorruptRunFile(t *testing.T) {
	runs := aheadTestRuns(3, 50)
	path := filepath.Join(t.TempDir(), "short.crun")
	writeTestRunFile(t, path, runs[1], rand.New(rand.NewPCG(278, 1)))
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	var count [8]byte
	binary.LittleEndian.PutUint64(count[:], uint64(len(runs[1])+3))
	if _, err := f.WriteAt(count[:], compressedRunCountOffset); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	r, err := OpenCompressedRunFile(path, 4096)
	if err != nil {
		t.Fatal(err)
	}
	sources := memSources(runs)
	sources[1] = r
	if _, err := drainAhead(sources, 8); !errors.Is(err, errRunRecordCorrupt) {
		t.Fatalf("err = %v, want errRunRecordCorrupt", err)
	}
}

// TestAheadMergeCloseMidStream: closing a merge partway stops every
// decoder, though they have batches nobody will read, and the merge
// fails from then on instead of waiting for them.
func TestAheadMergeCloseMidStream(t *testing.T) {
	runs := aheadTestRuns(16, 2000)
	before := runtime.NumGoroutine()
	m, err := newAheadMerge(memSources(runs), 64)
	if err != nil {
		t.Fatal(err)
	}
	for range 100 {
		if _, err := m.Next(); err != nil {
			t.Fatal(err)
		}
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	if err := m.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if _, err := m.Next(); !errors.Is(err, errMergeClosed) {
		t.Fatalf("Next after Close = %v, want errMergeClosed", err)
	}
	checkNoGoroutineLeak(t, before)
}

// TestAheadMergeRemaining: Remaining starts at every source's rows and
// counts down by the rows each merged row took, to zero at the end.
func TestAheadMergeRemaining(t *testing.T) {
	runs := [][]PrefixRow{
		sortedTestRun([]string{"a/", "b/", "c/"}, 1),
		sortedTestRun([]string{"b/", "d/"}, 2),
		nil,
	}
	m, err := newAheadMerge(memSources(runs), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	for _, want := range []uint64{5, 4, 2, 1, 0} {
		if got := m.Remaining(); got != want {
			t.Fatalf("Remaining = %d, want %d", got, want)
		}
		if _, err := m.Next(); err != nil && want > 0 {
			t.Fatal(err)
		}
	}
	if _, err := m.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("err = %v, want io.EOF", err)
	}
}

// TestAheadRunHeapBytesCoversReadAhead: the read-ahead the final merge's
// memory budget counts per run covers its batches' rows and the arena
// chunks their prefixes may hold.
func TestAheadRunHeapBytesCoversReadAhead(t *testing.T) {
	rowBytes := int(unsafe.Sizeof(PrefixRow{}) + unsafe.Sizeof(int(0)))
	// A batch ends at the first row past aheadBatchPrefixBytes, a key of
	// up to 1 KiB, and its prefixes span at most two chunks more.
	prefixBytes := aheadBatchPrefixBytes + 1024 + 2*prefixArenaChunk
	if need := aheadBatches * (aheadBatchRows*rowBytes + prefixBytes); need > aheadRunHeapBytes {
		t.Fatalf("a run's read-ahead holds up to %d bytes, aheadRunHeapBytes is %d", need, aheadRunHeapBytes)
	}
}

// TestFinalFanInLeavesRoomForMemoryRuns: the memory runs' read-ahead
// comes out of the open-run budget a sized final fan-in fills.
func TestFinalFanInLeavesRoomForMemoryRuns(t *testing.T) {
	const limit, memRuns = 256 << 20, 8
	fanIn := finalFanInFor(limit, 16, memRuns)
	if fanIn >= finalFanInFor(limit, 16, 0) {
		t.Fatalf("fan-in %d with %d memory runs, no fewer than without", fanIn, memRuns)
	}
	budget, _ := openRunBytes(limit)
	if held := int64(fanIn)*(openRunHeapBytes+aheadRunHeapBytes) + memRuns*aheadRunHeapBytes; held > budget {
		t.Fatalf("final merge holds %d bytes, budget %d", held, budget)
	}
}
