package format

import (
	"math"
	"slices"
	"sync"
	"testing"
)

// stepRecorder records a StepProgress's reports.
type stepRecorder struct {
	mu   sync.Mutex
	done []int64
}

func (r *stepRecorder) report(_ string, done, _ int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.done = append(r.done, done)
}

// TestBBHashScanEstimate checks that the build's progress total is
// close to the key reads the build actually makes.
func TestBBHashScanEstimate(t *testing.T) {
	const n = 1 << 20
	var rec stepRecorder
	estimate := bbhashScanEstimate(n)
	scan := newStepCounter(rec.report, StepBuildHash, estimate)
	if _, err := newBBHashWith(randomKeys(n, 7), heapKeys, func([]uint64) ([]bbFallback, error) { return nil, nil }, scan); err != nil {
		t.Fatalf("build: %v", err)
	}
	actual := scan.done.Load()
	if diff := math.Abs(float64(actual-estimate)) / float64(actual); diff > 0.03 {
		t.Errorf("estimate %d is %.1f%% off the %d keys read", estimate, diff*100, actual)
	}
}

// TestNextLevelKeysAcrossBlocks checks nextLevelKeys when each worker's
// chunk spans several progress blocks: it must hand on exactly the
// colliding keys and mark exactly them on the next level.
func TestNextLevelKeysAcrossBlocks(t *testing.T) {
	n := mphfWorkerCount(math.MaxInt32)*progressBlock*2 + 123
	keys := randomKeys(n, 7)
	words := levelWords(n)
	var rec stepRecorder
	scan := newStepCounter(rec.report, StepBuildHash, int64(2*n))
	marks := make([]uint64, 2*words)
	markLevel(keys, 0, marks, scan)
	set, placed := placedBits(marks)
	var want []uint64
	for _, k := range keys {
		i := bbPosition(k, 0, words)
		if set[i/64]&(1<<(i%64)) == 0 {
			want = append(want, k)
		}
	}
	if len(want) != n-placed {
		t.Fatalf("%d colliding keys, but %d keys minus %d placed bits is %d", len(want), n, placed, n-placed)
	}
	nextWords := levelWords(len(want))
	gotMarks := make([]uint64, 2*nextWords)
	got := make([]uint64, len(want))
	nextLevelKeys(keys, 0, set, got, gotMarks, make([]uint64, mphfWorkerCount(n)*collectBlock), scan)

	slices.Sort(got)
	if !slices.Equal(got, slices.Sorted(slices.Values(want))) {
		t.Fatalf("nextLevelKeys handed on %d keys that differ from the %d colliding keys", len(got), len(want))
	}
	wantMarks := make([]uint64, 2*nextWords)
	markKeys(want, 1, wantMarks)
	if !slices.Equal(gotMarks, wantMarks) {
		t.Fatal("next level's marks differ from marking the colliding keys")
	}
	if d := scan.done.Load(); d != int64(2*n) {
		t.Errorf("counted %d key reads, want %d (two passes)", d, 2*n)
	}
}

// TestStepCounterNil checks that a nil counter, a build without a
// progress callback, counts nothing and does not panic.
func TestStepCounterNil(t *testing.T) {
	c := newStepCounter(nil, "step", 10)
	if c != nil {
		t.Fatalf("newStepCounter(nil, ...) = %v, want nil", c)
	}
	c.add(5)
	c.finish()
}

// TestStepCounterReports checks that a counter reports 0 on start, the
// total on finish, and nothing above the total in between.
func TestStepCounterReports(t *testing.T) {
	var rec stepRecorder
	c := newStepCounter(rec.report, "step", 10)
	c.last.Store(0) // let the next add report
	c.add(25)
	c.finish()
	if want := []int64{0, 10, 10}; !slices.Equal(rec.done, want) {
		t.Errorf("reported %v, want %v", rec.done, want)
	}
}
