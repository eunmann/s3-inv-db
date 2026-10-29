package format

import (
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
