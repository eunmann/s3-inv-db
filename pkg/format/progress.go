package format

import (
	"sync/atomic"
	"time"
)

// StepProgress receives a build step's progress: done of total, in the
// step's own unit (rows, keys, slots, bytes). It may be called from
// several goroutines at once.
type StepProgress func(step string, done, total int64)

// progressBlock is how many items a worker processes between progress
// reports: the report is off the per-item path, and a block of keys
// takes well under a millisecond.
const progressBlock = 1 << 17

// progressInterval is the least time between two reports of a step, so
// a step's progress reaches the UI a few times a second however many
// workers report it.
const progressInterval = 250 * time.Millisecond

// stepCounter counts one step's progress across workers and reports it
// at most every progressInterval. A nil counter counts nothing, so a
// build without a progress callback passes nil.
type stepCounter struct {
	report StepProgress
	step   string
	total  int64
	done   atomic.Int64
	last   atomic.Int64 // unix nanos of the last report
}

// newStepCounter starts step and reports it at 0 of total. It returns
// nil when report is nil.
func newStepCounter(report StepProgress, step string, total int64) *stepCounter {
	if report == nil {
		return nil
	}
	c := &stepCounter{report: report, step: step, total: total}
	c.last.Store(time.Now().UnixNano())
	report(step, 0, total)

	return c
}

// add counts n more items done, reporting when progressInterval has
// passed since the last report. Done is capped at total, which may be
// an estimate.
func (c *stepCounter) add(n int) {
	if c == nil {
		return
	}
	c.done.Add(int64(n))
	now := time.Now().UnixNano()
	last := c.last.Load()
	if now-last < int64(progressInterval) || !c.last.CompareAndSwap(last, now) {
		return
	}
	// Read done after winning the report, not before: a worker delayed
	// between its add and its report would otherwise send a count older
	// than one already sent.
	c.report(c.step, min(c.done.Load(), c.total), c.total)
}

// finish reports the step complete.
func (c *stepCounter) finish() {
	if c == nil {
		return
	}
	c.report(c.step, c.total, c.total)
}
