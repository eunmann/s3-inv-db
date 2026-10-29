package extsort

import (
	"sync"
	"time"

	"github.com/eunmann/s3-inv-db/pkg/extsort/events"
	"github.com/eunmann/s3-inv-db/pkg/humanfmt"
	"github.com/eunmann/s3-inv-db/pkg/sysmem"
	"github.com/rs/zerolog"
)

// finalizingStage is the pipeline stage Finalize's steps belong to.
const finalizingStage = "finalizing"

// stepWriteLevels is Finalize's first step: replaying the spooled rows
// depth by depth into the index files. It counts rows.
const stepWriteLevels = "Ordering rows by depth"

// stepReportInterval is the least time between two forwarded reports of
// one step; a step change and a step's completion always go through.
const stepReportInterval = 250 * time.Millisecond

// finalizeSteps follows Finalize's steps. It forwards each step's
// progress as the finalizing stage's, at most every stepReportInterval,
// and logs each step at info level when the next begins (or end is
// called) with its duration and how much it waited on storage, so a
// slow finalize shows which step was slow and why.
type finalizeSteps struct {
	log        *zerolog.Logger
	onProgress events.ProgressFunc

	mu        sync.Mutex
	step      string
	started   time.Time
	pressure  sysmem.Pressure
	reported  time.Time
	lastDone  int64
	lastTotal int64
}

func newFinalizeSteps(log *zerolog.Logger, onProgress events.ProgressFunc) *finalizeSteps {
	return &finalizeSteps{log: log, onProgress: onProgress}
}

// report is a format.StepProgress. It may be called from several
// goroutines.
func (s *finalizeSteps) report(step string, done, total int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	if step != s.step {
		s.endLocked(now)
		s.step, s.started, s.pressure = step, now, sysmem.ReadPressure()
	} else if now.Sub(s.reported) < stepReportInterval && done < total {
		s.lastDone, s.lastTotal = done, total

		return
	}
	s.reported, s.lastDone, s.lastTotal = now, done, total
	if s.onProgress != nil {
		s.onProgress(events.Progress{Stage: finalizingStage, Step: step, Done: done, Total: total})
	}
}

// end logs the current step, if any.
func (s *finalizeSteps) end() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.endLocked(time.Now())
	s.step = ""
}

func (s *finalizeSteps) endLocked(now time.Time) {
	if s.step == "" {
		return
	}
	d := now.Sub(s.started)
	logPressure(s.log.Info(), sysmem.ReadPressure().Since(s.pressure)).
		Str("step", s.step).
		Int64("done", s.lastDone).
		Int64("total", s.lastTotal).
		Str("duration", humanfmt.Duration(d)).
		Dur("duration_ms", d).
		Msg("finalize step complete")
}

// logPressure adds a phase's storage-wait counters to e (see
// sysmem.Pressure); counters the platform does not expose are left out.
func logPressure(e *zerolog.Event, p sysmem.Pressure) *zerolog.Event {
	e = e.Int64("major_faults", p.MajorFaults)
	if p.ReadBytes > 0 || p.WrittenBytes > 0 {
		e = e.Str("storage_read", humanfmt.Bytes(p.ReadBytes)).
			Str("storage_written", humanfmt.Bytes(p.WrittenBytes))
	}
	if p.FileRefaults > 0 {
		e = e.Int64("file_refaults", p.FileRefaults)
	}
	if p.IOStallFull > 0 {
		e = e.Str("io_stall_full", humanfmt.Duration(p.IOStallFull))
	}

	return e
}
