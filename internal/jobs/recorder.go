package jobs

import (
	"sync"
	"time"

	"github.com/eunmann/s3-inv-db/pkg/extsort/events"
)

type Recorder struct {
	bus         *events.Bus
	report      func(Update)
	sub         *events.Subscription
	stages      []StageRecord
	spillCount  int
	spillBytes  int64
	mergeRounds int
	mergeBytes  int64
	mu          sync.Mutex
	closing     bool
	wg          sync.WaitGroup
	nowFn       func() time.Time
}

func NewRecorder(report func(Update)) *Recorder {
	r := &Recorder{
		bus:    events.NewBus(),
		report: report,
		nowFn:  time.Now,
	}
	r.sub = r.bus.Subscribe(256)
	r.wg.Add(1)
	go r.drain()

	return r
}

func (r *Recorder) Bus() *events.Bus { return r.bus }

func (r *Recorder) OnProgress(stage string, done, total int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closing {
		return
	}
	r.applyStageLocked(stage, done, total)
	r.report(Update{
		Stage:      stage,
		StageDone:  done,
		StageTotal: total,
		Stages:     snapshotStages(r.stages),
	})
}

func (r *Recorder) Close() {
	r.mu.Lock()
	if r.closing {
		r.mu.Unlock()

		return
	}
	// closing blocks new OnProgress appends, but the drain goroutine
	// keeps processing already-buffered events so the final stage-end
	// + diagnostic events published just before the pipeline returned
	// aren't dropped.
	r.closing = true
	r.mu.Unlock()

	// bus.Close stops further publishes and closes the subscription
	// channel; drain consumes the buffered tail, then ranges to exit.
	r.bus.Close()
	r.wg.Wait()
}

func (r *Recorder) Snapshot() []StageRecord {
	r.mu.Lock()
	defer r.mu.Unlock()

	return snapshotStages(r.stages)
}

func (r *Recorder) applyStageLocked(stage string, done, total int64) {
	now := r.nowFn()
	if stage == "" {
		return
	}
	if len(r.stages) == 0 || r.stages[len(r.stages)-1].Name != stage {
		// Fallback timing; EvtStageEnd, if it arrives, overwrites with pipeline-measured values.
		if n := len(r.stages); n > 0 && r.stages[n-1].InProgress() {
			prev := &r.stages[n-1]
			prev.EndedAt = now
			prev.Duration = now.Sub(prev.StartedAt)
		}
		r.stages = append(r.stages, StageRecord{
			Name:      stage,
			StartedAt: now,
		})
	}
	// done/total are the current stage's progress counter (chunk index,
	// merged-run count) — they flow to the UI via Update.StageDone/Total,
	// NOT the stage's byte/row totals, which come from BatchCommitted +
	// StageEnd events. Writing done here would clobber the real byte sum.
	_ = done
	_ = total
}

func (r *Recorder) drain() {
	defer r.wg.Done()
	for ev := range r.sub.C {
		r.handleEvent(ev)
	}
}

func (r *Recorder) handleEvent(ev events.Event) {
	r.mu.Lock()
	defer r.mu.Unlock()
	// No closing guard: Close() relies on the drain goroutine processing
	// the buffered tail of events after closing is set. The report
	// callback stays safe because Close() is called from inside the job's
	// work function, before the scheduler writes the terminal state.
	var stagesChanged, diagChanged bool
	switch ev.Type {
	case events.EvtStageEnd:
		st, ok := ev.Payload.(events.StageTiming)
		if !ok {
			return
		}
		stagesChanged = r.enrichOnStageEndLocked(ev, st)
		if st.Stage == events.StageMerge && st.BytesWritten > 0 {
			r.mergeBytes = st.BytesWritten
			diagChanged = true
		}
	case events.EvtBatchCommitted:
		bc, ok := ev.Payload.(events.BatchCommitted)
		if !ok {
			return
		}
		stagesChanged = r.accumulateBatchLocked(ev.Stage, bc)
	case events.EvtSpillCompleted:
		// SpillCompleted.Rows is post-aggregation prefix-row count and
		// would inflate the user-facing stage Rows; keep counts/bytes as
		// diagnostics only.
		sc, ok := ev.Payload.(events.SpillCompleted)
		if !ok {
			return
		}
		r.spillCount++
		r.spillBytes += sc.Bytes
		diagChanged = true
	case events.EvtRoundCompleted:
		r.mergeRounds++
		diagChanged = true
	default:
		return
	}
	if !stagesChanged && !diagChanged {
		return
	}
	u := Update{
		SpillCount:  r.spillCount,
		SpillBytes:  r.spillBytes,
		MergeRounds: r.mergeRounds,
		MergeBytes:  r.mergeBytes,
	}
	if stagesChanged {
		u.Stages = snapshotStages(r.stages)
	}
	r.report(u)
}

func (r *Recorder) enrichOnStageEndLocked(ev events.Event, st events.StageTiming) bool {
	target := jobStageName(st.Stage)
	if target == "" {
		return false
	}
	end := ev.Time
	if end.IsZero() {
		end = r.nowFn()
	}
	for i := len(r.stages) - 1; i >= 0; i-- {
		if r.stages[i].Name != target {
			continue
		}
		s := &r.stages[i]
		// Only stamp timing when the stage is still open. StageMerge's
		// end-event fires after the pipeline has already moved on to the
		// "finalizing" sub-stage, which closed "building" via the
		// OnProgress transition. Overwriting here would extend building's
		// duration through finalize, double-counting it in both rows.
		// Rows/Bytes (prefix count, merge bytes) are still worth
		// attaching either way.
		if s.InProgress() {
			s.EndedAt = end
			s.Duration = st.Duration
		}
		if st.Bytes > 0 {
			s.Bytes = st.Bytes
		}
		if st.Rows > 0 {
			s.Rows = st.Rows
		}

		return true
	}

	return false
}

func (r *Recorder) accumulateBatchLocked(stage events.Stage, bc events.BatchCommitted) bool {
	target := jobStageName(stage)
	if target == "" {
		return false
	}
	idx := r.findInProgressIndexLocked(target)
	if idx < 0 {
		return false
	}
	r.stages[idx].Rows += bc.Rows
	r.stages[idx].Bytes += bc.Bytes

	return true
}

func (r *Recorder) findInProgressIndexLocked(name string) int {
	for i := len(r.stages) - 1; i >= 0; i-- {
		if r.stages[i].Name == name && r.stages[i].InProgress() {
			return i
		}
	}

	return -1
}

func jobStageName(s events.Stage) string {
	switch s {
	case events.StageDownload, events.StageParse, events.StageAggregator, events.StageSpill:
		return "downloading"
	case events.StageMerge, events.StageIndexBuild:
		return "building"
	case events.StagePipeline:
		return ""
	}

	return ""
}

func snapshotStages(s []StageRecord) []StageRecord {
	if len(s) == 0 {
		return nil
	}
	out := make([]StageRecord, len(s))
	copy(out, s)

	return out
}
