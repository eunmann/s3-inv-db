package jobs

import (
	"sync"
	"time"

	"github.com/eunmann/s3-inv-db/pkg/extsort/events"
)

type Recorder struct {
	bus      *events.Bus
	report   func(Update)
	sub      *events.Subscription
	stages   []StageRecord
	mu       sync.Mutex
	closed   bool
	wg       sync.WaitGroup
	nowFn    func() time.Time
	closedCh chan struct{}
}

func NewRecorder(report func(Update)) *Recorder {
	r := &Recorder{
		bus:      events.NewBus(),
		report:   report,
		nowFn:    time.Now,
		closedCh: make(chan struct{}),
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
	if r.closed {
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
	if r.closed {
		r.mu.Unlock()

		return
	}
	r.closed = true
	close(r.closedCh)
	r.mu.Unlock()

	r.sub.Cancel()
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
	if done > 0 {
		cur := &r.stages[len(r.stages)-1]
		cur.Bytes = uint64(done)
	}
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
	if r.closed {
		return
	}
	var changed bool
	switch ev.Type {
	case events.EvtStageEnd:
		st, ok := ev.Payload.(events.StageTiming)
		if !ok {
			return
		}
		changed = r.enrichOnStageEndLocked(ev, st)
	case events.EvtBatchCommitted:
		bc, ok := ev.Payload.(events.BatchCommitted)
		if !ok {
			return
		}
		changed = r.accumulateBatchLocked(ev.Stage, bc)
	// EvtSpillCompleted is deliberately not consumed: SpillCompleted.Rows
	// counts post-aggregation prefix-row writes (an internal pipeline
	// metric, can be ~10× the inventory-row count for deep keys). Adding
	// it onto the user-facing Rows would inflate "downloading: N rows"
	// past the actual object count as soon as a real inventory spills.
	// The authoritative final count arrives on EvtStageEnd.
	default:
		return
	}
	if !changed {
		return
	}
	r.report(Update{Stages: snapshotStages(r.stages)})
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
		s.EndedAt = end
		s.Duration = st.Duration
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
