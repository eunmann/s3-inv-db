package jobs_test

import (
	"sync"
	"testing"
	"time"

	"github.com/eunmann/s3-inv-db/internal/jobs"
	"github.com/eunmann/s3-inv-db/pkg/extsort/events"
)

const stageDownloading = "downloading"

type captureReporter struct {
	mu      sync.Mutex
	updates []jobs.Update
}

func (c *captureReporter) report(u jobs.Update) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.updates = append(c.updates, u)
}

func (c *captureReporter) last() jobs.Update {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.updates) == 0 {
		return jobs.Update{}
	}

	return c.updates[len(c.updates)-1]
}

func (c *captureReporter) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return len(c.updates)
}

func TestRecorder_StageTransitionsAppendRecords(t *testing.T) {
	rep := &captureReporter{}
	rec := jobs.NewRecorder(rep.report)
	defer rec.Close()

	rec.OnProgress("preparing", 0, 0)
	rec.OnProgress(stageDownloading, 0, 0)
	rec.OnProgress("building", 0, 0)
	rec.OnProgress("done", 0, 0)

	stages := rec.Snapshot()
	if len(stages) != 4 {
		t.Fatalf("len(stages) = %d, want 4 (preparing, downloading, building, done): %+v", len(stages), stages)
	}
	for i, name := range []string{"preparing", stageDownloading, "building", "done"} {
		if stages[i].Name != name {
			t.Errorf("stages[%d].Name = %q, want %q", i, stages[i].Name, name)
		}
	}
	for i := range 3 {
		if stages[i].InProgress() {
			t.Errorf("stages[%d] (%s) still in-progress after later transition", i, stages[i].Name)
		}
		if stages[i].Duration <= 0 {
			t.Errorf("stages[%d] (%s) Duration not stamped: %v", i, stages[i].Name, stages[i].Duration)
		}
	}
	if !stages[3].InProgress() {
		t.Errorf("final stage should still be in-progress until close")
	}
}

func TestRecorder_StageEndEventOverridesDurationAndBytes(t *testing.T) {
	rep := &captureReporter{}
	rec := jobs.NewRecorder(rep.report)
	defer rec.Close()

	rec.OnProgress(stageDownloading, 0, 0)
	want := events.StageTiming{
		Stage:    events.StageDownload,
		Duration: 1234 * time.Millisecond,
		Rows:     999,
		Bytes:    8675309,
	}
	rec.Bus().Publish(events.Event{
		Stage:   events.StagePipeline,
		Type:    events.EvtStageEnd,
		Payload: want,
		Time:    time.Now(),
	})

	if !waitFor(50*time.Millisecond, func() bool {
		for _, s := range rec.Snapshot() {
			if s.Name == stageDownloading && s.Duration == want.Duration {
				return true
			}
		}

		return false
	}) {
		t.Fatalf("drainer did not apply EvtStageEnd in time: %+v", rec.Snapshot())
	}
	stages := rec.Snapshot()
	dl := stages[0]
	if dl.Name != stageDownloading {
		t.Fatalf("unexpected stage[0]: %+v", dl)
	}
	if dl.Duration != want.Duration || dl.Bytes != want.Bytes || dl.Rows != want.Rows {
		t.Errorf("stage[0] = %+v, want Duration=%v Bytes=%d Rows=%d", dl, want.Duration, want.Bytes, want.Rows)
	}
	if dl.InProgress() {
		t.Error("stage[0] should be closed after EvtStageEnd")
	}
}

func TestRecorder_MergeStageEndAttachesToBuilding(t *testing.T) {
	rep := &captureReporter{}
	rec := jobs.NewRecorder(rep.report)
	defer rec.Close()

	rec.OnProgress(stageDownloading, 0, 0)
	rec.OnProgress("building", 0, 0)
	rec.Bus().Publish(events.Event{
		Stage: events.StagePipeline,
		Type:  events.EvtStageEnd,
		Payload: events.StageTiming{
			Stage:    events.StageMerge,
			Duration: 5 * time.Second,
			Rows:     42,
		},
		Time: time.Now(),
	})

	if !waitFor(50*time.Millisecond, func() bool {
		for _, s := range rec.Snapshot() {
			if s.Name == "building" && s.Duration == 5*time.Second {
				return true
			}
		}

		return false
	}) {
		t.Fatalf("merge end did not attach to building stage: %+v", rec.Snapshot())
	}
}

func TestRecorder_ReportFiresOnEveryProgressAndEvent(t *testing.T) {
	rep := &captureReporter{}
	rec := jobs.NewRecorder(rep.report)
	defer rec.Close()

	rec.OnProgress(stageDownloading, 0, 0)
	rec.OnProgress(stageDownloading, 3, 10)
	rec.Bus().Publish(events.Event{
		Stage: events.StagePipeline,
		Type:  events.EvtStageEnd,
		Payload: events.StageTiming{
			Stage:    events.StageDownload,
			Duration: time.Second,
			Bytes:    100,
		},
		Time: time.Now(),
	})

	if !waitFor(50*time.Millisecond, func() bool { return rep.count() >= 3 }) {
		t.Fatalf("expected >= 3 reports, got %d", rep.count())
	}
	last := rep.last()
	if len(last.Stages) != 1 || last.Stages[0].Name != stageDownloading {
		t.Errorf("last update stages = %+v", last.Stages)
	}
}

func TestRecorder_CloseIsIdempotentAndStopsDrainer(t *testing.T) {
	rep := &captureReporter{}
	rec := jobs.NewRecorder(rep.report)

	rec.OnProgress(stageDownloading, 0, 0)
	rec.Close()
	rec.Close()

	defer func() {
		if r := recover(); r != nil {
			t.Errorf("Bus().Publish after Close panicked: %v", r)
		}
	}()
	rec.Bus().Publish(events.Event{
		Stage: events.StagePipeline,
		Type:  events.EvtStageEnd,
	})
}

func TestRecorder_OnProgressAfterCloseIsNoop(t *testing.T) {
	rep := &captureReporter{}
	rec := jobs.NewRecorder(rep.report)
	rec.OnProgress(stageDownloading, 0, 0)
	before := rep.count()
	rec.Close()
	rec.OnProgress("building", 0, 0)
	if rep.count() != before {
		t.Errorf("post-close OnProgress should not emit; before=%d after=%d", before, rep.count())
	}
}

func TestRecorder_SpillCompletedDoesNotInflateRows(t *testing.T) {
	// SpillCompleted carries the post-aggregation prefix-row count from
	// an internal spill — adding it to the user-facing Rows would
	// inflate "downloading: N rows" past the actual inventory-object
	// count as soon as a real (spilling) inventory ran. The recorder
	// deliberately ignores these events; the authoritative final
	// objects count comes in via EvtStageEnd.
	rep := &captureReporter{}
	rec := jobs.NewRecorder(rep.report)
	defer rec.Close()

	rec.OnProgress(stageDownloading, 0, 0)
	rec.Bus().Publish(events.Event{
		Stage: events.StageSpill,
		Type:  events.EvtSpillCompleted,
		Payload: events.SpillCompleted{
			WorkerID: 1, Rows: 500, Bytes: 4096, Duration: 10 * time.Millisecond,
		},
		Time: time.Now(),
	})
	rec.Bus().Publish(events.Event{
		Stage: events.StageSpill,
		Type:  events.EvtSpillCompleted,
		Payload: events.SpillCompleted{
			WorkerID: 2, Rows: 250, Bytes: 2048,
		},
		Time: time.Now(),
	})

	// Give the drainer a moment so a stale +=Rows path would have
	// surfaced; verify Rows stays at 0.
	time.Sleep(50 * time.Millisecond)
	stage := rec.Snapshot()[0]
	if stage.Name != stageDownloading {
		t.Errorf("stage[0].Name = %q, want %q", stage.Name, stageDownloading)
	}
	if stage.Rows != 0 {
		t.Errorf("Rows after two spills = %d, want 0 (spills do not contribute to Rows)", stage.Rows)
	}
}

func TestRecorder_BatchCommittedAccumulatesIntoStage(t *testing.T) {
	rep := &captureReporter{}
	rec := jobs.NewRecorder(rep.report)
	defer rec.Close()

	rec.OnProgress(stageDownloading, 0, 0)
	for range 3 {
		rec.Bus().Publish(events.Event{
			Stage: events.StageParse,
			Type:  events.EvtBatchCommitted,
			Payload: events.BatchCommitted{
				Rows:  100,
				Bytes: 4096,
			},
			Time: time.Now(),
		})
	}
	if !waitFor(50*time.Millisecond, func() bool {
		s := rec.Snapshot()

		return len(s) == 1 && s[0].Rows >= 300
	}) {
		t.Fatalf("batch_committed did not accumulate: %+v", rec.Snapshot())
	}
	stage := rec.Snapshot()[0]
	if stage.Rows != 300 || stage.Bytes != 12288 {
		t.Errorf("accumulator = (rows=%d bytes=%d), want (300, 12288)", stage.Rows, stage.Bytes)
	}
}

func waitFor(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(time.Millisecond)
	}

	return cond()
}
