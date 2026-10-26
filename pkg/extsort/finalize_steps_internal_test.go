package extsort

import (
	"bytes"
	"encoding/json"
	"fmt"
	"slices"
	"sync"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/extsort/events"
	"github.com/eunmann/s3-inv-db/pkg/format"
	"github.com/rs/zerolog"
)

// progressLog records the progress a build reports.
type progressLog struct {
	mu  sync.Mutex
	got []events.Progress
}

func (l *progressLog) record(p events.Progress) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.got = append(l.got, p)
}

// TestFinalizeReportsEachStep builds an index with a progress callback
// and checks that Finalize reports every step under the finalizing
// stage, in order, each counting up to its total, and logs each step.
func TestFinalizeReportsEachStep(t *testing.T) {
	b, err := NewIndexBuilder(t.TempDir(), t.TempDir())
	if err != nil {
		t.Fatalf("NewIndexBuilder: %v", err)
	}
	const children = 2000
	root := &PrefixRow{Prefix: "", Count: children}
	root.TierCounts[0] = children
	if err := b.Add(root); err != nil {
		t.Fatalf("Add root: %v", err)
	}
	for i := range children {
		row := &PrefixRow{Prefix: fmt.Sprintf("p%05d/", i), Depth: 1, Count: 1}
		row.TierCounts[0] = 1
		if err := b.Add(row); err != nil {
			t.Fatalf("Add: %v", err)
		}
	}
	var logs bytes.Buffer
	ctx := zerolog.New(&logs).WithContext(t.Context())
	var rec progressLog
	b.SetProgress(rec.record)
	if err := b.FinalizeWithContext(ctx); err != nil {
		t.Fatalf("Finalize: %v", err)
	}

	wantSteps := []string{stepWriteLevels, format.StepBuildHash, format.StepPlaceSlots, format.StepSlotFile, format.StepChecksum}
	var steps []string
	last := map[string]events.Progress{}
	for _, p := range rec.got {
		if p.Stage != finalizingStage {
			t.Fatalf("progress %+v outside the finalizing stage", p)
		}
		if len(steps) == 0 || steps[len(steps)-1] != p.Step {
			steps = append(steps, p.Step)
		}
		if prev, ok := last[p.Step]; ok && p.Done < prev.Done {
			t.Errorf("step %q went backwards: %d after %d", p.Step, p.Done, prev.Done)
		}
		last[p.Step] = p
	}
	if !slices.Equal(steps, wantSteps) {
		t.Errorf("steps = %q, want %q", steps, wantSteps)
	}
	for _, step := range wantSteps {
		if p := last[step]; p.Total <= 0 || p.Done != p.Total {
			t.Errorf("step %q ended at %d of %d, want done == total > 0", step, p.Done, p.Total)
		}
	}
	if p := last[stepWriteLevels]; p.Total != children+1 {
		t.Errorf("%q total = %d, want 2001 rows", stepWriteLevels, p.Total)
	}

	var logged []string
	for line := range bytes.Lines(logs.Bytes()) {
		var entry struct {
			Level   string `json:"level"`
			Message string `json:"message"`
			Step    string `json:"step"`
		}
		if err := json.Unmarshal(line, &entry); err != nil {
			t.Fatalf("log line %q: %v", line, err)
		}
		if entry.Message == "finalize step complete" {
			if entry.Level != "info" {
				t.Errorf("step %q logged at %s, want info", entry.Step, entry.Level)
			}
			logged = append(logged, entry.Step)
		}
	}
	if !slices.Equal(logged, wantSteps) {
		t.Errorf("logged steps = %q, want %q", logged, wantSteps)
	}
}

// TestFinalizeStepsThrottles checks that repeated reports of one step
// are forwarded at most every stepReportInterval, while a new step and
// a step's completion always are.
func TestFinalizeStepsThrottles(t *testing.T) {
	var rec progressLog
	log := zerolog.Nop()
	s := newFinalizeSteps(&log, rec.record)
	s.report("a", 0, 100)
	for done := range int64(50) {
		s.report("a", done+1, 100)
	}
	s.report("a", 100, 100)
	s.report("b", 0, 10)
	s.end()

	want := []events.Progress{
		{Stage: finalizingStage, Step: "a", Done: 0, Total: 100},
		{Stage: finalizingStage, Step: "a", Done: 100, Total: 100},
		{Stage: finalizingStage, Step: "b", Done: 0, Total: 10},
	}
	if !slices.Equal(rec.got, want) {
		t.Errorf("forwarded %+v, want %+v", rec.got, want)
	}
}
