package cli

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/eunmann/s3-inv-db/pkg/extsort"
	"github.com/eunmann/s3-inv-db/pkg/extsort/events"
	"github.com/rs/zerolog"
)

// TestBuildTracker_SidecarLifecycle exercises start → publish → finish
// and verifies the build.json sidecar lands with the expected shape.
func TestBuildTracker_SidecarLifecycle(t *testing.T) {
	tmp := t.TempDir()
	logger := zerolog.Nop()

	tr := newBuildTracker(tmp, "s3://b/inv/manifest.json", "", &logger)
	defer tr.close()

	// Wire the tracker's bus into a synthetic ObserveConfig — the same
	// path the build subcommand uses, just without the pipeline.
	bus := tr.wire().EventBus
	if bus == nil {
		t.Fatal("wire returned nil bus")
	}

	tr.start()

	bus.Publish(events.Event{
		Stage: events.StagePipeline,
		Type:  events.EvtStageEnd,
		Payload: events.StageTiming{
			Stage:    events.StageDownload,
			Duration: 50 * time.Millisecond,
			Rows:     1000,
			Bytes:    4096,
		},
	})
	bus.Publish(events.Event{
		Stage: events.StageSpill,
		Type:  events.EvtSpillCompleted,
		Payload: events.SpillCompleted{
			WorkerID: 0,
			Rows:     500,
			Bytes:    2048,
			Duration: 10 * time.Millisecond,
		},
	})

	tr.finish(&extsort.Result{
		ChunksProcessed:  3,
		ObjectsProcessed: 1000,
		PrefixCount:      400,
		MaxDepth:         5,
		RunFilesCreated:  1,
		Duration:         60 * time.Millisecond,
	}, nil)

	data, err := os.ReadFile(filepath.Join(tmp, "build.json"))
	if err != nil {
		t.Fatalf("read build.json: %v", err)
	}

	var s buildSummary
	if err := json.Unmarshal(data, &s); err != nil {
		t.Fatalf("unmarshal build.json: %v", err)
	}

	if s.Outcome != "success" {
		t.Errorf("Outcome = %q, want success", s.Outcome)
	}
	if s.NodeCount != 400 {
		t.Errorf("NodeCount = %d, want 400", s.NodeCount)
	}
	if s.RunFilesCreated != 1 {
		t.Errorf("RunFilesCreated = %d, want 1", s.RunFilesCreated)
	}
	if s.SpillsWritten != 1 {
		t.Errorf("SpillsWritten = %d, want 1", s.SpillsWritten)
	}
	if s.SpillRows != 500 {
		t.Errorf("SpillRows = %d, want 500", s.SpillRows)
	}
	if len(s.Stages) == 0 {
		t.Errorf("Stages empty; expected at least one StageTiming record")
	}
}

// TestBuildTracker_FailurePath verifies finish(nil, err) records a
// failure outcome with the error message.
func TestBuildTracker_FailurePath(t *testing.T) {
	tmp := t.TempDir()
	logger := zerolog.Nop()

	tr := newBuildTracker(tmp, "s3://b/inv/manifest.json", "", &logger)
	defer tr.close()
	_ = tr.wire()
	tr.start()
	tr.finish(nil, errBoom)

	data, err := os.ReadFile(filepath.Join(tmp, "build.json"))
	if err != nil {
		t.Fatalf("read build.json: %v", err)
	}
	var s buildSummary
	if err := json.Unmarshal(data, &s); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if s.Outcome != "failure" {
		t.Errorf("Outcome = %q, want failure", s.Outcome)
	}
	if s.Error == "" {
		t.Errorf("Error empty; want non-empty")
	}
}

// TestBuildTracker_EventLogJSONL writes events to a JSONL file when
// eventLogPath is set.
func TestBuildTracker_EventLogJSONL(t *testing.T) {
	tmp := t.TempDir()
	logger := zerolog.Nop()

	logPath := filepath.Join(tmp, "events.jsonl")
	tr := newBuildTracker(tmp, "s3://b/inv/manifest.json", logPath, &logger)
	defer tr.close()
	bus := tr.wire().EventBus
	tr.start()

	bus.Publish(events.Event{Stage: events.StageDownload, Type: events.EvtStageStart})
	bus.Publish(events.Event{Stage: events.StageDownload, Type: events.EvtStageEnd, Payload: events.StageTiming{Stage: events.StageDownload, Rows: 1}})

	tr.finish(nil, nil)
	tr.close()

	data, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("read event log: %v", err)
	}
	if len(data) == 0 {
		t.Errorf("event log is empty")
	}
}

// Sentinel for the failure test.
//

var errBoom = boomError("boom")

type boomError string

func (e boomError) Error() string { return string(e) }
