package handlers_test

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/eunmann/s3-inv-db/internal/handlers"
	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/internal/jobs"
	"github.com/eunmann/s3-inv-db/internal/templates"
)

// TestDiscoveredRowPartial_StepAndETA pins that a busy row names the
// current step and extrapolates its ETA from when that step's counter
// started, not from the job's start: halfway through a step that began
// 10s into an hour-long build is ~10s left, not ~1h.
func TestDiscoveredRowPartial_StepAndETA(t *testing.T) {
	renderer, err := templates.New()
	if err != nil {
		t.Fatalf("templates.New: %v", err)
	}
	now := time.Now()
	view := handlers.DiscoveredRowView{
		SourceBucket: "b", Name: "i", Run: "2026-05-13T03-00Z",
		ManifestKey: "k/2026-05-13T03-00Z/manifest.json",
		State:       inventory.StateLoading,
		LatestJob: &jobs.Job{
			Kind: jobs.KindBuild, State: jobs.StateRunning,
			Stage: "finalizing", Step: "Writing hash slots",
			StageDone: 50, StageTotal: 100,
			StartedAt: now.Add(-time.Hour), StepStartedAt: now.Add(-10 * time.Second),
		},
	}
	var buf bytes.Buffer
	if err := renderer.RenderPartial(&buf, "discovered_row.html", view); err != nil {
		t.Fatalf("RenderPartial: %v", err)
	}
	body := buf.String()
	if !strings.Contains(body, "Finalizing · Writing hash slots") {
		t.Errorf("row does not name the step\n%s", body)
	}
	if !strings.Contains(body, "50% · ~10.") {
		t.Errorf("row ETA is not extrapolated from the step's start\n%s", body)
	}
}
