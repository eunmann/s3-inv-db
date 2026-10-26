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

// TestDiscoveredRowPartial_LiveWiring pins the row's contract with
// scripts/live.js: every row names its run and the job it was rendered
// with, refreshes only on live-refresh (the script fires it when the job
// changes state, never per progress tick), and never polls; a busy row
// carries the in-place progress bar and text.
func TestDiscoveredRowPartial_LiveWiring(t *testing.T) {
	renderer, err := templates.New()
	if err != nil {
		t.Fatalf("templates.New: %v", err)
	}

	inv := inventory.Inventory{
		SourceBucket: "b", Name: "i", Run: "2026-05-13T03-00Z",
		ManifestKey: "k/2026-05-13T03-00Z/manifest.json",
	}
	row := func(state inventory.State, j *jobs.Job) handlers.DiscoveredRowView {
		return handlers.DiscoveredRowView{
			Inventory: inv, State: state,
			LatestJob: j,
		}
	}

	tests := []struct {
		name     string
		view     handlers.DiscoveredRowView
		wantBusy bool
		want     []string
	}{
		{
			name:     "running job",
			view:     row(inventory.StateLoading, &jobs.Job{ID: "j1", Kind: jobs.KindBuild, State: jobs.StateRunning, Stage: "building", StageDone: 1, StageTotal: 4}),
			wantBusy: true,
			want:     []string{`data-job-id="j1"`, `data-job-state="running"`, "Building index · 25%", "Cancel"},
		},
		{
			// The job store marks the job running before the catalog flips
			// the run to loading; the row must already read as busy.
			name:     "running job on a not-loaded run",
			view:     row(inventory.StateNotLoaded, &jobs.Job{ID: "j2", Kind: jobs.KindBuild, State: jobs.StateRunning}),
			wantBusy: true,
			want:     []string{"Loading…", "Starting…"},
		},
		{
			name:     "queued job says why it waits",
			view:     row(inventory.StateNotLoaded, &jobs.Job{ID: "j3", Kind: jobs.KindBuild, State: jobs.StateQueued}),
			wantBusy: true,
			want:     []string{`data-job-state="queued"`, "Queued — waiting for a free build slot"},
		},
		{
			name: "loaded run",
			view: row(inventory.StateLoaded, &jobs.Job{ID: "j4", Kind: jobs.KindBuild, State: jobs.StateSucceeded}),
			want: []string{`data-job-state="succeeded"`},
		},
		{
			// A cancel is the user's choice, not an error: a quiet note.
			name: "cancelled build",
			view: row(inventory.StateNotLoaded, &jobs.Job{ID: "j5", Kind: jobs.KindBuild, State: jobs.StateCancelled, FinishedAt: time.Now()}),
			want: []string{`data-job-state="cancelled"`, "Build cancelled just now"},
		},
		{
			name: "build stopped by a shutdown",
			view: row(inventory.StateNotLoaded, &jobs.Job{ID: "j6", Kind: jobs.KindBuild, State: jobs.StateAborted, Error: "interrupted: the server shut down"}),
			want: []string{"Build interrupted by a server restart"},
		},
		{
			name: "never-built run",
			view: row(inventory.StateNotLoaded, nil),
			want: []string{`data-job-id=""`, `data-job-state=""`},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			if err := renderer.RenderPartial(&buf, "discovered_row.html", tt.view); err != nil {
				t.Fatalf("RenderPartial: %v", err)
			}
			body := buf.String()
			for _, want := range append([]string{
				`data-live-inv="b/i/2026-05-13T03-00Z"`,
				`hx-trigger="live-refresh"`,
				`hx-sync="this:queue last"`,
			}, tt.want...) {
				if !strings.Contains(body, want) {
					t.Errorf("row missing %q\n%s", want, body)
				}
			}
			if strings.Contains(body, "every ") {
				t.Errorf("row polls; live.js drives refreshes\n%s", body)
			}
			if got := strings.Contains(body, "data-live-text"); got != tt.wantBusy {
				t.Errorf("progress line present = %v, want %v\n%s", got, tt.wantBusy, body)
			}
			if tt.wantBusy && tt.view.LatestJob != nil && !strings.Contains(body, "data-live-bar") {
				t.Errorf("busy row missing its progress bar\n%s", body)
			}
		})
	}
}
