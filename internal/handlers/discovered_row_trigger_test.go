package handlers_test

import (
	"bytes"
	"strings"
	"testing"

	"github.com/eunmann/s3-inv-db/internal/handlers"
	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/internal/jobs"
	"github.com/eunmann/s3-inv-db/internal/templates"
)

// TestDiscoveredRowPartial_PollingFallbackTrigger pins the SSE-race
// fix. A build that completes in under the time htmx-sse takes to
// attach the new row's listener would otherwise leave the row stuck
// on "Loading…"; the row partial therefore emits `every 2s` alongside
// `sse:row-<composite>` in its hx-trigger ONLY while busy.
//
// Without this guard the row either polls forever (if it carries the
// trigger after settling) or never recovers from a missed SSE event
// (if the trigger never carries it). Pin both sides explicitly.
func TestDiscoveredRowPartial_PollingFallbackTrigger(t *testing.T) {
	renderer, err := templates.New()
	if err != nil {
		t.Fatalf("templates.New: %v", err)
	}

	const composite = "b/i/2026-05-13T03-00Z"
	const expectedSSE = "sse:row-" + composite

	tests := []struct {
		name          string
		view          handlers.DiscoveredRowView
		wantPollEvery bool // true → expect "every 2s" in hx-trigger
	}{
		{
			name: "busy row polls as SSE-race fallback",
			view: handlers.DiscoveredRowView{
				MergedInventory: inventory.MergedInventory{
					Inventory: inventory.Inventory{
						SourceBucket: "b", Name: "i", Run: "2026-05-13T03-00Z",
						ManifestKey: "k/2026-05-13T03-00Z/manifest.json",
					},
					State: inventory.StateLoading,
				},
				LatestJob: &jobs.Job{Kind: jobs.KindBuild, State: jobs.StateRunning, Stage: "downloading"},
			},
			wantPollEvery: true,
		},
		{
			name: "loaded row drops the poll",
			view: handlers.DiscoveredRowView{
				MergedInventory: inventory.MergedInventory{
					Inventory: inventory.Inventory{
						SourceBucket: "b", Name: "i", Run: "2026-05-13T03-00Z",
						ManifestKey: "k/2026-05-13T03-00Z/manifest.json",
					},
					State: inventory.StateLoaded,
				},
			},
			wantPollEvery: false,
		},
		{
			name: "not-loaded row drops the poll",
			view: handlers.DiscoveredRowView{
				MergedInventory: inventory.MergedInventory{
					Inventory: inventory.Inventory{
						SourceBucket: "b", Name: "i", Run: "2026-05-13T03-00Z",
						ManifestKey: "k/2026-05-13T03-00Z/manifest.json",
					},
					State: inventory.StateNotLoaded,
				},
			},
			wantPollEvery: false,
		},
		{
			name: "error row drops the poll",
			view: handlers.DiscoveredRowView{
				MergedInventory: inventory.MergedInventory{
					Inventory: inventory.Inventory{
						SourceBucket: "b", Name: "i", Run: "2026-05-13T03-00Z",
						ManifestKey: "k/2026-05-13T03-00Z/manifest.json",
					},
					State: inventory.StateError,
					Error: "boom",
				},
			},
			wantPollEvery: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			if err := renderer.RenderPartial(&buf, "discovered_row.html", tt.view); err != nil {
				t.Fatalf("RenderPartial: %v", err)
			}
			body := buf.String()
			if !strings.Contains(body, expectedSSE) {
				t.Errorf("body missing %q\n%s", expectedSSE, body)
			}
			hasPoll := strings.Contains(body, "every 2s")
			if hasPoll != tt.wantPollEvery {
				t.Errorf("every 2s present=%v want=%v\n%s", hasPoll, tt.wantPollEvery, body)
			}
		})
	}
}
