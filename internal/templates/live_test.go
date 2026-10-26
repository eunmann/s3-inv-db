package templates_test

import (
	"slices"
	"testing"
	"time"

	"github.com/eunmann/s3-inv-db/internal/templates"
)

// TestClock: whole-unit clocks in the same shape live.js renders, so a
// clock's first tick in the browser doesn't change its format.
func TestClock(t *testing.T) {
	now := time.Date(2026, 10, 26, 12, 0, 0, 0, time.UTC)
	cases := map[time.Duration]string{
		3*time.Second + 440*time.Millisecond:        "3s",
		65 * time.Second:                            "1m5s",
		2 * time.Minute:                             "2m",
		2*time.Hour + 3*time.Minute + 9*time.Second: "2h3m",
		3 * time.Hour:                               "3h",
	}
	for d, want := range cases {
		if got := templates.Clock(now, now.Add(-d)); got != want {
			t.Errorf("Clock(%v) = %q, want %q", d, got, want)
		}
	}
	if got := templates.Clock(now, time.Time{}); got != "" {
		t.Errorf("Clock(zero) = %q, want empty", got)
	}
}

// TestLiveSegments: the bar fills stage by stage and never moves
// backwards when a stage starts; a running stage with no total yet is
// active with no fill (drawn indeterminate).
func TestLiveSegments(t *testing.T) {
	type seg = templates.LiveSegment
	cases := []struct {
		name, state, stage string
		done, total        int64
		want               []seg
	}{
		{"queued", "queued", "", 0, 0, []seg{{}, {}, {}}},
		{"initializing", "running", "initializing", 0, 0, []seg{{Active: true}, {}, {}}},
		{"downloading half", "running", "downloading", 5, 10, []seg{{Fill: 50, Active: true}, {}, {}}},
		{"merging spilled runs", "running", "merging", 3, 8, []seg{{Fill: 100}, {Active: true}, {}}},
		{"building starts", "running", "building", 0, 0, []seg{{Fill: 100}, {Active: true}, {}}},
		{"finalizing", "running", "finalizing", 1, 4, []seg{{Fill: 100}, {Fill: 100}, {Fill: 25, Active: true}}},
		{"succeeded", "succeeded", "finalizing", 0, 0, []seg{{Fill: 100}, {Fill: 100}, {Fill: 100}}},
		{"failed while building", "failed", "building", 3, 10, []seg{{Fill: 100}, {Fill: 30}, {}}},
		{"fetching a published index", "running", "fetching", 2, 8, []seg{{Fill: 25, Active: true}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := templates.LiveSegments(tc.state, tc.stage, tc.done, tc.total)
			if !slices.Equal(got, tc.want) {
				t.Errorf("LiveSegments(%s, %s, %d, %d) = %+v, want %+v", tc.state, tc.stage, tc.done, tc.total, got, tc.want)
			}
		})
	}
}
