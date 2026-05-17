package templates_test

import (
	"strings"
	"testing"
	"time"

	"github.com/eunmann/s3-inv-db/internal/templates"
)

func TestFormatETA_ZeroOnUnknownInputs(t *testing.T) {
	now := time.Now()
	cases := []struct {
		startedAt time.Time
		name      string
		done      int64
		total     int64
	}{
		{name: "no start", startedAt: time.Time{}, done: 5, total: 10},
		{name: "no progress", startedAt: now.Add(-time.Minute), done: 0, total: 10},
		{name: "no total", startedAt: now.Add(-time.Minute), done: 5, total: 0},
		{name: "already done", startedAt: now.Add(-time.Minute), done: 10, total: 10},
		{name: "past done", startedAt: now.Add(-time.Minute), done: 11, total: 10},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := templates.FormatETA(c.startedAt, c.done, c.total); got != "" {
				t.Errorf("formatETA(%v, %d, %d) = %q, want \"\"", c.startedAt, c.done, c.total, got)
			}
		})
	}
}

func TestFormatETA_LinearProjection(t *testing.T) {
	// Half done after 1m → ~1m remaining. humanfmt.Duration formats as
	// "1m" or "60s" depending on rounding — accept anything non-empty
	// that looks roughly like a minute.
	start := time.Now().Add(-time.Minute)
	got := templates.FormatETA(start, 5, 10)
	if got == "" {
		t.Fatal("formatETA returned empty for valid inputs")
	}
	if !strings.Contains(got, "m") && !strings.Contains(got, "s") {
		t.Errorf("formatETA = %q, expected a duration unit", got)
	}
}

func TestProgressPct(t *testing.T) {
	cases := []struct {
		done, total int64
		want        int
	}{
		{0, 10, 0},
		{1, 10, 10},
		{5, 10, 50},
		{9, 10, 90},
		{10, 10, 100},
		{11, 10, 100}, // clamp
		{0, 0, 0},
		{5, 0, 0},
	}
	for _, c := range cases {
		if got := templates.ProgressPct(c.done, c.total); got != c.want {
			t.Errorf("progressPct(%d, %d) = %d, want %d", c.done, c.total, got, c.want)
		}
	}
}
