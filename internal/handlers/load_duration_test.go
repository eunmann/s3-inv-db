package handlers_test

import (
	"testing"
	"time"

	"github.com/eunmann/s3-inv-db/internal/handlers"
	"github.com/eunmann/s3-inv-db/internal/jobs"
)

func TestLoadDurationLabel(t *testing.T) {
	start := time.Date(2026, 5, 18, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name         string
		infoDuration time.Duration
		job          *jobs.Job
		want         string
	}{
		{
			name: "nil job + no info duration → empty",
			job:  nil,
			want: "",
		},
		{
			name: "non-build job kind, no info → empty",
			job: &jobs.Job{
				Kind: jobs.KindUnload, StartedAt: start, FinishedAt: start.Add(time.Second),
			},
			want: "",
		},
		{
			name: "missing finished → empty (still in-flight)",
			job: &jobs.Job{
				Kind: jobs.KindBuild, StartedAt: start,
			},
			want: "",
		},
		{
			name: "finished before started, no info → empty (clock skew)",
			job: &jobs.Job{
				Kind: jobs.KindBuild, StartedAt: start, FinishedAt: start.Add(-time.Second),
			},
			want: "",
		},
		{
			name: "happy path renders humanfmt duration from job",
			job: &jobs.Job{
				Kind: jobs.KindBuild, StartedAt: start, FinishedAt: start.Add(90 * time.Second),
			},
			want: "1m30s",
		},
		{
			name:         "info duration wins even when no job",
			infoDuration: 90 * time.Second,
			job:          nil,
			want:         "1m30s",
		},
		{
			name:         "info duration preferred over job",
			infoDuration: 5 * time.Second,
			job: &jobs.Job{
				Kind: jobs.KindBuild, StartedAt: start, FinishedAt: start.Add(90 * time.Second),
			},
			want: "5.00s",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := handlers.LoadDurationLabelForTest(tt.infoDuration, tt.job); got != tt.want {
				t.Errorf("loadDurationLabel = %q, want %q", got, tt.want)
			}
		})
	}
}
