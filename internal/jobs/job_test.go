package jobs_test

import (
	"testing"

	"github.com/eunmann/s3-inv-db/internal/jobs"
)

func TestIDString(t *testing.T) {
	if got := jobs.ID("job-123").String(); got != "job-123" {
		t.Errorf("ID.String() = %q, want %q", got, "job-123")
	}
}

func TestNewJobID_NonEmptyAndUnique(t *testing.T) {
	const zero = "000000000000000000000000"
	seen := map[jobs.ID]struct{}{}
	for range 50 {
		id, err := jobs.NewJobIDForTest()
		if err != nil {
			t.Fatalf("newJobID: %v", err)
		}
		if string(id) == zero {
			t.Errorf("newJobID returned the all-zeros sentinel")
		}
		if _, dup := seen[id]; dup {
			t.Errorf("newJobID produced a duplicate ID %q within 50 calls", id)
		}
		seen[id] = struct{}{}
	}
}

func TestStatePredicates(t *testing.T) {
	cases := []struct {
		state        jobs.State
		wantTerminal bool
		wantLive     bool
	}{
		{jobs.StateQueued, false, true},
		{jobs.StateRunning, false, true},
		{jobs.StateSucceeded, true, false},
		{jobs.StateFailed, true, false},
		{jobs.StateCancelled, true, false},
		{jobs.StateAborted, true, false},
		{jobs.State("bogus"), false, false},
	}
	for _, tc := range cases {
		t.Run(string(tc.state), func(t *testing.T) {
			if got := tc.state.IsTerminal(); got != tc.wantTerminal {
				t.Errorf("IsTerminal(%q) = %v, want %v", tc.state, got, tc.wantTerminal)
			}
			if got := tc.state.IsLive(); got != tc.wantLive {
				t.Errorf("IsLive(%q) = %v, want %v", tc.state, got, tc.wantLive)
			}
			if tc.state.IsLive() && tc.state.IsTerminal() {
				t.Errorf("%q reports IsLive && IsTerminal", tc.state)
			}
		})
	}
}
