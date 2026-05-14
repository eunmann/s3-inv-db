package jobs

import "testing"

func TestIDString(t *testing.T) {
	if got := ID("job-123").String(); got != "job-123" {
		t.Errorf("ID.String() = %q, want %q", got, "job-123")
	}
}

func TestStatePredicates(t *testing.T) {
	cases := []struct {
		state        State
		wantTerminal bool
		wantLive     bool
	}{
		{StateQueued, false, true},
		{StateRunning, false, true},
		{StateSucceeded, true, false},
		{StateFailed, true, false},
		{StateCancelled, true, false},
		{StateAborted, true, false},
		{State("bogus"), false, false},
	}
	for _, tc := range cases {
		t.Run(string(tc.state), func(t *testing.T) {
			if got := tc.state.IsTerminal(); got != tc.wantTerminal {
				t.Errorf("IsTerminal(%q) = %v, want %v", tc.state, got, tc.wantTerminal)
			}
			if got := tc.state.IsLive(); got != tc.wantLive {
				t.Errorf("IsLive(%q) = %v, want %v", tc.state, got, tc.wantLive)
			}
			// Sanity invariant: a state can't be both live and terminal.
			if tc.state.IsLive() && tc.state.IsTerminal() {
				t.Errorf("%q reports IsLive && IsTerminal — incoherent", tc.state)
			}
		})
	}
}
