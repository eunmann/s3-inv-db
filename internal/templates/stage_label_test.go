package templates

import "testing"

func TestStageLabel(t *testing.T) {
	cases := []struct {
		stage string
		want  string
	}{
		{"preparing", "Preparing"},
		{"initializing", "Initializing"},
		{"downloading", "Downloading & parsing"},
		{"building", "Building index"},
		{"done", "Done"},
		// Unknown stages fall through verbatim so the UI can still surface
		// new pipeline phases without a template+code change.
		{"unknown-future-stage", "unknown-future-stage"},
		{"", ""},
	}
	for _, c := range cases {
		t.Run(c.stage, func(t *testing.T) {
			if got := stageLabel(c.stage); got != c.want {
				t.Errorf("stageLabel(%q) = %q, want %q", c.stage, got, c.want)
			}
		})
	}
}
