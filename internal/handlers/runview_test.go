package handlers_test

import (
	"regexp"
	"testing"

	"github.com/eunmann/s3-inv-db/internal/handlers"
)

func TestDomID_SafeAndDistinct(t *testing.T) {
	safe := regexp.MustCompile(`^[A-Za-z0-9_-]+$`)
	inputs := []string{"b/i/2026-10-25T20-50Z", "b.i/x", "b_i/x", "b-i/x", "logs.example.com/daily inv/r"}
	seen := map[string]string{}
	for _, in := range inputs {
		got := handlers.DomIDForTest(in)
		if !safe.MatchString(got) {
			t.Errorf("domID(%q) = %q has characters unsafe in an id selector", in, got)
		}
		if prev, dup := seen[got]; dup {
			t.Errorf("domID(%q) = domID(%q) = %q", in, prev, got)
		}
		seen[got] = in
	}
}

func TestRunStatsLine(t *testing.T) {
	s := handlers.RunStats{ObjectsH: "1.5M", BytesH: "549.43 TiB", CostH: "$7.4K", Objects: 1_500_000}
	if got, want := s.Line(), "1.5M objects · 549.43 TiB · est. $7.4K/mo"; got != want {
		t.Errorf("Line = %q, want %q", got, want)
	}
	if got, want := (handlers.RunStats{ObjectsH: "1", BytesH: "10 B", Objects: 1}).Line(), "1 object · 10 B"; got != want {
		t.Errorf("unpriced Line = %q, want %q", got, want)
	}
}
