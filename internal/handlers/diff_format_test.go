package handlers

import (
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/humanfmt"
)

func TestNumericLabel(t *testing.T) {
	cases := []struct {
		name    string
		n       uint64
		missing bool
		fn      func(uint64) string
		want    string
	}{
		{"missing wins", 100, true, humanfmt.CountUint64, "—"},
		{"present formats", 100, false, humanfmt.CountUint64, "100"},
		{"zero is shown when not missing", 0, false, humanfmt.CountUint64, "0"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := numericLabel(tc.n, tc.missing, tc.fn); got != tc.want {
				t.Errorf("numericLabel(%d, %v) = %q, want %q", tc.n, tc.missing, got, tc.want)
			}
		})
	}
}

func TestFormatDelta(t *testing.T) {
	id := func(_ uint64) string { return "x" }
	cases := []struct {
		name          string
		before, after uint64
		delta         int64
		wantDeltaH    string
		wantPct       string
		wantSign      int
	}{
		{"positive growth", 100, 120, 20, "+x", "+20%", 1},
		{"shrink", 100, 80, -20, "−x", "-20%", -1},
		{"zero delta", 100, 100, 0, "±0", "", 0},
		{"new (zero before)", 0, 50, 50, "+x", "new", 1},
		{"gone (zero after)", 50, 0, -50, "−x", "gone", -1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			deltaH, pct, sign := formatDelta(tc.before, tc.after, tc.delta, id)
			if deltaH != tc.wantDeltaH || pct != tc.wantPct || sign != tc.wantSign {
				t.Errorf("formatDelta = (%q, %q, %d), want (%q, %q, %d)",
					deltaH, pct, sign, tc.wantDeltaH, tc.wantPct, tc.wantSign)
			}
		})
	}
}

func TestFormatCostDelta(t *testing.T) {
	cases := []struct {
		name          string
		before, after uint64
		wantSign      int
		wantPctIsZero bool
	}{
		{"cost grew", 1_000_000, 1_500_000, 1, false},
		{"cost shrunk", 1_000_000, 500_000, -1, false},
		{"cost identical", 1_000_000, 1_000_000, 0, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, pct, sign := formatCostDelta(tc.before, tc.after)
			if sign != tc.wantSign {
				t.Errorf("formatCostDelta sign = %d, want %d", sign, tc.wantSign)
			}
			if (pct == "") != tc.wantPctIsZero {
				t.Errorf("formatCostDelta pct = %q, wantEmpty=%v", pct, tc.wantPctIsZero)
			}
		})
	}
}

func TestPctChange(t *testing.T) {
	cases := []struct {
		name          string
		before, after uint64
		want          string
	}{
		{"new (before=0)", 0, 100, "new"},
		{"gone (after=0)", 100, 0, "gone"},
		{"both zero", 0, 0, ""},
		{"+20%", 100, 120, "+20%"},
		{"-50%", 200, 100, "-50%"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := pctChange(tc.before, tc.after); got != tc.want {
				t.Errorf("pctChange(%d, %d) = %q, want %q", tc.before, tc.after, got, tc.want)
			}
		})
	}
}

func TestAbsInt64(t *testing.T) {
	cases := []struct {
		in   int64
		want uint64
	}{
		{0, 0}, {1, 1}, {-1, 1}, {123456, 123456}, {-987654, 987654},
	}
	for _, tc := range cases {
		if got := absInt64(tc.in); got != tc.want {
			t.Errorf("absInt64(%d) = %d, want %d", tc.in, got, tc.want)
		}
	}
}
