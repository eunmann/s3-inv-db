package handlers

import (
	"testing"

	"github.com/eunmann/s3-inv-db/internal/inventory"
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

func TestDescribeRun(t *testing.T) {
	cases := []struct {
		id              string
		wantLabel, want string
	}{
		{"src/inv/2026-05-13T03-02Z", "src/inv", ""},
		{"src/inv", "", "src/inv"},
		{"placeholder", "", "placeholder"},
	}
	for _, tc := range cases {
		t.Run(tc.id, func(t *testing.T) {
			label, run := describeRun(inventory.ID(tc.id))
			if label != tc.wantLabel {
				t.Errorf("describeRun(%q) configLabel = %q, want %q", tc.id, label, tc.wantLabel)
			}
			if tc.want != "" && run != tc.want {
				t.Errorf("describeRun(%q) runLabel = %q, want %q", tc.id, run, tc.want)
			}
		})
	}
}

func TestBuildDiffSelfView_FormatsBeforeAfterAndDelta(t *testing.T) {
	h := newTestHandlers(t)
	self := inventory.DiffSelf{
		Prefix:  "data/",
		Objects: inventory.DiffNumeric{Before: 100, After: 150, Delta: 50},
		Bytes:   inventory.DiffNumeric{Before: 1_000, After: 2_000, Delta: 1_000},
	}
	v := h.buildDiffSelfView(self)
	if v.Prefix != "data/" {
		t.Errorf("Prefix = %q, want data/", v.Prefix)
	}
	if v.ObjectsSign != 1 || v.BytesSign != 1 {
		t.Errorf("sign for growth: objects=%d bytes=%d, want both 1", v.ObjectsSign, v.BytesSign)
	}
	if v.ObjectsDeltaH == "" || v.BytesDeltaH == "" {
		t.Errorf("empty delta strings: %+v", v)
	}
	if v.HasCost {
		t.Error("HasCost = true with no tier data")
	}
}

func TestBuildDiffSelfView_MissingSideRendersDash(t *testing.T) {
	h := newTestHandlers(t)
	self := inventory.DiffSelf{
		Prefix:      "data/",
		NotFoundInA: true,
		Objects:     inventory.DiffNumeric{Before: 0, After: 50, Delta: 50},
		Bytes:       inventory.DiffNumeric{Before: 0, After: 500, Delta: 500},
	}
	v := h.buildDiffSelfView(self)
	if v.ObjectsBeforeH != "—" || v.BytesBeforeH != "—" {
		t.Errorf("before columns: objects=%q bytes=%q, want both '—'", v.ObjectsBeforeH, v.BytesBeforeH)
	}
	if v.ObjectsAfterH == "—" || v.BytesAfterH == "—" {
		t.Errorf("after columns: objects=%q bytes=%q, want neither '—'", v.ObjectsAfterH, v.BytesAfterH)
	}
}

func TestBuildDiffChildView_StatusOrderMatchesPublicHelper(t *testing.T) {
	h := newTestHandlers(t)
	child := inventory.DiffChild{
		Segment: "logs/", Prefix: "data/logs/",
		Status:  inventory.DiffChanged,
		Objects: inventory.DiffNumeric{Before: 10, After: 8, Delta: -2},
		Bytes:   inventory.DiffNumeric{Before: 1000, After: 900, Delta: -100},
	}
	v := h.buildDiffChildView(&child)
	if v.Status != "changed" {
		t.Errorf("Status = %q, want changed", v.Status)
	}
	if v.StatusOrder != inventory.StatusOrder(inventory.DiffChanged) {
		t.Errorf("StatusOrder = %d, want %d", v.StatusOrder, inventory.StatusOrder(inventory.DiffChanged))
	}
	if v.ObjectsSign != -1 || v.BytesSign != -1 {
		t.Errorf("sign for shrinkage: objects=%d bytes=%d, want both -1", v.ObjectsSign, v.BytesSign)
	}
	if v.AbsByteDelta != 100 {
		t.Errorf("AbsByteDelta = %d, want 100", v.AbsByteDelta)
	}
}

func TestBuildDiffChildView_AddedHidesBeforeColumn(t *testing.T) {
	h := newTestHandlers(t)
	child := inventory.DiffChild{
		Segment: "new/", Prefix: "data/new/",
		Status:  inventory.DiffAdded,
		Objects: inventory.DiffNumeric{Before: 0, After: 5, Delta: 5},
		Bytes:   inventory.DiffNumeric{Before: 0, After: 50, Delta: 50},
	}
	v := h.buildDiffChildView(&child)
	if v.ObjectsBeforeH != "—" {
		t.Errorf("ObjectsBeforeH = %q, want '—'", v.ObjectsBeforeH)
	}
}

func TestBuildDiffChildView_RemovedHidesAfterColumn(t *testing.T) {
	h := newTestHandlers(t)
	child := inventory.DiffChild{
		Segment: "old/", Prefix: "data/old/",
		Status:  inventory.DiffRemoved,
		Objects: inventory.DiffNumeric{Before: 5, After: 0, Delta: -5},
		Bytes:   inventory.DiffNumeric{Before: 50, After: 0, Delta: -50},
	}
	v := h.buildDiffChildView(&child)
	if v.ObjectsAfterH != "—" {
		t.Errorf("ObjectsAfterH = %q, want '—'", v.ObjectsAfterH)
	}
}
