package handlers_test

import (
	"testing"

	"github.com/eunmann/s3-inv-db/internal/handlers"
	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/pkg/humanfmt"
)

// missingGlyph is the em-dash rendered when a metric is unavailable on
// one side of a comparison; centralised here so goconst doesn't flag
// the repeated literal across assertions in this file.
const missingGlyph = "—"

func TestNumericLabel(t *testing.T) {
	cases := []struct {
		fn      func(uint64) string
		name    string
		want    string
		n       uint64
		missing bool
	}{
		{name: "missing wins", n: 100, missing: true, fn: humanfmt.CountUint64, want: missingGlyph},
		{name: "present formats", n: 100, missing: false, fn: humanfmt.CountUint64, want: "100"},
		{name: "zero is shown when not missing", n: 0, missing: false, fn: humanfmt.CountUint64, want: "0"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := handlers.NumericLabelForTest(tc.n, tc.missing, tc.fn); got != tc.want {
				t.Errorf("numericLabel(%d, %v) = %q, want %q", tc.n, tc.missing, got, tc.want)
			}
		})
	}
}

func TestFormatDelta(t *testing.T) {
	id := func(_ uint64) string { return "x" }
	cases := []struct {
		name       string
		wantDeltaH string
		wantPct    string
		before     uint64
		after      uint64
		delta      int64
		wantSign   int
	}{
		{name: "positive growth", before: 100, after: 120, delta: 20, wantDeltaH: "+x", wantPct: "+20%", wantSign: 1},
		{name: "shrink", before: 100, after: 80, delta: -20, wantDeltaH: "−x", wantPct: "-20%", wantSign: -1},
		{name: "zero delta", before: 100, after: 100, delta: 0, wantDeltaH: "±0", wantPct: "", wantSign: 0},
		{name: "new (zero before)", before: 0, after: 50, delta: 50, wantDeltaH: "+x", wantPct: "new", wantSign: 1},
		{name: "gone (zero after)", before: 50, after: 0, delta: -50, wantDeltaH: "−x", wantPct: "gone", wantSign: -1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := handlers.FormatDeltaForTest(tc.before, tc.after, tc.delta, id)
			if got.DeltaH != tc.wantDeltaH || got.Pct != tc.wantPct || got.Sign != tc.wantSign {
				t.Errorf("formatDelta = (%q, %q, %d), want (%q, %q, %d)",
					got.DeltaH, got.Pct, got.Sign, tc.wantDeltaH, tc.wantPct, tc.wantSign)
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
			got := handlers.FormatCostDeltaForTest(tc.before, tc.after)
			if got.Sign != tc.wantSign {
				t.Errorf("formatCostDelta sign = %d, want %d", got.Sign, tc.wantSign)
			}
			if (got.Pct == "") != tc.wantPctIsZero {
				t.Errorf("formatCostDelta pct = %q, wantEmpty=%v", got.Pct, tc.wantPctIsZero)
			}
		})
	}
}

func TestPctChange(t *testing.T) {
	cases := []struct {
		name   string
		want   string
		before uint64
		after  uint64
	}{
		{name: "new (before=0)", before: 0, after: 100, want: "new"},
		{name: "gone (after=0)", before: 100, after: 0, want: "gone"},
		{name: "both zero", before: 0, after: 0, want: ""},
		{name: "+20%", before: 100, after: 120, want: "+20%"},
		{name: "-50%", before: 200, after: 100, want: "-50%"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := handlers.PctChangeForTest(tc.before, tc.after); got != tc.want {
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
		if got := handlers.AbsInt64ForTest(tc.in); got != tc.want {
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
			got := handlers.DescribeRunForTest(inventory.ID(tc.id))
			if got.ConfigLabel != tc.wantLabel {
				t.Errorf("describeRun(%q) configLabel = %q, want %q", tc.id, got.ConfigLabel, tc.wantLabel)
			}
			if tc.want != "" && got.RunLabel != tc.want {
				t.Errorf("describeRun(%q) runLabel = %q, want %q", tc.id, got.RunLabel, tc.want)
			}
		})
	}
}

func TestBuildCompareSelfView_FormatsBeforeAfterAndDelta(t *testing.T) {
	h := newTestHandlers(t)
	self := inventory.CompareSelf{
		Prefix:  "data/",
		Objects: inventory.CompareNumeric{Before: 100, After: 150, Delta: 50},
		Bytes:   inventory.CompareNumeric{Before: 1_000, After: 2_000, Delta: 1_000},
	}
	v := h.BuildCompareSelfViewForTest(self)
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

func TestBuildCompareSelfView_MissingSideRendersDash(t *testing.T) {
	h := newTestHandlers(t)
	self := inventory.CompareSelf{
		Prefix:      "data/",
		NotFoundInA: true,
		Objects:     inventory.CompareNumeric{Before: 0, After: 50, Delta: 50},
		Bytes:       inventory.CompareNumeric{Before: 0, After: 500, Delta: 500},
	}
	v := h.BuildCompareSelfViewForTest(self)
	if v.ObjectsBeforeH != missingGlyph || v.BytesBeforeH != missingGlyph {
		t.Errorf("before columns: objects=%q bytes=%q, want both '—'", v.ObjectsBeforeH, v.BytesBeforeH)
	}
	if v.ObjectsAfterH == missingGlyph || v.BytesAfterH == missingGlyph {
		t.Errorf("after columns: objects=%q bytes=%q, want neither '—'", v.ObjectsAfterH, v.BytesAfterH)
	}
}

func TestBuildCompareChildView_StatusOrderMatchesPublicHelper(t *testing.T) {
	h := newTestHandlers(t)
	child := inventory.CompareChild{
		Segment: "logs/", Prefix: "data/logs/",
		Status:  inventory.CompareChanged,
		Objects: inventory.CompareNumeric{Before: 10, After: 8, Delta: -2},
		Bytes:   inventory.CompareNumeric{Before: 1000, After: 900, Delta: -100},
	}
	v := h.BuildCompareChildViewForTest(&child)
	if v.Status != "changed" {
		t.Errorf("Status = %q, want changed", v.Status)
	}
	if v.StatusOrder != inventory.StatusOrder(inventory.CompareChanged) {
		t.Errorf("StatusOrder = %d, want %d", v.StatusOrder, inventory.StatusOrder(inventory.CompareChanged))
	}
	if v.ObjectsSign != -1 || v.BytesSign != -1 {
		t.Errorf("sign for shrinkage: objects=%d bytes=%d, want both -1", v.ObjectsSign, v.BytesSign)
	}
	if v.AbsByteDelta != 100 {
		t.Errorf("AbsByteDelta = %d, want 100", v.AbsByteDelta)
	}
}

func TestBuildCompareChildView_AddedHidesBeforeColumn(t *testing.T) {
	h := newTestHandlers(t)
	child := inventory.CompareChild{
		Segment: "new/", Prefix: "data/new/",
		Status:  inventory.CompareAdded,
		Objects: inventory.CompareNumeric{Before: 0, After: 5, Delta: 5},
		Bytes:   inventory.CompareNumeric{Before: 0, After: 50, Delta: 50},
	}
	v := h.BuildCompareChildViewForTest(&child)
	if v.ObjectsBeforeH != missingGlyph {
		t.Errorf("ObjectsBeforeH = %q, want '—'", v.ObjectsBeforeH)
	}
}

func TestBuildCompareChildView_RemovedHidesAfterColumn(t *testing.T) {
	h := newTestHandlers(t)
	child := inventory.CompareChild{
		Segment: "old/", Prefix: "data/old/",
		Status:  inventory.CompareRemoved,
		Objects: inventory.CompareNumeric{Before: 5, After: 0, Delta: -5},
		Bytes:   inventory.CompareNumeric{Before: 50, After: 0, Delta: -50},
	}
	v := h.BuildCompareChildViewForTest(&child)
	if v.ObjectsAfterH != missingGlyph {
		t.Errorf("ObjectsAfterH = %q, want '—'", v.ObjectsAfterH)
	}
}
