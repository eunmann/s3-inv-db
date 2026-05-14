package inventory

import "testing"

func TestStatePredicates(t *testing.T) {
	cases := []struct {
		state State
		want  [5]bool // [IsLoaded, IsNotLoaded, IsLoading, IsError, CanLoad]
	}{
		{StateLoaded, [5]bool{true, false, false, false, false}},
		{StateNotLoaded, [5]bool{false, true, false, false, true}},
		{StateLoading, [5]bool{false, false, true, false, false}},
		{StateError, [5]bool{false, false, false, true, true}},
		{State("bogus"), [5]bool{false, false, false, false, false}},
	}
	for _, tc := range cases {
		t.Run(string(tc.state), func(t *testing.T) {
			got := [5]bool{tc.state.IsLoaded(), tc.state.IsNotLoaded(), tc.state.IsLoading(), tc.state.IsError(), tc.state.CanLoad()}
			if got != tc.want {
				t.Errorf("predicates for %q = %v, want %v", tc.state, got, tc.want)
			}
		})
	}
}

func TestIDString(t *testing.T) {
	if got := ID("src/inv/run").String(); got != "src/inv/run" {
		t.Errorf("String() = %q, want %q", got, "src/inv/run")
	}
}

func TestIDSplit(t *testing.T) {
	cases := []struct {
		id                        ID
		wantSrc, wantInv, wantRun string
		wantOK                    bool
	}{
		{"src/inv/run", "src", "inv", "run", true},
		{"src/inv/2026-05-13T03-02Z", "src", "inv", "2026-05-13T03-02Z", true},
		// Slashes inside the run segment must stay in the run — SplitN(_, 3).
		{"src/inv/2026/05/13", "src", "inv", "2026/05/13", true},
		{"src/inv", "", "", "", false},
		{"placeholder", "", "", "", false},
		{"", "", "", "", false},
	}
	for _, tc := range cases {
		t.Run(string(tc.id), func(t *testing.T) {
			src, inv, run, ok := tc.id.Split()
			if ok != tc.wantOK || src != tc.wantSrc || inv != tc.wantInv || run != tc.wantRun {
				t.Errorf("Split(%q) = (%q,%q,%q,%v), want (%q,%q,%q,%v)",
					tc.id, src, inv, run, ok, tc.wantSrc, tc.wantInv, tc.wantRun, tc.wantOK)
			}
		})
	}
}

func TestIDConfigID(t *testing.T) {
	// 3-part: drops the run segment.
	if got := ID("src/inv/run").ConfigID(); got != "src/inv" {
		t.Errorf("ConfigID(3-part) = %q, want %q", got, "src/inv")
	}
	// 2-part: returns the whole string (no separate config to extract).
	if got := ID("placeholder").ConfigID(); got != "placeholder" {
		t.Errorf("ConfigID(unsplittable) = %q, want %q (preserves grouping key)", got, "placeholder")
	}
}

func TestInventoryCompositeID(t *testing.T) {
	with := Inventory{SourceBucket: "src", InventoryName: "inv", Run: "2026-01"}
	if got := with.CompositeID(); got != ID("src/inv/2026-01") {
		t.Errorf("CompositeID(with run) = %q, want %q", got, "src/inv/2026-01")
	}
	without := Inventory{SourceBucket: "src", InventoryName: "inv"}
	if got := without.CompositeID(); got != ID("src/inv") {
		t.Errorf("CompositeID(no run) = %q, want %q", got, "src/inv")
	}
}

func TestInventoryConfigID(t *testing.T) {
	inv := Inventory{SourceBucket: "src", InventoryName: "inv", Run: "2026-01"}
	if got := inv.ConfigID(); got != "src/inv" {
		t.Errorf("ConfigID = %q, want %q (independent of Run)", got, "src/inv")
	}
}
