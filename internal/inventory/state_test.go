package inventory_test

import (
	"context"
	"testing"

	"github.com/eunmann/s3-inv-db/internal/inventory"
)

func TestStatePredicates(t *testing.T) {
	// want = [IsLoaded, IsNotLoaded, IsLoading, IsError, CanLoad]
	cases := []struct {
		state inventory.State
		want  [5]bool
	}{
		{inventory.StateLoaded, [5]bool{true, false, false, false, false}},
		{inventory.StateNotLoaded, [5]bool{false, true, false, false, true}},
		{inventory.StateLoading, [5]bool{false, false, true, false, false}},
		{inventory.StateError, [5]bool{false, false, false, true, true}},
		{inventory.State("bogus"), [5]bool{false, false, false, false, false}},
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
	if got := inventory.ID("src/inv/run").String(); got != "src/inv/run" {
		t.Errorf("String() = %q, want %q", got, "src/inv/run")
	}
}

func TestIDSplit(t *testing.T) {
	cases := []struct {
		id                        inventory.ID
		wantSrc, wantInv, wantRun string
		wantOK                    bool
	}{
		{"src/inv/run", "src", "inv", "run", true},
		{"src/inv/2026-05-13T03-02Z", "src", "inv", "2026-05-13T03-02Z", true},
		{"src/inv/2026/05/13", "src", "inv", "2026/05/13", true}, // SplitN keeps slashes in the third segment.
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
	if got := inventory.ID("src/inv/run").ConfigID(); got != "src/inv" {
		t.Errorf("ConfigID(3-part) = %q, want %q", got, "src/inv")
	}
	if got := inventory.ID("placeholder").ConfigID(); got != "placeholder" {
		t.Errorf("ConfigID(unsplittable) = %q, want %q", got, "placeholder")
	}
}

func TestInventoryCompositeID(t *testing.T) {
	with := inventory.Inventory{SourceBucket: "src", InventoryName: "inv", Run: "2026-01"}
	if got := with.CompositeID(); got != inventory.ID("src/inv/2026-01") {
		t.Errorf("CompositeID(with run) = %q, want %q", got, "src/inv/2026-01")
	}
	without := inventory.Inventory{SourceBucket: "src", InventoryName: "inv"}
	if got := without.CompositeID(); got != inventory.ID("src/inv") {
		t.Errorf("CompositeID(no run) = %q, want %q", got, "src/inv")
	}
}

func TestInventoryConfigID(t *testing.T) {
	inv := inventory.Inventory{SourceBucket: "src", InventoryName: "inv", Run: "2026-01"}
	if got := inv.ConfigID(); got != "src/inv" {
		t.Errorf("ConfigID = %q, want %q", got, "src/inv")
	}
}

func TestManagerLoad_UsesOpenLocalPath(t *testing.T) {
	mgr := inventory.NewManager()
	t.Cleanup(func() { _ = mgr.Close() })
	if err := mgr.Register("inv", "n", "/tmp/this/path/does/not/exist"); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := mgr.Load(context.Background(), "inv"); err == nil {
		t.Error("Load on a bogus path returned nil, want non-nil")
	}
}
