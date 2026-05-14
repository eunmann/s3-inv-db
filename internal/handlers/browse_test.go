package handlers

import (
	"testing"

	"github.com/eunmann/s3-inv-db/internal/inventory"
)

func TestGroupLoadedInventories_GroupsByConfigAndSortsRunsNewestFirst(t *testing.T) {
	in := []inventory.Info{
		{ID: "bucket-b/inv-1/2026-05-13T03-00Z", State: inventory.StateLoaded},
		{ID: "bucket-a/inv-1/2026-05-10T03-00Z", State: inventory.StateLoaded},
		{ID: "bucket-a/inv-1/2026-05-12T03-00Z", State: inventory.StateLoaded},
		{ID: "bucket-a/inv-2/2026-05-11T03-00Z", State: inventory.StateLoaded},
		{ID: "bucket-a/inv-1/2026-05-09T03-00Z", State: inventory.StateNotLoaded}, // filtered
	}
	got := groupLoadedInventories(in)
	if len(got) != 3 {
		t.Fatalf("len(groups) = %d, want 3", len(got))
	}
	if got[0].ConfigLabel != "bucket-a/inv-1" || got[1].ConfigLabel != "bucket-a/inv-2" || got[2].ConfigLabel != "bucket-b/inv-1" {
		t.Errorf("group order = %q, %q, %q; want bucket-a/inv-1, bucket-a/inv-2, bucket-b/inv-1", got[0].ConfigLabel, got[1].ConfigLabel, got[2].ConfigLabel)
	}
	if len(got[0].Options) != 2 {
		t.Fatalf("bucket-a/inv-1 options = %d, want 2", len(got[0].Options))
	}
	if got[0].Options[0].RunLabel != "2026-05-12T03-00Z" || got[0].Options[1].RunLabel != "2026-05-10T03-00Z" {
		t.Errorf("runs not newest-first: got %q then %q", got[0].Options[0].RunLabel, got[0].Options[1].RunLabel)
	}
	if got[0].Options[0].ID != "bucket-a/inv-1/2026-05-12T03-00Z" {
		t.Errorf("option preserves composite ID: got %q", got[0].Options[0].ID)
	}
}

func TestGroupLoadedInventories_LegacyTwoPartIDFallsBackToOther(t *testing.T) {
	in := []inventory.Info{
		{ID: "old-inv", Name: "Old Inventory", State: inventory.StateLoaded},
	}
	got := groupLoadedInventories(in)
	if len(got) != 1 || got[0].ConfigLabel != "Other" {
		t.Fatalf("groups = %+v, want one 'Other' group", got)
	}
	if got[0].Options[0].RunLabel != "Old Inventory" {
		t.Errorf("RunLabel = %q, want fallback to Name 'Old Inventory'", got[0].Options[0].RunLabel)
	}
}

func TestGroupLoadedInventories_Empty(t *testing.T) {
	if got := groupLoadedInventories(nil); len(got) != 0 {
		t.Errorf("nil → groups = %+v, want empty", got)
	}
	if got := groupLoadedInventories([]inventory.Info{{ID: "x/y/z", State: inventory.StateNotLoaded}}); len(got) != 0 {
		t.Errorf("no loaded → groups = %+v, want empty", got)
	}
}
