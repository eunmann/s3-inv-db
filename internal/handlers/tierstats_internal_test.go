package handlers

import (
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/format"
	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

func TestAssembleTierStats_EmptyReturnsNonNilEmpty(t *testing.T) {
	// The helper uses make([]TierStats, len(breakdown)), so a nil or
	// zero-length input must yield a non-nil zero-length slice. Callers
	// then assign to a struct field; JSON's omitempty drops both nil
	// and empty, but in-process callers iterate the slice and must not
	// nil-check.
	got := assembleTierStats(nil)
	if got == nil {
		t.Fatal("assembleTierStats(nil) returned nil; want non-nil empty slice")
	}
	if len(got) != 0 {
		t.Errorf("len = %d, want 0", len(got))
	}
}

func TestAssembleTierStats_PreservesOrder(t *testing.T) {
	in := []format.TierBreakdown{
		{TierName: "STANDARD", TierID: tiers.Standard, ObjectCount: 10, Bytes: 1024},
		{TierName: "GLACIER", TierID: tiers.GlacierFR, ObjectCount: 2, Bytes: 2048},
	}
	got := assembleTierStats(in)
	if len(got) != len(in) {
		t.Fatalf("len = %d, want %d", len(got), len(in))
	}
	for i, want := range in {
		if got[i].TierName != want.TierName {
			t.Errorf("[%d] TierName = %q, want %q", i, got[i].TierName, want.TierName)
		}
		if got[i].ObjectCount != want.ObjectCount {
			t.Errorf("[%d] ObjectCount = %d, want %d", i, got[i].ObjectCount, want.ObjectCount)
		}
		if got[i].Bytes != want.Bytes {
			t.Errorf("[%d] Bytes = %d, want %d", i, got[i].Bytes, want.Bytes)
		}
	}
}

func TestAssembleTierStats_AttachesHumanizedFields(t *testing.T) {
	in := []format.TierBreakdown{
		{TierName: "STANDARD", ObjectCount: 1234, Bytes: 1024 * 1024},
	}
	got := assembleTierStats(in)
	if got[0].ObjectCountH == "" {
		t.Error("ObjectCountH empty; humanfmt should produce a non-empty string")
	}
	if got[0].BytesH == "" {
		t.Error("BytesH empty; humanfmt should produce a non-empty string")
	}
}
