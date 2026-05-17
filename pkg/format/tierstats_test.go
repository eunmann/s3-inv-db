package format_test

import (
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/format"
)

// TestTierStatsReader_MissingDir verifies OpenTierStats returns a
// usable empty reader (HasTierData() == false) when no tier data
// exists at the given index directory. Row-format read/write
// behaviour is covered by extsort.TestTierStatsRow_PreorderAlignment.
func TestTierStatsReader_MissingDir(t *testing.T) {
	dir := t.TempDir()

	reader, err := format.OpenTierStats(dir)
	if err != nil {
		t.Fatalf("OpenTierStats: %v", err)
	}
	if reader == nil {
		t.Fatal("expected non-nil reader when tiers.json is absent")
	}
	defer reader.Close()
	if reader.HasTierData() {
		t.Error("HasTierData should be false when tiers.json absent")
	}
}
