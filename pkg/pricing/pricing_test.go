package pricing

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/format"
	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

func TestComputeMonthlyCost(t *testing.T) {
	pt := DefaultUSEast1Prices()

	// 1 GB of STANDARD storage
	breakdown := []format.TierBreakdown{
		{
			TierID:      tiers.Standard,
			TierName:    "STANDARD",
			Bytes:       1024 * 1024 * 1024, // 1 GB
			ObjectCount: 100,
		},
	}

	cost := ComputeMonthlyCost(breakdown, pt)

	// Expected: 1 GB * $0.023/GB = $0.023 = 23000 microdollars
	expectedMicrodollars := uint64(23000)
	if cost.TotalMicrodollars != expectedMicrodollars {
		t.Errorf("got %d microdollars, expected %d", cost.TotalMicrodollars, expectedMicrodollars)
	}

	// Check per-tier
	if cost.PerTierMicrodollars["STANDARD"] != expectedMicrodollars {
		t.Errorf("STANDARD: got %d, expected %d", cost.PerTierMicrodollars["STANDARD"], expectedMicrodollars)
	}
}

func TestComputeMonthlyCost_MultipleTiers(t *testing.T) {
	pt := DefaultUSEast1Prices()

	// 1 GB STANDARD + 1 GB DEEP_ARCHIVE
	breakdown := []format.TierBreakdown{
		{TierName: "STANDARD", Bytes: 1024 * 1024 * 1024},
		{TierName: "DEEP_ARCHIVE", Bytes: 1024 * 1024 * 1024},
	}

	cost := ComputeMonthlyCost(breakdown, pt)

	// STANDARD: $0.023, DEEP_ARCHIVE: $0.00099
	// Total: ~$0.02399 = 23990 microdollars
	expectedStandard := uint64(23000)
	expectedDeep := uint64(990)

	if cost.PerTierMicrodollars["STANDARD"] != expectedStandard {
		t.Errorf("STANDARD: got %d, expected %d", cost.PerTierMicrodollars["STANDARD"], expectedStandard)
	}
	if cost.PerTierMicrodollars["DEEP_ARCHIVE"] != expectedDeep {
		t.Errorf("DEEP_ARCHIVE: got %d, expected %d", cost.PerTierMicrodollars["DEEP_ARCHIVE"], expectedDeep)
	}

	expectedTotal := expectedStandard + expectedDeep
	if cost.TotalMicrodollars != expectedTotal {
		t.Errorf("Total: got %d, expected %d", cost.TotalMicrodollars, expectedTotal)
	}
}

func TestComputeMonthlyCost_UnknownTier(t *testing.T) {
	pt := DefaultUSEast1Prices()

	breakdown := []format.TierBreakdown{
		{TierName: "UNKNOWN_TIER", Bytes: 1024 * 1024 * 1024},
	}

	cost := ComputeMonthlyCost(breakdown, pt)

	// Unknown tiers should be skipped
	if cost.TotalMicrodollars != 0 {
		t.Errorf("expected 0 for unknown tier, got %d", cost.TotalMicrodollars)
	}
}

func TestComputeMonthlyCost_ZeroBytes(t *testing.T) {
	pt := DefaultUSEast1Prices()

	breakdown := []format.TierBreakdown{
		{TierName: "STANDARD", Bytes: 0},
	}

	cost := ComputeMonthlyCost(breakdown, pt)

	if cost.TotalMicrodollars != 0 {
		t.Errorf("expected 0 for zero bytes, got %d", cost.TotalMicrodollars)
	}
}

func TestTotalDollars(t *testing.T) {
	result := CostResult{
		TotalMicrodollars: 1_000_000, // $1.00
	}

	if result.TotalDollars() != 1.0 {
		t.Errorf("expected $1.00, got $%.2f", result.TotalDollars())
	}
}

func TestPerTierDollars(t *testing.T) {
	result := CostResult{
		PerTierMicrodollars: map[string]uint64{
			"STANDARD": 500_000, // $0.50
		},
	}

	dollars := result.PerTierDollars()
	if dollars["STANDARD"] != 0.5 {
		t.Errorf("expected $0.50, got $%.2f", dollars["STANDARD"])
	}
}

func TestFormatCost(t *testing.T) {
	tests := []struct {
		microdollars uint64
		want         string
	}{
		{0, "$0.000000"},
		{100, "$0.000100"},
		{10_000, "$0.0100"},
		{100_000, "$0.1000"},
		{1_000_000, "$1.00"},
		{50_000_000, "$50.00"},
		{100_000_000, "$100"},
	}

	for _, tt := range tests {
		got := FormatCost(tt.microdollars)
		if got != tt.want {
			t.Errorf("FormatCost(%d) = %q, want %q", tt.microdollars, got, tt.want)
		}
	}
}

func TestLoadSavePriceTable(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "prices.json")

	original := PriceTable{
		PerGBMonth: map[string]float64{
			"STANDARD":     0.023,
			"DEEP_ARCHIVE": 0.001,
		},
	}

	if err := SavePriceTable(path, original); err != nil {
		t.Fatalf("SavePriceTable failed: %v", err)
	}

	loaded, err := LoadPriceTable(path)
	if err != nil {
		t.Fatalf("LoadPriceTable failed: %v", err)
	}

	if loaded.PerGBMonth["STANDARD"] != 0.023 {
		t.Errorf("STANDARD price: got %f, want 0.023", loaded.PerGBMonth["STANDARD"])
	}
	if loaded.PerGBMonth["DEEP_ARCHIVE"] != 0.001 {
		t.Errorf("DEEP_ARCHIVE price: got %f, want 0.001", loaded.PerGBMonth["DEEP_ARCHIVE"])
	}
}

func TestLoadPriceTable_NotExists(t *testing.T) {
	_, err := LoadPriceTable("/nonexistent/path/prices.json")
	if err == nil {
		t.Error("expected error for missing file")
	}
}

func TestDefaultUSEast1Prices_Complete(t *testing.T) {
	pt := DefaultUSEast1Prices()

	// Verify all expected tiers have prices
	expectedTiers := []string{
		"STANDARD",
		"STANDARD_IA",
		"ONEZONE_IA",
		"GLACIER_IR",
		"GLACIER",
		"DEEP_ARCHIVE",
		"REDUCED_REDUNDANCY",
		"INTELLIGENT_TIERING_FREQUENT",
		"INTELLIGENT_TIERING_INFREQUENT",
		"INTELLIGENT_TIERING_ARCHIVE_INSTANT",
		"INTELLIGENT_TIERING_ARCHIVE",
		"INTELLIGENT_TIERING_DEEP_ARCHIVE",
	}

	for _, tier := range expectedTiers {
		if _, ok := pt.PerGBMonth[tier]; !ok {
			t.Errorf("missing price for tier %s", tier)
		}
	}
}

func TestLoadPriceTable_InvalidJSON(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "invalid.json")

	if err := os.WriteFile(path, []byte("not valid json"), 0644); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}

	_, err := LoadPriceTable(path)
	if err == nil {
		t.Error("expected error for invalid JSON")
	}
}
