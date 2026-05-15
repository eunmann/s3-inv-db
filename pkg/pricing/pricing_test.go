package pricing_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/format"
	"github.com/eunmann/s3-inv-db/pkg/pricing"
	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

func TestComputeMonthlyCost_StandardStorage(t *testing.T) {
	pt := pricing.DefaultUSEast1Prices()

	// 1 GB of STANDARD storage with large objects (no penalties).
	breakdown := []format.TierBreakdown{
		{
			TierID:      tiers.Standard,
			TierName:    "STANDARD",
			Bytes:       1024 * 1024 * 1024, // 1 GB
			ObjectCount: 10,                 // 100MB average = well above 128KB
		},
	}

	cost := pricing.ComputeMonthlyCost(breakdown, pt)

	// Expected: 1 GB * $0.023/GB = $0.023 = 23000 microdollars.
	expectedMicrodollars := uint64(23000)
	if cost.TotalMicrodollars != expectedMicrodollars {
		t.Errorf("got %d microdollars, expected %d", cost.TotalMicrodollars, expectedMicrodollars)
	}

	if cost.MonitoringMicrodollars != 0 {
		t.Errorf("expected no monitoring cost for STANDARD, got %d", cost.MonitoringMicrodollars)
	}
	if cost.MinObjectSizeMicrodollars != 0 {
		t.Errorf("expected no min object size penalty for STANDARD, got %d", cost.MinObjectSizeMicrodollars)
	}
}

func TestComputeMonthlyCost_StandardIA_MinObjectSize(t *testing.T) {
	pt := pricing.DefaultUSEast1Prices()

	breakdown := []format.TierBreakdown{
		{
			TierName:    "STANDARD_IA",
			Bytes:       10 * 1024 * 1000,
			ObjectCount: 1000,
		},
	}

	cost := pricing.ComputeMonthlyCost(breakdown, pt)

	if cost.MinObjectSizeMicrodollars == 0 {
		t.Error("expected min object size penalty for small objects in STANDARD_IA")
	}

	if cost.TotalMicrodollars <= cost.PerTierMicrodollars["STANDARD_IA"]-cost.MinObjectSizeMicrodollars {
		t.Error("total should include min object size penalty")
	}
}

func TestComputeMonthlyCost_IntelligentTiering_Monitoring(t *testing.T) {
	pt := pricing.DefaultUSEast1Prices()

	breakdown := []format.TierBreakdown{
		{
			TierName:    "INTELLIGENT_TIERING_FREQUENT",
			Bytes:       1024 * 1024 * 1000,
			ObjectCount: 1000,
		},
	}

	cost := pricing.ComputeMonthlyCost(breakdown, pt)

	expectedMonitoring := uint64(2500)
	if cost.MonitoringMicrodollars != expectedMonitoring {
		t.Errorf("got monitoring %d, expected %d", cost.MonitoringMicrodollars, expectedMonitoring)
	}

	if cost.TotalMicrodollars < cost.MonitoringMicrodollars {
		t.Error("total should include monitoring cost")
	}
}

func TestComputeMonthlyCost_IntelligentTiering_SmallObjectsAreUnmonitored(t *testing.T) {
	pt := pricing.DefaultUSEast1Prices()

	breakdown := []format.TierBreakdown{
		{
			TierName:    "INTELLIGENT_TIERING_FREQUENT_SMALL",
			Bytes:       50 * 1024 * 1000,
			ObjectCount: 1000,
		},
	}

	cost := pricing.ComputeMonthlyCost(breakdown, pt)

	if cost.MonitoringMicrodollars != 0 {
		t.Errorf("small-IT monitoring cost = %d, want 0", cost.MonitoringMicrodollars)
	}
}

func TestComputeMonthlyCost_IntelligentTiering_MonitoringPerObject(t *testing.T) {
	pt := pricing.DefaultUSEast1Prices()

	breakdown := []format.TierBreakdown{
		{TierName: "INTELLIGENT_TIERING_FREQUENT", Bytes: 200 * 1024 * 1024, ObjectCount: 2000},
	}
	cost := pricing.ComputeMonthlyCost(breakdown, pt)
	if cost.MonitoringMicrodollars != 5000 {
		t.Errorf("monitoring = %d, want 5000", cost.MonitoringMicrodollars)
	}
}

func TestComputePutCost(t *testing.T) {
	pt := pricing.DefaultUSEast1Prices() // $0.005 per 1000 PUTs.
	cases := []struct {
		name  string
		count uint64
		want  uint64
	}{
		{"zero", 0, 0},
		{"one thousand", 1000, 5000},
		{"five thousand", 5_000, 25_000},
		{"odd count", 1234, uint64(float64(1234) / 1000.0 * 0.005 * 1_000_000)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := pricing.ComputePutCost(tc.count, pt); got != tc.want {
				t.Errorf("ComputePutCost(%d) = %d, want %d", tc.count, got, tc.want)
			}
		})
	}
}

func TestComputeMonthlyCost_Glacier_Overhead(t *testing.T) {
	pt := pricing.DefaultUSEast1Prices()

	breakdown := []format.TierBreakdown{
		{
			TierName:    "GLACIER",
			Bytes:       100 * 1024 * 1024,
			ObjectCount: 100,
		},
	}

	cost := pricing.ComputeMonthlyCost(breakdown, pt)

	if cost.GlacierOverheadMicrodollars == 0 {
		t.Error("expected glacier overhead for GLACIER objects")
	}
}

func TestComputeMonthlyCost_DeepArchive_Overhead(t *testing.T) {
	pt := pricing.DefaultUSEast1Prices()

	breakdown := []format.TierBreakdown{
		{
			TierName:    "DEEP_ARCHIVE",
			Bytes:       1024 * 1024 * 1024,
			ObjectCount: 1000,
		},
	}

	cost := pricing.ComputeMonthlyCost(breakdown, pt)

	if cost.GlacierOverheadMicrodollars == 0 {
		t.Error("expected glacier overhead for DEEP_ARCHIVE objects")
	}
}

func TestComputeMonthlyCost_MultipleTiers(t *testing.T) {
	pt := pricing.DefaultUSEast1Prices()

	breakdown := []format.TierBreakdown{
		{TierName: "STANDARD", Bytes: 1024 * 1024 * 1024, ObjectCount: 100},
		{TierName: "DEEP_ARCHIVE", Bytes: 1024 * 1024 * 1024, ObjectCount: 1000},
	}

	cost := pricing.ComputeMonthlyCost(breakdown, pt)

	if _, ok := cost.PerTierMicrodollars["STANDARD"]; !ok {
		t.Error("expected STANDARD in per-tier costs")
	}
	if _, ok := cost.PerTierMicrodollars["DEEP_ARCHIVE"]; !ok {
		t.Error("expected DEEP_ARCHIVE in per-tier costs")
	}

	perTierSum := uint64(0)
	for _, v := range cost.PerTierMicrodollars {
		perTierSum += v
	}
	expectedTotal := perTierSum + cost.MonitoringMicrodollars
	if cost.TotalMicrodollars != expectedTotal {
		t.Errorf("total %d != per-tier sum %d + monitoring %d", cost.TotalMicrodollars, perTierSum, cost.MonitoringMicrodollars)
	}
}

func TestComputeMonthlyCost_UnknownTier(t *testing.T) {
	pt := pricing.DefaultUSEast1Prices()

	breakdown := []format.TierBreakdown{
		{TierName: "UNKNOWN_TIER", Bytes: 1024 * 1024 * 1024, ObjectCount: 100},
	}

	cost := pricing.ComputeMonthlyCost(breakdown, pt)

	if cost.TotalMicrodollars != 0 {
		t.Errorf("expected 0 for unknown tier, got %d", cost.TotalMicrodollars)
	}
}

func TestComputeMonthlyCost_ZeroBytes(t *testing.T) {
	pt := pricing.DefaultUSEast1Prices()

	breakdown := []format.TierBreakdown{
		{TierName: "STANDARD", Bytes: 0, ObjectCount: 0},
	}

	cost := pricing.ComputeMonthlyCost(breakdown, pt)

	if cost.TotalMicrodollars != 0 {
		t.Errorf("expected 0 for zero bytes, got %d", cost.TotalMicrodollars)
	}
}

func TestComputeDetailedBreakdown(t *testing.T) {
	pt := pricing.DefaultUSEast1Prices()

	breakdown := []format.TierBreakdown{
		{TierName: "STANDARD_IA", Bytes: 10 * 1024 * 1000, ObjectCount: 1000},
		{TierName: "INTELLIGENT_TIERING_FREQUENT", Bytes: 1024 * 1024 * 100, ObjectCount: 100},
		{TierName: "GLACIER", Bytes: 1024 * 1024 * 1024, ObjectCount: 500},
	}

	detailed := pricing.ComputeDetailedBreakdown(breakdown, pt)

	if len(detailed) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(detailed))
	}

	for _, cb := range detailed {
		if cb.TierName == "STANDARD_IA" {
			if cb.MinSizePenalty == 0 {
				t.Error("expected min size penalty for STANDARD_IA with small objects")
			}
			if cb.AvgObjectSizeBytes != 10*1024 {
				t.Errorf("expected avg size 10KB, got %d", cb.AvgObjectSizeBytes)
			}
		}
		if cb.TierName == "INTELLIGENT_TIERING_FREQUENT" {
			if cb.MonitoringCost == 0 {
				t.Error("expected monitoring cost for IT tier")
			}
		}
		if cb.TierName == "GLACIER" {
			if cb.GlacierOverhead == 0 {
				t.Error("expected glacier overhead for GLACIER tier")
			}
		}
	}
}

func TestTotalDollars(t *testing.T) {
	result := pricing.CostResult{
		TotalMicrodollars: 1_000_000,
	}

	if result.TotalDollars() != 1.0 {
		t.Errorf("expected $1.00, got $%.2f", result.TotalDollars())
	}
}

func TestPerTierDollars(t *testing.T) {
	result := pricing.CostResult{
		PerTierMicrodollars: map[string]uint64{
			"STANDARD": 500_000,
		},
	}

	dollars := result.PerTierDollars()
	if dollars["STANDARD"] != 0.5 {
		t.Errorf("expected $0.50, got $%.2f", dollars["STANDARD"])
	}
}

func TestFormatCost(t *testing.T) {
	tests := []struct {
		name         string
		microdollars uint64
		want         string
	}{
		{"zero", 0, "$0.00"},
		{"sub-penny, just-above-zero", 1, "<$0.01"},
		{"sub-penny, just-below-cent", 9_999, "<$0.01"},
		{"exactly one cent", 10_000, "$0.01"},
		{"between cents rounds up", 10_001, "$0.02"},
		{"$0.99", 990_000, "$0.99"},
		{"$0.999 rounds up to $1.00", 999_000, "$1.00"},
		{"$1 even", 1_000_000, "$1.00"},
		{"$5.50", 5_500_000, "$5.50"},
		{"$50.12 even", 50_120_000, "$50.12"},
		{"$50.121 rounds to $50.13", 50_121_000, "$50.13"},
		{"$999.99", 999_990_000, "$999.99"},
		{"$1,000 -> $1.0K", 1_000_000_000, "$1.0K"},
		{"$1,234 -> $1.2K", 1_234_000_000, "$1.2K"},
		{"$1,250 -> $1.3K (round half up)", 1_250_000_000, "$1.3K"},
		{"$12,500 -> $12.5K", 12_500_000_000, "$12.5K"},
		{"$999,999 -> $1000.0K", 999_999_000_000, "$1000.0K"},
		{"$1,000,000 -> $1.0M", 1_000_000_000_000, "$1.0M"},
		{"$1,500,000 -> $1.5M", 1_500_000_000_000, "$1.5M"},
		{"$1B -> $1.0B", 1_000_000_000_000_000, "$1.0B"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := pricing.FormatCost(tt.microdollars)
			if got != tt.want {
				t.Errorf("FormatCost(%d) = %q, want %q", tt.microdollars, got, tt.want)
			}
		})
	}
}

func TestLoadSavePriceTable(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "prices.json")

	original := pricing.PriceTable{
		PerGBMonth: map[string]float64{
			"STANDARD":     0.023,
			"DEEP_ARCHIVE": 0.001,
		},
		MonitoringPer1000Objects: 0.0025,
		StandardPricePerGB:       0.023,
	}

	if err := pricing.SavePriceTable(path, original); err != nil {
		t.Fatalf("SavePriceTable failed: %v", err)
	}

	loaded, err := pricing.LoadPriceTable(path)
	if err != nil {
		t.Fatalf("LoadPriceTable failed: %v", err)
	}

	if loaded.PerGBMonth["STANDARD"] != 0.023 {
		t.Errorf("STANDARD price: got %f, want 0.023", loaded.PerGBMonth["STANDARD"])
	}
	if loaded.MonitoringPer1000Objects != 0.0025 {
		t.Errorf("Monitoring: got %f, want 0.0025", loaded.MonitoringPer1000Objects)
	}
}

func TestLoadPriceTable_NotExists(t *testing.T) {
	_, err := pricing.LoadPriceTable("/nonexistent/path/prices.json")
	if err == nil {
		t.Error("expected error for missing file")
	}
}

func TestDefaultUSEast1Prices_Complete(t *testing.T) {
	pt := pricing.DefaultUSEast1Prices()

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

	if pt.MonitoringPer1000Objects == 0 {
		t.Error("expected non-zero monitoring cost")
	}

	if pt.StandardPricePerGB == 0 {
		t.Error("expected non-zero standard price for overhead")
	}
}

func TestLoadPriceTable_InvalidJSON(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "invalid.json")

	if err := os.WriteFile(path, []byte("not valid json"), 0o600); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}

	_, err := pricing.LoadPriceTable(path)
	if err == nil {
		t.Error("expected error for invalid JSON")
	}
}

func TestTierHasMinObjectSize(t *testing.T) {
	expectedTiers := []string{"STANDARD_IA", "ONEZONE_IA", "GLACIER_IR"}
	for _, tier := range expectedTiers {
		if !pricing.TierHasMinObjectSize(tier) {
			t.Errorf("expected %s to have min object size", tier)
		}
	}

	if pricing.TierHasMinObjectSize("STANDARD") {
		t.Error("STANDARD should not have min object size")
	}
	if pricing.TierHasMinObjectSize("GLACIER") {
		t.Error("GLACIER should not have min object size penalty (only overhead)")
	}
}

func TestTierHasMonitoringCost(t *testing.T) {
	itTiers := []string{
		"INTELLIGENT_TIERING_FREQUENT",
		"INTELLIGENT_TIERING_INFREQUENT",
		"INTELLIGENT_TIERING_ARCHIVE_INSTANT",
		"INTELLIGENT_TIERING_ARCHIVE",
		"INTELLIGENT_TIERING_DEEP_ARCHIVE",
	}

	for _, tier := range itTiers {
		if !pricing.TierHasMonitoringCost(tier) {
			t.Errorf("expected %s to have monitoring cost", tier)
		}
	}

	if pricing.TierHasMonitoringCost("STANDARD") {
		t.Error("STANDARD should not have monitoring cost")
	}
}

func TestTierHasGlacierOverhead(t *testing.T) {
	glacierTiers := []string{
		"GLACIER",
		"DEEP_ARCHIVE",
		"INTELLIGENT_TIERING_ARCHIVE",
		"INTELLIGENT_TIERING_DEEP_ARCHIVE",
	}

	for _, tier := range glacierTiers {
		if !pricing.TierHasGlacierOverhead(tier) {
			t.Errorf("expected %s to have glacier overhead", tier)
		}
	}

	if pricing.TierHasGlacierOverhead("GLACIER_IR") {
		t.Error("GLACIER_IR should not have glacier overhead")
	}
}
