package pricing

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/format"
	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

func TestComputeMonthlyCost_StandardStorage(t *testing.T) {
	pt := DefaultUSEast1Prices()

	// 1 GB of STANDARD storage with large objects (no penalties)
	breakdown := []format.TierBreakdown{
		{
			TierID:      tiers.Standard,
			TierName:    "STANDARD",
			Bytes:       1024 * 1024 * 1024, // 1 GB
			ObjectCount: 10,                 // 100MB average = well above 128KB
		},
	}

	cost := ComputeMonthlyCost(breakdown, pt)

	// Expected: 1 GB * $0.023/GB = $0.023 = 23000 microdollars
	expectedMicrodollars := uint64(23000)
	if cost.TotalMicrodollars != expectedMicrodollars {
		t.Errorf("got %d microdollars, expected %d", cost.TotalMicrodollars, expectedMicrodollars)
	}

	// No monitoring, min size, or glacier overhead for STANDARD
	if cost.MonitoringMicrodollars != 0 {
		t.Errorf("expected no monitoring cost for STANDARD, got %d", cost.MonitoringMicrodollars)
	}
	if cost.MinObjectSizeMicrodollars != 0 {
		t.Errorf("expected no min object size penalty for STANDARD, got %d", cost.MinObjectSizeMicrodollars)
	}
}

func TestComputeMonthlyCost_StandardIA_MinObjectSize(t *testing.T) {
	pt := DefaultUSEast1Prices()

	// Small objects in Standard-IA (should trigger 128KB minimum penalty)
	// 1000 objects, 10KB each = 10MB total
	// Each object is charged as 128KB instead of 10KB
	breakdown := []format.TierBreakdown{
		{
			TierName:    "STANDARD_IA",
			Bytes:       10 * 1024 * 1000, // 10KB * 1000 objects = 10MB
			ObjectCount: 1000,             // Average 10KB per object
		},
	}

	cost := ComputeMonthlyCost(breakdown, pt)

	// Base storage: 10MB = 0.009765625 GB * $0.0125 = ~$0.000122
	// Min size penalty: (128KB - 10KB) * 1000 = 118MB additional
	// 118MB = 0.115234375 GB * $0.0125 = ~$0.00144
	// Total should include both base + penalty

	if cost.MinObjectSizeMicrodollars == 0 {
		t.Error("expected min object size penalty for small objects in STANDARD_IA")
	}

	// Verify penalty is added to total
	if cost.TotalMicrodollars <= cost.PerTierMicrodollars["STANDARD_IA"]-cost.MinObjectSizeMicrodollars {
		t.Error("total should include min object size penalty")
	}
}

func TestComputeMonthlyCost_IntelligentTiering_Monitoring(t *testing.T) {
	pt := DefaultUSEast1Prices()

	// 1000 objects in IT Frequent Access, each 1MB (above 128KB threshold)
	breakdown := []format.TierBreakdown{
		{
			TierName:    "INTELLIGENT_TIERING_FREQUENT",
			Bytes:       1024 * 1024 * 1000, // 1MB * 1000 = 1GB
			ObjectCount: 1000,               // Average 1MB per object
		},
	}

	cost := ComputeMonthlyCost(breakdown, pt)

	// Monitoring: 1000 objects / 1000 * $0.0025 = $0.0025 = 2500 microdollars
	expectedMonitoring := uint64(2500)
	if cost.MonitoringMicrodollars != expectedMonitoring {
		t.Errorf("got monitoring %d, expected %d", cost.MonitoringMicrodollars, expectedMonitoring)
	}

	// Total should include monitoring
	if cost.TotalMicrodollars < cost.MonitoringMicrodollars {
		t.Error("total should include monitoring cost")
	}
}

func TestComputeMonthlyCost_IntelligentTiering_SmallObjects(t *testing.T) {
	pt := DefaultUSEast1Prices()

	// Small objects in IT (below 128KB - no monitoring fees for these)
	breakdown := []format.TierBreakdown{
		{
			TierName:    "INTELLIGENT_TIERING_FREQUENT",
			Bytes:       50 * 1024 * 1000, // 50KB * 1000 = 50MB
			ObjectCount: 1000,             // Average 50KB per object
		},
	}

	cost := ComputeMonthlyCost(breakdown, pt)

	// With average size < 128KB, we estimate only half are monitored (conservative)
	// 500 objects / 1000 * $0.0025 = $0.00125 = 1250 microdollars
	expectedMonitoring := uint64(1250)
	if cost.MonitoringMicrodollars != expectedMonitoring {
		t.Errorf("got monitoring %d, expected %d", cost.MonitoringMicrodollars, expectedMonitoring)
	}
}

func TestComputeMonthlyCost_Glacier_Overhead(t *testing.T) {
	pt := DefaultUSEast1Prices()

	// 100 objects in Glacier (should have 32KB + 8KB overhead per object)
	breakdown := []format.TierBreakdown{
		{
			TierName:    "GLACIER",
			Bytes:       100 * 1024 * 1024, // 100MB total
			ObjectCount: 100,               // 1MB average per object
		},
	}

	cost := ComputeMonthlyCost(breakdown, pt)

	// Glacier overhead per object: 32KB at $0.0036/GB + 8KB at $0.023/GB
	// 100 objects * 32KB = 3.2MB at Glacier rate
	// 100 objects * 8KB = 0.8MB at Standard rate
	if cost.GlacierOverheadMicrodollars == 0 {
		t.Error("expected glacier overhead for GLACIER objects")
	}
}

func TestComputeMonthlyCost_DeepArchive_Overhead(t *testing.T) {
	pt := DefaultUSEast1Prices()

	// 1000 objects in Deep Archive
	breakdown := []format.TierBreakdown{
		{
			TierName:    "DEEP_ARCHIVE",
			Bytes:       1024 * 1024 * 1024, // 1GB
			ObjectCount: 1000,
		},
	}

	cost := ComputeMonthlyCost(breakdown, pt)

	// Deep Archive should have overhead
	if cost.GlacierOverheadMicrodollars == 0 {
		t.Error("expected glacier overhead for DEEP_ARCHIVE objects")
	}

	// Base storage is very cheap: 1GB * $0.00099/GB = $0.00099
	// But overhead adds: 1000 * (32KB * $0.00099/GB + 8KB * $0.023/GB)
	// The overhead should be significant relative to the base cost
}

func TestComputeMonthlyCost_MultipleTiers(t *testing.T) {
	pt := DefaultUSEast1Prices()

	breakdown := []format.TierBreakdown{
		{TierName: "STANDARD", Bytes: 1024 * 1024 * 1024, ObjectCount: 100},
		{TierName: "DEEP_ARCHIVE", Bytes: 1024 * 1024 * 1024, ObjectCount: 1000},
	}

	cost := ComputeMonthlyCost(breakdown, pt)

	// Should have per-tier costs
	if _, ok := cost.PerTierMicrodollars["STANDARD"]; !ok {
		t.Error("expected STANDARD in per-tier costs")
	}
	if _, ok := cost.PerTierMicrodollars["DEEP_ARCHIVE"]; !ok {
		t.Error("expected DEEP_ARCHIVE in per-tier costs")
	}

	// Total should be sum of per-tier + monitoring
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
	pt := DefaultUSEast1Prices()

	breakdown := []format.TierBreakdown{
		{TierName: "UNKNOWN_TIER", Bytes: 1024 * 1024 * 1024, ObjectCount: 100},
	}

	cost := ComputeMonthlyCost(breakdown, pt)

	if cost.TotalMicrodollars != 0 {
		t.Errorf("expected 0 for unknown tier, got %d", cost.TotalMicrodollars)
	}
}

func TestComputeMonthlyCost_ZeroBytes(t *testing.T) {
	pt := DefaultUSEast1Prices()

	breakdown := []format.TierBreakdown{
		{TierName: "STANDARD", Bytes: 0, ObjectCount: 0},
	}

	cost := ComputeMonthlyCost(breakdown, pt)

	if cost.TotalMicrodollars != 0 {
		t.Errorf("expected 0 for zero bytes, got %d", cost.TotalMicrodollars)
	}
}

func TestComputeDetailedBreakdown(t *testing.T) {
	pt := DefaultUSEast1Prices()

	breakdown := []format.TierBreakdown{
		{
			TierName:    "STANDARD_IA",
			Bytes:       10 * 1024 * 1000, // 10KB * 1000 = 10MB
			ObjectCount: 1000,
		},
		{
			TierName:    "INTELLIGENT_TIERING_FREQUENT",
			Bytes:       1024 * 1024 * 100, // 100MB
			ObjectCount: 100,
		},
		{
			TierName:    "GLACIER",
			Bytes:       1024 * 1024 * 1024, // 1GB
			ObjectCount: 500,
		},
	}

	detailed := ComputeDetailedBreakdown(breakdown, pt)

	if len(detailed) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(detailed))
	}

	// Check STANDARD_IA has min size penalty
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

func TestFormatCostDollars(t *testing.T) {
	tests := []struct {
		dollars float64
		want    string
	}{
		{0.0001, "$0.000100"},
		{0.05, "$0.0500"},
		{5.50, "$5.50"},
		{150.0, "$150"},
	}

	for _, tt := range tests {
		got := FormatCostDollars(tt.dollars)
		if got != tt.want {
			t.Errorf("FormatCostDollars(%f) = %q, want %q", tt.dollars, got, tt.want)
		}
	}
}

func TestFormatBytes(t *testing.T) {
	tests := []struct {
		bytes uint64
		want  string
	}{
		{500, "500 B"},
		{2048, "2.00 KB"},
		{1024 * 1024 * 5, "5.00 MB"},
		{1024 * 1024 * 1024 * 2, "2.00 GB"},
		{1024 * 1024 * 1024 * 1024 * 3, "3.00 TB"},
	}

	for _, tt := range tests {
		got := FormatBytes(tt.bytes)
		if got != tt.want {
			t.Errorf("FormatBytes(%d) = %q, want %q", tt.bytes, got, tt.want)
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
		MonitoringPer1000Objects: 0.0025,
		StandardPricePerGB:       0.023,
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
	if loaded.MonitoringPer1000Objects != 0.0025 {
		t.Errorf("Monitoring: got %f, want 0.0025", loaded.MonitoringPer1000Objects)
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

	// Verify monitoring cost is set
	if pt.MonitoringPer1000Objects == 0 {
		t.Error("expected non-zero monitoring cost")
	}

	// Verify standard price for overhead calculation
	if pt.StandardPricePerGB == 0 {
		t.Error("expected non-zero standard price for overhead")
	}
}

func TestLoadPriceTable_InvalidJSON(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "invalid.json")

	if err := os.WriteFile(path, []byte("not valid json"), 0o644); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}

	_, err := LoadPriceTable(path)
	if err == nil {
		t.Error("expected error for invalid JSON")
	}
}

func TestTiersWithMinObjectSize(t *testing.T) {
	// Verify the right tiers have min object size
	expectedTiers := []string{"STANDARD_IA", "ONEZONE_IA", "GLACIER_IR"}
	for _, tier := range expectedTiers {
		if !TiersWithMinObjectSize[tier] {
			t.Errorf("expected %s to have min object size", tier)
		}
	}

	// STANDARD and GLACIER should not have min object size
	if TiersWithMinObjectSize["STANDARD"] {
		t.Error("STANDARD should not have min object size")
	}
	if TiersWithMinObjectSize["GLACIER"] {
		t.Error("GLACIER should not have min object size penalty (only overhead)")
	}
}

func TestTiersWithMonitoringCost(t *testing.T) {
	itTiers := []string{
		"INTELLIGENT_TIERING_FREQUENT",
		"INTELLIGENT_TIERING_INFREQUENT",
		"INTELLIGENT_TIERING_ARCHIVE_INSTANT",
		"INTELLIGENT_TIERING_ARCHIVE",
		"INTELLIGENT_TIERING_DEEP_ARCHIVE",
	}

	for _, tier := range itTiers {
		if !TiersWithMonitoringCost[tier] {
			t.Errorf("expected %s to have monitoring cost", tier)
		}
	}

	// Non-IT tiers should not have monitoring
	if TiersWithMonitoringCost["STANDARD"] {
		t.Error("STANDARD should not have monitoring cost")
	}
}

func TestTiersWithGlacierOverhead(t *testing.T) {
	glacierTiers := []string{
		"GLACIER",
		"DEEP_ARCHIVE",
		"INTELLIGENT_TIERING_ARCHIVE",
		"INTELLIGENT_TIERING_DEEP_ARCHIVE",
	}

	for _, tier := range glacierTiers {
		if !TiersWithGlacierOverhead[tier] {
			t.Errorf("expected %s to have glacier overhead", tier)
		}
	}

	// Glacier IR should NOT have overhead (only min size)
	if TiersWithGlacierOverhead["GLACIER_IR"] {
		t.Error("GLACIER_IR should not have glacier overhead")
	}
}
