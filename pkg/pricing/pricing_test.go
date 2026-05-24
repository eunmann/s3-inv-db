package pricing_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/format"
	"github.com/eunmann/s3-inv-db/pkg/pricing"
	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

// TestComputeMonthlyCost_NegativePriceDoesNotWrap guards against a
// uint64 wrap when a malformed price table feeds a negative price into
// the float-to-microdollars conversion. Uint64(negative_float) wraps to
// a value near math.MaxUint64 and used to surface as a wildly inflated
// cost. Negative prices must clamp to zero, not explode.
func TestComputeMonthlyCost_NegativePriceDoesNotWrap(t *testing.T) {
	pt := pricing.PriceTable{
		PerGBMonth: map[string]float64{
			"STANDARD": -0.023, // corrupted price
		},
	}
	breakdown := []format.TierBreakdown{
		{
			TierID:      tiers.Standard,
			TierName:    "STANDARD",
			Bytes:       1024 * 1024 * 1024,
			ObjectCount: 10,
		},
	}

	cost := pricing.ComputeMonthlyCost(breakdown, pt)

	// 23 cents of "real" cost; a wrap would put the total near
	// math.MaxUint64 (~1.8e19). Anything above ~1e18 is the wrap.
	const wrapFloor = uint64(1) << 60
	if cost.TotalMicrodollars >= wrapFloor {
		t.Errorf("TotalMicrodollars = %d looks like a uint64 wrap (>=2^60); want clamped to 0", cost.TotalMicrodollars)
	}
}

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

func TestFormatCost(t *testing.T) {
	tests := []struct {
		name         string
		want         string
		microdollars uint64
	}{
		{name: "zero", microdollars: 0, want: "$0.00"},
		{name: "sub-penny, just-above-zero", microdollars: 1, want: "<$0.01"},
		{name: "sub-penny, just-below-cent", microdollars: 9_999, want: "<$0.01"},
		{name: "exactly one cent", microdollars: 10_000, want: "$0.01"},
		{name: "between cents rounds up", microdollars: 10_001, want: "$0.02"},
		{name: "$0.99", microdollars: 990_000, want: "$0.99"},
		{name: "$0.999 rounds up to $1.00", microdollars: 999_000, want: "$1.00"},
		{name: "$1 even", microdollars: 1_000_000, want: "$1.00"},
		{name: "$5.50", microdollars: 5_500_000, want: "$5.50"},
		{name: "$50.12 even", microdollars: 50_120_000, want: "$50.12"},
		{name: "$50.121 rounds to $50.13", microdollars: 50_121_000, want: "$50.13"},
		{name: "$999.99", microdollars: 999_990_000, want: "$999.99"},
		{name: "$1,000 -> $1.0K", microdollars: 1_000_000_000, want: "$1.0K"},
		{name: "$1,234 -> $1.2K", microdollars: 1_234_000_000, want: "$1.2K"},
		{name: "$1,250 -> $1.3K (round half up)", microdollars: 1_250_000_000, want: "$1.3K"},
		{name: "$12,500 -> $12.5K", microdollars: 12_500_000_000, want: "$12.5K"},
		{name: "$999,999 -> $1000.0K", microdollars: 999_999_000_000, want: "$1000.0K"},
		{name: "$1,000,000 -> $1.0M", microdollars: 1_000_000_000_000, want: "$1.0M"},
		{name: "$1,500,000 -> $1.5M", microdollars: 1_500_000_000_000, want: "$1.5M"},
		{name: "$1B -> $1.0B", microdollars: 1_000_000_000_000_000, want: "$1.0B"},
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

func TestLoadPriceTable(t *testing.T) {
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
	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
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
