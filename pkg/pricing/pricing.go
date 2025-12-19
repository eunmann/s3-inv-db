// Package pricing provides cost estimation for S3 storage tiers.
package pricing

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/eunmann/s3-inv-db/pkg/format"
)

// PriceTable contains per-GB-month pricing for each storage tier.
type PriceTable struct {
	// PerGBMonth maps tier names to USD per GB per month.
	PerGBMonth map[string]float64 `json:"per_gb_month"`
}

// DefaultUSEast1Prices returns the default pricing for US East 1 region (as of 2025).
// These are approximate prices and should be updated regularly.
func DefaultUSEast1Prices() PriceTable {
	return PriceTable{
		PerGBMonth: map[string]float64{
			"STANDARD":                            0.023,
			"STANDARD_IA":                         0.0125,
			"ONEZONE_IA":                          0.01,
			"GLACIER_IR":                          0.004,
			"GLACIER":                             0.0036,
			"DEEP_ARCHIVE":                        0.00099,
			"REDUCED_REDUNDANCY":                  0.024, // Deprecated, same as STANDARD
			"INTELLIGENT_TIERING_FREQUENT":        0.023,
			"INTELLIGENT_TIERING_INFREQUENT":      0.0125,
			"INTELLIGENT_TIERING_ARCHIVE_INSTANT": 0.004,
			"INTELLIGENT_TIERING_ARCHIVE":         0.0036,
			"INTELLIGENT_TIERING_DEEP_ARCHIVE":    0.00099,
		},
	}
}

// LoadPriceTable loads a price table from a JSON file.
func LoadPriceTable(path string) (PriceTable, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return PriceTable{}, fmt.Errorf("read price table: %w", err)
	}

	var pt PriceTable
	if err := json.Unmarshal(data, &pt); err != nil {
		return PriceTable{}, fmt.Errorf("parse price table: %w", err)
	}

	return pt, nil
}

// SavePriceTable saves a price table to a JSON file.
func SavePriceTable(path string, pt PriceTable) error {
	data, err := json.MarshalIndent(pt, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal price table: %w", err)
	}

	if err := os.WriteFile(path, data, 0644); err != nil {
		return fmt.Errorf("write price table: %w", err)
	}

	return nil
}

// CostResult contains the estimated monthly storage costs.
type CostResult struct {
	// TotalMicrodollars is the total cost in microdollars (1 USD = 1,000,000 microdollars).
	TotalMicrodollars uint64
	// PerTierMicrodollars maps tier names to their cost in microdollars.
	PerTierMicrodollars map[string]uint64
}

// TotalDollars returns the total cost in dollars.
func (r CostResult) TotalDollars() float64 {
	return float64(r.TotalMicrodollars) / 1_000_000
}

// PerTierDollars returns per-tier costs in dollars.
func (r CostResult) PerTierDollars() map[string]float64 {
	result := make(map[string]float64, len(r.PerTierMicrodollars))
	for tier, microdollars := range r.PerTierMicrodollars {
		result[tier] = float64(microdollars) / 1_000_000
	}
	return result
}

const bytesPerGB = 1024 * 1024 * 1024

// ComputeMonthlyCost calculates the monthly storage cost for a tier breakdown.
func ComputeMonthlyCost(breakdown []format.TierBreakdown, pt PriceTable) CostResult {
	result := CostResult{
		PerTierMicrodollars: make(map[string]uint64, len(breakdown)),
	}

	for _, tb := range breakdown {
		price, ok := pt.PerGBMonth[tb.TierName]
		if !ok {
			// Skip unknown tiers (no price available)
			continue
		}

		// Calculate cost in microdollars
		// cost = (bytes / bytesPerGB) * pricePerGB * 1_000_000
		// To avoid floating point issues, we compute:
		// cost_microdollars = bytes * pricePerGB * 1_000_000 / bytesPerGB
		gbFrac := float64(tb.Bytes) / float64(bytesPerGB)
		microdollars := uint64(gbFrac * price * 1_000_000)

		result.PerTierMicrodollars[tb.TierName] = microdollars
		result.TotalMicrodollars += microdollars
	}

	return result
}

// FormatCost formats a cost in microdollars as a human-readable string.
func FormatCost(microdollars uint64) string {
	dollars := float64(microdollars) / 1_000_000

	switch {
	case dollars < 0.01:
		return fmt.Sprintf("$%.6f", dollars)
	case dollars < 1:
		return fmt.Sprintf("$%.4f", dollars)
	case dollars < 100:
		return fmt.Sprintf("$%.2f", dollars)
	default:
		return fmt.Sprintf("$%.0f", dollars)
	}
}
