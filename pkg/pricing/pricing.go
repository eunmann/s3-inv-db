// Package pricing provides cost estimation for S3 storage tiers.
package pricing

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/eunmann/s3-inv-db/pkg/format"
)

const (
	bytesPerGB = 1024 * 1024 * 1024
	bytesPerKB = 1024

	// Minimum billable object size for Standard-IA, One Zone-IA, and Glacier Instant Retrieval.
	minObjectSizeBytes = 128 * bytesPerKB

	// Glacier metadata overhead per object:
	// - 32KB charged at the Glacier tier rate
	// - 8KB charged at S3 Standard rate
	glacierMetadataOverheadBytes = 32 * bytesPerKB
	glacierIndexOverheadBytes    = 8 * bytesPerKB
)

// PriceTable contains comprehensive S3 pricing information.
type PriceTable struct {
	// PerGBMonth maps tier names to USD per GB per month for storage.
	PerGBMonth map[string]float64 `json:"per_gb_month"`

	// MonitoringPer1000Objects is the Intelligent-Tiering monitoring fee
	// per 1,000 objects per month. Only applies to objects >= 128KB.
	MonitoringPer1000Objects float64 `json:"monitoring_per_1000_objects"`

	// StandardPricePerGB is used for Glacier index overhead calculation.
	StandardPricePerGB float64 `json:"standard_price_per_gb"`
}

// TiersWithMinObjectSize lists tiers that have 128KB minimum object size billing.
// Objects smaller than 128KB are billed as if they were 128KB.
var TiersWithMinObjectSize = map[string]bool{
	"STANDARD_IA": true,
	"ONEZONE_IA":  true,
	"GLACIER_IR":  true,
}

// TiersWithMonitoringCost lists Intelligent-Tiering tiers that incur monitoring fees.
var TiersWithMonitoringCost = map[string]bool{
	"INTELLIGENT_TIERING_FREQUENT":        true,
	"INTELLIGENT_TIERING_INFREQUENT":      true,
	"INTELLIGENT_TIERING_ARCHIVE_INSTANT": true,
	"INTELLIGENT_TIERING_ARCHIVE":         true,
	"INTELLIGENT_TIERING_DEEP_ARCHIVE":    true,
}

// TiersWithGlacierOverhead lists Glacier tiers that have per-object metadata overhead.
// For each object: 32KB at Glacier rate + 8KB at Standard rate.
var TiersWithGlacierOverhead = map[string]bool{
	"GLACIER":                          true,
	"DEEP_ARCHIVE":                     true,
	"INTELLIGENT_TIERING_ARCHIVE":      true,
	"INTELLIGENT_TIERING_DEEP_ARCHIVE": true,
}

// DefaultUSEast1Prices returns the default pricing for US East 1 region (as of 2025).
// Sources:
// - https://aws.amazon.com/s3/pricing/
// - https://aws.amazon.com/s3/storage-classes/intelligent-tiering/
func DefaultUSEast1Prices() PriceTable {
	return PriceTable{
		PerGBMonth: map[string]float64{
			// Standard storage classes
			"STANDARD":           0.023,
			"STANDARD_IA":        0.0125,
			"ONEZONE_IA":         0.01,
			"REDUCED_REDUNDANCY": 0.024, // Deprecated

			// Glacier storage classes
			"GLACIER_IR":   0.004,  // Glacier Instant Retrieval
			"GLACIER":      0.0036, // Glacier Flexible Retrieval
			"DEEP_ARCHIVE": 0.00099,

			// Intelligent-Tiering access tiers
			"INTELLIGENT_TIERING_FREQUENT":        0.023,
			"INTELLIGENT_TIERING_INFREQUENT":      0.0125,
			"INTELLIGENT_TIERING_ARCHIVE_INSTANT": 0.004,
			"INTELLIGENT_TIERING_ARCHIVE":         0.0036,
			"INTELLIGENT_TIERING_DEEP_ARCHIVE":    0.00099,
		},
		// $0.0025 per 1,000 objects per month for objects >= 128KB
		MonitoringPer1000Objects: 0.0025,
		// Standard price used for Glacier index overhead
		StandardPricePerGB: 0.023,
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

// CostResult contains the estimated monthly storage costs with detailed breakdown.
type CostResult struct {
	// TotalMicrodollars is the total cost in microdollars (1 USD = 1,000,000 microdollars).
	TotalMicrodollars uint64

	// PerTierMicrodollars maps tier names to their storage cost in microdollars.
	PerTierMicrodollars map[string]uint64

	// MonitoringMicrodollars is the Intelligent-Tiering monitoring fee.
	MonitoringMicrodollars uint64

	// MinObjectSizeMicrodollars is the additional cost from 128KB minimum billing.
	MinObjectSizeMicrodollars uint64

	// GlacierOverheadMicrodollars is the metadata overhead cost for Glacier objects.
	GlacierOverheadMicrodollars uint64
}

// TotalDollars returns the total cost in dollars.
func (r CostResult) TotalDollars() float64 {
	return float64(r.TotalMicrodollars) / 1_000_000
}

// PerTierDollars returns per-tier storage costs in dollars.
func (r CostResult) PerTierDollars() map[string]float64 {
	result := make(map[string]float64, len(r.PerTierMicrodollars))
	for tier, microdollars := range r.PerTierMicrodollars {
		result[tier] = float64(microdollars) / 1_000_000
	}
	return result
}

// ComputeMonthlyCost calculates the monthly storage cost for a tier breakdown.
// This is a simplified calculation using average object size assumptions.
// For accurate minimum object size and monitoring calculations, use ComputeMonthlyCostDetailed.
func ComputeMonthlyCost(breakdown []format.TierBreakdown, pt PriceTable) CostResult {
	result := CostResult{
		PerTierMicrodollars: make(map[string]uint64, len(breakdown)),
	}

	for _, tb := range breakdown {
		price, ok := pt.PerGBMonth[tb.TierName]
		if !ok {
			continue
		}

		// Base storage cost
		gbFrac := float64(tb.Bytes) / float64(bytesPerGB)
		storageMicrodollars := uint64(gbFrac * price * 1_000_000)

		// Calculate average object size for this tier
		var avgObjectSize uint64
		if tb.ObjectCount > 0 {
			avgObjectSize = tb.Bytes / tb.ObjectCount
		}

		// Minimum object size penalty for applicable tiers
		var minSizePenalty uint64
		if TiersWithMinObjectSize[tb.TierName] && avgObjectSize < minObjectSizeBytes && avgObjectSize > 0 {
			// Estimate: each object is charged as 128KB
			// Additional bytes = (128KB - avgSize) * objectCount
			additionalBytes := (minObjectSizeBytes - avgObjectSize) * tb.ObjectCount
			additionalGB := float64(additionalBytes) / float64(bytesPerGB)
			minSizePenalty = uint64(additionalGB * price * 1_000_000)
			result.MinObjectSizeMicrodollars += minSizePenalty
		}

		// Intelligent-Tiering monitoring cost
		var monitoringCost uint64
		if TiersWithMonitoringCost[tb.TierName] && pt.MonitoringPer1000Objects > 0 {
			// Only objects >= 128KB incur monitoring fees
			// Estimate: if average size >= 128KB, all objects are monitored
			// Otherwise, estimate fraction of objects that are >= 128KB
			var monitoredObjects uint64
			if avgObjectSize >= minObjectSizeBytes {
				monitoredObjects = tb.ObjectCount
			} else if avgObjectSize > 0 {
				// Rough estimate: assume uniform distribution
				// fraction >= 128KB = 1 - (128KB / (2 * avgSize))
				// This is a simplification; actual distribution matters
				monitoredObjects = tb.ObjectCount / 2 // Conservative estimate
			}
			if monitoredObjects > 0 {
				monitoringCost = uint64(float64(monitoredObjects) / 1000.0 * pt.MonitoringPer1000Objects * 1_000_000)
				result.MonitoringMicrodollars += monitoringCost
			}
		}

		// Glacier metadata overhead
		var glacierOverhead uint64
		if TiersWithGlacierOverhead[tb.TierName] && tb.ObjectCount > 0 {
			// 32KB at Glacier rate per object
			glacierOverheadGB := float64(tb.ObjectCount*glacierMetadataOverheadBytes) / float64(bytesPerGB)
			glacierOverhead = uint64(glacierOverheadGB * price * 1_000_000)

			// 8KB at Standard rate per object
			indexOverheadGB := float64(tb.ObjectCount*glacierIndexOverheadBytes) / float64(bytesPerGB)
			indexOverhead := uint64(indexOverheadGB * pt.StandardPricePerGB * 1_000_000)
			glacierOverhead += indexOverhead

			result.GlacierOverheadMicrodollars += glacierOverhead
		}

		tierTotal := storageMicrodollars + minSizePenalty + glacierOverhead
		result.PerTierMicrodollars[tb.TierName] = tierTotal
		result.TotalMicrodollars += tierTotal
	}

	// Add monitoring costs to total (not per-tier as it spans all IT tiers)
	result.TotalMicrodollars += result.MonitoringMicrodollars

	return result
}

// CostBreakdown provides a detailed breakdown of costs for display.
type CostBreakdown struct {
	TierName           string
	StorageCost        float64 // Base storage cost in dollars
	MinSizePenalty     float64 // 128KB minimum penalty in dollars
	MonitoringCost     float64 // IT monitoring fee in dollars
	GlacierOverhead    float64 // Glacier metadata overhead in dollars
	TotalCost          float64 // Total cost for this tier
	ObjectCount        uint64
	Bytes              uint64
	AvgObjectSizeBytes uint64
}

// ComputeDetailedBreakdown returns detailed cost information per tier.
func ComputeDetailedBreakdown(breakdown []format.TierBreakdown, pt PriceTable) []CostBreakdown {
	var results []CostBreakdown

	for _, tb := range breakdown {
		price, ok := pt.PerGBMonth[tb.TierName]
		if !ok {
			continue
		}

		cb := CostBreakdown{
			TierName:    tb.TierName,
			ObjectCount: tb.ObjectCount,
			Bytes:       tb.Bytes,
		}

		// Average object size
		if tb.ObjectCount > 0 {
			cb.AvgObjectSizeBytes = tb.Bytes / tb.ObjectCount
		}

		// Base storage cost
		gbFrac := float64(tb.Bytes) / float64(bytesPerGB)
		cb.StorageCost = gbFrac * price

		// Minimum object size penalty
		if TiersWithMinObjectSize[tb.TierName] && cb.AvgObjectSizeBytes < minObjectSizeBytes && cb.AvgObjectSizeBytes > 0 {
			additionalBytes := (minObjectSizeBytes - cb.AvgObjectSizeBytes) * tb.ObjectCount
			additionalGB := float64(additionalBytes) / float64(bytesPerGB)
			cb.MinSizePenalty = additionalGB * price
		}

		// Intelligent-Tiering monitoring cost
		if TiersWithMonitoringCost[tb.TierName] && pt.MonitoringPer1000Objects > 0 {
			var monitoredObjects uint64
			if cb.AvgObjectSizeBytes >= minObjectSizeBytes {
				monitoredObjects = tb.ObjectCount
			} else if cb.AvgObjectSizeBytes > 0 {
				monitoredObjects = tb.ObjectCount / 2
			}
			cb.MonitoringCost = float64(monitoredObjects) / 1000.0 * pt.MonitoringPer1000Objects
		}

		// Glacier metadata overhead
		if TiersWithGlacierOverhead[tb.TierName] && tb.ObjectCount > 0 {
			glacierOverheadGB := float64(tb.ObjectCount*glacierMetadataOverheadBytes) / float64(bytesPerGB)
			cb.GlacierOverhead = glacierOverheadGB * price

			indexOverheadGB := float64(tb.ObjectCount*glacierIndexOverheadBytes) / float64(bytesPerGB)
			cb.GlacierOverhead += indexOverheadGB * pt.StandardPricePerGB
		}

		cb.TotalCost = cb.StorageCost + cb.MinSizePenalty + cb.MonitoringCost + cb.GlacierOverhead
		results = append(results, cb)
	}

	return results
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

// FormatCostDollars formats a cost in dollars as a human-readable string.
func FormatCostDollars(dollars float64) string {
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

// FormatBytes formats bytes as a human-readable string.
func FormatBytes(bytes uint64) string {
	switch {
	case bytes >= 1024*1024*1024*1024:
		return fmt.Sprintf("%.2f TB", float64(bytes)/(1024*1024*1024*1024))
	case bytes >= 1024*1024*1024:
		return fmt.Sprintf("%.2f GB", float64(bytes)/(1024*1024*1024))
	case bytes >= 1024*1024:
		return fmt.Sprintf("%.2f MB", float64(bytes)/(1024*1024))
	case bytes >= 1024:
		return fmt.Sprintf("%.2f KB", float64(bytes)/1024)
	default:
		return fmt.Sprintf("%d B", bytes)
	}
}
