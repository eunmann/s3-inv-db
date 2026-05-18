// Package pricing provides cost estimation for S3 storage tiers.
package pricing

import (
	"encoding/json"
	"fmt"
	"math"
	"os"

	"github.com/eunmann/s3-inv-db/pkg/format"
)

const (
	bytesPerGB = 1024 * 1024 * 1024
	bytesPerKB = 1024

	// Minimum billable object size for Standard-IA, One Zone-IA, and
	// Glacier Instant Retrieval.
	minObjectSizeBytes = 128 * bytesPerKB

	// Per-object Glacier-rate metadata overhead (32 KiB).
	glacierMetadataOverheadBytes = 32 * bytesPerKB

	// Per-object Standard-rate index overhead (8 KiB) added to
	// Glacier-class objects.
	glacierIndexOverheadBytes = 8 * bytesPerKB
)

// US-East-1 (2025) per-GB-month pricing for every supported storage
// class. Centralising the literals here lets DefaultUSEast1Prices
// stay declaration-only and lets mnd ignore the table.
const (
	priceStandard          = 0.023
	priceStandardIA        = 0.0125
	priceOneZoneIA         = 0.01
	priceReducedRedundancy = 0.024 // Deprecated S3 class.
	priceGlacierIR         = 0.004
	priceGlacier           = 0.0036
	priceDeepArchive       = 0.00099

	// Intelligent-Tiering access tiers (same rate as their standard
	// class equivalents). Listed separately so the JSON keys stay
	// self-documenting.
	priceITFrequent       = priceStandard
	priceITInfrequent     = priceStandardIA
	priceITArchiveInstant = priceGlacierIR
	priceITArchive        = priceGlacier
	priceITDeepArchive    = priceDeepArchive

	// IT objects below 128 KiB are charged at the Frequent rate with
	// no monitoring fee.
	priceITFrequentSmall = priceITFrequent

	// IT monitoring fee per 1,000 objects per month for objects >=
	// 128 KiB.
	monitoringPer1000Objects = 0.0025

	// Per-PUT/COPY/POST charge per 1,000 requests. Used by the
	// Compare view to estimate one-time upload cost.
	putPer1000Requests = 0.005
)

// microdollarsPerDollar is the cost unit the package quotes results
// in: 1 USD = 1,000,000 microdollars. Lets us keep integer math
// throughout without losing sub-cent precision on per-tier totals.
const microdollarsPerDollar = 1_000_000

// requestsPerThousand is the unit the AWS request-rate fields are
// quoted in (e.g. monitoring fee per 1,000 objects).
const requestsPerThousand = 1000.0

// PriceTable contains comprehensive S3 pricing information.
type PriceTable struct {
	// PerGBMonth maps tier names to USD per GB per month for storage.
	PerGBMonth map[string]float64 `json:"per_gb_month"`

	// MonitoringPer1000Objects is the Intelligent-Tiering monitoring fee
	// per 1,000 objects per month. Only applies to objects >= 128 KiB
	// (small objects land in INTELLIGENT_TIERING_FREQUENT_SMALL, which
	// is excluded from monitoring).
	MonitoringPer1000Objects float64 `json:"monitoring_per_1000_objects"`

	// PutPer1000Requests is the per-PUT/COPY/POST request charge, USD per
	// 1,000 requests. Used by the Compare view to estimate the one-time
	// upload cost for the objects that exist in a run.
	PutPer1000Requests float64 `json:"put_per_1000_requests"`

	// StandardPricePerGB is used for Glacier index overhead calculation.
	StandardPricePerGB float64 `json:"standard_price_per_gb"`
}

// TierHasMinObjectSize reports whether a tier bills objects smaller
// than 128 KiB as if they were 128 KiB (Standard-IA, One Zone-IA,
// Glacier Instant Retrieval).
func TierHasMinObjectSize(tier string) bool {
	switch tier {
	case "STANDARD_IA", "ONEZONE_IA", "GLACIER_IR":
		return true
	}

	return false
}

// TierHasMonitoringCost reports whether a tier incurs the Intelligent-
// Tiering per-object monitoring fee.
func TierHasMonitoringCost(tier string) bool {
	switch tier {
	case "INTELLIGENT_TIERING_FREQUENT",
		"INTELLIGENT_TIERING_INFREQUENT",
		"INTELLIGENT_TIERING_ARCHIVE_INSTANT",
		"INTELLIGENT_TIERING_ARCHIVE",
		"INTELLIGENT_TIERING_DEEP_ARCHIVE":
		return true
	}

	return false
}

// TierHasGlacierOverhead reports whether a tier carries per-object
// Glacier metadata overhead: 32 KiB at the tier's own rate plus 8 KiB
// at the Standard rate.
func TierHasGlacierOverhead(tier string) bool {
	switch tier {
	case "GLACIER",
		"DEEP_ARCHIVE",
		"INTELLIGENT_TIERING_ARCHIVE",
		"INTELLIGENT_TIERING_DEEP_ARCHIVE":
		return true
	}

	return false
}

// DefaultUSEast1Prices returns the default pricing for US East 1 region (as of 2025).
// Sources:
// - https://aws.amazon.com/s3/pricing/
// - https://aws.amazon.com/s3/storage-classes/intelligent-tiering/
func DefaultUSEast1Prices() PriceTable {
	return PriceTable{
		PerGBMonth: map[string]float64{
			"STANDARD":           priceStandard,
			"STANDARD_IA":        priceStandardIA,
			"ONEZONE_IA":         priceOneZoneIA,
			"REDUCED_REDUNDANCY": priceReducedRedundancy,

			"GLACIER_IR":   priceGlacierIR,
			"GLACIER":      priceGlacier,
			"DEEP_ARCHIVE": priceDeepArchive,

			"INTELLIGENT_TIERING_FREQUENT":        priceITFrequent,
			"INTELLIGENT_TIERING_INFREQUENT":      priceITInfrequent,
			"INTELLIGENT_TIERING_ARCHIVE_INSTANT": priceITArchiveInstant,
			"INTELLIGENT_TIERING_ARCHIVE":         priceITArchive,
			"INTELLIGENT_TIERING_DEEP_ARCHIVE":    priceITDeepArchive,
			"INTELLIGENT_TIERING_FREQUENT_SMALL":  priceITFrequentSmall,
		},
		MonitoringPer1000Objects: monitoringPer1000Objects,
		PutPer1000Requests:       putPer1000Requests,
		StandardPricePerGB:       priceStandard,
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

// CostResult contains the estimated monthly storage costs with detailed breakdown.
type CostResult struct {
	PerTierMicrodollars         map[string]uint64
	TotalMicrodollars           uint64
	MonitoringMicrodollars      uint64
	MinObjectSizeMicrodollars   uint64
	GlacierOverheadMicrodollars uint64
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

		// Base storage cost.
		gbFrac := float64(tb.Bytes) / float64(bytesPerGB)
		storageMicrodollars := uint64(gbFrac * price * microdollarsPerDollar)

		// Average object size for this tier.
		var avgObjectSize uint64
		if tb.ObjectCount > 0 {
			avgObjectSize = tb.Bytes / tb.ObjectCount
		}

		// Minimum object size penalty for applicable tiers.
		var minSizePenalty uint64
		if TierHasMinObjectSize(tb.TierName) && avgObjectSize < minObjectSizeBytes && avgObjectSize > 0 {
			// Each object is charged as 128 KiB.
			additionalBytes := (minObjectSizeBytes - avgObjectSize) * tb.ObjectCount
			additionalGB := float64(additionalBytes) / float64(bytesPerGB)
			minSizePenalty = uint64(additionalGB * price * microdollarsPerDollar)
			result.MinObjectSizeMicrodollars += minSizePenalty
		}

		// Intelligent-Tiering monitoring cost. Every object in a
		// monitored tier is billable; objects < 128 KiB are pre-
		// segregated into INTELLIGENT_TIERING_FREQUENT_SMALL by the
		// ingest classifier and therefore never reach this branch.
		var monitoringCost uint64
		if TierHasMonitoringCost(tb.TierName) && pt.MonitoringPer1000Objects > 0 {
			monitoringCost = uint64(float64(tb.ObjectCount) / requestsPerThousand * pt.MonitoringPer1000Objects * microdollarsPerDollar)
			result.MonitoringMicrodollars += monitoringCost
		}

		// Glacier metadata overhead.
		var glacierOverhead uint64
		if TierHasGlacierOverhead(tb.TierName) && tb.ObjectCount > 0 {
			// 32 KiB at Glacier rate per object.
			glacierOverheadGB := float64(tb.ObjectCount*glacierMetadataOverheadBytes) / float64(bytesPerGB)
			glacierOverhead = uint64(glacierOverheadGB * price * microdollarsPerDollar)

			// 8 KiB at Standard rate per object.
			indexOverheadGB := float64(tb.ObjectCount*glacierIndexOverheadBytes) / float64(bytesPerGB)
			indexOverhead := uint64(indexOverheadGB * pt.StandardPricePerGB * microdollarsPerDollar)
			glacierOverhead += indexOverhead

			result.GlacierOverheadMicrodollars += glacierOverhead
		}

		tierTotal := storageMicrodollars + minSizePenalty + glacierOverhead
		result.PerTierMicrodollars[tb.TierName] = tierTotal
		result.TotalMicrodollars += tierTotal
	}

	// Monitoring costs feed the global total, not the per-tier slice
	// (the IT monitoring fee spans multiple tiers).
	result.TotalMicrodollars += result.MonitoringMicrodollars

	return result
}

// ComputePutCost returns the one-time PUT/COPY/POST charge for ingesting
// objectCount objects, in microdollars. Used by the Compare view to
// surface the upload cost of a run alongside its monthly storage cost.
func ComputePutCost(objectCount uint64, pt PriceTable) uint64 {
	if pt.PutPer1000Requests <= 0 || objectCount == 0 {
		return 0
	}

	return uint64(float64(objectCount) / requestsPerThousand * pt.PutPer1000Requests * microdollarsPerDollar)
}

// FormatCost formats a cost in microdollars for display.
//
//   - 0                       → "$0.00"
//   - 0 < c < $0.01           → "<$0.01"
//   - $0.01 ≤ c < $1,000      → "$X.XX" (ceiling to next cent)
//   - $1,000 ≤ c < $1M        → "$X.YK"
//   - $1M ≤ c < $1B           → "$X.YM"
//   - c ≥ $1B                 → "$X.YB"
//
// K/M/B suffixes use one decimal, round-half-away-from-zero.
func FormatCost(microdollars uint64) string {
	const (
		microsPerCent   = uint64(10_000)
		microsPerDollar = uint64(microdollarsPerDollar)
		thousand        = 1_000.0
		million         = 1_000_000.0
		billion         = 1_000_000_000.0
		centsPerDollar  = 100
	)

	if microdollars == 0 {
		return "$0.00"
	}
	if microdollars < microsPerCent {
		return "<$0.01"
	}

	dollars := float64(microdollars) / float64(microsPerDollar)
	switch {
	case dollars >= billion:
		return fmt.Sprintf("$%.1fB", roundHalfAway(dollars/billion, 1))
	case dollars >= million:
		return fmt.Sprintf("$%.1fM", roundHalfAway(dollars/million, 1))
	case dollars >= thousand:
		return fmt.Sprintf("$%.1fK", roundHalfAway(dollars/thousand, 1))
	}

	cents := (microdollars + microsPerCent - 1) / microsPerCent

	return fmt.Sprintf("$%d.%02d", cents/centsPerDollar, cents%centsPerDollar)
}

// roundHalfAway rounds x to decimals decimal places, half-away-from-zero.
func roundHalfAway(x float64, decimals int) float64 {
	factor := math.Pow(10, float64(decimals))

	return math.Round(x*factor) / factor
}
