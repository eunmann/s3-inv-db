package benchutil

import (
	"math"
	"math/rand"

	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

// TierDistribution names the canonical tier-mass configurations the
// grid sweep exercises. Each is a probability map summing to 1.0.
type TierDistribution struct {
	Name  string
	Probs map[tiers.ID]float64
}

// TierDistributions returns the canonical set used by the grid sweep.
// Each shapes the per-prefix populated-tier set in a distinct regime.
func TierDistributions() []TierDistribution {
	return []TierDistribution{
		SingleTier(),
		TwoTierSkewed(),
		ParetoTiers(),
		UniformAllTiers(),
		BimodalTiers(),
	}
}

// SingleTier puts 100% of mass on STANDARD — best case for tier
// sparsity wins, worst case for tier-row width assumptions.
func SingleTier() TierDistribution {
	return TierDistribution{
		Name:  "single_tier",
		Probs: map[tiers.ID]float64{tiers.Standard: 1.0},
	}
}

// Probability mass for the named TierDistribution presets. Spelled
// out so the lint forbidden-magic-numbers check stays happy and so
// each preset's intent is reviewable in one place.
const (
	twoTierSkewedHot  = 0.95
	twoTierSkewedCold = 0.05

	paretoStandard   = 0.50
	paretoStandardIA = 0.20
	paretoGlacierIR  = 0.12
	paretoGlacierFR  = 0.08
	paretoDeepArch   = 0.06
	paretoITFrequent = 0.04

	uniformAllTierCount = 11

	bimodalHot  = 0.5
	bimodalCold = 0.5
)

// TwoTierSkewed: 95% STANDARD + 5% DEEP_ARCHIVE. Common lifecycle
// shape.
func TwoTierSkewed() TierDistribution {
	return TierDistribution{
		Name: "two_tier_skewed",
		Probs: map[tiers.ID]float64{
			tiers.Standard:    twoTierSkewedHot,
			tiers.DeepArchive: twoTierSkewedCold,
		},
	}
}

// ParetoTiers: heavy head, long tail. Six tiers populated with
// sharply-skewed mass.
func ParetoTiers() TierDistribution {
	return TierDistribution{
		Name: "pareto",
		Probs: map[tiers.ID]float64{
			tiers.Standard:    paretoStandard,
			tiers.StandardIA:  paretoStandardIA,
			tiers.GlacierIR:   paretoGlacierIR,
			tiers.GlacierFR:   paretoGlacierFR,
			tiers.DeepArchive: paretoDeepArch,
			tiers.ITFrequent:  paretoITFrequent,
		},
	}
}

// UniformAllTiers spreads mass evenly across all 11 tiers. Worst case
// for sparse-tier encoding wins.
func UniformAllTiers() TierDistribution {
	p := 1.0 / uniformAllTierCount
	return TierDistribution{
		Name: "uniform_all",
		Probs: map[tiers.ID]float64{
			tiers.Standard:         p,
			tiers.StandardIA:       p,
			tiers.GlacierIR:        p,
			tiers.GlacierFR:        p,
			tiers.DeepArchive:      p,
			tiers.ITFrequent:       p,
			tiers.ITInfrequent:     p,
			tiers.ITArchiveInstant: p,
			tiers.ITArchive:        p,
			tiers.ITDeepArchive:    p,
			tiers.ITFrequentSmall:  p,
		},
	}
}

// BimodalTiers: 50/50 split between two tiers at opposite cost ends.
func BimodalTiers() TierDistribution {
	return TierDistribution{
		Name: "bimodal",
		Probs: map[tiers.ID]float64{
			tiers.Standard:    bimodalHot,
			tiers.DeepArchive: bimodalCold,
		},
	}
}

// SizeDistribution names a canonical object-size sampler used by the
// grid sweep. Distributions differ in how the per-prefix accumulated
// `total_bytes` value grows, which is the input that fixed-width vs
// varint encodings see.
type SizeDistribution struct {
	Name   string
	Sample func(rng *rand.Rand) uint64
}

// SizeDistributions returns the canonical set used by the grid sweep.
func SizeDistributions() []SizeDistribution {
	return []SizeDistribution{
		SmallUniform(),
		PowerLawSize(),
		BimodalSize(),
		WideTailSize(),
	}
}

// smallUniformMax is the upper bound for the SmallUniform sampler:
// fits in 16 bits so per-prefix totals almost always fit in 32.
const smallUniformMax = 65535

// SmallUniform: every object is 1..65535 bytes. Per-prefix totals
// almost always fit in 32 bits.
func SmallUniform() SizeDistribution {
	return SizeDistribution{
		Name:   "small_uniform",
		Sample: func(rng *rand.Rand) uint64 { return uint64(rng.Intn(smallUniformMax) + 1) },
	}
}

// PowerLawSize: log-normal-ish; mostly small with occasional very
// large. Matches the existing default Generator behaviour.
func PowerLawSize() SizeDistribution {
	return SizeDistribution{
		Name:   "power_law",
		Sample: samplePowerLawSize,
	}
}

func samplePowerLawSize(rng *rand.Rand) uint64 {
	const (
		buckets         = 10
		smallSpan       = 1024 * 1024
		mediumSpan      = 100 * 1024 * 1024
		largeSpan       = 900 * 1024 * 1024
		veryLargeSpan   = int64(4 * 1024 * 1024 * 1024)
		thresholdTiny   = 1
		thresholdSmall  = thresholdTiny + 3
		thresholdMedium = thresholdSmall + 4
		thresholdLarge  = thresholdMedium + 1
	)
	switch b := rng.Intn(buckets); {
	case b < thresholdTiny:
		return uint64(rng.Intn(1024))
	case b < thresholdSmall:
		return uint64(1024 + rng.Intn(smallSpan))
	case b < thresholdMedium:
		return uint64(1024*1024 + rng.Intn(mediumSpan))
	case b < thresholdLarge:
		return uint64(100*1024*1024 + rng.Intn(largeSpan))
	default:
		return uint64(1024*1024*1024 + rng.Int63n(veryLargeSpan))
	}
}

// Bimodal sampler bucket boundaries.
const (
	bimodalBuckets   = 10
	bimodalTinyShare = 9 // 0..tinyShare-1 → tiny; tinyShare..buckets-1 → huge
	bimodalTinyMax   = 2048
	bimodalHugeSpan  = 4 // GiB above bimodalGiB
)

// BimodalSize: 90% tiny (1 KiB ± 1024) + 10% huge (1..5 GiB). Stresses
// any "small ints fit in N bytes" assumption.
func BimodalSize() SizeDistribution {
	return SizeDistribution{
		Name: "bimodal",
		Sample: func(rng *rand.Rand) uint64 {
			if rng.Intn(bimodalBuckets) < bimodalTinyShare {
				return uint64(rng.Intn(bimodalTinyMax) + 1)
			}
			const giB = uint64(1024 * 1024 * 1024)
			return giB + uint64(rng.Int63n(int64(bimodalHugeSpan*giB)))
		},
	}
}

// WideTailSize: log-uniform 1 B .. 1 TiB. Per-prefix totals can
// realistically reach 2⁴⁰.
func WideTailSize() SizeDistribution {
	return SizeDistribution{
		Name: "wide_tail",
		Sample: func(rng *rand.Rand) uint64 {
			const maxExp = 40.0 // log2(1 TiB)
			x := rng.Float64() * maxExp
			return uint64(math.Exp2(x))
		},
	}
}

// ShapesForGrid returns the named shape preset list the grid sweep
// uses. These cover the orthogonal "tree structure" axis.
func ShapesForGrid() []string {
	return []string{
		"deep_narrow",
		"wide_shallow",
		"balanced",
		"wide_single_level",
		"s3_dated",
	}
}
