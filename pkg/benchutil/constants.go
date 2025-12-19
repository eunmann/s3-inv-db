package benchutil

// Shared constants for benchmarks across packages.

// BenchmarkSeed is the default seed for reproducible benchmark data generation.
const BenchmarkSeed = 42

// Standard benchmark sizes for quick runs.
var BenchmarkSizes = []int{1000, 10000, 100000}

// ScalingSizes are larger sizes for comprehensive scaling tests.
// Used with S3INV_LONG_BENCH=1 environment variable.
var ScalingSizes = []int{10000, 50000, 100000, 250000, 500000}

// TreeShapes are the standard tree structures for benchmarking.
// Each shape has different characteristics:
//   - deep_narrow: 20 levels deep, single file per branch
//   - wide_shallow: 1 level deep, many prefixes
//   - balanced: 3 levels with branching factor 26
//   - s3_realistic: Simulates S3 date-partitioned paths
//   - wide_single_level: All files under root/childN/
var TreeShapes = []string{
	"deep_narrow",
	"wide_shallow",
	"balanced",
	"s3_realistic",
	"wide_single_level",
}
