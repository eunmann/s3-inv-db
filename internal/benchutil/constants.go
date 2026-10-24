package benchutil

// Shared constants for benchmarks across packages.

// BenchmarkSeed is the default seed for reproducible benchmark data generation.
const BenchmarkSeed = 42

// BenchmarkSizes returns the standard benchmark sizes for quick runs.
// A function is used (rather than a package-level slice) so the returned
// slice is private to each caller and the package owns no mutable globals.
func BenchmarkSizes() []int {
	return []int{1000, 10_000, 100_000}
}

// Tree shape names accepted by GenerateKeys and NewGridGenerator.
const (
	ShapeDeepNarrow      = "deep_narrow"
	ShapeWideShallow     = "wide_shallow"
	ShapeBalanced        = "balanced"
	ShapeS3Realistic     = "s3_realistic"
	ShapeWideSingleLevel = "wide_single_level"
)

// TreeShapes returns the standard tree structures for benchmarking.
// Each shape has different characteristics:
//   - deep_narrow: 20 levels deep, single file per branch
//   - wide_shallow: 1 level deep, many prefixes
//   - balanced: 3 levels with branching factor 26
//   - s3_realistic: Simulates S3 date-partitioned paths
//   - wide_single_level: All files under root/childN/
func TreeShapes() []string {
	return []string{
		ShapeDeepNarrow,
		ShapeWideShallow,
		ShapeBalanced,
		ShapeS3Realistic,
		ShapeWideSingleLevel,
	}
}
