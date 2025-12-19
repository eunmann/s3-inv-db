package benchutil

import (
	"os"
	"sort"
	"testing"
)

// SkipIfNoLongBench skips the benchmark if S3INV_LONG_BENCH is not set.
// Use this to gate long-running benchmarks that shouldn't run by default.
func SkipIfNoLongBench(b *testing.B) {
	if os.Getenv("S3INV_LONG_BENCH") == "" {
		b.Skip("set S3INV_LONG_BENCH=1 to run scaling benchmark")
	}
}

// SortKeys returns a lexicographically sorted copy of the keys.
// Required for trie building which expects sorted input.
func SortKeys(keys []string) []string {
	sorted := make([]string, len(keys))
	copy(sorted, keys)
	sort.Strings(sorted)
	return sorted
}

// KeysToSizes generates synthetic sizes for a slice of keys.
// Returns sizes with a pattern that varies based on position.
func KeysToSizes(keys []string) []uint64 {
	sizes := make([]uint64, len(keys))
	for i := range keys {
		sizes[i] = uint64((i%1000 + 1) * 100)
	}
	return sizes
}
