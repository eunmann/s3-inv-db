package indexread_test

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/indexread"
)

// queryBenchSizes is the parameterised set of index sizes the
// query-path benches sweep. Defaults span the regimes where mmap
// and CPU-cache behaviour change shape; overridden by
// S3INV_BENCH_SIZES (comma-separated decimal list of object counts).
//
// Examples:
//
//	S3INV_BENCH_SIZES=1000000,10000000 go test -bench=BenchmarkQueryScale ...
//	S3INV_BENCH_SIZES=50000000 go test -bench=BenchmarkQueryScale ...
//
// 10M is gated behind S3INV_LONG_BENCH by default because the build
// fixture takes ~10s and the lookup loop ~30s.
func queryBenchSizes() []int {
	if env := os.Getenv("S3INV_BENCH_SIZES"); env != "" {
		return parseSizes(env)
	}
	sizes := []int{100_000, 1_000_000}
	if os.Getenv("S3INV_LONG_BENCH") != "" {
		sizes = append(sizes, 10_000_000)
	}

	return sizes
}

func parseSizes(env string) []int {
	parts := strings.Split(env, ",")
	out := make([]int, 0, len(parts))
	for _, p := range parts {
		if n, err := strconv.Atoi(p); err == nil && n > 0 {
			out = append(out, n)
		}
	}

	return out
}

// BenchmarkQueryScale_Lookup measures warm per-prefix Lookup latency
// across a parameterised set of index sizes. The fixture index is
// built once per size via b.Cleanup; subsequent iterations hit it
// hot.
func BenchmarkQueryScale_Lookup(b *testing.B) {
	silenceZerolog(b)
	for _, n := range queryBenchSizes() {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			dir := buildFixtureIndex(b, n)
			idx, err := indexread.Open(dir)
			if err != nil {
				b.Fatalf("Open: %v", err)
			}
			defer idx.Close()
			prefixes := generateLookupPrefixes(n)
			b.ResetTimer()
			b.ReportAllocs()
			for i := range b.N {
				_, _ = idx.Lookup(prefixes[i%len(prefixes)])
			}
		})
	}
}

// BenchmarkQueryScale_StatsForPrefix measures the full "give me stats
// for this prefix string" round-trip — Lookup + Stats — which is
// what HTTP browse/stats handlers actually do per request.
func BenchmarkQueryScale_StatsForPrefix(b *testing.B) {
	silenceZerolog(b)
	for _, n := range queryBenchSizes() {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			dir := buildFixtureIndex(b, n)
			idx, err := indexread.Open(dir)
			if err != nil {
				b.Fatalf("Open: %v", err)
			}
			defer idx.Close()
			prefixes := generateLookupPrefixes(n)
			b.ResetTimer()
			b.ReportAllocs()
			for i := range b.N {
				_, _ = idx.StatsForPrefix(prefixes[i%len(prefixes)])
			}
		})
	}
}

// BenchmarkQueryScale_StatsByPos measures Stats(pos) given a
// pre-resolved preorder position — isolates the depth/objectCount/
// totalBytes columnar fetch cost from the Lookup cost. Useful for
// regressions in the W1.6 width-dispatch path.
func BenchmarkQueryScale_StatsByPos(b *testing.B) {
	silenceZerolog(b)
	for _, n := range queryBenchSizes() {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			dir := buildFixtureIndex(b, n)
			idx, err := indexread.Open(dir)
			if err != nil {
				b.Fatalf("Open: %v", err)
			}
			defer idx.Close()
			positions := resolvePositions(b, idx, generateLookupPrefixes(n))
			if len(positions) == 0 {
				b.Skip("no resolvable prefixes")
			}
			b.ResetTimer()
			b.ReportAllocs()
			for i := range b.N {
				_ = idx.Stats(positions[i%len(positions)])
			}
		})
	}
}

// BenchmarkQueryScale_Depth measures Depth(pos) latency — exercises
// the ArrayReader.UnsafeGetUint32 width-dispatcher introduced in
// W1.6/W1.7. The dispatcher is a single perfectly-predicted branch
// but worth verifying against a no-branch baseline.
func BenchmarkQueryScale_Depth(b *testing.B) {
	silenceZerolog(b)
	for _, n := range queryBenchSizes() {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			dir := buildFixtureIndex(b, n)
			idx, err := indexread.Open(dir)
			if err != nil {
				b.Fatalf("Open: %v", err)
			}
			defer idx.Close()
			positions := resolvePositions(b, idx, generateLookupPrefixes(n))
			if len(positions) == 0 {
				b.Skip("no resolvable prefixes")
			}
			b.ResetTimer()
			b.ReportAllocs()
			for i := range b.N {
				_ = idx.Depth(positions[i%len(positions)])
			}
		})
	}
}

// resolvePositions converts a slice of prefix strings to their
// preorder positions, dropping any that don't resolve. Used by the
// position-based benches above so each iteration of the timed loop
// pays only the field-fetch cost, not the Lookup cost.
func resolvePositions(b *testing.B, idx *indexread.Index, prefixes []string) []uint64 {
	b.Helper()
	out := make([]uint64, 0, len(prefixes))
	for _, p := range prefixes {
		pos, ok := idx.Lookup(p)
		if !ok {
			continue
		}
		out = append(out, pos)
	}

	return out
}
