package indexread_test

import (
	"fmt"
	"testing"

	"github.com/eunmann/s3-inv-db/internal/benchutil"
	"github.com/eunmann/s3-inv-db/pkg/indexread"
)

// Cold-cache query benches. Each iteration:
//  1. close the index, evict every index file from the page cache and
//     reopen it (untimed; see reopenCold)
//  2. issue the queries
//  3. measure the latency including the resulting page faults
//
// This isolates the structural query-cost — how many independent
// pages a single per-prefix call touches — from CPU-cache effects.
// Used as the baseline for the Q-tier (query-time) row-major
// interleaving work; lower page-fault fanout per call means
// dramatically lower cold latency.
//
// The number of timed iterations stays small per outer b.N because
// each query pays disk I/O; benchstat over -count=N gives noise.

// coldQueryBatch is the number of queries we run after each cache
// eviction, so the fixed cost of reopenCold (which walks every file
// under the index directory and FADV_DONTNEEDs each one, then reopens
// the index) amortises across many per-query measurements. Each batched query touches a distinct
// prefix, so the *first-page-fault-per-query* cost still dominates
// the reported per-op time.
const coldQueryBatch = 64

// BenchmarkQueryScale_StatsForPrefix_Cold measures the round-trip
// "give me objCount+totalBytes for this prefix string" against a
// freshly-evicted page cache. The reopenCold cost is amortised
// across coldQueryBatch queries per eviction, so the reported per-op
// time reflects the actual cold-query latency rather than the
// eviction overhead.
func BenchmarkQueryScale_StatsForPrefix_Cold(b *testing.B) {
	benchutil.SilenceZerolog(b)
	for _, n := range queryBenchSizes() {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			dir := buildFixtureIndex(b, n)
			idx, err := indexread.Open(dir)
			if err != nil {
				b.Fatalf("Open: %v", err)
			}
			defer func() { _ = idx.Close() }()
			prefixes := generateLookupPrefixes(n)
			if len(prefixes) == 0 {
				b.Skip("no prefixes")
			}
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i += coldQueryBatch {
				b.StopTimer()
				idx = reopenCold(b, idx, dir)
				b.StartTimer()
				batch := coldQueryBatch
				if i+batch > b.N {
					batch = b.N - i
				}
				for j := range batch {
					_, _ = idx.StatsForPrefix(prefixes[(i+j)%len(prefixes)])
				}
			}
		})
	}
}

// BenchmarkQueryScale_TierBreakdown_Cold measures the full per-prefix
// tier breakdown call with cold pages. Today fans out to 22 random
// reads across 22 separate mmap'd files (11 tiers × {count, bytes}) —
// 22 page faults per call on a cold index. Target for Q1 row-major
// tier_stats: 1 page fault.
func BenchmarkQueryScale_TierBreakdown_Cold(b *testing.B) {
	benchutil.SilenceZerolog(b)
	for _, n := range queryBenchSizes() {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			dir := buildFixtureIndex(b, n)
			idx, err := indexread.Open(dir)
			if err != nil {
				b.Fatalf("Open: %v", err)
			}
			defer func() { _ = idx.Close() }()
			if !idx.HasTierData() {
				b.Skip("no tier data")
			}
			positions := resolvePositions(b, idx, generateLookupPrefixes(n))
			if len(positions) == 0 {
				b.Skip("no resolvable prefixes")
			}
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i += coldQueryBatch {
				b.StopTimer()
				idx = reopenCold(b, idx, dir)
				b.StartTimer()
				batch := coldQueryBatch
				if i+batch > b.N {
					batch = b.N - i
				}
				for j := range batch {
					_ = idx.TierBreakdown(positions[(i+j)%len(positions)])
				}
			}
		})
	}
}

// BenchmarkQueryScale_ChildrenIterate_Cold sweeps a prefix's subtree
// reading {objCount, totalBytes, depth} for each child. Today: 3
// random reads per child across separate columnar files; with N
// children, 3N page faults on a cold index (sequential within each
// file, so MADV_SEQUENTIAL helps once enabled, but cross-file still
// fans out). Target for Q2 row-major core stats: 1 page fault per
// child, sequential within the core-stats file.
func BenchmarkQueryScale_ChildrenIterate_Cold(b *testing.B) {
	benchutil.SilenceZerolog(b)
	for _, n := range queryBenchSizes() {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			dir := buildFixtureIndex(b, n)
			idx, err := indexread.Open(dir)
			if err != nil {
				b.Fatalf("Open: %v", err)
			}
			defer func() { _ = idx.Close() }()
			positions := resolvePositions(b, idx, generateLookupPrefixes(n))
			parents := pickParentPositions(idx, positions, 64)
			if len(parents) == 0 {
				b.Skip("no parent positions")
			}
			b.ResetTimer()
			b.ReportAllocs()
			for i := range b.N {
				p := parents[i%len(parents)]
				b.StopTimer()
				idx = reopenCold(b, idx, dir)
				b.StartTimer()
				children, err := idx.DescendantSpan(p, 1)
				if err != nil {
					b.Fatal(err)
				}
				for c := children.Lo; c < children.Hi; c++ {
					_ = idx.Stats(c)
					_ = idx.Depth(c)
				}
			}
		})
	}
}

// pickParentPositions returns up to want positions that have at least
// child. These are the interesting
// parents for a Children/Browse cold sweep; leaves would short-circuit.
func pickParentPositions(idx *indexread.Index, candidates []uint64, want int) []uint64 {
	out := make([]uint64, 0, want)
	for _, p := range candidates {
		if idx.HasChildren(p) {
			out = append(out, p)
			if len(out) >= want {
				break
			}
		}
	}

	return out
}
