package indexread_test

import (
	"fmt"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/indexread"
)

// Cold-cache query benches. Each iteration:
//  1. evict every index file (including tier_stats/) from the page cache
//  2. issue one query against the still-open index
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

// BenchmarkQueryScale_StatsForPrefix_Cold measures the round-trip
// "give me objCount+totalBytes for this prefix string" with cold
// pages. Today this fans out to: combined fp+pos read (1), object_count
// (1), total_bytes (1) — 3 separate mmap'd files. Per-iteration cost
// is dominated by those page faults.
func BenchmarkQueryScale_StatsForPrefix_Cold(b *testing.B) {
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
			if len(prefixes) == 0 {
				b.Skip("no prefixes")
			}
			b.ResetTimer()
			b.ReportAllocs()
			for i := range b.N {
				b.StopTimer()
				dropPageCache(b, dir)
				b.StartTimer()
				_, _ = idx.StatsForPrefix(prefixes[i%len(prefixes)])
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
	silenceZerolog(b)
	for _, n := range queryBenchSizes() {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			dir := buildFixtureIndex(b, n)
			idx, err := indexread.Open(dir)
			if err != nil {
				b.Fatalf("Open: %v", err)
			}
			defer idx.Close()
			if !idx.HasTierData() {
				b.Skip("no tier data")
			}
			positions := resolvePositions(b, idx, generateLookupPrefixes(n))
			if len(positions) == 0 {
				b.Skip("no resolvable prefixes")
			}
			b.ResetTimer()
			b.ReportAllocs()
			for i := range b.N {
				b.StopTimer()
				dropPageCache(b, dir)
				b.StartTimer()
				_ = idx.TierBreakdown(positions[i%len(positions)])
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
			parents := pickParentPositions(idx, positions, 64)
			if len(parents) == 0 {
				b.Skip("no parent positions")
			}
			b.ResetTimer()
			b.ReportAllocs()
			for i := range b.N {
				p := parents[i%len(parents)]
				b.StopTimer()
				dropPageCache(b, dir)
				b.StartTimer()
				end := idx.SubtreeEnd(p)
				for c := p + 1; c <= end; c++ {
					_ = idx.Stats(c)
					_ = idx.Depth(c)
				}
			}
		})
	}
}

// pickParentPositions returns up to want positions that have at least
// one descendant — i.e. SubtreeEnd > pos. These are the interesting
// parents for a Children/Browse cold sweep; leaves would short-circuit.
func pickParentPositions(idx *indexread.Index, candidates []uint64, want int) []uint64 {
	out := make([]uint64, 0, want)
	for _, p := range candidates {
		if idx.SubtreeEnd(p) > p {
			out = append(out, p)
			if len(out) >= want {
				break
			}
		}
	}

	return out
}
