package extsort

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/eunmann/s3-inv-db/internal/benchutil"
	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

// BenchmarkAggregator_AddObject measures the cost of the central
// AddObject hot loop across realistic key shapes. The aggregator
// climbs every parent prefix per object — so the per-call cost is
// O(depth). We bench at depth 1, 4, 8 to make the depth-axis
// regression/win visible after Wave 1/3 changes.
//
// Object counts span the regimes where the underlying map structure
// changes behaviour: 10K (L2-resident), 100K (spills to L3), 1M
// (spills to DRAM). The 10M / 100M tier is gated behind
// S3INV_LONG_BENCH so default `make test` stays fast.
func BenchmarkAggregator_AddObject(b *testing.B) {
	cases := []struct {
		n     int
		depth int
	}{
		{10_000, 4},
		{100_000, 4},
		{100_000, 8},
		{1_000_000, 4},
		{1_000_000, 8},
	}
	for _, c := range cases {
		b.Run(fmt.Sprintf("n=%d/depth=%d", c.n, c.depth), func(b *testing.B) {
			runAggregatorAddBench(b, generateBenchKeys(c.n, c.depth))
		})
		// The same keys in order, as an inventory file may list them.
		b.Run(fmt.Sprintf("n=%d/depth=%d/sorted", c.n, c.depth), func(b *testing.B) {
			keys := generateBenchKeys(c.n, c.depth)
			slices.Sort(keys)
			runAggregatorAddBench(b, keys)
		})
	}
	// 100 objects per deepest directory, in key order: most objects
	// add to prefixes that already exist, as in real inventories.
	for _, depth := range []int{4, 8} {
		b.Run(fmt.Sprintf("n=1000000/depth=%d/dense/sorted", depth), func(b *testing.B) {
			keys := generateDenseKeys(1_000_000, depth, 100)
			slices.Sort(keys)
			runAggregatorAddBench(b, keys)
		})
	}

	if benchutil.LongBenchEnabled() {
		long := []struct {
			n     int
			depth int
		}{
			{10_000_000, 4},
			{10_000_000, 8},
		}
		for _, c := range long {
			b.Run(fmt.Sprintf("n=%d/depth=%d", c.n, c.depth), func(b *testing.B) {
				runAggregatorAddBench(b, generateBenchKeys(c.n, c.depth))
			})
		}
	}
}

func runAggregatorAddBench(b *testing.B, keys []string) {
	b.Helper()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		agg := NewAggregator(0, 0)
		for _, k := range keys {
			agg.AddObject(k, 1024, tiers.Standard)
		}
		agg.Drain()
	}
}

// BenchmarkAggregator_AddObjectSegmented measures AddObject as the
// pipeline calls it, a StartSegment per inventory file, on 1M keys in
// ten segments. "sorted" takes the ordered segments' path, "random" the
// node table's from each segment's second key, and "sorted_mapped" swaps
// each segment's first two keys so it maps at once and its remaining
// keys, in order, go through the node table while sharing most of their
// path. The aggregator is reused and drained outside the timer, as a
// worker's is across spills, so only AddObject is measured. Sparse keys
// (a directory per key) stop at depth 4: at depth 8 they make 8M
// prefixes, over a gigabyte of nodes.
func BenchmarkAggregator_AddObjectSegmented(b *testing.B) {
	const n, segments = 1_000_000, 10
	shapes := []struct {
		name  string
		depth int
		keys  func() []string
	}{
		{"sparse", 4, func() []string { return generateBenchKeys(n, 4) }},
		{"dense", 4, func() []string { return generateDenseKeys(n, 4, 100) }},
		{"dense", 8, func() []string { return generateDenseKeys(n, 8, 100) }},
	}
	for _, s := range shapes {
		sorted := s.keys()
		slices.Sort(sorted)
		random := slices.Clone(sorted)
		rand.New(rand.NewPCG(1, 2)).Shuffle(len(random), func(i, j int) { random[i], random[j] = random[j], random[i] })
		mapped := slices.Clone(sorted)
		for i := range segments {
			seg := mapped[i*n/segments:]
			seg[0], seg[1] = seg[1], seg[0]
		}
		orders := []struct {
			name string
			keys []string
		}{{"random", random}, {"sorted", sorted}, {"sorted_mapped", mapped}}
		for _, o := range orders {
			b.Run(fmt.Sprintf("keys=%s/depth=%d/order=%s", s.name, s.depth, o.name), func(b *testing.B) {
				runSegmentedAddBench(b, o.keys, segments)
			})
		}
	}
}

func runSegmentedAddBench(b *testing.B, keys []string, segments int) {
	b.Helper()
	agg := NewAggregator(0, 0)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		for i := range segments {
			agg.StartSegment()
			for _, k := range keys[i*len(keys)/segments : (i+1)*len(keys)/segments] {
				agg.AddObject(k, 1024, tiers.Standard)
			}
		}
		b.StopTimer()
		agg.Drain()
		b.StartTimer()
	}
}

// generateBenchKeys produces n synthetic keys with the given path
// depth. Keeps fanout per level small so the aggregator's distinct
// prefix count stays in a realistic ratio (~5-10% of object count).
func generateBenchKeys(n, depth int) []string {
	keys := make([]string, n)
	for i := range n {
		keys[i] = benchDir(i, depth) + "/file" + strconv.Itoa(i)
	}

	return keys
}

// benchDir is generateBenchKeys' directory for index i: depth segments,
// the first varying fastest.
func benchDir(i, depth int) string {
	const fanout = 64
	segs := make([]string, depth)
	for d := range depth {
		segs[d] = strconv.Itoa(i % fanout)
		i /= fanout
	}

	return strings.Join(segs, "/")
}

// generateDenseKeys is generateBenchKeys with perDir objects in each
// deepest directory.
func generateDenseKeys(n, depth, perDir int) []string {
	keys := make([]string, n)
	for i := range n {
		keys[i] = benchDir(i/perDir, depth) + "/obj" + strconv.Itoa(i)
	}

	return keys
}

// BenchmarkAggregator_DrainSorted measures the cost of Drain()
// returning a sorted slice — the work that happens at every flush.
// At billion-object scale this runs ~28× per build.
func BenchmarkAggregator_DrainSorted(b *testing.B) {
	for _, n := range []int{10_000, 100_000, 1_000_000} {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			keys := generateBenchKeys(n, 4)
			for range b.N {
				b.StopTimer()
				agg := NewAggregator(0, 0)
				for _, k := range keys {
					agg.AddObject(k, 1024, tiers.Standard)
				}
				b.StartTimer()
				rows := agg.Drain()
				_ = rows
			}
		})
	}
}
