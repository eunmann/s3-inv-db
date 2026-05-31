package extsort

import (
	"fmt"
	"strconv"
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
			runAggregatorAddBench(b, c.n, c.depth)
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
				runAggregatorAddBench(b, c.n, c.depth)
			})
		}
	}
}

func runAggregatorAddBench(b *testing.B, n, depth int) {
	b.Helper()
	b.ReportAllocs()
	keys := generateBenchKeys(n, depth)
	b.ResetTimer()
	for range b.N {
		agg := NewAggregator(0, 0)
		for _, k := range keys {
			agg.AddObject(k, 1024, tiers.Standard)
		}
		agg.Drain()
	}
}

// generateBenchKeys produces n synthetic keys with the given path
// depth. Keeps fanout per level small so the aggregator's distinct
// prefix count stays in a realistic ratio (~5-10% of object count).
func generateBenchKeys(n, depth int) []string {
	const fanout = 64
	keys := make([]string, n)
	for i := range n {
		var k string
		v := i
		for d := range depth {
			seg := strconv.Itoa(v % fanout)
			if d == 0 {
				k = seg
			} else {
				k = k + "/" + seg
			}
			v /= fanout
		}
		k += "/file" + strconv.Itoa(i)
		keys[i] = k
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
