package extsort

import (
	"fmt"
	"slices"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

// BenchmarkSortPrefixRows sorts a drained aggregator's rows, in the map
// order Drain hands them out, as every spill does.
func BenchmarkSortPrefixRows(b *testing.B) {
	for _, n := range []int{100_000, 1_000_000} {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			agg := NewAggregator(0, 0)
			for _, k := range generateBenchKeys(n, 4) {
				agg.AddObject(k, 1024, tiers.Standard)
			}
			drained := slices.Clone(agg.Drain())
			work := make([]*PrefixRow, len(drained))
			b.ReportAllocs()
			for b.Loop() {
				copy(work, drained)
				SortPrefixRows(work)
			}
		})
	}
}
