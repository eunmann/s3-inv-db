package extsort

import (
	"fmt"
	"slices"
	"testing"

	"github.com/eunmann/s3-inv-db/internal/benchutil"
	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

// BenchmarkSortPrefixRows sorts a drained aggregator's rows, in the map
// order Drain hands them out, as every spill does. N is the object
// count; the rows sorted are the prefixes those objects make. The
// pyramid shape's prefixes share 100+ leading bytes, as dated
// partition trees do.
func BenchmarkSortPrefixRows(b *testing.B) {
	shapes := []struct {
		name string
		keys func(n int) []string
	}{
		{"numeric", func(n int) []string { return generateBenchKeys(n, 4) }},
		{"realistic", func(n int) []string { return generatorKeys(benchutil.S3RealisticConfig(n)) }},
		{"deep_pyramid", func(n int) []string { return generatorKeys(benchutil.S3DeepPyramidConfig(n)) }},
	}
	for _, shape := range shapes {
		for _, n := range []int{100_000, 1_000_000} {
			b.Run(fmt.Sprintf("shape=%s/n=%d", shape.name, n), func(b *testing.B) {
				agg := NewAggregator(0, 0)
				for _, k := range shape.keys(n) {
					agg.AddObject(k, 1024, tiers.Standard)
				}
				drained := slices.Clone(agg.Drain())
				work := make([]*PrefixRow, len(drained))
				b.ReportAllocs()
				for b.Loop() {
					copy(work, drained)
					SortPrefixRows(work)
				}
				b.ReportMetric(float64(len(drained)), "rows")
			})
		}
	}
}

func generatorKeys(cfg benchutil.GeneratorConfig) []string {
	keys := make([]string, 0, cfg.NumObjects)
	benchutil.NewGenerator(cfg).Stream(func(o benchutil.FakeObject) {
		keys = append(keys, o.Key)
	})

	return keys
}
