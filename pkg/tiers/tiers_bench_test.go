package tiers_test

import (
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

// fromS3BenchRows is the rows classified per op: enough work per op
// for the A/B gate's rounds to agree.
const fromS3BenchRows = 1 << 16

func BenchmarkFromS3(b *testing.B) {
	cases := []struct {
		name, storageClass, accessTier string
	}{
		{"standard", "STANDARD", ""},
		{"it_frequent", "INTELLIGENT_TIERING", "FREQUENT"},
		{"lowercase", "standard_ia", ""},
	}
	m := tiers.NewMapping()
	for _, c := range cases {
		b.Run(c.name, func(b *testing.B) {
			b.ReportAllocs()
			var sink tiers.ID
			for b.Loop() {
				for range fromS3BenchRows {
					sink ^= tiers.Resolve(m.FromS3(c.storageClass, c.accessTier), 1024)
				}
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*fromS3BenchRows), "ns/row")
			_ = sink
		})
	}
}
