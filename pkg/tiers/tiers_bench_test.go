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
		unknown                        bool
	}{
		{name: "standard", storageClass: "STANDARD"},
		{name: "it_frequent", storageClass: "INTELLIGENT_TIERING", accessTier: "FREQUENT"},
		{name: "express_onezone", storageClass: "EXPRESS_ONEZONE"},
		{name: "snow", storageClass: "SNOW"},
		{name: "no_storage_class", storageClass: ""},
		{name: "lowercase", storageClass: "standard_ia"},
		{name: "lowercase_outposts", storageClass: "outposts"},
		{name: "lowercase_it", storageClass: "intelligent_tiering", accessTier: "archive_access"},
		{name: "unknown", storageClass: "UNKNOWN_CLASS", unknown: true},
		{name: "unknown_access_tier", storageClass: "INTELLIGENT_TIERING", accessTier: "UNKNOWN", unknown: true},
	}
	for _, c := range cases {
		if _, err := tiers.FromS3(c.storageClass, c.accessTier); (err != nil) != c.unknown {
			b.Fatalf("FromS3(%q, %q): %v", c.storageClass, c.accessTier, err)
		}
		b.Run(c.name, func(b *testing.B) {
			b.ReportAllocs()
			var sink tiers.ID
			for b.Loop() {
				for range fromS3BenchRows {
					id, _ := tiers.FromS3(c.storageClass, c.accessTier)
					sink ^= tiers.Resolve(id, 1024)
				}
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*fromS3BenchRows), "ns/row")
			_ = sink
		})
	}
}
