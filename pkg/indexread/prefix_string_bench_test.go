package indexread_test

import (
	"fmt"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/indexread"
)

// BenchmarkPrefixString_Matrix measures `idx.PrefixString(pos)` per-call
// latency across the {encoding × cache-state × size} matrix.
//
// PrefixString is called per child during Browse, so if its cost is N
// times higher under segmented encoding it shows up as a linear penalty
// on the Browse query — the second of the two production-critical
// queries.
//
// Matrix axes:
//
//	encoding   raw | segmented
//	cache      warm | cold
//	n          100K | 1M  (+ 10M when S3INV_LONG_BENCH is set)
//
// Reports per-call ns + per-call allocs/B.
func BenchmarkPrefixString_Matrix(b *testing.B) {
	silenceZerolog(b)
	for _, useSeg := range []bool{false, true} {
		for _, n := range queryBenchSizes() {
			label := fmt.Sprintf("encoding=%s/n=%d", encodingLabel(useSeg), n)
			b.Run(label, func(b *testing.B) {
				dir := buildFixtureIndexWithEncoding(b, n, useSeg)
				idx, err := indexread.Open(dir)
				if err != nil {
					b.Fatalf("Open: %v", err)
				}
				defer idx.Close()
				positions := resolvePositions(b, idx, generateLookupPrefixes(n))
				if len(positions) == 0 {
					b.Skip("no resolvable prefixes")
				}
				b.Run("warm", func(b *testing.B) {
					b.ResetTimer()
					b.ReportAllocs()
					for i := range b.N {
						_, _ = idx.PrefixString(positions[i%len(positions)])
					}
				})
				b.Run("cold", func(b *testing.B) {
					b.ResetTimer()
					b.ReportAllocs()
					for i := 0; i < b.N; i += coldQueryBatch {
						b.StopTimer()
						dropPageCache(b, dir)
						b.StartTimer()
						batch := coldQueryBatch
						if i+batch > b.N {
							batch = b.N - i
						}
						for j := range batch {
							_, _ = idx.PrefixString(positions[(i+j)%len(positions)])
						}
					}
				})
			})
		}
	}
}

func encodingLabel(useSeg bool) string {
	if useSeg {
		return "segmented"
	}

	return "raw"
}
