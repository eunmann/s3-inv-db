package indexread_test

import (
	"fmt"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/indexread"
)

// BenchmarkPrefixString_Matrix measures `idx.PrefixString(pos)` per-call
// latency across {cache × size}. PrefixString is called per child
// during Browse, so its cost multiplies. Reports per-call ns + B/op.
func BenchmarkPrefixString_Matrix(b *testing.B) {
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
