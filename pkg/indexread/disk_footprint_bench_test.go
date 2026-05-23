package indexread_test

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/format"
)

// BenchmarkDiskFootprint reports on-disk byte sizes for an index built
// from the S3-realistic fixture. Not a regression gate — used to
// compare disk footprint across format changes (hybrid tier-stats vs
// dense, prefix dictionary on/off, etc).
//
//	Run with: go test -bench=BenchmarkDiskFootprint -benchtime=1x \
//		-run=^$ ./pkg/indexread/
func BenchmarkDiskFootprint(b *testing.B) {
	silenceZerolog(b)
	for _, n := range []int{10000, 100000, 1000000} {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			dir := buildFixtureIndex(b, n)
			tierRow := filepath.Join("tier_stats", "tier_stats_row.bin")
			report := map[string]int64{}
			var total int64
			if err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
				if err != nil {
					return fmt.Errorf("walk %q: %w", path, err)
				}
				if d.IsDir() {
					return nil
				}
				info, err := d.Info()
				if err != nil {
					return fmt.Errorf("stat %q: %w", path, err)
				}
				rel, err := filepath.Rel(dir, path)
				if err != nil {
					return fmt.Errorf("rel %q under %q: %w", path, dir, err)
				}
				report[rel] = info.Size()
				total += info.Size()

				return nil
			}); err != nil {
				b.Fatalf("walk: %v", err)
			}

			// Read row count from the tier_stats_row.bin header — the
			// fixture aggregates per-prefix from n synthetic objects,
			// so the real prefix count is higher than n.
			tierRowPath := filepath.Join(dir, tierRow)
			prefixCount := uint64(0)
			tierRowStride := 0
			if f, err := os.Open(tierRowPath); err == nil {
				hb := make([]byte, format.HeaderSize)
				if _, rerr := f.ReadAt(hb, 0); rerr == nil {
					if h, herr := format.DecodeHeader(hb); herr == nil {
						prefixCount = h.Count
						tierRowStride = int(h.Width)
					}
				}
				f.Close()
			}

			b.ReportMetric(float64(prefixCount), "prefixes")
			b.ReportMetric(float64(tierRowStride), "tier_row_stride_B")
			b.ReportMetric(float64(total), "total_B")
			if sz, ok := report[tierRow]; ok {
				b.ReportMetric(float64(sz), "tier_row_B")
			}
			if prefixCount > 0 {
				b.ReportMetric(float64(total)/float64(prefixCount), "total_B/prefix")
				if sz, ok := report[tierRow]; ok {
					b.ReportMetric(float64(sz)/float64(prefixCount), "tier_row_B/prefix")
				}
			}
		})
	}
}
