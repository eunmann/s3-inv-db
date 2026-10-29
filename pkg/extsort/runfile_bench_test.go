package extsort_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/extsort"
	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

// makePrefixRows returns N rows with realistic tier sparsity — only
// Standard populated, matching a typical S3 bucket. Most of the on-disk
// tier columns are zero.
func makePrefixRows(n int) []*extsort.PrefixRow {
	rows := make([]*extsort.PrefixRow, n)
	for i := range rows {
		r := &extsort.PrefixRow{
			Prefix:     fmt.Sprintf("tenant-%05d/year=2024/month=%02d/object-%08d.parquet", i%1000, (i%12)+1, i),
			Depth:      4,
			Count:      uint64(i + 1),
			TotalBytes: uint64((i + 1) * 4096),
		}
		r.TierCounts[tiers.Standard] = uint64(i + 1)
		r.TierBytes[tiers.Standard] = uint64((i + 1) * 4096)
		rows[i] = r
	}

	return rows
}

// runCodecs are the run codecs the run-file benchmarks compare.
var runCodecs = []extsort.RunCodec{extsort.RunCodecZstd, extsort.RunCodecS2}

// BenchmarkRunFileWriteCompressed measures the compressed write path
// per codec; bytes/row compares their scratch-disk cost.
func BenchmarkRunFileWriteCompressed(b *testing.B) {
	const N = 50_000
	rows := makePrefixRows(N)

	for _, codec := range runCodecs {
		b.Run("codec="+codec.String(), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			var totalBytes int64
			for i := range b.N {
				path := filepath.Join(b.TempDir(), fmt.Sprintf("run_%d.crun", i))
				w, err := extsort.NewCompressedRunWriter(path, extsort.CompressedRunWriterOptions{
					BufferSize:       extsort.DefaultRunBufferSize,
					CompressionLevel: extsort.CompressionFastest,
					Codec:            codec,
				})
				if err != nil {
					b.Fatal(err)
				}
				if err := w.WriteAll(rows); err != nil {
					b.Fatal(err)
				}
				if err := w.Close(); err != nil {
					b.Fatal(err)
				}
				st, err := os.Stat(path)
				if err != nil {
					b.Fatal(err)
				}
				totalBytes += st.Size()
			}
			b.ReportMetric(float64(totalBytes)/float64(b.N), "bytes/file")
			b.ReportMetric(float64(totalBytes)/float64(b.N*N), "bytes/row")
		})
	}
}

// BenchmarkRunFileReadCompressed measures read throughput per codec
// including decoder setup cost (relevant for parallel-merge fan-in).
func BenchmarkRunFileReadCompressed(b *testing.B) {
	const N = 50_000
	rows := makePrefixRows(N)

	for _, codec := range runCodecs {
		b.Run("codec="+codec.String(), func(b *testing.B) {
			path := filepath.Join(b.TempDir(), "bench.crun")
			w, err := extsort.NewCompressedRunWriter(path, extsort.CompressedRunWriterOptions{
				BufferSize:       extsort.DefaultRunBufferSize,
				CompressionLevel: extsort.CompressionFastest,
				Codec:            codec,
			})
			if err != nil {
				b.Fatal(err)
			}
			if err := w.WriteAll(rows); err != nil {
				b.Fatal(err)
			}
			if err := w.Close(); err != nil {
				b.Fatal(err)
			}

			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				r, err := extsort.OpenCompressedRunFile(path, 4*1024*1024)
				if err != nil {
					b.Fatal(err)
				}
				var row extsort.PrefixRow
				for {
					if err := r.ReadInto(&row); err != nil {
						break
					}
				}
				r.Close()
			}
		})
	}
}
