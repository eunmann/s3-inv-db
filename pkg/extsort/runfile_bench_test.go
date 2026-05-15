package extsort

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

// makePrefixRows returns N rows with realistic tier sparsity — only
// Standard populated, matching a typical S3 bucket. Most of the on-disk
// tier columns are zero.
func makePrefixRows(n int) []*PrefixRow {
	rows := make([]*PrefixRow, n)
	for i := range rows {
		r := &PrefixRow{
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

// BenchmarkRunFileWriteUncompressed measures uncompressed run file write
// throughput and on-disk size. With the current fat fixed-width format,
// per-row overhead is ~214 + len(prefix) bytes regardless of tier sparsity.
func BenchmarkRunFileWriteUncompressed(b *testing.B) {
	const N = 50_000
	rows := makePrefixRows(N)

	b.ReportAllocs()
	b.ResetTimer()
	var totalBytes int64
	for i := range b.N {
		path := filepath.Join(b.TempDir(), fmt.Sprintf("run_%d.bin", i))
		w, err := NewRunFileWriter(path, 4*1024*1024)
		if err != nil {
			b.Fatal(err)
		}
		if err := w.WriteAll(rows); err != nil {
			b.Fatal(err)
		}
		if err := w.Close(); err != nil {
			b.Fatal(err)
		}
		st, _ := os.Stat(path)
		totalBytes += st.Size()
	}
	b.ReportMetric(float64(totalBytes)/float64(b.N), "bytes/file")
	b.ReportMetric(float64(totalBytes)/float64(b.N*N), "bytes/row")
}

// BenchmarkRunFileWriteCompressed measures the compressed (zstd) path.
func BenchmarkRunFileWriteCompressed(b *testing.B) {
	const N = 50_000
	rows := makePrefixRows(N)

	b.ReportAllocs()
	b.ResetTimer()
	var totalBytes int64
	for i := range b.N {
		path := filepath.Join(b.TempDir(), fmt.Sprintf("run_%d.crun", i))
		w, err := NewCompressedRunWriter(path, CompressedRunWriterOptions{
			BufferSize:       4 * 1024 * 1024,
			CompressionLevel: CompressionFastest,
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
		st, _ := os.Stat(path)
		totalBytes += st.Size()
	}
	b.ReportMetric(float64(totalBytes)/float64(b.N), "bytes/file")
	b.ReportMetric(float64(totalBytes)/float64(b.N*N), "bytes/row")
}

// BenchmarkRunFileReadCompressed measures read throughput including zstd
// decoder setup cost (relevant for parallel-merge fan-in).
func BenchmarkRunFileReadCompressed(b *testing.B) {
	const N = 50_000
	rows := makePrefixRows(N)
	path := filepath.Join(b.TempDir(), "bench.crun")
	w, err := NewCompressedRunWriter(path, CompressedRunWriterOptions{
		BufferSize:       4 * 1024 * 1024,
		CompressionLevel: CompressionFastest,
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
		r, err := OpenCompressedRunFile(path, 4*1024*1024)
		if err != nil {
			b.Fatal(err)
		}
		for {
			_, err := r.Read()
			if err != nil {
				break
			}
		}
		r.Close()
	}
}
