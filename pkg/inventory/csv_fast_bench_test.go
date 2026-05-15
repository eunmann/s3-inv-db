package inventory

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"strings"
	"testing"
)

// buildInventoryGzip generates n AWS-inventory-shaped rows and gzips them.
// 9-column rows averaging ~190 bytes uncompressed match production size.
func buildInventoryGzip(n int) []byte {
	var raw bytes.Buffer
	for i := range n {
		fmt.Fprintf(&raw,
			"my-bucket,tenant-%05d/year=2024/month=%02d/day=%02d/object-%08d.parquet,%d,2024-01-01T00:00:00.000Z,a1b2c3d4e5f60718293a4b5c6d7e8f90,STANDARD,false,REPLICA,SSE-KMS\n",
			i%1000, (i%12)+1, (i%28)+1, i, 4096+i)
	}
	var gz bytes.Buffer
	gzw, _ := gzip.NewWriterLevel(&gz, gzip.BestSpeed)
	gzw.Write(raw.Bytes())
	gzw.Close()
	return gz.Bytes()
}

// BenchmarkCSVGzip_Stdlib parses N gzipped AWS-inventory rows through
// the existing encoding/csv path used in production.
func BenchmarkCSVGzip_Stdlib(b *testing.B) {
	for _, n := range []int{100_000, 1_000_000, 10_000_000} {
		b.Run(fmt.Sprintf("rows=%d", n), func(b *testing.B) {
			data := buildInventoryGzip(n)
			b.SetBytes(int64(len(data)))
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				rc := io.NopCloser(bytes.NewReader(data))
				r, err := NewCSVInventoryReaderFromStream(rc, "data.csv.gz",
					CSVReaderConfig{KeyCol: 1, SizeCol: 2, StorageCol: 5, AccessTierCol: -1})
				if err != nil {
					b.Fatal(err)
				}
				var keySink string
				var bytesSum uint64
				for {
					row, err := r.Next()
					if err == io.EOF {
						break
					}
					if err != nil {
						b.Fatal(err)
					}
					keySink = row.Key
					bytesSum += row.Size
				}
				_ = keySink
				_ = bytesSum
				r.Close()
			}
		})
	}
}

// BenchmarkCSVGzip_Fast parses the same data through the hand-rolled
// parser. Same input, same output materialization (Key is a fresh
// string). Reports MB/s on the gzipped bytes.
func BenchmarkCSVGzip_Fast(b *testing.B) {
	for _, n := range []int{100_000, 1_000_000, 10_000_000} {
		b.Run(fmt.Sprintf("rows=%d", n), func(b *testing.B) {
			data := buildInventoryGzip(n)
			b.SetBytes(int64(len(data)))
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				rc := io.NopCloser(bytes.NewReader(data))
				r, err := NewCSVInventoryReaderFastFromStream(rc, "data.csv.gz",
					CSVReaderConfig{KeyCol: 1, SizeCol: 2, StorageCol: 5, AccessTierCol: -1})
				if err != nil {
					b.Fatal(err)
				}
				var keySink string
				var bytesSum uint64
				for {
					row, err := r.Next()
					if err == io.EOF {
						break
					}
					if err != nil {
						b.Fatal(err)
					}
					keySink = row.Key
					bytesSum += row.Size
				}
				_ = keySink
				_ = bytesSum
				r.Close()
			}
		})
	}
}

// Reference: how many objects are in the synthetic dataset?
// 10M rows × ~190 B = 1.9 GB raw; gzip BestSpeed ≈ 6:1 → ~320 MB on disk.
// Production S3 inventory chunks are commonly 200-500 MB gzipped, so
// 10M rows is in the same ballpark as one large real chunk.
var _ = strings.NewReader

// BenchmarkCSVPlain_Stdlib parses uncompressed CSV — isolates CSV cost
// from gzip cost. This is the bench that originally suggested the 2.7x
// hand-rolled win; we re-run it for honesty.
func BenchmarkCSVPlain_Stdlib(b *testing.B) {
	const N = 1_000_000
	raw := buildInventoryPlain(N)
	cfg := CSVReaderConfig{KeyCol: 1, SizeCol: 2, StorageCol: 5, AccessTierCol: -1}
	b.SetBytes(int64(len(raw)))
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		r := NewCSVInventoryReader(bytes.NewReader(raw), cfg)
		var sink uint64
		for {
			row, err := r.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				b.Fatal(err)
			}
			sink += row.Size
			_ = row.Key
		}
		_ = sink
	}
}

// BenchmarkCSVPlain_Fast same but hand-rolled.
func BenchmarkCSVPlain_Fast(b *testing.B) {
	const N = 1_000_000
	raw := buildInventoryPlain(N)
	cfg := CSVReaderConfig{KeyCol: 1, SizeCol: 2, StorageCol: 5, AccessTierCol: -1}
	b.SetBytes(int64(len(raw)))
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		r := NewCSVInventoryReaderFast(bytes.NewReader(raw), cfg)
		var sink uint64
		for {
			row, err := r.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				b.Fatal(err)
			}
			sink += row.Size
			_ = row.Key
		}
		_ = sink
	}
}

// BenchmarkCSVPlain_FastKeyOnly removes StorageClass extraction to
// isolate the per-field alloc effect.
func BenchmarkCSVPlain_FastKeyOnly(b *testing.B) {
	const N = 1_000_000
	raw := buildInventoryPlain(N)
	cfg := CSVReaderConfig{KeyCol: 1, SizeCol: 2, StorageCol: -1, AccessTierCol: -1}
	b.SetBytes(int64(len(raw)))
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		r := NewCSVInventoryReaderFast(bytes.NewReader(raw), cfg)
		var sink uint64
		for {
			row, err := r.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				b.Fatal(err)
			}
			sink += row.Size
			_ = row.Key
		}
		_ = sink
	}
}

func buildInventoryPlain(n int) []byte {
	var raw bytes.Buffer
	for i := range n {
		fmt.Fprintf(&raw,
			"my-bucket,tenant-%05d/year=2024/month=%02d/day=%02d/object-%08d.parquet,%d,2024-01-01T00:00:00.000Z,a1b2c3d4e5f60718293a4b5c6d7e8f90,STANDARD,false,REPLICA,SSE-KMS\n",
			i%1000, (i%12)+1, (i%28)+1, i, 4096+i)
	}
	return raw.Bytes()
}
