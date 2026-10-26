package s3inventory_test

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/s3inventory"
)

// csvKeyShapes are the key encodings real CSV inventories show:
// escapes in every directory (chunked-array stores encode the commas
// in chunk coordinates), escapes in file names only, and none at all.
var csvKeyShapes = []struct {
	name string
	key  func(i int) string
}{
	{"escaped_dirs", func(i int) string {
		return fmt.Sprintf("store/grids/%d/%d%%2C%d%%2C0%%2C10/image_data/0.0.%d", i/4096, i/64%64, i%64, i%8)
	}},
	{"escaped_names", func(i int) string {
		return fmt.Sprintf("logs/2026/10/%02d/report+%d%%20final%%281%%29.json", i/40000, i)
	}},
	{"plain", func(i int) string {
		return fmt.Sprintf("data/2026/10/%02d/part-%07d.parquet", i/40000, i)
	}},
}

// csvInventory returns n inventory records as S3 writes them: every
// field quoted, keys URL encoded.
func csvInventory(n int, key func(int) string) []byte {
	var buf bytes.Buffer
	for i := range n {
		fmt.Fprintf(&buf, "\"example-bucket\",\"%s\",\"%d\",\"STANDARD\"\n", key(i), 1024+i%4096)
	}

	return buf.Bytes()
}

// BenchmarkCSVReader reads a CSV inventory per key shape, so it
// measures splitting and key decoding without download or gunzip.
func BenchmarkCSVReader(b *testing.B) {
	const n = 200_000
	cfg := s3inventory.CSVReaderConfig{KeyCol: 1, SizeCol: 2, StorageCol: 3, AccessTierCol: -1, DeleteMarkerCol: -1}
	for _, shape := range csvKeyShapes {
		data := csvInventory(n, shape.key)
		b.Run("keys="+shape.name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(data)))
			for b.Loop() {
				r := s3inventory.NewCSVReader(bytes.NewReader(data), cfg)
				count := 0
				for {
					_, err := r.Next()
					if errors.Is(err, io.EOF) {
						break
					}
					if err != nil {
						b.Fatal(err)
					}
					count++
				}
				if count != n {
					b.Fatalf("got %d rows, want %d", count, n)
				}
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*n), "ns/row")
		})
	}
}
