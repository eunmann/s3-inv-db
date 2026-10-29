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

// mix scrambles i (splitmix64's finalizer), so per-object fields such
// as ETags and version IDs vary as they do in a real inventory and
// gzip compresses them no better than it would those.
func mix(i int) uint64 {
	z := uint64(i) + 0x9e3779b97f4a7c15
	z = (z ^ z>>30) * 0xbf58476d1ce4e5b9
	z = (z ^ z>>27) * 0x94d049bb133111eb

	return z ^ z>>31
}

// csvLayouts are the row layouts a CSV inventory comes in: S3's own
// (every field quoted), the same fields unquoted, and a versioned
// inventory with most optional fields, quoted, whose long fields (ETag,
// ACL, owner) follow the key, size and storage class.
var csvLayouts = []struct {
	name string
	cfg  s3inventory.CSVReaderConfig
	row  func(w io.Writer, i int)
}{
	{
		"quoted",
		s3inventory.CSVReaderConfig{KeyCol: 1, SizeCol: 2, StorageCol: 3, AccessTierCol: -1, DeleteMarkerCol: -1},
		func(w io.Writer, i int) {
			fmt.Fprintf(w, "\"example-bucket\",\"%s\",\"%d\",\"STANDARD\"\n", csvKeyShapes[1].key(i), 1024+i%4096)
		},
	},
	{
		"unquoted",
		s3inventory.CSVReaderConfig{KeyCol: 1, SizeCol: 2, StorageCol: 3, AccessTierCol: -1, DeleteMarkerCol: -1},
		func(w io.Writer, i int) {
			fmt.Fprintf(w, "example-bucket,%s,%d,STANDARD\n", csvKeyShapes[1].key(i), 1024+i%4096)
		},
	},
	{
		// Bucket, Key, VersionId, IsLatest, IsDeleteMarker, Size,
		// LastModifiedDate, ETag, StorageClass, IsMultipartUploaded,
		// ReplicationStatus, EncryptionStatus, IntelligentTieringAccessTier,
		// ChecksumAlgorithm, ObjectAccessControlList, ObjectOwner
		"wide",
		s3inventory.CSVReaderConfig{KeyCol: 1, SizeCol: 5, StorageCol: 8, AccessTierCol: 12, DeleteMarkerCol: 4},
		func(w io.Writer, i int) {
			h := mix(i)
			storage, tier := "STANDARD", ""
			if i%3 == 0 {
				storage, tier = "INTELLIGENT_TIERING", "FREQUENT"
			}
			fmt.Fprintf(w, "\"example-bucket\",\"%s\",\"%016x%016x\",\"true\",\"false\",\"%d\",\"2026-10-%02dT%02d:%02d:%02d.000Z\",\"%016x%016x\",\"%s\",\"%t\",\"\",\"SSE-S3\",\"%s\",\"CRC64NVME\",\"%s\",\"%s\"\n",
				csvKeyShapes[1].key(i), h, mix(i+1), 1024+h%(1<<30),
				1+i%28, h%24, h>>8%60, h>>16%60, mix(i+2), mix(i+3),
				storage, h%7 == 0, tier, wideACL, wideOwner)
		},
	},
}

const (
	wideACL   = "eyJ2ZXJzaW9uIjoiMjAyMi0xMS0zMCIsInN0YXR1cyI6IkFWQUlMQUJMRSIsImdyYW50cyI6W3sicGVybWlzc2lvbiI6IkZVTExfQ09OVFJPTCIsImdyYW50ZWVUeXBlIjoiQ0FOT05JQ0FMIiwiY2Fub25pY2FsSWQiOiJleGFtcGxlIn1dfQ=="
	wideOwner = "79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be"
)

// layoutInventory returns n records of a layout.
func layoutInventory(n int, row func(io.Writer, int)) []byte {
	var buf bytes.Buffer
	for i := range n {
		row(&buf, i)
	}

	return buf.Bytes()
}

// countRows reads r to EOF and returns the rows read, failing on any
// anomaly so a fixture the reader misreads can't pass as fast.
func countRows(b *testing.B, r s3inventory.Reader) int {
	b.Helper()
	count := 0
	for {
		_, err := r.Next()
		if errors.Is(err, io.EOF) {
			if a := r.Anomalies(); a.Total() > 0 {
				b.Fatalf("anomalies: %s", a)
			}

			return count
		}
		if err != nil {
			b.Fatal(err)
		}
		count++
	}
}

// BenchmarkCSVReader reads a CSV inventory per key shape and per
// layout, so it measures splitting and key decoding without download or
// gunzip.
func BenchmarkCSVReader(b *testing.B) {
	const n = 200_000
	cfg := s3inventory.CSVReaderConfig{KeyCol: 1, SizeCol: 2, StorageCol: 3, AccessTierCol: -1, DeleteMarkerCol: -1}
	for _, shape := range csvKeyShapes {
		data := csvInventory(n, shape.key)
		b.Run("keys="+shape.name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(data)))
			for b.Loop() {
				if count := countRows(b, s3inventory.NewCSVReader(bytes.NewReader(data), cfg)); count != n {
					b.Fatalf("got %d rows, want %d", count, n)
				}
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*n), "ns/row")
		})
	}
	for _, layout := range csvLayouts[1:] {
		b.Run("layout="+layout.name, func(b *testing.B) {
			data := layoutInventory(n, layout.row)
			b.ReportAllocs()
			b.SetBytes(int64(len(data)))
			for b.Loop() {
				if count := countRows(b, s3inventory.NewCSVReader(bytes.NewReader(data), layout.cfg)); count != n {
					b.Fatalf("got %d rows, want %d", count, n)
				}
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*n), "ns/row")
		})
	}
}

// BenchmarkCSVReaderGzip reads a gzipped CSV inventory per layout
// through NewCSVReaderFromStream, as the build does a downloaded chunk:
// gunzip, splitting and key decoding, without the download. Bytes per
// op are the uncompressed CSV.
func BenchmarkCSVReaderGzip(b *testing.B) {
	const n = 200_000
	for _, layout := range csvLayouts {
		b.Run("layout="+layout.name, func(b *testing.B) {
			data := layoutInventory(n, layout.row)
			gz := gzipMembers(b, string(data))
			b.ReportAllocs()
			b.SetBytes(int64(len(data)))
			for b.Loop() {
				r, err := s3inventory.NewCSVReaderFromStream(io.NopCloser(bytes.NewReader(gz)), layout.cfg)
				if err != nil {
					b.Fatal(err)
				}
				if count := countRows(b, r); count != n {
					b.Fatalf("got %d rows, want %d", count, n)
				}
				if err := r.Close(); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*n), "ns/row")
		})
	}
}
