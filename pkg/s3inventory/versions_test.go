package s3inventory_test

import (
	"bytes"
	"encoding/csv"
	"errors"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/s3inventory"
	"github.com/parquet-go/parquet-go"
)

// specialKeys exercise S3 Inventory's CSV key encoding: spaces, a literal
// '+', '%', unicode, and characters that need quoting in CSV.
//

var specialKeys = []string{
	"plain/key.txt",
	"my folder/a b.txt",
	"plus/a+b.txt",
	"percent/100%.txt",
	"données/été.csv",
	`quote/"x",y.txt`,
}

// encodeCSVKey mirrors what S3 Inventory writes for a key in CSV.
func encodeCSVKey(k string) string {
	return strings.ReplaceAll(url.QueryEscape(k), "%2F", "/")
}

func readAllRows(t *testing.T, r s3inventory.Reader) []s3inventory.Row {
	t.Helper()
	var out []s3inventory.Row
	for {
		row, err := r.Next()
		if errors.Is(err, io.EOF) {
			return out
		}
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		out = append(out, row)
	}
}

func csvBytes(t *testing.T, records [][]string) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := csv.NewWriter(&buf)
	if err := w.WriteAll(records); err != nil {
		t.Fatalf("write csv: %v", err)
	}

	return buf.Bytes()
}

func TestCSVReader_DecodesURLEncodedKeys(t *testing.T) {
	records := make([][]string, 0, len(specialKeys))
	for _, k := range specialKeys {
		records = append(records, []string{encodeCSVKey(k), "1"})
	}
	r := s3inventory.NewCSVReader(bytes.NewReader(csvBytes(t, records)),
		s3inventory.CSVReaderConfig{KeyCol: 0, SizeCol: 1, StorageCol: -1, AccessTierCol: -1, DeleteMarkerCol: -1})

	rows := readAllRows(t, r)
	if len(rows) != len(specialKeys) {
		t.Fatalf("got %d rows, want %d", len(rows), len(specialKeys))
	}
	for i, want := range specialKeys {
		if rows[i].Key != want {
			t.Errorf("row %d key = %q, want %q", i, rows[i].Key, want)
		}
	}
}

func TestCSVReader_KeepsUndecodableKeyVerbatim(t *testing.T) {
	data := csvBytes(t, [][]string{{"bad%zz/key", "5"}, {"ok%20key", "6"}})
	r := s3inventory.NewCSVReader(bytes.NewReader(data),
		s3inventory.CSVReaderConfig{KeyCol: 0, SizeCol: 1, StorageCol: -1, AccessTierCol: -1, DeleteMarkerCol: -1})

	rows := readAllRows(t, r)
	if len(rows) != 2 || rows[0].Key != "bad%zz/key" || rows[1].Key != "ok key" {
		t.Fatalf("rows = %+v", rows)
	}
	if got := r.UndecodableKeyCount(); got != 1 {
		t.Errorf("UndecodableKeyCount = %d, want 1", got)
	}
}

func TestCSVReader_SkipsDeleteMarkers(t *testing.T) {
	// Key, Size, IsLatest, IsDeleteMarker — the all-versions layout.
	data := csvBytes(t, [][]string{
		{"a/live.txt", "10", "true", "false"},
		{"a/old.txt", "20", "false", "false"},
		{"a/gone.txt", "", "true", "true"},
		{"a/gone2.txt", "", "false", "TRUE"},
	})
	r := s3inventory.NewCSVReader(bytes.NewReader(data),
		s3inventory.CSVReaderConfig{KeyCol: 0, SizeCol: 1, StorageCol: -1, AccessTierCol: -1, DeleteMarkerCol: 3})

	rows := readAllRows(t, r)
	if len(rows) != 2 || rows[0].Key != "a/live.txt" || rows[1].Key != "a/old.txt" {
		t.Fatalf("rows = %+v, want the two stored versions only", rows)
	}
	if r.MalformedRowCount() != 0 {
		t.Errorf("delete markers counted as malformed rows: %d", r.MalformedRowCount())
	}
}

type versionedRecord struct {
	Key            string `parquet:"key"`
	Size           int64  `parquet:"size,optional"`
	IsLatest       bool   `parquet:"is_latest"`
	IsDeleteMarker bool   `parquet:"is_delete_marker"`
}

func openParquet(t *testing.T, rows any) *s3inventory.ParquetReader {
	t.Helper()
	path := filepath.Join(t.TempDir(), "inv.parquet")
	var err error
	switch v := rows.(type) {
	case []versionedRecord:
		err = parquet.WriteFile(path, v)
	case []S3InventoryRecord:
		err = parquet.WriteFile(path, v)
	}
	if err != nil {
		t.Fatalf("write parquet: %v", err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.Close() })
	info, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	r, err := s3inventory.NewParquetReaderFromReaderAt(f, info.Size())
	if err != nil {
		t.Fatalf("NewParquetReaderFromReaderAt: %v", err)
	}
	t.Cleanup(func() { _ = r.Close() })

	return r
}

func TestParquetReader_SkipsDeleteMarkers(t *testing.T) {
	r := openParquet(t, []versionedRecord{
		{Key: "a/live.txt", Size: 10, IsLatest: true},
		{Key: "a/gone.txt", IsLatest: true, IsDeleteMarker: true},
		{Key: "a/old.txt", Size: 20},
	})

	rows := readAllRows(t, r)
	if len(rows) != 2 || rows[0].Key != "a/live.txt" || rows[1].Key != "a/old.txt" {
		t.Fatalf("rows = %+v, want the two stored versions only", rows)
	}
}

// The same bucket inventoried as CSV (encoded keys) and Parquet (raw
// keys) must produce identical rows.
func TestCSVAndParquet_SameKeysForSpecialCharacters(t *testing.T) {
	records := make([][]string, 0, len(specialKeys))
	pq := make([]S3InventoryRecord, 0, len(specialKeys))
	for i, k := range specialKeys {
		records = append(records, []string{encodeCSVKey(k), strconv.Itoa(i + 1)})
		pq = append(pq, S3InventoryRecord{Key: k, Size: int64(i + 1)})
	}
	csvRows := readAllRows(t, s3inventory.NewCSVReader(bytes.NewReader(csvBytes(t, records)),
		s3inventory.CSVReaderConfig{KeyCol: 0, SizeCol: 1, StorageCol: -1, AccessTierCol: -1, DeleteMarkerCol: -1}))
	pqRows := readAllRows(t, openParquet(t, pq))

	if len(csvRows) != len(pqRows) {
		t.Fatalf("CSV %d rows, Parquet %d rows", len(csvRows), len(pqRows))
	}
	for i := range csvRows {
		if csvRows[i].Key != pqRows[i].Key || csvRows[i].Size != pqRows[i].Size {
			t.Errorf("row %d: CSV %+v, Parquet %+v", i, csvRows[i], pqRows[i])
		}
	}
}
