package s3inventory_test

import (
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/s3inventory"
	"github.com/parquet-go/parquet-go"
)

// Test fixture keys shared between CSV and Parquet tests so changes
// stay in sync; also satisfies goconst.
const (
	testKeyABC = "a/b/c.txt"
	testKeyDE  = "d/e.txt"
)

func TestCSVInventoryReader(t *testing.T) {
	csv := testKeyABC + ",100,STANDARD,\n" + testKeyDE + ",200,GLACIER,\n"
	r := s3inventory.NewCSVReader(bytes.NewReader([]byte(csv)), s3inventory.CSVReaderConfig{
		KeyCol:        0,
		SizeCol:       1,
		StorageCol:    2,
		AccessTierCol: 3,
	})

	row, err := r.Next()
	if err != nil {
		t.Fatalf("Next failed: %v", err)
	}
	if row.Key != testKeyABC || row.Size != 100 || row.StorageClass != "STANDARD" {
		t.Errorf("got %+v, want {Key:%s Size:100 StorageClass:STANDARD}", row, testKeyABC)
	}

	row, err = r.Next()
	if err != nil {
		t.Fatalf("Next failed: %v", err)
	}
	if row.Key != testKeyDE || row.Size != 200 || row.StorageClass != "GLACIER" {
		t.Errorf("got %+v, want {Key:%s Size:200 StorageClass:GLACIER}", row, testKeyDE)
	}

	_, err = r.Next()
	if !errors.Is(err, io.EOF) {
		t.Errorf("expected EOF, got %v", err)
	}
}

func TestCSVInventoryReaderFromStream(t *testing.T) {
	csv := "file.txt,1024,STANDARD,\n"

	// Create a mock ReadCloser
	r, err := s3inventory.NewCSVReaderFromStream(
		io.NopCloser(bytes.NewReader([]byte(csv))),
		"test.csv",
		s3inventory.CSVReaderConfig{
			KeyCol:     0,
			SizeCol:    1,
			StorageCol: 2,
		},
	)
	if err != nil {
		t.Fatalf("NewCSVReaderFromStream failed: %v", err)
	}
	defer r.Close()

	row, err := r.Next()
	if err != nil {
		t.Fatalf("Next failed: %v", err)
	}
	if row.Key != "file.txt" || row.Size != 1024 {
		t.Errorf("got %+v, want {Key:file.txt Size:1024}", row)
	}
}

func TestCSVInventoryReaderFromStream_Gzip(t *testing.T) {
	csv := "file.txt,1024,STANDARD,\n"

	var buf bytes.Buffer
	gzw := gzip.NewWriter(&buf)
	_, _ = gzw.Write([]byte(csv))
	gzw.Close()

	r, err := s3inventory.NewCSVReaderFromStream(
		io.NopCloser(bytes.NewReader(buf.Bytes())),
		"test.csv.gz",
		s3inventory.CSVReaderConfig{
			KeyCol:     0,
			SizeCol:    1,
			StorageCol: 2,
		},
	)
	if err != nil {
		t.Fatalf("NewCSVReaderFromStream failed: %v", err)
	}
	defer r.Close()

	row, err := r.Next()
	if err != nil {
		t.Fatalf("Next failed: %v", err)
	}
	if row.Key != "file.txt" || row.Size != 1024 {
		t.Errorf("got %+v, want {Key:file.txt Size:1024}", row)
	}
}

// S3InventoryRecord represents a row in an S3 inventory Parquet file.
type S3InventoryRecord struct {
	Key          string `parquet:"key"`
	StorageClass string `parquet:"storage_class,optional"`
	Size         int64  `parquet:"size"`
}

func TestParquetInventoryReader(t *testing.T) {
	dir := t.TempDir()
	parquetPath := filepath.Join(dir, "test.parquet")

	// Create test Parquet file
	rows := []S3InventoryRecord{
		{Key: testKeyABC, Size: 100, StorageClass: "STANDARD"},
		{Key: testKeyDE, Size: 200, StorageClass: "GLACIER"},
		{Key: "f/g/h.txt", Size: 300, StorageClass: "STANDARD_IA"},
	}

	if err := parquet.WriteFile(parquetPath, rows); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	// Read with parquet inventory reader
	f, err := os.Open(parquetPath)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}

	reader, err := s3inventory.NewParquetReader(f, info.Size(), s3inventory.ParquetReaderConfig{
		KeyCol:     0,
		StorageCol: 1,
		SizeCol:    2,
	})
	if err != nil {
		t.Fatalf("NewParquetReader failed: %v", err)
	}
	defer reader.Close()

	// Verify rows
	for i, expected := range rows {
		row, err := reader.Next()
		if err != nil {
			t.Fatalf("Next(%d) failed: %v", i, err)
		}
		if row.Key != expected.Key {
			t.Errorf("row %d: Key = %q, want %q", i, row.Key, expected.Key)
		}
		if row.Size != uint64(expected.Size) {
			t.Errorf("row %d: Size = %d, want %d", i, row.Size, expected.Size)
		}
		if row.StorageClass != expected.StorageClass {
			t.Errorf("row %d: StorageClass = %q, want %q", i, row.StorageClass, expected.StorageClass)
		}
	}

	_, err = reader.Next()
	if !errors.Is(err, io.EOF) {
		t.Errorf("expected EOF, got %v", err)
	}
}

func TestParquetInventoryReaderFromStream(t *testing.T) {
	dir := t.TempDir()
	parquetPath := filepath.Join(dir, "test.parquet")

	// Create test Parquet file
	rows := []S3InventoryRecord{
		{Key: "file1.txt", Size: 100, StorageClass: "STANDARD"},
		{Key: "file2.txt", Size: 200, StorageClass: "GLACIER"},
	}

	if err := parquet.WriteFile(parquetPath, rows); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	// Read file content
	content, err := os.ReadFile(parquetPath)
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}

	// Create reader from stream
	reader, err := s3inventory.NewParquetReaderFromStream(
		io.NopCloser(bytes.NewReader(content)),
		int64(len(content)),
	)
	if err != nil {
		t.Fatalf("NewParquetReaderFromStream failed: %v", err)
	}
	defer reader.Close()

	// Verify first row
	row, err := reader.Next()
	if err != nil {
		t.Fatalf("Next failed: %v", err)
	}
	if row.Key != "file1.txt" || row.Size != 100 {
		t.Errorf("got %+v, want {Key:file1.txt Size:100}", row)
	}

	// Verify second row
	row, err = reader.Next()
	if err != nil {
		t.Fatalf("Next failed: %v", err)
	}
	if row.Key != "file2.txt" || row.Size != 200 {
		t.Errorf("got %+v, want {Key:file2.txt Size:200}", row)
	}

	_, err = reader.Next()
	if !errors.Is(err, io.EOF) {
		t.Errorf("expected EOF, got %v", err)
	}
}

func TestCSVAndParquetEquivalence(t *testing.T) {
	dir := t.TempDir()

	// Create test data
	testRows := []struct {
		Key          string
		StorageClass string
		Size         uint64
	}{
		{Key: "data/file1.txt", Size: 100, StorageClass: "STANDARD"},
		{Key: "data/file2.txt", Size: 200, StorageClass: "GLACIER"},
		{Key: "data/subfolder/file3.txt", Size: 300, StorageClass: "STANDARD_IA"},
		{Key: "archive/old.zip", Size: 1000, StorageClass: "DEEP_ARCHIVE"},
	}

	// Create CSV file
	csvPath := filepath.Join(dir, "s3inventory.csv")
	var csvBuf bytes.Buffer
	for _, r := range testRows {
		fmt.Fprintf(&csvBuf, "%s,%d,%s,\n", r.Key, r.Size, r.StorageClass)
	}
	if err := os.WriteFile(csvPath, csvBuf.Bytes(), 0o600); err != nil {
		t.Fatalf("WriteFile CSV failed: %v", err)
	}

	// Create Parquet file
	parquetPath := filepath.Join(dir, "s3inventory.parquet")
	parquetRows := make([]S3InventoryRecord, len(testRows))
	for i, r := range testRows {
		parquetRows[i] = S3InventoryRecord{
			Key:          r.Key,
			Size:         int64(r.Size),
			StorageClass: r.StorageClass,
		}
	}
	if err := parquet.WriteFile(parquetPath, parquetRows); err != nil {
		t.Fatalf("WriteFile Parquet failed: %v", err)
	}

	// Read CSV
	csvReader := s3inventory.NewCSVReader(
		bytes.NewReader(csvBuf.Bytes()),
		s3inventory.CSVReaderConfig{KeyCol: 0, SizeCol: 1, StorageCol: 2},
	)
	defer csvReader.Close()

	// Read Parquet
	f, _ := os.Open(parquetPath)
	defer f.Close()
	info, _ := f.Stat()
	parquetReader, err := s3inventory.NewParquetReader(f, info.Size(), s3inventory.ParquetReaderConfig{
		KeyCol: 0, StorageCol: 1, SizeCol: 2,
	})
	if err != nil {
		t.Fatalf("NewParquetReader failed: %v", err)
	}
	defer parquetReader.Close()

	// Compare outputs
	for i := range testRows {
		csvRow, csvErr := csvReader.Next()
		parquetRow, parquetErr := parquetReader.Next()

		if !errors.Is(csvErr, parquetErr) {
			t.Errorf("row %d: CSV err=%v, Parquet err=%v", i, csvErr, parquetErr)

			continue
		}
		if csvErr != nil {
			continue
		}

		if csvRow.Key != parquetRow.Key {
			t.Errorf("row %d: Key mismatch: CSV=%q, Parquet=%q", i, csvRow.Key, parquetRow.Key)
		}
		if csvRow.Size != parquetRow.Size {
			t.Errorf("row %d: Size mismatch: CSV=%d, Parquet=%d", i, csvRow.Size, parquetRow.Size)
		}
		if csvRow.StorageClass != parquetRow.StorageClass {
			t.Errorf("row %d: StorageClass mismatch: CSV=%q, Parquet=%q", i, csvRow.StorageClass, parquetRow.StorageClass)
		}
	}
}

func TestParquetInventoryReader_EmptyFile(t *testing.T) {
	dir := t.TempDir()
	parquetPath := filepath.Join(dir, "empty.parquet")

	// Create empty Parquet file
	rows := []S3InventoryRecord{}
	if err := parquet.WriteFile(parquetPath, rows); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	f, _ := os.Open(parquetPath)
	defer f.Close()
	info, _ := f.Stat()

	reader, err := s3inventory.NewParquetReader(f, info.Size(), s3inventory.ParquetReaderConfig{
		KeyCol: 0, SizeCol: 1, StorageCol: 2,
	})
	if err != nil {
		t.Fatalf("NewParquetReader failed: %v", err)
	}
	defer reader.Close()

	_, err = reader.Next()
	if !errors.Is(err, io.EOF) {
		t.Errorf("expected EOF for empty file, got %v", err)
	}
}

func TestParquetInventoryReader_LargeRowGroups(t *testing.T) {
	dir := t.TempDir()
	parquetPath := filepath.Join(dir, "large.parquet")

	// Create many rows to test row group handling
	numRows := 5000
	rows := make([]S3InventoryRecord, numRows)
	for i := range numRows {
		rows[i] = S3InventoryRecord{
			Key:          "file" + string(rune('0'+i%10)) + ".txt",
			Size:         int64(i * 100),
			StorageClass: "STANDARD",
		}
	}

	if err := parquet.WriteFile(parquetPath, rows); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	f, _ := os.Open(parquetPath)
	defer f.Close()
	info, _ := f.Stat()

	reader, err := s3inventory.NewParquetReader(f, info.Size(), s3inventory.ParquetReaderConfig{
		KeyCol: 0, SizeCol: 1, StorageCol: 2,
	})
	if err != nil {
		t.Fatalf("NewParquetReader failed: %v", err)
	}
	defer reader.Close()

	// Count rows
	count := 0
	for {
		_, err := reader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("Next failed at row %d: %v", count, err)
		}
		count++
	}

	if count != numRows {
		t.Errorf("got %d rows, want %d", count, numRows)
	}
}

// S3PascalCaseRecord mirrors what AWS S3 Inventory actually publishes
// for Parquet exports: PascalCase column names that match the CSV
// FileSchema convention, not the snake_case our older test record used.
// Regression coverage for the silent-drop bug that left StorageClass
// and IntelligentTieringAccessTier at -1 whenever the producer's
// column naming didn't match the hardcoded lowercase strings.
type S3PascalCaseRecord struct {
	Key                          string `parquet:"Key"`
	StorageClass                 string `parquet:"StorageClass,optional"`
	IntelligentTieringAccessTier string `parquet:"IntelligentTieringAccessTier,optional"`
	Size                         int64  `parquet:"Size"`
}

func TestParquetInventoryReader_DetectsPascalCaseColumns(t *testing.T) {
	dir := t.TempDir()
	parquetPath := filepath.Join(dir, "pascal.parquet")
	rows := []S3PascalCaseRecord{
		{Key: testKeyABC, Size: 100, StorageClass: "STANDARD", IntelligentTieringAccessTier: "FREQUENT"},
		{Key: testKeyDE, Size: 200, StorageClass: "INTELLIGENT_TIERING", IntelligentTieringAccessTier: "INFREQUENT"},
	}
	if err := parquet.WriteFile(parquetPath, rows); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	f, err := os.Open(parquetPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}

	reader, err := s3inventory.NewParquetReaderFromReaderAt(f, info.Size())
	if err != nil {
		t.Fatalf("NewParquetReaderFromReaderAt: %v", err)
	}
	defer reader.Close()

	got := make([]s3inventory.Row, 0, len(rows))
	for {
		row, err := reader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		got = append(got, row)
	}
	if len(got) != len(rows) {
		t.Fatalf("len(rows) = %d, want %d", len(got), len(rows))
	}
	for i, want := range rows {
		if got[i].Key != want.Key {
			t.Errorf("row %d Key = %q, want %q", i, got[i].Key, want.Key)
		}
		if got[i].StorageClass != want.StorageClass {
			t.Errorf("row %d StorageClass = %q, want %q (silent drop regression)", i, got[i].StorageClass, want.StorageClass)
		}
		if got[i].AccessTier != want.IntelligentTieringAccessTier {
			t.Errorf("row %d AccessTier = %q, want %q (silent drop regression)", i, got[i].AccessTier, want.IntelligentTieringAccessTier)
		}
	}
}

// S3MixedCaseRecord checks a worst-case producer that mixes naming
// styles in a single schema. The canonicaliser should still find every
// column.
type S3MixedCaseRecord struct {
	Key          string `parquet:"key"`
	StorageClass string `parquet:"storage_class,optional"`
	Size         int64  `parquet:"SIZE"`
}

func TestParquetInventoryReader_DetectsMixedCaseColumns(t *testing.T) {
	dir := t.TempDir()
	parquetPath := filepath.Join(dir, "mixed.parquet")
	rows := []S3MixedCaseRecord{
		{Key: testKeyABC, Size: 100, StorageClass: "STANDARD"},
	}
	if err := parquet.WriteFile(parquetPath, rows); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	f, err := os.Open(parquetPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer f.Close()
	info, _ := f.Stat()
	reader, err := s3inventory.NewParquetReaderFromReaderAt(f, info.Size())
	if err != nil {
		t.Fatalf("NewParquetReaderFromReaderAt: %v", err)
	}
	defer reader.Close()

	row, err := reader.Next()
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if row.Key != testKeyABC || row.Size != 100 || row.StorageClass != "STANDARD" {
		t.Errorf("row = %+v, want Key=%s Size=100 StorageClass=STANDARD", row, testKeyABC)
	}
}

// Benchmark functions

func BenchmarkCSVInventoryReader(b *testing.B) {
	// Generate CSV data
	numRows := 10000
	var buf bytes.Buffer
	for i := range numRows {
		fmt.Fprintf(&buf, "data/folder%d/subfolder/file%d.txt,%d,STANDARD,\n", i%100, i, i*1000)
	}
	csvData := buf.Bytes()

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		reader := s3inventory.NewCSVReader(bytes.NewReader(csvData), s3inventory.CSVReaderConfig{
			KeyCol: 0, SizeCol: 1, StorageCol: 2, AccessTierCol: 3,
		})

		count := 0
		for {
			_, err := reader.Next()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				b.Fatal(err)
			}
			count++
		}
		reader.Close()

		if count != numRows {
			b.Fatalf("got %d rows, want %d", count, numRows)
		}
	}
}

func BenchmarkParquetInventoryReader(b *testing.B) {
	// Create temp Parquet file
	dir := b.TempDir()
	parquetPath := filepath.Join(dir, "bench.parquet")

	numRows := 10000
	rows := make([]S3InventoryRecord, numRows)
	for i := range numRows {
		rows[i] = S3InventoryRecord{
			Key:          fmt.Sprintf("data/folder%d/subfolder/file%d.txt", i%100, i),
			Size:         int64(i * 1000),
			StorageClass: "STANDARD",
		}
	}

	if err := parquet.WriteFile(parquetPath, rows); err != nil {
		b.Fatalf("WriteFile failed: %v", err)
	}

	// Read file content into memory for fair comparison
	content, err := os.ReadFile(parquetPath)
	if err != nil {
		b.Fatalf("ReadFile failed: %v", err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		reader, err := s3inventory.NewParquetReaderFromStream(
			io.NopCloser(bytes.NewReader(content)),
			int64(len(content)),
		)
		if err != nil {
			b.Fatal(err)
		}

		count := 0
		for {
			_, err := reader.Next()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				b.Fatal(err)
			}
			count++
		}
		reader.Close()

		if count != numRows {
			b.Fatalf("got %d rows, want %d", count, numRows)
		}
	}
}
