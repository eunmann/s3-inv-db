package s3inventory_test

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/s3inventory"
	"github.com/parquet-go/parquet-go"
)

// The column-wise reader must reassemble rows exactly across row
// groups and pages, where each column's page boundaries differ.
func TestParquetReader_WideSchemaAcrossRowGroupsAndPages(t *testing.T) {
	const n = 5000
	rows := make([]awsInventoryRecord, n)
	for i := range rows {
		rows[i] = awsInventoryRecord{
			Key:          fmt.Sprintf("dir%d/file-%05d.bin", i%7, i),
			Size:         int64(i * 3),
			ETag:         fmt.Sprintf("%032x", i),
			ObjectOwner:  fmt.Sprintf("%064x", i*i),
			IsLatest:     true,
			StorageClass: []string{"STANDARD", "", "GLACIER", "INTELLIGENT_TIERING"}[i%4],
		}
		if rows[i].StorageClass == "INTELLIGENT_TIERING" {
			rows[i].IntelligentTieringAccessTier = "ARCHIVE_ACCESS"
		}
		rows[i].IsDeleteMarker = i%11 == 0
	}
	path := filepath.Join(t.TempDir(), "wide.parquet")
	if err := parquet.WriteFile(path, rows,
		parquet.MaxRowsPerRowGroup(1200), parquet.PageBufferSize(4096)); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	got := readAllRows(t, openParquetFile(t, path))

	var want []s3inventory.Row
	for _, r := range rows {
		if r.IsDeleteMarker {
			continue
		}
		want = append(want, s3inventory.Row{
			Key: r.Key, Size: uint64(r.Size), StorageClass: r.StorageClass, AccessTier: r.IntelligentTieringAccessTier,
		})
	}
	if len(got) != len(want) {
		t.Fatalf("got %d rows, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("row %d = %+v, want %+v", i, got[i], want[i])
		}
	}
}

func TestNewParquetReader_RejectsColumnOutOfRange(t *testing.T) {
	path := filepath.Join(t.TempDir(), "narrow.parquet")
	if err := parquet.WriteFile(path, []S3InventoryRecord{{Key: "a", Size: 1}}); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	_, err = s3inventory.NewParquetReader(f, info.Size(), s3inventory.ParquetReaderConfig{
		KeyCol: 0, SizeCol: 1, StorageCol: 9, AccessTierCol: -1, DeleteMarkerCol: -1,
	})
	if !errors.Is(err, s3inventory.ErrColumnOutOfRange) {
		t.Fatalf("err = %v, want ErrColumnOutOfRange", err)
	}
}

func openParquetFile(t *testing.T, path string) *s3inventory.ParquetReader {
	t.Helper()
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
