package s3inventory_test

import (
	"errors"
	"fmt"
	"io"
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
	f := openTestParquet(t, path)
	_, err := s3inventory.NewParquetReader(f, f.Size(), s3inventory.ParquetReaderConfig{
		KeyCol: s3inventory.ColumnAt(0), SizeCol: s3inventory.ColumnAt(2), StorageCol: s3inventory.ColumnAt(9),
	})
	if !errors.Is(err, s3inventory.ErrColumnOutOfRange) {
		t.Fatalf("err = %v, want ErrColumnOutOfRange", err)
	}
}

// TestNewParquetReader_RequiresKeyAndSizeColumns: a config without the
// key or size column, or a file whose schema lacks one, is refused
// rather than read from column 0.
func TestNewParquetReader_RequiresKeyAndSizeColumns(t *testing.T) {
	path := filepath.Join(t.TempDir(), "narrow.parquet")
	if err := parquet.WriteFile(path, []S3InventoryRecord{{Key: "a", Size: 1}}); err != nil {
		t.Fatal(err)
	}
	cases := map[string]struct {
		cfg  s3inventory.ParquetReaderConfig
		want error
	}{
		"zero":    {s3inventory.ParquetReaderConfig{}, s3inventory.ErrMissingKeyColumn},
		"no_key":  {s3inventory.ParquetReaderConfig{SizeCol: s3inventory.ColumnAt(2)}, s3inventory.ErrMissingKeyColumn},
		"no_size": {s3inventory.ParquetReaderConfig{KeyCol: s3inventory.ColumnAt(0), StorageCol: s3inventory.ColumnAt(1)}, s3inventory.ErrMissingSizeColumn},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			f := openTestParquet(t, path)
			if _, err := s3inventory.NewParquetReader(f, f.Size(), tc.cfg); !errors.Is(err, tc.want) {
				t.Errorf("NewParquetReader error = %v, want %v", err, tc.want)
			}
		})
	}

	type keyOnly struct {
		Key string `parquet:"key"`
	}
	sizeless := filepath.Join(t.TempDir(), "sizeless.parquet")
	if err := parquet.WriteFile(sizeless, []keyOnly{{Key: "a"}}); err != nil {
		t.Fatal(err)
	}
	f := openTestParquet(t, sizeless)
	if _, err := s3inventory.NewParquetReaderFromReaderAt(f, f.Size()); !errors.Is(err, s3inventory.ErrMissingSizeColumn) {
		t.Errorf("NewParquetReaderFromReaderAt error = %v, want %v", err, s3inventory.ErrMissingSizeColumn)
	}
}

// TestParquetReader_OmittedOptionalColumnsAreAbsent: a config naming
// only the key and size columns reads no storage class, access tier or
// delete marker, even when column 0 holds what would pass for one.
func TestParquetReader_OmittedOptionalColumnsAreAbsent(t *testing.T) {
	type record struct {
		Bucket string `parquet:"bucket"`
		Key    string `parquet:"key"`
		Size   int64  `parquet:"size"`
	}
	path := filepath.Join(t.TempDir(), "bucket-first.parquet")
	if err := parquet.WriteFile(path, []record{{"STANDARD", "k1", 100}, {"GLACIER", "k2", 200}}); err != nil {
		t.Fatal(err)
	}
	f := openTestParquet(t, path)
	r, err := s3inventory.NewParquetReader(f, f.Size(), s3inventory.ParquetReaderConfig{
		KeyCol: s3inventory.ColumnAt(1), SizeCol: s3inventory.ColumnAt(2),
	})
	if err != nil {
		t.Fatalf("NewParquetReader: %v", err)
	}
	t.Cleanup(func() { _ = r.Close() })

	got := readAllRows(t, r)
	want := []s3inventory.Row{{Key: "k1", Size: 100}, {Key: "k2", Size: 200}}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("rows = %+v, want %+v", got, want)
	}
}

func openParquetFile(t *testing.T, path string) *s3inventory.ParquetReader {
	t.Helper()
	f := openTestParquet(t, path)
	r, err := s3inventory.NewParquetReaderFromReaderAt(f, f.Size())
	if err != nil {
		t.Fatalf("NewParquetReaderFromReaderAt: %v", err)
	}
	t.Cleanup(func() { _ = r.Close() })

	return r
}

// openTestParquet opens the file at path for the test.
func openTestParquet(t *testing.T, path string) *io.SectionReader {
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

	return io.NewSectionReader(f, 0, info.Size())
}
