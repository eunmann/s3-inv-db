package s3inventory

import (
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go"
)

// TestToRow_NegativeConfiguredColsDontMatchZeroValue locks in the
// outer-guard refactor: when StorageCol/AccessTierCol are configured
// as -1 (column absent from the schema), no parquet value — including
// the default zero Value whose Column() is 0 — must populate those
// fields. The prior code relied on an inner `>= 0` check; the refactor
// hoists the guard before the switch. This test would fail if a future
// edit drops the outer guard while leaving a `case r.storageCol`
// (== -1) that matches a sentinel value.
func TestToRow_NegativeConfiguredColsDontMatchZeroValue(t *testing.T) {
	r := &ParquetReader{
		keyCol:        0,
		sizeCol:       1,
		storageCol:    -1,
		accessTierCol: -1,
	}

	keyVal := parquet.ValueOf("a/b/c.txt").Level(0, 0, 0)
	sizeVal := parquet.ValueOf(int64(42)).Level(0, 0, 1)

	inv := r.toRow(parquet.Row{keyVal, sizeVal})

	if inv.Key != "a/b/c.txt" {
		t.Errorf("Key = %q, want %q", inv.Key, "a/b/c.txt")
	}
	if inv.Size != 42 {
		t.Errorf("Size = %d, want 42", inv.Size)
	}
	if inv.StorageClass != "" {
		t.Errorf("StorageClass = %q, want empty (storageCol=-1 must not populate)", inv.StorageClass)
	}
	if inv.AccessTier != "" {
		t.Errorf("AccessTier = %q, want empty (accessTierCol=-1 must not populate)", inv.AccessTier)
	}
}

// TestParquetReader_Close_PropagatesTempFileRemoveErr exercises the
// errors.Join path: if the temp file no longer exists at remove time,
// Close must surface that error rather than swallow it.
func TestParquetReader_Close_PropagatesTempFileRemoveErr(t *testing.T) {
	tmp, err := os.CreateTemp(t.TempDir(), "parquet-close-*.tmp")
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}
	name := tmp.Name()
	// Remove the file before Close runs so os.Remove inside Close fails.
	if err := os.Remove(name); err != nil {
		t.Fatalf("pre-remove: %v", err)
	}

	r := &ParquetReader{tempFile: tmp}
	closeErr := r.Close()
	if closeErr == nil {
		t.Fatal("Close: expected error from failed remove, got nil")
	}
	if !strings.Contains(closeErr.Error(), "remove temp file") {
		t.Errorf("Close error = %v, want remove-temp-file message", closeErr)
	}
}

// TestDetectParquetSchema_DuplicateCanonicalNames asserts that two
// columns canonicalizing to the same logical name surface as an error
// rather than silently last-write-wins.
func TestDetectParquetSchema_DuplicateCanonicalNames(t *testing.T) {
	type dupRecord struct {
		Key  string `parquet:"Key"`
		Key2 string `parquet:"key"`
		Size int64  `parquet:"Size"`
	}

	schema := parquet.SchemaOf(new(dupRecord))

	_, err := detectParquetSchema(schema)
	if err == nil {
		t.Fatal("detectParquetSchema: expected error for duplicate canonical column names, got nil")
	}
	if !errors.Is(err, ErrDuplicateColumn) {
		t.Errorf("err = %v, want ErrDuplicateColumn", err)
	}
}
