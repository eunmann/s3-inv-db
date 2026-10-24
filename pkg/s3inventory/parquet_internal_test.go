package s3inventory

import (
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go"
)

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
