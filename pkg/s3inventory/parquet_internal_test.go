package s3inventory

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/encoding"
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

// corruptPages hands out its column chunk's pages through corrupt.
type corruptPages struct {
	parquet.Pages

	corrupt func(parquet.Page) parquet.Page
}

//nolint:ireturn // implements parquet.Pages, whose ReadPage returns the parquet.Page interface
func (p corruptPages) ReadPage() (parquet.Page, error) {
	page, err := p.Pages.ReadPage()
	if err != nil {
		return nil, err //nolint:wrapcheck // the reader tests for an unwrapped io.EOF
	}

	return p.corrupt(page), nil
}

// shortDefsPage drops its page's last definition level.
type shortDefsPage struct{ parquet.Page }

func (p shortDefsPage) DefinitionLevels() []byte {
	defs := p.Page.DefinitionLevels()

	return defs[:len(defs)-1]
}

// badIndexPage points its first value past the end of the dictionary.
type badIndexPage struct{ parquet.Page }

func (p badIndexPage) Data() encoding.Values {
	data := p.Page.Data()
	indexes := append([]int32(nil), data.Int32()...)
	indexes[0] = int32(p.Dictionary().Len())

	return encoding.Int32Values(indexes)
}

// A malformed page in a column other than the first fails the read
// with ErrMalformedPage, not as a row count the columns disagree on.
func TestParquetReader_MalformedPageInLaterColumn(t *testing.T) {
	path := filepath.Join(t.TempDir(), "inv.parquet")
	if err := parquet.WriteFile(path, dictRecords()); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name    string
		corrupt func(parquet.Page) parquet.Page
	}{
		{"definition levels", func(p parquet.Page) parquet.Page { return shortDefsPage{p} }},
		{"dictionary index", func(p parquet.Page) parquet.Page { return badIndexPage{p} }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := openTestFile(t, path)
			r, err := NewParquetReaderFromReaderAt(f, f.size)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = r.Close() })
			r.currentRGIdx = 0
			r.openRowGroup(r.rowGroups[0])
			r.storage.pages = corruptPages{r.storage.pages, tt.corrupt}

			_, err = r.Next()
			if !errors.Is(err, ErrMalformedPage) {
				t.Fatalf("Next() error = %v, want %v", err, ErrMalformedPage)
			}
			if errors.Is(err, ErrColumnsMisaligned) {
				t.Errorf("Next() error = %v, want no %v", err, ErrColumnsMisaligned)
			}
		})
	}
}
