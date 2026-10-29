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

// TestDetectParquetSchema_Columns: the config detected from an AWS
// inventory schema names each column it reads, leaves out the optional
// columns the inventory lacks, and errors without a key or size column
// or when one it reads is not a top-level primitive.
func TestDetectParquetSchema_Columns(t *testing.T) {
	type allVersions struct {
		Bucket                       string `parquet:"bucket"`
		Key                          string `parquet:"key"`
		VersionID                    string `parquet:"version_id,optional"`
		IsLatest                     bool   `parquet:"is_latest"`
		IsDeleteMarker               bool   `parquet:"is_delete_marker"`
		Size                         int64  `parquet:"size,optional"`
		StorageClass                 string `parquet:"storage_class,optional"`
		IntelligentTieringAccessTier string `parquet:"intelligent_tiering_access_tier,optional"`
	}
	type currentVersion struct {
		Bucket       string `parquet:"Bucket"`
		Key          string `parquet:"Key"`
		Size         int64  `parquet:"Size,optional"`
		StorageClass string `parquet:"StorageClass,optional"`
	}
	type noKey struct {
		Bucket string `parquet:"bucket"`
		Size   int64  `parquet:"size"`
	}
	type noSize struct {
		Bucket string `parquet:"bucket"`
		Key    string `parquet:"key"`
	}
	type object struct {
		Key  string `parquet:"key"`
		Size int64  `parquet:"size"`
	}
	type keyInGroup struct {
		Bucket string `parquet:"bucket"`
		Object object `parquet:"object"`
	}
	type keyIsGroup struct {
		Key  object `parquet:"key"`
		Size int64  `parquet:"size"`
	}
	type nestedFirst struct {
		Meta   object            `parquet:"meta"`
		Tags   []string          `parquet:"tags,list"`
		Labels map[string]string `parquet:"labels"`
		Key    string            `parquet:"key"`
		Size   int64             `parquet:"size"`
	}
	type storageInList struct {
		Key     string   `parquet:"key"`
		Size    int64    `parquet:"size"`
		Classes []string `parquet:"storage_class,list"`
	}
	cases := map[string]struct {
		schema  *parquet.Schema
		want    ParquetReaderConfig
		wantErr error
		wantMsg string
	}{
		"key_in_group":    {schema: parquet.SchemaOf(new(keyInGroup)), wantErr: ErrNestedColumn, wantMsg: `"object.key"`},
		"key_is_group":    {schema: parquet.SchemaOf(new(keyIsGroup)), wantErr: ErrNestedColumn, wantMsg: `"key"`},
		"storage_in_list": {schema: parquet.SchemaOf(new(storageInList)), wantErr: ErrNestedColumn, wantMsg: `"storage_class"`},
		"nested_first": {
			schema: parquet.SchemaOf(new(nestedFirst)),
			want:   ParquetReaderConfig{KeyCol: ColumnAt(5), SizeCol: ColumnAt(6)},
		},
		"all_versions": {
			schema: parquet.SchemaOf(new(allVersions)),
			want: ParquetReaderConfig{
				KeyCol: ColumnAt(1), SizeCol: ColumnAt(5), StorageCol: ColumnAt(6),
				AccessTierCol: ColumnAt(7), DeleteMarkerCol: ColumnAt(4),
			},
		},
		"current_version_without_tier": {
			schema: parquet.SchemaOf(new(currentVersion)),
			want:   ParquetReaderConfig{KeyCol: ColumnAt(1), SizeCol: ColumnAt(2), StorageCol: ColumnAt(3)},
		},
		"no_key":  {schema: parquet.SchemaOf(new(noKey)), wantErr: ErrMissingKeyColumn},
		"no_size": {schema: parquet.SchemaOf(new(noSize)), wantErr: ErrMissingSizeColumn},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got, err := detectParquetSchema(tc.schema)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("err = %v, want %v", err, tc.wantErr)
			}
			if err != nil && !strings.Contains(err.Error(), tc.wantMsg) {
				t.Errorf("err = %v, want it to name %s", err, tc.wantMsg)
			}
			if tc.wantErr == nil && got != tc.want {
				t.Errorf("config = %+v, want %+v", got, tc.want)
			}
		})
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
