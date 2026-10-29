package s3inventory

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go"
)

// equivalenceRows is enough rows for several row groups of 1500 and
// several reader batches per row group.
const equivalenceRows = 7000

var equivalenceClasses = [...]string{"STANDARD", "", "GLACIER", "INTELLIGENT_TIERING", "STANDARD_IA", "DEEP_ARCHIVE"}

type equivalenceRow struct {
	key          string
	storageClass string
	accessTier   string
	size         int64
	deleteMarker bool
}

// equivalenceInput spreads keys of varying length, empty keys, empty
// and missing sizes, nulls and delete markers over the rows.
func equivalenceInput(i int) equivalenceRow {
	r := equivalenceRow{
		key:          fmt.Sprintf("p%d/%s-%06d.bin", i%13, strings.Repeat("x", i%37), i),
		size:         int64(i*7919) - 50,
		storageClass: equivalenceClasses[i%len(equivalenceClasses)],
		deleteMarker: i%11 == 0,
	}
	if i%97 == 0 {
		r.key = ""
	}
	if r.storageClass == "INTELLIGENT_TIERING" {
		r.accessTier = []string{"FREQUENT_ACCESS", "ARCHIVE_ACCESS", ""}[i%3]
	}

	return r
}

// Required key and size, plain-encoded strings.
type plainRecord struct {
	Key            string `parquet:"key"`
	Size           int64  `parquet:"size"`
	StorageClass   string `parquet:"storage_class,optional"`
	AccessTier     string `parquet:"intelligent_tiering_access_tier,optional"`
	IsDeleteMarker bool   `parquet:"is_delete_marker"`
}

// Every read column optional and dictionary-encoded where it can be.
type dictRecord struct {
	Key            string `parquet:"key,optional,dict"`
	Size           int64  `parquet:"size,optional"`
	StorageClass   string `parquet:"storage_class,optional,dict"`
	AccessTier     string `parquet:"intelligent_tiering_access_tier,optional,dict"`
	IsDeleteMarker bool   `parquet:"is_delete_marker,optional"`
}

// Types the page decoder has no fast path for: INT32 sizes and a
// string delete marker, plain and dictionary-encoded.
type otherTypesRecord struct {
	Key            string `parquet:"Key"`
	Size           int32  `parquet:"Size,optional"`
	StorageClass   string `parquet:"StorageClass"`
	IsDeleteMarker string `parquet:"IsDeleteMarker,dict"`
	AccessTier     string `parquet:"IntelligentTieringAccessTier,optional"`
}

// Only the required columns.
type minimalRecord struct {
	Key  string `parquet:"key"`
	Size int64  `parquet:"size"`
}

func minimalRecords() []minimalRecord {
	rows := make([]minimalRecord, equivalenceRows)
	for i := range rows {
		r := equivalenceInput(i)
		rows[i] = minimalRecord{r.key, r.size}
	}

	return rows
}

func plainRecords() []plainRecord {
	rows := make([]plainRecord, equivalenceRows)
	for i := range rows {
		r := equivalenceInput(i)
		rows[i] = plainRecord{r.key, r.size, r.storageClass, r.accessTier, r.deleteMarker}
	}

	return rows
}

func dictRecords() []dictRecord {
	rows := make([]dictRecord, equivalenceRows)
	for i := range rows {
		r := equivalenceInput(i)
		if i%5 == 0 {
			r.size = 0 // null: a missing size
		}
		rows[i] = dictRecord{r.key, r.size, r.storageClass, r.accessTier, r.deleteMarker}
	}

	return rows
}

func otherTypesRecords() []otherTypesRecord {
	rows := make([]otherTypesRecord, equivalenceRows)
	for i := range rows {
		r := equivalenceInput(i)
		marker := []string{"false", "true", "TRUE", "yes", ""}[i%5]
		rows[i] = otherTypesRecord{r.key, int32(r.size), r.storageClass, marker, r.accessTier}
	}

	return rows
}

// keyChunkPages describes the key column's pages per row group: whether
// each is dictionary-encoded, and its row count.
type keyChunkPages struct {
	dict []bool
	rows []int64
}

func inspectKeyPages(t *testing.T, path string) []keyChunkPages {
	t.Helper()
	f := openTestFile(t, path)
	file, err := parquet.OpenFile(f, f.size)
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := detectParquetSchema(file.Schema())
	if err != nil {
		t.Fatal(err)
	}
	rowGroups := file.RowGroups()
	chunks := make([]keyChunkPages, 0, len(rowGroups))
	for _, rg := range rowGroups {
		pages := rg.ColumnChunks()[cfg.KeyCol].Pages()
		var c keyChunkPages
		for {
			p, err := pages.ReadPage()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			c.dict = append(c.dict, p.Dictionary() != nil)
			c.rows = append(c.rows, p.NumRows())
		}
		if err := pages.Close(); err != nil {
			t.Fatal(err)
		}
		chunks = append(chunks, c)
	}

	return chunks
}

type testFile struct {
	*os.File

	size int64
}

func openTestFile(t *testing.T, path string) testFile {
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

	return testFile{f, info.Size()}
}

type equivalenceCase struct {
	name  string
	write func(path string) error
	// wantSwitch requires a key column chunk whose pages go from
	// dictionary to plain encoding.
	wantSwitch bool
	wantDict   bool
}

func equivalenceCases() []equivalenceCase {
	small := []parquet.WriterOption{parquet.PageBufferSize(1024), parquet.MaxRowsPerRowGroup(1500)}
	with := func(opts ...parquet.WriterOption) []parquet.WriterOption {
		return append(append([]parquet.WriterOption{}, small...), opts...)
	}

	return []equivalenceCase{
		{name: "plain/v2/uncompressed", write: func(p string) error {
			return parquet.WriteFile(p, plainRecords(), with(parquet.DataPageVersion(2), parquet.Compression(&parquet.Uncompressed))...)
		}},
		{name: "plain/v1/snappy", write: func(p string) error {
			return parquet.WriteFile(p, plainRecords(), with(parquet.DataPageVersion(1), parquet.Compression(&parquet.Snappy))...)
		}},
		{name: "dict-optional/v2/snappy", wantDict: true, write: func(p string) error {
			return parquet.WriteFile(p, dictRecords(), with(parquet.DataPageVersion(2), parquet.Compression(&parquet.Snappy))...)
		}},
		{name: "dict-optional/v1/uncompressed", wantDict: true, write: func(p string) error {
			return parquet.WriteFile(p, dictRecords(), with(parquet.DataPageVersion(1), parquet.Compression(&parquet.Uncompressed))...)
		}},
		{name: "dict-to-plain/v2/snappy", wantDict: true, wantSwitch: true, write: func(p string) error {
			return parquet.WriteFile(p, dictRecords(), with(parquet.DataPageVersion(2), parquet.Compression(&parquet.Snappy), parquet.DictionaryMaxBytes(4096))...)
		}},
		{name: "dict-to-plain/v1/uncompressed", wantDict: true, wantSwitch: true, write: func(p string) error {
			return parquet.WriteFile(p, dictRecords(), with(parquet.DataPageVersion(1), parquet.Compression(&parquet.Uncompressed), parquet.DictionaryMaxBytes(4096))...)
		}},
		{name: "other-types/v2/snappy", write: func(p string) error {
			return parquet.WriteFile(p, otherTypesRecords(), with(parquet.DataPageVersion(2), parquet.Compression(&parquet.Snappy))...)
		}},
		{name: "minimal/v2/uncompressed", write: func(p string) error {
			return parquet.WriteFile(p, minimalRecords(), with(parquet.DataPageVersion(2), parquet.Compression(&parquet.Uncompressed))...)
		}},
		{name: "bloom-and-page-index/v2/snappy", wantDict: true, write: func(p string) error {
			return parquet.WriteFile(p, dictRecords(), with(parquet.DataPageVersion(2), parquet.Compression(&parquet.Snappy),
				parquet.BloomFilters(parquet.SplitBlockFilter(10, "key")))...)
		}},
	}
}

// Every input must read to exactly the rows and anomalies of the
// generic-Value reader. Each row is checked when Next returns it, since
// its strings may point into a page the reader releases afterwards: the
// pages are small, so every batch holds keys from several of them.
func TestParquetReader_MatchesGenericValueReader(t *testing.T) {
	for _, tc := range equivalenceCases() {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "inv.parquet")
			if err := tc.write(path); err != nil {
				t.Fatalf("write: %v", err)
			}
			checkFixture(t, tc, inspectKeyPages(t, path))

			f := openTestFile(t, path)
			oracle, err := newOracleParquetReader(f, f.size)
			if err != nil {
				t.Fatal(err)
			}
			want := readCloned(t, oracle.Next)
			r, err := NewParquetReaderFromReaderAt(f, f.size)
			if err != nil {
				t.Fatal(err)
			}
			got := readCloned(t, r.Next)
			if err := r.Close(); err != nil {
				t.Fatal(err)
			}

			if len(want) == 0 {
				t.Fatal("oracle read no rows")
			}
			if len(got) != len(want) {
				t.Fatalf("got %d rows, want %d", len(got), len(want))
			}
			for i := range want {
				if got[i] != want[i] {
					t.Fatalf("row %d = %+v, want %+v", i, got[i], want[i])
				}
			}
			if r.Anomalies() != oracle.anomalies {
				t.Fatalf("anomalies = %+v, want %+v", r.Anomalies(), oracle.anomalies)
			}
		})
	}
}

// checkFixture fails when a writer change stops the fixture covering
// what the case is for.
func checkFixture(t *testing.T, tc equivalenceCase, chunks []keyChunkPages) {
	t.Helper()
	if len(chunks) < 2 {
		t.Fatalf("fixture has %d row groups, want several", len(chunks))
	}
	var dict, switched, small bool
	for _, c := range chunks {
		for i, d := range c.dict {
			dict = dict || d
			switched = switched || (i > 0 && c.dict[i-1] && !d)
			small = small || c.rows[i] < parquetBatchRows/2
		}
	}
	if !small {
		t.Fatal("fixture pages hold a batch or more each; keys would not cross pages within a batch")
	}
	if dict != tc.wantDict {
		t.Fatalf("fixture dictionary pages = %v, want %v", dict, tc.wantDict)
	}
	if switched != tc.wantSwitch {
		t.Fatalf("fixture dictionary-to-plain switch = %v, want %v", switched, tc.wantSwitch)
	}
}

func readCloned(t *testing.T, next func() (Row, error)) []Row {
	t.Helper()
	var rows []Row
	for {
		row, err := next()
		if errors.Is(err, io.EOF) {
			return rows
		}
		if err != nil {
			t.Fatal(err)
		}
		rows = append(rows, Row{
			Key:          strings.Clone(row.Key),
			StorageClass: strings.Clone(row.StorageClass),
			AccessTier:   strings.Clone(row.AccessTier),
			Size:         row.Size,
		})
	}
}
