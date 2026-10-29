package s3inventory

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"unsafe"

	"github.com/parquet-go/parquet-go"
)

// Sentinel errors for parquet inventory reading. Static errors satisfy
// err113 by letting callers use errors.Is and fmt.Errorf("...: %w", err).
var (
	// ErrSizeMismatch indicates buffered bytes did not match the declared size.
	ErrSizeMismatch = errors.New("size mismatch")
	// ErrMissingKeyColumn indicates the parquet schema has no recognized key column.
	ErrMissingKeyColumn = errors.New("parquet schema missing 'key' column")
	// ErrMissingSizeColumn indicates the parquet schema has no recognized size column.
	ErrMissingSizeColumn = errors.New("parquet schema missing 'size' column")
	// ErrDuplicateColumn indicates two parquet columns canonicalize to the same name.
	ErrDuplicateColumn = errors.New("parquet schema has duplicate column")
	// ErrColumnOutOfRange indicates a configured column index beyond the schema.
	ErrColumnOutOfRange = errors.New("parquet column index out of range")
	// ErrColumnsMisaligned indicates the read columns disagree on a row group's row count.
	ErrColumnsMisaligned = errors.New("parquet columns hold different row counts")
)

// ParquetReader reads S3 inventory records from Parquet files. It reads
// only the column chunks it needs (key, size, storage class, access
// tier, delete marker), a batch of values at a time per column, so the
// other columns of an inventory (ETags, dates, owners, ACLs) are
// never decoded.
type ParquetReader struct {
	tempFile     *os.File
	file         *parquet.File
	rowGroups    []parquet.RowGroup
	pendingErr   error
	cols         [numParquetColumns]parquetColumn
	interned     map[string]string
	currentRGIdx int
	anomalies    Anomalies
	rgOpen       bool
	bufIdx       int
	bufLen       int
}

// The columns a ParquetReader reads, in parquetColumn order.
const (
	pqKey = iota
	pqSize
	pqStorage
	pqAccessTier
	pqDeleteMarker
	numParquetColumns
)

// parquetBatchRows is how many values each column decodes per batch.
const parquetBatchRows = 1024

// parquetColumn reads one leaf column of the current row group.
type parquetColumn struct {
	pages  parquet.Pages
	values parquet.ValueReader
	buf    []parquet.Value
	index  int // leaf column index in the file; -1 when absent
}

// ParquetReaderConfig configures the Parquet reader.
type ParquetReaderConfig struct {
	// KeyCol is the column index for the object key (required).
	KeyCol int

	// SizeCol is the column index for the object size (required).
	SizeCol int

	// StorageCol is the column index for the storage class (-1 if not available).
	StorageCol int

	// AccessTierCol is the column index for the access tier (-1 if not available).
	AccessTierCol int

	// DeleteMarkerCol is the column index for is_delete_marker (-1 if not
	// available). Only all-versions inventories carry it.
	DeleteMarkerCol int
}

// NewParquetReader creates a Parquet inventory reader from an io.ReaderAt.
// This is used when you already have a ReaderAt (e.g., a local file or memory-mapped data).
func NewParquetReader(r io.ReaderAt, size int64, cfg ParquetReaderConfig) (*ParquetReader, error) {
	file, err := parquet.OpenFile(r, size)
	if err != nil {
		return nil, fmt.Errorf("open parquet file: %w", err)
	}

	return newParquetReader(file, nil, cfg)
}

// NewParquetReaderFromReaderAt creates a Parquet inventory reader from an io.ReaderAt.
// This auto-detects the schema from the Parquet file and is more efficient than
// NewParquetReaderFromStream when you already have a ReaderAt (e.g., from temp file).
func NewParquetReaderFromReaderAt(r io.ReaderAt, size int64) (*ParquetReader, error) {
	file, err := parquet.OpenFile(r, size)
	if err != nil {
		return nil, fmt.Errorf("open parquet file: %w", err)
	}

	cfg, err := detectParquetSchema(file.Schema())
	if err != nil {
		return nil, err
	}

	return newParquetReader(file, nil, cfg)
}

// NewParquetReaderFromStream creates a Parquet inventory reader from a stream.
// Since Parquet requires random access, this buffers the entire stream to a temp file
// in tempDir (os.TempDir when empty).
// The size parameter is used for validation (if non-zero) but the full stream is read regardless.
func NewParquetReaderFromStream(r io.ReadCloser, size int64, tempDir string) (*ParquetReader, error) {
	tempFile, err := os.CreateTemp(tempDir, "parquet-inventory-*.parquet")
	if err != nil {
		r.Close()

		return nil, fmt.Errorf("create temp file: %w", err)
	}

	written, err := io.Copy(tempFile, r)
	r.Close()
	if err != nil {
		tempFile.Close()
		os.Remove(tempFile.Name())

		return nil, fmt.Errorf("buffer parquet data: %w", err)
	}

	// Validate size if provided
	if size > 0 && written != size {
		tempFile.Close()
		os.Remove(tempFile.Name())

		return nil, fmt.Errorf("%w: expected %d, got %d", ErrSizeMismatch, size, written)
	}

	if _, err := tempFile.Seek(0, io.SeekStart); err != nil {
		tempFile.Close()
		os.Remove(tempFile.Name())

		return nil, fmt.Errorf("seek temp file: %w", err)
	}

	file, err := parquet.OpenFile(tempFile, written)
	if err != nil {
		tempFile.Close()
		os.Remove(tempFile.Name())

		return nil, fmt.Errorf("open parquet file: %w", err)
	}

	cfg, err := detectParquetSchema(file.Schema())
	if err == nil {
		var r *ParquetReader
		if r, err = newParquetReader(file, tempFile, cfg); err == nil {
			return r, nil
		}
	}
	tempFile.Close()
	os.Remove(tempFile.Name())

	return nil, err
}

// detectParquetSchema detects column indices from the Parquet schema.
// Column names are matched case-insensitively after stripping underscores
// so AWS's PascalCase ("Key", "StorageClass") and the snake_case form
// ("key", "storage_class") that some other producers emit both work.
//
// Without normalisation, a Parquet inventory whose schema differs in
// case from the four hardcoded strings would silently drop the
// StorageClass / IntelligentTieringAccessTier columns (KeyCol/SizeCol
// failure is a hard error; tier columns silently default to -1), so
// every tier bucket would collapse to "unknown" with no error surfaced.
func detectParquetSchema(schema *parquet.Schema) (ParquetReaderConfig, error) {
	cfg := ParquetReaderConfig{
		KeyCol:          -1,
		SizeCol:         -1,
		StorageCol:      -1,
		AccessTierCol:   -1,
		DeleteMarkerCol: -1,
	}

	seen := make(map[string]string, len(schema.Fields()))
	for i, field := range schema.Fields() {
		canon := canonicalColumnName(field.Name())
		if prev, ok := seen[canon]; ok {
			return cfg, fmt.Errorf("%w: %q and %q both canonicalize to %q",
				ErrDuplicateColumn, prev, field.Name(), canon)
		}
		seen[canon] = field.Name()

		switch canon {
		case "key":
			cfg.KeyCol = i
		case "size":
			cfg.SizeCol = i
		case "storageclass":
			cfg.StorageCol = i
		case "intelligenttieringaccesstier":
			cfg.AccessTierCol = i
		case "isdeletemarker":
			cfg.DeleteMarkerCol = i
		}
	}

	if cfg.KeyCol < 0 {
		return cfg, ErrMissingKeyColumn
	}
	if cfg.SizeCol < 0 {
		return cfg, ErrMissingSizeColumn
	}

	return cfg, nil
}

// canonicalColumnName normalises a manifest/parquet column name to
// lowercase with underscores stripped. The CSV reader already does
// the same on its header row (pkg/inventory/reader.go); this keeps
// the Parquet path consistent so both formats accept any reasonable
// naming convention a producer publishes.
func canonicalColumnName(name string) string {
	return strings.ReplaceAll(strings.ToLower(strings.TrimSpace(name)), "_", "")
}

// newParquetReader creates a ParquetReader from an open file.
func newParquetReader(file *parquet.File, tempFile *os.File, cfg ParquetReaderConfig) (*ParquetReader, error) {
	r := &ParquetReader{
		file:         file,
		tempFile:     tempFile,
		rowGroups:    file.RowGroups(),
		interned:     make(map[string]string, maxInterned),
		currentRGIdx: -1,
	}
	numLeaves := len(file.Schema().Columns())
	indexes := [numParquetColumns]int{cfg.KeyCol, cfg.SizeCol, cfg.StorageCol, cfg.AccessTierCol, cfg.DeleteMarkerCol}
	for i, idx := range indexes {
		if idx >= numLeaves {
			return nil, fmt.Errorf("%w: column %d of %d", ErrColumnOutOfRange, idx, numLeaves)
		}
		r.cols[i].index = idx
		if idx >= 0 {
			r.cols[i].buf = make([]parquet.Value, parquetBatchRows)
		}
	}

	return r, nil
}

// Next returns the next inventory row.
func (r *ParquetReader) Next() (Row, error) {
	for {
		if r.bufIdx < r.bufLen {
			i := r.bufIdx
			r.bufIdx++
			inv, deleteMarker, hasSize := r.rowAt(i)
			if deleteMarker {
				continue // a delete marker is not an object and stores no bytes
			}
			if inv.Key == "" {
				r.anomalies.MissingKeys++

				continue
			}
			if !hasSize {
				r.anomalies.BadSizes++
			}

			return inv, nil
		}
		if r.pendingErr != nil {
			err := r.pendingErr
			r.pendingErr = nil

			return Row{}, err
		}
		if !r.rgOpen {
			r.currentRGIdx++
			if r.currentRGIdx >= len(r.rowGroups) {
				return Row{}, io.EOF
			}
			r.openRowGroup(r.rowGroups[r.currentRGIdx])
		}

		n, err := r.readBatch()
		if n > 0 {
			r.bufIdx, r.bufLen = 0, n
			// Surface a read error after the rows read before it.
			if err != nil && !errors.Is(err, io.EOF) {
				r.pendingErr = err
			}

			continue
		}
		if err != nil && !errors.Is(err, io.EOF) {
			return Row{}, err
		}
		if err := r.closeRowGroup(); err != nil {
			return Row{}, err
		}
	}
}

// openRowGroup starts reading the needed column chunks of rg.
func (r *ParquetReader) openRowGroup(rg parquet.RowGroup) {
	chunks := rg.ColumnChunks()
	for i := range r.cols {
		if c := &r.cols[i]; c.index >= 0 {
			c.pages = chunks[c.index].Pages()
		}
	}
	r.rgOpen = true
}

// closeRowGroup releases the current row group's page readers.
func (r *ParquetReader) closeRowGroup() error {
	var errs []error
	for i := range r.cols {
		c := &r.cols[i]
		if c.pages != nil {
			if err := c.pages.Close(); err != nil {
				errs = append(errs, fmt.Errorf("close parquet pages: %w", err))
			}
		}
		c.pages, c.values = nil, nil
	}
	r.rgOpen = false

	return errors.Join(errs...)
}

// readBatch decodes the next batch of every read column and returns the
// row count, which every column must agree on.
func (r *ParquetReader) readBatch() (int, error) {
	rows := -1
	var readErr error
	for i := range r.cols {
		c := &r.cols[i]
		if c.index < 0 {
			continue
		}
		n, err := c.read()
		if err != nil && !errors.Is(err, io.EOF) {
			readErr = err
		}
		if rows >= 0 && n != rows {
			return 0, fmt.Errorf("%w: column %d has %d, column %d has %d",
				ErrColumnsMisaligned, r.cols[pqKey].index, rows, c.index, n)
		}
		rows = n
	}
	if readErr != nil {
		return rows, readErr
	}
	if rows == 0 {
		return 0, io.EOF
	}

	return rows, nil
}

// read fills buf with the column's next values, across pages, and
// returns how many it read; io.EOF ends the row group.
func (c *parquetColumn) read() (int, error) {
	n := 0
	for n < len(c.buf) {
		if c.values == nil {
			page, err := c.pages.ReadPage()
			if errors.Is(err, io.EOF) {
				return n, io.EOF
			}
			if err != nil {
				return n, fmt.Errorf("read parquet page: %w", err)
			}
			c.values = page.Values()
		}
		k, err := c.values.ReadValues(c.buf[n:])
		n += k
		if errors.Is(err, io.EOF) {
			c.values = nil

			continue
		}
		if err != nil {
			return n, fmt.Errorf("read parquet values: %w", err)
		}
	}

	return n, nil
}

// rowAt assembles batch row i into an inventory row and reports
// whether the row is a delete marker and whether it has a size.
func (r *ParquetReader) rowAt(i int) (Row, bool, bool) {
	var inv Row
	if v, ok := r.value(pqKey, i); ok {
		inv.Key = valueString(v)
	}
	v, hasSize := r.value(pqSize, i)
	if hasSize {
		inv.Size = v.Uint64()
	}
	if v, ok := r.value(pqStorage, i); ok {
		inv.StorageClass = r.intern(v)
	}
	if v, ok := r.value(pqAccessTier, i); ok {
		inv.AccessTier = r.intern(v)
	}
	deleteMarker := false
	if v, ok := r.value(pqDeleteMarker, i); ok {
		if v.Kind() == parquet.Boolean {
			deleteMarker = v.Boolean()
		} else {
			deleteMarker = isTrue(v.String())
		}
	}

	return inv, deleteMarker, hasSize
}

// valueString returns v as a string. A byte array points into its page
// rather than being copied: a Row's strings are valid only until the
// next call, and the page outlives that.
func valueString(v parquet.Value) string {
	if k := v.Kind(); k == parquet.ByteArray || k == parquet.FixedLenByteArray {
		b := v.ByteArray()

		return unsafe.String(unsafe.SliceData(b), len(b))
	}

	return v.String()
}

// Anomalies counts the rows read so far that could not be read exactly.
func (r *ParquetReader) Anomalies() Anomalies { return r.anomalies }

// maxInterned bounds the strings intern keeps: storage classes and
// access tiers number a dozen or so, and a file with more distinct
// values just allocates them.
const maxInterned = 64

// intern returns v as a string, shared across rows for the few distinct
// values of a column such as storage class.
func (r *ParquetReader) intern(v parquet.Value) string {
	if v.Kind() != parquet.ByteArray {
		return v.String()
	}
	b := v.ByteArray()
	if s, ok := r.interned[string(b)]; ok {
		return s
	}
	s := string(b)
	if len(r.interned) < maxInterned {
		r.interned[s] = s
	}

	return s
}

// value returns column col's value for batch row i, if the column is
// read and the value is not null.
func (r *ParquetReader) value(col, i int) (parquet.Value, bool) {
	c := &r.cols[col]
	if c.index < 0 || c.buf[i].IsNull() {
		return parquet.Value{}, false
	}

	return c.buf[i], true
}

// Close releases resources.
func (r *ParquetReader) Close() error {
	var errs []error
	if err := r.closeRowGroup(); err != nil {
		errs = append(errs, err)
	}

	if r.tempFile != nil {
		name := r.tempFile.Name()
		if err := r.tempFile.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close temp file: %w", err))
		}
		if err := os.Remove(name); err != nil {
			errs = append(errs, fmt.Errorf("remove temp file: %w", err))
		}
	}

	return errors.Join(errs...)
}
