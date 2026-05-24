package s3inventory

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

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
)

// ParquetReader reads S3 inventory records from Parquet files.
// It implements streaming by iterating through row groups.
type ParquetReader struct {
	currentRows   parquet.Rows
	tempFile      *os.File
	schema        *parquet.Schema
	file          *parquet.File
	rowGroups     []parquet.RowGroup
	rowBuf        []parquet.Row
	keyCol        int
	accessTierCol int
	currentRGIdx  int
	storageCol    int
	sizeCol       int
	bufIdx        int
	bufLen        int
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
}

// NewParquetReader creates a Parquet inventory reader from an io.ReaderAt.
// This is used when you already have a ReaderAt (e.g., a local file or memory-mapped data).
func NewParquetReader(r io.ReaderAt, size int64, cfg ParquetReaderConfig) (*ParquetReader, error) {
	file, err := parquet.OpenFile(r, size)
	if err != nil {
		return nil, fmt.Errorf("open parquet file: %w", err)
	}

	return newParquetReader(file, nil, cfg), nil
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

	return newParquetReader(file, nil, cfg), nil
}

// NewParquetReaderFromStream creates a Parquet inventory reader from a stream.
// Since Parquet requires random access, this buffers the entire stream to a temp file.
// The size parameter is used for validation (if non-zero) but the full stream is read regardless.
func NewParquetReaderFromStream(r io.ReadCloser, size int64) (*ParquetReader, error) {
	tempFile, err := os.CreateTemp("", "parquet-inventory-*.parquet")
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
	if err != nil {
		tempFile.Close()
		os.Remove(tempFile.Name())

		return nil, err
	}

	return newParquetReader(file, tempFile, cfg), nil
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
		KeyCol:        -1,
		SizeCol:       -1,
		StorageCol:    -1,
		AccessTierCol: -1,
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
func newParquetReader(file *parquet.File, tempFile *os.File, cfg ParquetReaderConfig) *ParquetReader {
	rowGroups := file.RowGroups()

	return &ParquetReader{
		file:          file,
		tempFile:      tempFile,
		schema:        file.Schema(),
		keyCol:        cfg.KeyCol,
		sizeCol:       cfg.SizeCol,
		storageCol:    cfg.StorageCol,
		accessTierCol: cfg.AccessTierCol,
		rowGroups:     rowGroups,
		currentRGIdx:  -1,
		rowBuf:        make([]parquet.Row, 1024), // Buffer 1024 rows at a time
	}
}

// Next returns the next inventory row.
func (r *ParquetReader) Next() (Row, error) {
	for {
		if r.bufIdx < r.bufLen {
			row := r.rowBuf[r.bufIdx]
			r.bufIdx++

			return r.toRow(row), nil
		}

		if r.currentRows != nil {
			n, err := r.currentRows.ReadRows(r.rowBuf)
			if n > 0 {
				r.bufIdx = 0
				r.bufLen = n

				continue
			}
			if err != nil && !errors.Is(err, io.EOF) {
				return Row{}, fmt.Errorf("read parquet rows: %w", err)
			}
			r.currentRows.Close()
			r.currentRows = nil
		}

		r.currentRGIdx++
		if r.currentRGIdx >= len(r.rowGroups) {
			return Row{}, io.EOF
		}

		r.currentRows = r.rowGroups[r.currentRGIdx].Rows()
	}
}

// toRow projects a parquet.Row into the unified inventory.Row.
func (r *ParquetReader) toRow(row parquet.Row) Row {
	inv := Row{}

	for _, val := range row {
		colIdx := val.Column()
		if colIdx < 0 || val.IsNull() {
			continue
		}

		switch colIdx {
		case r.keyCol:
			inv.Key = val.String()
		case r.sizeCol:
			inv.Size = val.Uint64()
		case r.storageCol:
			inv.StorageClass = val.String()
		case r.accessTierCol:
			inv.AccessTier = val.String()
		}
	}

	return inv
}

// Close releases resources.
func (r *ParquetReader) Close() error {
	var errs []error
	if r.currentRows != nil {
		if err := r.currentRows.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close parquet rows: %w", err))
		}
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
