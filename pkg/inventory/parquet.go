package inventory

import (
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/parquet-go/parquet-go"
)

// parquetInventoryReader reads S3 inventory records from Parquet files.
// It implements streaming by iterating through row groups.
type parquetInventoryReader struct {
	file          *parquet.File
	tempFile      *os.File // Temp file for buffering (only if created by us)
	schema        *parquet.Schema
	keyCol        int
	sizeCol       int
	storageCol    int // -1 if not available
	accessTierCol int // -1 if not available

	// Row group iteration state
	rowGroups    []parquet.RowGroup
	currentRGIdx int
	currentRows  parquet.Rows
	rowBuf       []parquet.Row
	bufIdx       int
	bufLen       int
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

// NewParquetInventoryReader creates a Parquet inventory reader from an io.ReaderAt.
// This is used when you already have a ReaderAt (e.g., a local file or memory-mapped data).
func NewParquetInventoryReader(r io.ReaderAt, size int64, cfg ParquetReaderConfig) (InventoryReader, error) {
	file, err := parquet.OpenFile(r, size)
	if err != nil {
		return nil, fmt.Errorf("open parquet file: %w", err)
	}

	return newParquetReader(file, nil, cfg), nil
}

// NewParquetInventoryReaderFromReaderAt creates a Parquet inventory reader from an io.ReaderAt.
// This auto-detects the schema from the Parquet file and is more efficient than
// NewParquetInventoryReaderFromStream when you already have a ReaderAt (e.g., from temp file).
func NewParquetInventoryReaderFromReaderAt(r io.ReaderAt, size int64) (InventoryReader, error) {
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

// NewParquetInventoryReaderFromStream creates a Parquet inventory reader from a stream.
// Since Parquet requires random access, this buffers the entire stream to a temp file.
// The size parameter is used for validation (if non-zero) but the full stream is read regardless.
func NewParquetInventoryReaderFromStream(r io.ReadCloser, size int64) (InventoryReader, error) {
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
		return nil, fmt.Errorf("size mismatch: expected %d, got %d", size, written)
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

// NewParquetInventoryReaderWithConfig creates a Parquet reader with explicit config.
// The size parameter is used for validation (if non-zero) but the full stream is read regardless.
func NewParquetInventoryReaderWithConfig(r io.ReadCloser, size int64, cfg ParquetReaderConfig) (InventoryReader, error) {
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
		return nil, fmt.Errorf("size mismatch: expected %d, got %d", size, written)
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

	return newParquetReader(file, tempFile, cfg), nil
}

// detectParquetSchema detects column indices from the Parquet schema.
func detectParquetSchema(schema *parquet.Schema) (ParquetReaderConfig, error) {
	cfg := ParquetReaderConfig{
		KeyCol:        -1,
		SizeCol:       -1,
		StorageCol:    -1,
		AccessTierCol: -1,
	}

	fields := schema.Fields()
	for i, field := range fields {
		name := field.Name()
		switch name {
		case "key":
			cfg.KeyCol = i
		case "size":
			cfg.SizeCol = i
		case "storage_class":
			cfg.StorageCol = i
		case "intelligent_tiering_access_tier":
			cfg.AccessTierCol = i
		}
	}

	if cfg.KeyCol < 0 {
		return cfg, errors.New("parquet schema missing 'key' column")
	}
	if cfg.SizeCol < 0 {
		return cfg, errors.New("parquet schema missing 'size' column")
	}

	return cfg, nil
}

// newParquetReader creates a parquetInventoryReader from an open file.
func newParquetReader(file *parquet.File, tempFile *os.File, cfg ParquetReaderConfig) *parquetInventoryReader {
	rowGroups := file.RowGroups()

	return &parquetInventoryReader{
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
func (r *parquetInventoryReader) Next() (Row, error) {
	for {
		if r.bufIdx < r.bufLen {
			row := r.rowBuf[r.bufIdx]
			r.bufIdx++
			return r.rowToRow(row), nil
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

// rowToRow converts a parquet.Row to an Row.
func (r *parquetInventoryReader) rowToRow(row parquet.Row) Row {
	inv := Row{}

	for _, val := range row {
		colIdx := val.Column()
		if val.IsNull() {
			continue
		}

		switch colIdx {
		case r.keyCol:
			inv.Key = val.String()
		case r.sizeCol:
			inv.Size = val.Uint64()
		case r.storageCol:
			if r.storageCol >= 0 {
				inv.StorageClass = val.String()
			}
		case r.accessTierCol:
			if r.accessTierCol >= 0 {
				inv.AccessTier = val.String()
			}
		}
	}

	return inv
}

// Close releases resources.
func (r *parquetInventoryReader) Close() error {
	if r.currentRows != nil {
		r.currentRows.Close()
	}

	if r.tempFile != nil {
		name := r.tempFile.Name()
		r.tempFile.Close()
		os.Remove(name)
	}

	return nil
}
