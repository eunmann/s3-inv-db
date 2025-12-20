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

	return newParquetReader(file, nil, cfg)
}

// NewParquetInventoryReaderFromStream creates a Parquet inventory reader from a stream.
// Since Parquet requires random access, this buffers the entire stream to a temp file.
func NewParquetInventoryReaderFromStream(r io.ReadCloser, size int64) (InventoryReader, error) {
	// Parquet requires random access, so we need to buffer to a temp file
	tempFile, err := os.CreateTemp("", "parquet-inventory-*.parquet")
	if err != nil {
		r.Close()
		return nil, fmt.Errorf("create temp file: %w", err)
	}

	// Copy stream to temp file
	written, err := io.Copy(tempFile, r)
	r.Close()
	if err != nil {
		tempFile.Close()
		os.Remove(tempFile.Name())
		return nil, fmt.Errorf("buffer parquet data: %w", err)
	}

	// Seek back to beginning
	if _, err := tempFile.Seek(0, io.SeekStart); err != nil {
		tempFile.Close()
		os.Remove(tempFile.Name())
		return nil, fmt.Errorf("seek temp file: %w", err)
	}

	// Open as parquet file
	file, err := parquet.OpenFile(tempFile, written)
	if err != nil {
		tempFile.Close()
		os.Remove(tempFile.Name())
		return nil, fmt.Errorf("open parquet file: %w", err)
	}

	// Detect column indices from schema
	cfg, err := detectParquetSchema(file.Schema())
	if err != nil {
		tempFile.Close()
		os.Remove(tempFile.Name())
		return nil, err
	}

	return newParquetReader(file, tempFile, cfg)
}

// NewParquetInventoryReaderWithConfig creates a Parquet reader with explicit config.
func NewParquetInventoryReaderWithConfig(r io.ReadCloser, size int64, cfg ParquetReaderConfig) (InventoryReader, error) {
	// Parquet requires random access, so we need to buffer to a temp file
	tempFile, err := os.CreateTemp("", "parquet-inventory-*.parquet")
	if err != nil {
		r.Close()
		return nil, fmt.Errorf("create temp file: %w", err)
	}

	// Copy stream to temp file
	written, err := io.Copy(tempFile, r)
	r.Close()
	if err != nil {
		tempFile.Close()
		os.Remove(tempFile.Name())
		return nil, fmt.Errorf("buffer parquet data: %w", err)
	}

	// Seek back to beginning
	if _, err := tempFile.Seek(0, io.SeekStart); err != nil {
		tempFile.Close()
		os.Remove(tempFile.Name())
		return nil, fmt.Errorf("seek temp file: %w", err)
	}

	// Open as parquet file
	file, err := parquet.OpenFile(tempFile, written)
	if err != nil {
		tempFile.Close()
		os.Remove(tempFile.Name())
		return nil, fmt.Errorf("open parquet file: %w", err)
	}

	return newParquetReader(file, tempFile, cfg)
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
func newParquetReader(file *parquet.File, tempFile *os.File, cfg ParquetReaderConfig) (*parquetInventoryReader, error) {
	rowGroups := file.RowGroups()

	r := &parquetInventoryReader{
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

	return r, nil
}

// Next returns the next inventory row.
func (r *parquetInventoryReader) Next() (InventoryRow, error) {
	for {
		// Check if we have buffered rows
		if r.bufIdx < r.bufLen {
			row := r.rowBuf[r.bufIdx]
			r.bufIdx++
			return r.rowToInventoryRow(row), nil
		}

		// Need to read more rows
		if r.currentRows != nil {
			n, err := r.currentRows.ReadRows(r.rowBuf)
			if n > 0 {
				r.bufIdx = 0
				r.bufLen = n
				continue // Process buffered rows
			}
			if err != nil && !errors.Is(err, io.EOF) {
				return InventoryRow{}, fmt.Errorf("read parquet rows: %w", err)
			}
			// Current row group exhausted
			r.currentRows.Close()
			r.currentRows = nil
		}

		// Move to next row group
		r.currentRGIdx++
		if r.currentRGIdx >= len(r.rowGroups) {
			return InventoryRow{}, io.EOF
		}

		r.currentRows = r.rowGroups[r.currentRGIdx].Rows()
	}
}

// rowToInventoryRow converts a parquet.Row to an InventoryRow.
func (r *parquetInventoryReader) rowToInventoryRow(row parquet.Row) InventoryRow {
	inv := InventoryRow{}

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

	// Clean up temp file if we created one
	if r.tempFile != nil {
		name := r.tempFile.Name()
		r.tempFile.Close()
		os.Remove(name)
	}

	return nil
}
