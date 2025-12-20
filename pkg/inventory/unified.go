// Package inventory provides readers for AWS S3 Inventory files.

package inventory

import (
	"compress/gzip"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// InventoryRow represents a single object from an S3 inventory file.
// This is the unified representation used by both CSV and Parquet readers.
type InventoryRow struct {
	// Key is the S3 object key.
	Key string

	// Size is the object size in bytes.
	Size uint64

	// StorageClass is the S3 storage class (e.g., "STANDARD", "GLACIER").
	// May be empty if not included in the inventory.
	StorageClass string

	// AccessTier is the Intelligent-Tiering access tier (e.g., "ARCHIVE_ACCESS").
	// May be empty if not applicable or not included in the inventory.
	AccessTier string
}

// InventoryReader is the unified interface for reading S3 inventory files.
// Implementations exist for both CSV and Parquet formats.
type InventoryReader interface {
	// Next returns the next inventory row.
	// Returns io.EOF when all rows have been read.
	Next() (InventoryRow, error)

	// Close releases resources associated with the reader.
	Close() error
}

// csvInventoryReader reads S3 inventory records from CSV streams.
type csvInventoryReader struct {
	csvReader     *csv.Reader
	keyCol        int
	sizeCol       int
	storageCol    int // -1 if not available
	accessTierCol int // -1 if not available
	closers       []io.Closer
}

// CSVReaderConfig configures column indices for the CSV reader.
type CSVReaderConfig struct {
	// KeyCol is the column index for the object key (required).
	KeyCol int

	// SizeCol is the column index for the object size (required).
	SizeCol int

	// StorageCol is the column index for the storage class (-1 if not available).
	StorageCol int

	// AccessTierCol is the column index for the access tier (-1 if not available).
	AccessTierCol int
}

// NewCSVInventoryReader creates a new CSV inventory reader from an io.Reader.
// The reader should provide the raw CSV data (already decompressed if needed).
// Use NewCSVInventoryReaderFromStream for automatic gzip handling.
func NewCSVInventoryReader(r io.Reader, cfg CSVReaderConfig) InventoryReader {
	csvr := csv.NewReader(r)
	csvr.ReuseRecord = true
	csvr.FieldsPerRecord = -1 // Variable field count
	csvr.LazyQuotes = true    // Handle malformed quotes

	return &csvInventoryReader{
		csvReader:     csvr,
		keyCol:        cfg.KeyCol,
		sizeCol:       cfg.SizeCol,
		storageCol:    cfg.StorageCol,
		accessTierCol: cfg.AccessTierCol,
	}
}

// NewCSVInventoryReaderFromStream creates a CSV inventory reader from an S3 stream.
// It handles gzip decompression based on the object key extension.
func NewCSVInventoryReaderFromStream(r io.ReadCloser, key string, cfg CSVReaderConfig) (InventoryReader, error) {
	var reader io.Reader = r
	closers := []io.Closer{r}

	// Handle gzip compression
	if strings.HasSuffix(strings.ToLower(key), ".gz") {
		gzr, err := gzip.NewReader(r)
		if err != nil {
			r.Close()
			return nil, fmt.Errorf("create gzip reader: %w", err)
		}
		closers = append(closers, gzr)
		reader = gzr
	}

	csvr := csv.NewReader(reader)
	csvr.ReuseRecord = true
	csvr.FieldsPerRecord = -1
	csvr.LazyQuotes = true

	return &csvInventoryReader{
		csvReader:     csvr,
		keyCol:        cfg.KeyCol,
		sizeCol:       cfg.SizeCol,
		storageCol:    cfg.StorageCol,
		accessTierCol: cfg.AccessTierCol,
		closers:       closers,
	}, nil
}

// Next returns the next inventory row.
func (r *csvInventoryReader) Next() (InventoryRow, error) {
	for {
		fields, err := r.csvReader.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return InventoryRow{}, io.EOF
			}
			return InventoryRow{}, fmt.Errorf("read CSV row: %w", err)
		}

		// Skip rows with insufficient columns
		if len(fields) <= r.keyCol || len(fields) <= r.sizeCol {
			continue
		}

		key := fields[r.keyCol]
		// Skip empty keys
		if key == "" {
			continue
		}

		sizeStr := strings.TrimSpace(fields[r.sizeCol])
		size, err := strconv.ParseUint(sizeStr, 10, 64)
		if err != nil {
			// Treat invalid size as 0 (could be empty or malformed)
			size = 0
		}

		row := InventoryRow{
			Key:  key,
			Size: size,
		}

		// Extract storage class if available
		if r.storageCol >= 0 && len(fields) > r.storageCol {
			row.StorageClass = fields[r.storageCol]
		}

		// Extract access tier if available
		if r.accessTierCol >= 0 && len(fields) > r.accessTierCol {
			row.AccessTier = fields[r.accessTierCol]
		}

		return row, nil
	}
}

// Close releases resources.
func (r *csvInventoryReader) Close() error {
	var firstErr error
	// Close in reverse order (gzip reader before underlying stream)
	for i := len(r.closers) - 1; i >= 0; i-- {
		if err := r.closers[i].Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
