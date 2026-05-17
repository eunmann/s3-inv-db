package s3inventory

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/klauspost/pgzip"
)

// Row represents a single object from an S3 inventory file.
// This is the unified representation used by both CSV and Parquet readers.
type Row struct {
	Key          string
	StorageClass string
	AccessTier   string
	Size         uint64
}

// Reader is the unified interface for reading S3 inventory files.
// CSV and Parquet implementations live in this package.
type Reader interface {
	// Next returns the next inventory row.
	// Returns io.EOF when all rows have been read.
	Next() (Row, error)

	// Close releases resources associated with the reader.
	Close() error
}

// CSVReader reads S3 inventory records from CSV streams.
type CSVReader struct {
	csvReader     *csv.Reader
	closers       []io.Closer
	keyCol        int
	sizeCol       int
	storageCol    int
	accessTierCol int
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

// NewCSVReader creates a new CSV inventory reader from an io.Reader.
// The reader should provide the raw CSV data (already decompressed if needed).
// Use NewCSVReaderFromStream for automatic gzip handling.
func NewCSVReader(r io.Reader, cfg CSVReaderConfig) *CSVReader {
	csvr := csv.NewReader(r)
	csvr.ReuseRecord = true
	csvr.FieldsPerRecord = -1
	csvr.LazyQuotes = true

	return &CSVReader{
		csvReader:     csvr,
		keyCol:        cfg.KeyCol,
		sizeCol:       cfg.SizeCol,
		storageCol:    cfg.StorageCol,
		accessTierCol: cfg.AccessTierCol,
	}
}

// NewCSVReaderFromStream creates a CSV inventory reader from an S3 stream.
// It handles gzip decompression based on the object key extension.
func NewCSVReaderFromStream(r io.ReadCloser, key string, cfg CSVReaderConfig) (*CSVReader, error) {
	var reader io.Reader = r
	closers := []io.Closer{r}

	if strings.HasSuffix(strings.ToLower(key), ".gz") {
		// Use parallel gzip for faster decompression on multi-core systems.
		// pgzip automatically uses multiple goroutines for decompression.
		gzr, err := pgzip.NewReader(r)
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

	return &CSVReader{
		csvReader:     csvr,
		keyCol:        cfg.KeyCol,
		sizeCol:       cfg.SizeCol,
		storageCol:    cfg.StorageCol,
		accessTierCol: cfg.AccessTierCol,
		closers:       closers,
	}, nil
}

// Next returns the next inventory row.
func (r *CSVReader) Next() (Row, error) {
	for {
		fields, err := r.csvReader.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return Row{}, io.EOF
			}

			return Row{}, fmt.Errorf("read CSV row: %w", err)
		}

		if len(fields) <= r.keyCol || len(fields) <= r.sizeCol {
			continue
		}

		key := fields[r.keyCol]
		if key == "" {
			continue
		}

		sizeStr := strings.TrimSpace(fields[r.sizeCol])
		size, err := strconv.ParseUint(sizeStr, 10, 64)
		if err != nil {
			// Treat invalid size as 0 (could be empty or malformed)
			size = 0
		}

		row := Row{
			Key:  key,
			Size: size,
		}

		if r.storageCol >= 0 && len(fields) > r.storageCol {
			row.StorageClass = fields[r.storageCol]
		}
		if r.accessTierCol >= 0 && len(fields) > r.accessTierCol {
			row.AccessTier = fields[r.accessTierCol]
		}

		return row, nil
	}
}

// Close releases resources.
func (r *CSVReader) Close() error {
	var firstErr error
	// Close in reverse order (gzip reader before underlying stream)
	for i := len(r.closers) - 1; i >= 0; i-- {
		if err := r.closers[i].Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	return firstErr
}
