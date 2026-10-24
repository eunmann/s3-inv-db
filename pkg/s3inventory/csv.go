package s3inventory

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"net/url"
	"slices"
	"strconv"
	"strings"

	"github.com/eunmann/s3-inv-db/pkg/logging"
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
	csvReader       *csv.Reader
	closers         []io.Closer
	keyCol          int
	sizeCol         int
	storageCol      int
	accessTierCol   int
	deleteMarkerCol int
	malformedRows   uint64
	undecodableKeys uint64
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

	// DeleteMarkerCol is the column index for IsDeleteMarker (-1 if not
	// available). Only all-versions inventories carry it.
	DeleteMarkerCol int
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
		csvReader:       csvr,
		keyCol:          cfg.KeyCol,
		sizeCol:         cfg.SizeCol,
		storageCol:      cfg.StorageCol,
		accessTierCol:   cfg.AccessTierCol,
		deleteMarkerCol: cfg.DeleteMarkerCol,
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
		csvReader:       csvr,
		keyCol:          cfg.KeyCol,
		sizeCol:         cfg.SizeCol,
		storageCol:      cfg.StorageCol,
		accessTierCol:   cfg.AccessTierCol,
		deleteMarkerCol: cfg.DeleteMarkerCol,
		closers:         closers,
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

		if r.deleteMarkerCol >= 0 && len(fields) > r.deleteMarkerCol && isTrue(fields[r.deleteMarkerCol]) {
			continue // a delete marker is not an object and stores no bytes
		}

		key := r.decodeKey(fields[r.keyCol])
		if key == "" {
			continue
		}

		sizeStr := strings.TrimSpace(fields[r.sizeCol])
		size, err := strconv.ParseUint(sizeStr, 10, 64)
		if err != nil {
			r.malformedRows++
			logging.L().Debug().
				Str("key", key).
				Str("size_field", sizeStr).
				Err(err).
				Msg("s3inventory: malformed size, defaulting to 0")
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

// decodeKey undoes the URL encoding S3 Inventory applies to keys in CSV
// reports (Parquet keys are raw). It is form decoding — '+' is a space
// and a literal '+' arrives as %2B — matching the url_decode AWS's
// Athena guidance uses. Most keys contain neither '%' nor '+', so they
// skip the decode and its allocation. A key that is not valid
// encoding is kept as-is and counted.
func (r *CSVReader) decodeKey(raw string) string {
	if !strings.ContainsAny(raw, "%+") {
		return raw
	}
	key, err := url.QueryUnescape(raw)
	if err != nil {
		r.undecodableKeys++

		return raw
	}

	return key
}

// isTrue parses the boolean columns S3 Inventory writes ("true"/"TRUE").
func isTrue(field string) bool {
	return strings.EqualFold(strings.TrimSpace(field), "true")
}

// UndecodableKeyCount returns the number of keys that were not valid URL
// encoding and were kept verbatim.
func (r *CSVReader) UndecodableKeyCount() uint64 {
	return r.undecodableKeys
}

// MalformedRowCount returns the number of rows whose size field failed to parse.
func (r *CSVReader) MalformedRowCount() uint64 {
	return r.malformedRows
}

// Close releases resources.
func (r *CSVReader) Close() error {
	var firstErr error
	// Close in reverse order (gzip reader before underlying stream)
	for _, c := range slices.Backward(r.closers) {
		if err := c.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	return firstErr
}
