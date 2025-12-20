// Package inventory provides readers for AWS S3 Inventory CSV files.
package inventory

import (
	"bufio"
	"compress/gzip"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

// Record represents a single object from the inventory.
//
// Note on Size: If the size field cannot be parsed (empty or malformed),
// Size will be 0. This matches the behavior of AWS S3 inventory which may
// have empty size fields for certain object types. Callers should be aware
// that Size=0 may indicate either a zero-byte object or an unparseable size.
type Record struct {
	Key    string
	Size   uint64
	TierID tiers.ID // Storage tier (only set when tier tracking is enabled)
}

// Reader is the interface for reading inventory records.
type Reader interface {
	// Read reads the next record. Returns io.EOF when done.
	Read() (Record, error)
	// Close releases resources.
	Close() error
}

// CSVReader reads S3 inventory records from a CSV or CSV.GZ file.
type CSVReader struct {
	file          *os.File
	csvReader     *csv.Reader
	keyCol        int
	sizeCol       int
	storageCol    int // StorageClass column (-1 if not tracking)
	accessTierCol int // IntelligentTieringAccessTier column (-1 if not tracking)
	trackTiers    bool
	tierMapping   *tiers.Mapping
	closers       []io.Closer
}

var (
	// ErrNoKeyColumn indicates the CSV header is missing a Key column.
	ErrNoKeyColumn = errors.New("CSV header missing 'Key' column")
	// ErrNoSizeColumn indicates the CSV header is missing a Size column.
	ErrNoSizeColumn = errors.New("CSV header missing 'Size' column")
)

// OpenOptions configures how to open an inventory file.
type OpenOptions struct {
	// TrackTiers enables parsing of StorageClass and IntelligentTieringAccessTier.
	TrackTiers bool
}

// OpenFile opens a CSV or CSV.GZ inventory file.
func OpenFile(path string) (*CSVReader, error) {
	return OpenFileWithOptions(path, OpenOptions{})
}

// OpenFileWithOptions opens a CSV or CSV.GZ inventory file with options.
func OpenFileWithOptions(path string, opts OpenOptions) (*CSVReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open inventory file: %w", err)
	}

	var reader io.Reader = bufio.NewReader(f)
	// Pre-allocate for file + optional gzip reader
	closers := make([]io.Closer, 0, 2)
	closers = append(closers, f)

	if strings.HasSuffix(strings.ToLower(path), ".gz") {
		gzr, err := gzip.NewReader(reader)
		if err != nil {
			f.Close()
			return nil, fmt.Errorf("create gzip reader: %w", err)
		}
		closers = append(closers, gzr)
		reader = gzr
	}

	csvr := csv.NewReader(reader)
	csvr.ReuseRecord = true
	csvr.FieldsPerRecord = -1
	csvr.LazyQuotes = true

	header, err := csvr.Read()
	if err != nil {
		for _, c := range closers {
			c.Close()
		}
		return nil, fmt.Errorf("read CSV header: %w", err)
	}

	keyCol := -1
	sizeCol := -1
	storageCol := -1
	accessTierCol := -1

	for i, col := range header {
		// Normalize column names (case-insensitive, trim whitespace)
		normalized := strings.TrimSpace(strings.ToLower(col))
		switch normalized {
		case "key":
			keyCol = i
		case "size":
			sizeCol = i
		case "storageclass":
			storageCol = i
		case "intelligenttieringaccesstier":
			accessTierCol = i
		}
	}

	if keyCol < 0 {
		for _, c := range closers {
			c.Close()
		}
		return nil, ErrNoKeyColumn
	}
	if sizeCol < 0 {
		for _, c := range closers {
			c.Close()
		}
		return nil, ErrNoSizeColumn
	}

	r := &CSVReader{
		file:          f,
		csvReader:     csvr,
		keyCol:        keyCol,
		sizeCol:       sizeCol,
		storageCol:    storageCol,
		accessTierCol: accessTierCol,
		trackTiers:    opts.TrackTiers,
		closers:       closers,
	}

	if opts.TrackTiers {
		r.tierMapping = tiers.NewMapping()
	}

	return r, nil
}

// NewReader creates a CSVReader from an io.Reader with an already-parsed header.
func NewReader(r io.Reader, keyCol, sizeCol int) *CSVReader {
	return NewReaderWithTiers(r, keyCol, sizeCol, -1, -1, false)
}

// NewReaderWithTiers creates a CSVReader with tier column support.
func NewReaderWithTiers(r io.Reader, keyCol, sizeCol, storageCol, accessTierCol int, trackTiers bool) *CSVReader {
	csvr := csv.NewReader(r)
	csvr.ReuseRecord = true
	csvr.FieldsPerRecord = -1
	csvr.LazyQuotes = true

	reader := &CSVReader{
		csvReader:     csvr,
		keyCol:        keyCol,
		sizeCol:       sizeCol,
		storageCol:    storageCol,
		accessTierCol: accessTierCol,
		trackTiers:    trackTiers,
	}

	if trackTiers {
		reader.tierMapping = tiers.NewMapping()
	}

	return reader
}

// SchemaOptions configures column indices for headerless files.
type SchemaOptions struct {
	KeyCol        int
	SizeCol       int
	StorageCol    int // -1 if not available
	AccessTierCol int // -1 if not available
	TrackTiers    bool
}

// OpenFileWithSchema opens a CSV or CSV.GZ file using pre-known column indices.
// This is used for AWS S3 inventory files which have no header row.
func OpenFileWithSchema(path string, keyCol, sizeCol int) (*CSVReader, error) {
	return OpenFileWithSchemaOptions(path, SchemaOptions{
		KeyCol:        keyCol,
		SizeCol:       sizeCol,
		StorageCol:    -1,
		AccessTierCol: -1,
		TrackTiers:    false,
	})
}

// OpenFileWithSchemaOptions opens a CSV or CSV.GZ file with full schema control.
func OpenFileWithSchemaOptions(path string, opts SchemaOptions) (*CSVReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open inventory file: %w", err)
	}

	var reader io.Reader = bufio.NewReader(f)
	// Pre-allocate for file + optional gzip reader
	closers := make([]io.Closer, 0, 2)
	closers = append(closers, f)

	if strings.HasSuffix(strings.ToLower(path), ".gz") {
		gzr, err := gzip.NewReader(reader)
		if err != nil {
			f.Close()
			return nil, fmt.Errorf("create gzip reader: %w", err)
		}
		closers = append(closers, gzr)
		reader = gzr
	}

	csvr := csv.NewReader(reader)
	csvr.ReuseRecord = true
	csvr.FieldsPerRecord = -1
	csvr.LazyQuotes = true

	r := &CSVReader{
		file:          f,
		csvReader:     csvr,
		keyCol:        opts.KeyCol,
		sizeCol:       opts.SizeCol,
		storageCol:    opts.StorageCol,
		accessTierCol: opts.AccessTierCol,
		trackTiers:    opts.TrackTiers,
		closers:       closers,
	}

	if opts.TrackTiers {
		r.tierMapping = tiers.NewMapping()
	}

	return r, nil
}

// Read reads the next record. Returns io.EOF when done.
func (r *CSVReader) Read() (Record, error) {
	for {
		fields, err := r.csvReader.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return Record{}, err
			}
			return Record{}, fmt.Errorf("read CSV row: %w", err)
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

		rec := Record{Key: key, Size: size}

		// Parse tier info if tracking is enabled
		if r.trackTiers && r.tierMapping != nil {
			storageClass := ""
			accessTier := ""

			if r.storageCol >= 0 && len(fields) > r.storageCol {
				storageClass = fields[r.storageCol]
			}
			if r.accessTierCol >= 0 && len(fields) > r.accessTierCol {
				accessTier = fields[r.accessTierCol]
			}

			rec.TierID = r.tierMapping.FromS3(storageClass, accessTier)
		}

		return rec, nil
	}
}

// Close releases all resources.
func (r *CSVReader) Close() error {
	var firstErr error
	// Close in reverse order (gzip reader before file)
	for i := len(r.closers) - 1; i >= 0; i-- {
		if err := r.closers[i].Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
