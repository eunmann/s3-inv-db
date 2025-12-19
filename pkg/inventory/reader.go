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
)

// Record represents a single object from the inventory.
type Record struct {
	Key  string
	Size uint64
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
	file      *os.File
	csvReader *csv.Reader
	keyCol    int
	sizeCol   int
	closers   []io.Closer
}

var (
	// ErrNoKeyColumn indicates the CSV header is missing a Key column.
	ErrNoKeyColumn = errors.New("CSV header missing 'Key' column")
	// ErrNoSizeColumn indicates the CSV header is missing a Size column.
	ErrNoSizeColumn = errors.New("CSV header missing 'Size' column")
)

// OpenFile opens a CSV or CSV.GZ inventory file.
func OpenFile(path string) (*CSVReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open inventory file: %w", err)
	}

	var reader io.Reader = bufio.NewReader(f)
	var closers []io.Closer
	closers = append(closers, f)

	// Check for gzip compression by extension or magic bytes
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
	csvr.FieldsPerRecord = -1 // Variable field count
	csvr.LazyQuotes = true    // Handle malformed quotes

	// Read header and find column indices
	header, err := csvr.Read()
	if err != nil {
		for _, c := range closers {
			c.Close()
		}
		return nil, fmt.Errorf("read CSV header: %w", err)
	}

	keyCol := -1
	sizeCol := -1
	for i, col := range header {
		// Normalize column names (case-insensitive, trim whitespace)
		normalized := strings.TrimSpace(strings.ToLower(col))
		switch normalized {
		case "key":
			keyCol = i
		case "size":
			sizeCol = i
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

	return &CSVReader{
		file:      f,
		csvReader: csvr,
		keyCol:    keyCol,
		sizeCol:   sizeCol,
		closers:   closers,
	}, nil
}

// NewReader creates a CSVReader from an io.Reader with an already-parsed header.
func NewReader(r io.Reader, keyCol, sizeCol int) *CSVReader {
	csvr := csv.NewReader(r)
	csvr.ReuseRecord = true
	csvr.FieldsPerRecord = -1
	csvr.LazyQuotes = true

	return &CSVReader{
		csvReader: csvr,
		keyCol:    keyCol,
		sizeCol:   sizeCol,
	}
}

// Read reads the next record. Returns io.EOF when done.
func (r *CSVReader) Read() (Record, error) {
	for {
		fields, err := r.csvReader.Read()
		if err != nil {
			return Record{}, err
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

		return Record{Key: key, Size: size}, nil
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
