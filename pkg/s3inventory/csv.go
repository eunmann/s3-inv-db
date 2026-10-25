package s3inventory

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"unsafe"

	"github.com/eunmann/s3-inv-db/pkg/logging"
	"github.com/klauspost/pgzip"
)

// Row represents a single object from an S3 inventory file.
// This is the unified representation used by both CSV and Parquet readers.
// Its strings are valid only until the reader's next Next: they may view
// the reader's buffer.
type Row struct {
	Key          string
	StorageClass string
	AccessTier   string
	Size         uint64
}

// Reader is the unified interface for reading S3 inventory files.
// CSV and Parquet implementations live in this package.
type Reader interface {
	// Next returns the next inventory row, whose strings are valid only
	// until the next call. Returns io.EOF when all rows have been read.
	Next() (Row, error)

	// Anomalies counts the rows read so far that could not be turned
	// into an exact inventory row.
	Anomalies() Anomalies

	// Close releases resources associated with the reader.
	Close() error
}

// Anomalies counts inventory rows a reader could not read exactly. A
// build that indexes them would report numbers that differ from a
// direct scan of the inventory, so the pipeline fails on any of them
// unless told to accept them.
type Anomalies struct {
	// ShortRows: CSV rows without the key or size column. Skipped.
	ShortRows uint64
	// MissingKeys: rows whose key is empty (CSV) or null (Parquet). Skipped.
	MissingKeys uint64
	// BadSizes: sizes that don't parse (CSV) or are null (Parquet).
	// Indexed as 0.
	BadSizes uint64
	// UndecodableKeys: CSV keys that aren't valid URL encoding. Kept
	// encoded, so their prefixes are wrong.
	UndecodableKeys uint64
}

// Total is the number of anomalous rows.
func (a Anomalies) Total() uint64 {
	return a.ShortRows + a.MissingKeys + a.BadSizes + a.UndecodableKeys
}

func (a Anomalies) String() string {
	return fmt.Sprintf("%d short rows, %d missing keys, %d bad sizes, %d undecodable keys",
		a.ShortRows, a.MissingKeys, a.BadSizes, a.UndecodableKeys)
}

// CSVReader reads S3 inventory records from CSV streams. S3 writes each
// record on one line with every field plain or quoted, and keys are URL
// encoded, so no field holds a quote, comma or newline: such lines are
// split in place, the fields viewing the line. The first line that is
// not of that shape (an escaped or stray quote, a quoted field running
// over the line end, a line longer than the buffer) hands that line
// and everything after it to encoding/csv, configured as before, so the
// records are what encoding/csv alone would return.
type CSVReader struct {
	lines           *bufio.Reader // nil once encoding/csv took over
	fields          []string
	csvReader       *csv.Reader
	closers         []io.Closer
	keyCol          int
	sizeCol         int
	storageCol      int
	accessTierCol   int
	deleteMarkerCol int
	anomalies       Anomalies
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
	return &CSVReader{
		lines:           bufio.NewReaderSize(r, csvLineBuffer),
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
		// It reads its input through a 4 KiB buffer unless given one.
		gzr, err := pgzip.NewReader(bufio.NewReaderSize(r, gzipInputBuffer))
		if err != nil {
			r.Close()

			return nil, fmt.Errorf("create gzip reader: %w", err)
		}
		closers = append(closers, gzr)
		reader = gzr
	}

	return &CSVReader{
		lines:           bufio.NewReaderSize(reader, csvLineBuffer),
		keyCol:          cfg.KeyCol,
		sizeCol:         cfg.SizeCol,
		storageCol:      cfg.StorageCol,
		accessTierCol:   cfg.AccessTierCol,
		deleteMarkerCol: cfg.DeleteMarkerCol,
		closers:         closers,
	}, nil
}

// Buffer sizes: the CSV line buffer bounds the lines split in place
// (longer ones go to encoding/csv), and the gzip input buffer saves
// pgzip a read per 4 KiB.
const (
	csvLineBuffer   = 64 << 10
	gzipInputBuffer = 1 << 20
)

// newFallbackCSV is the encoding/csv reader the CSVReader has always
// used, reading r.
func newFallbackCSV(r io.Reader) *csv.Reader {
	csvr := csv.NewReader(r)
	csvr.ReuseRecord = true
	csvr.FieldsPerRecord = -1
	csvr.LazyQuotes = true

	return csvr
}

// record returns the next record's fields, valid until the next call.
func (r *CSVReader) record() ([]string, error) {
	if r.csvReader != nil {
		return r.csvReader.Read() //nolint:wrapcheck // Next wraps it
	}
	for {
		line, err := r.lines.ReadSlice('\n')
		if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, bufio.ErrBufferFull) {
			return nil, err //nolint:wrapcheck // Next wraps it
		}
		if len(line) == 0 && errors.Is(err, io.EOF) {
			return nil, io.EOF
		}
		if err == nil || errors.Is(err, io.EOF) {
			body := trimLineEnd(line)
			if len(body) == 0 {
				// encoding/csv skips empty lines.
				continue
			}
			if fields, ok := splitPlainCSV(body, r.fields[:0]); ok {
				r.fields = fields

				return fields, nil
			}
		}
		// Not a plain line: encoding/csv reads it and the rest.
		r.csvReader = newFallbackCSV(io.MultiReader(bytes.NewReader(slices.Clone(line)), r.lines))
		r.lines = nil

		return r.csvReader.Read() //nolint:wrapcheck // Next wraps it
	}
}

// trimLineEnd drops a line's "\n" or "\r\n", and the "\r" of a last line
// without "\n", as encoding/csv does.
func trimLineEnd(line []byte) []byte {
	line = bytes.TrimSuffix(line, []byte{'\n'})

	return bytes.TrimSuffix(line, []byte{'\r'})
}

// splitPlainCSV splits a line whose fields are each either plain
// (no '"') or wholly quoted with no '"' inside, appending the fields to
// dst as views of line; it reports false for any other line.
func splitPlainCSV(line []byte, dst []string) ([]string, bool) {
	for start := 0; ; {
		if start < len(line) && line[start] == '"' {
			end := bytes.IndexByte(line[start+1:], '"')
			if end < 0 {
				return nil, false
			}
			end += start + 1
			if end+1 < len(line) && line[end+1] != ',' {
				return nil, false
			}
			dst = append(dst, bytesView(line[start+1:end]))
			if end+1 >= len(line) {
				return dst, true
			}
			start = end + 2

			continue
		}
		field := line[start:]
		comma := bytes.IndexByte(field, ',')
		if comma >= 0 {
			field = field[:comma]
		}
		if bytes.IndexByte(field, '"') >= 0 {
			return nil, false
		}
		dst = append(dst, bytesView(field))
		if comma < 0 {
			return dst, true
		}
		start += comma + 1
	}
}

// bytesView returns b as a string without copying; it is valid while b
// is not overwritten.
func bytesView(b []byte) string {
	if len(b) == 0 {
		return ""
	}

	return unsafe.String(unsafe.SliceData(b), len(b))
}

// Next returns the next inventory row. Its strings are valid until the
// next call.
func (r *CSVReader) Next() (Row, error) {
	for {
		fields, err := r.record()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return Row{}, io.EOF
			}

			return Row{}, fmt.Errorf("read CSV row: %w", err)
		}

		if len(fields) <= r.keyCol || len(fields) <= r.sizeCol {
			r.anomalies.ShortRows++

			continue
		}

		if r.deleteMarkerCol >= 0 && len(fields) > r.deleteMarkerCol && isTrue(fields[r.deleteMarkerCol]) {
			continue // a delete marker is not an object and stores no bytes
		}

		key := r.decodeKey(fields[r.keyCol])
		if key == "" {
			r.anomalies.MissingKeys++

			continue
		}

		sizeStr := strings.TrimSpace(fields[r.sizeCol])
		size, err := strconv.ParseUint(sizeStr, 10, 64)
		if err != nil {
			r.anomalies.BadSizes++
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
		r.anomalies.UndecodableKeys++

		return raw
	}

	return key
}

// isTrue parses the boolean columns S3 Inventory writes ("true"/"TRUE").
func isTrue(field string) bool {
	return strings.EqualFold(strings.TrimSpace(field), "true")
}

// Anomalies counts the rows read so far that could not be read exactly.
func (r *CSVReader) Anomalies() Anomalies { return r.anomalies }

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
