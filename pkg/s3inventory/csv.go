package s3inventory

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"
	"sync"
	"unsafe"

	"github.com/eunmann/s3-inv-db/pkg/logging"
	"github.com/klauspost/compress/gzip"
)

// Row represents a single object from an S3 inventory file.
// This is the unified representation used by both CSV and Parquet readers.
// Its strings are valid only until the reader's next Next or Close: they
// may view the reader's buffer.
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
	// until the next call or Close. Returns io.EOF when all rows have
	// been read.
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
	stream          io.Closer      // the stream NewCSVReaderFromStream read
	bufs            *streamBuffers // pooled; nil once Close returned them
	gzipped         bool
	keyCol          int
	sizeCol         int
	storageCol      int
	accessTierCol   int
	deleteMarkerCol int
	anomalies       Anomalies
	keyBuf          []byte // decoded key; Next's key views it
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

// NewCSVReaderFromStream creates a CSV inventory reader from an S3
// stream, gunzipping it when it starts with the gzip magic bytes. The
// content decides, not the key: a gzip chunk named without ".gz" would
// otherwise be split as text. Gunzip runs on the caller's goroutine:
// the build already reads one chunk per CPU. Close returns the reader's
// buffers for the next chunk to reuse.
func NewCSVReaderFromStream(r io.ReadCloser, cfg CSVReaderConfig) (*CSVReader, error) {
	bufs := streamBufferPool.Get().(*streamBuffers) //nolint:forcetypeassert // the pool only holds *streamBuffers
	bufs.in.Reset(r)
	lines := bufs.in

	gzipped, err := isGzip(bufs.in)
	if err == nil && gzipped {
		if err = bufs.gz.Reset(bufs.in); err != nil {
			err = fmt.Errorf("create gzip reader: %w", err)
		}
		bufs.lines.Reset(&bufs.gz)
		lines = bufs.lines
	}
	if err != nil {
		bufs.release()
		r.Close()

		return nil, err
	}

	return &CSVReader{
		lines:           lines,
		keyCol:          cfg.KeyCol,
		sizeCol:         cfg.SizeCol,
		storageCol:      cfg.StorageCol,
		accessTierCol:   cfg.AccessTierCol,
		deleteMarkerCol: cfg.DeleteMarkerCol,
		stream:          r,
		bufs:            bufs,
		gzipped:         gzipped,
	}, nil
}

// streamBuffers are a stream reader's input and line buffers and its
// gunzip state (~1.1 MiB with the flate window), kept across chunks.
type streamBuffers struct {
	in    *bufio.Reader
	lines *bufio.Reader
	gz    gzip.Reader
}

//nolint:gochecknoglobals // intentional package-level buffer pool
var streamBufferPool = sync.Pool{New: func() any {
	return &streamBuffers{
		in:    bufio.NewReaderSize(nil, streamInputBuffer),
		lines: bufio.NewReaderSize(nil, csvLineBuffer),
	}
}}

// release drops the buffers' hold on the stream and returns them to the
// pool. The gunzip state reads only from in.
func (b *streamBuffers) release() {
	b.in.Reset(nil)
	b.lines.Reset(nil)
	streamBufferPool.Put(b)
}

// Buffer sizes: the CSV line buffer bounds the lines split in place
// (longer ones go to encoding/csv), and the input buffer saves the
// decompressor a read per 4 KiB. A plain stream is split straight from
// the input buffer, so its lines are bounded by that.
const (
	csvLineBuffer     = 64 << 10
	streamInputBuffer = 1 << 20
)

// isGzip reports whether in starts with the gzip magic bytes 1f 8b. A
// stream shorter than that is not gzip.
func isGzip(in *bufio.Reader) (bool, error) {
	magic, err := in.Peek(2)
	if err != nil && !errors.Is(err, io.EOF) {
		return false, fmt.Errorf("peek gzip magic: %w", err)
	}

	return len(magic) == 2 && magic[0] == 0x1f && magic[1] == 0x8b, nil
}

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
// Athena guidance uses. A key with neither '%' nor '+' is returned as
// is; any other decodes into the reader's key buffer, which the
// returned string views until the next call. A key that is not valid
// encoding is kept as-is and counted.
func (r *CSVReader) decodeKey(raw string) string {
	if !hasEscape(raw) {
		return raw
	}
	var ok bool
	r.keyBuf, ok = appendUnescape(r.keyBuf[:0], raw)
	if !ok {
		r.anomalies.UndecodableKeys++

		return raw
	}

	return bytesView(r.keyBuf)
}

// hasEscape reports whether s holds '%' or '+'. Two IndexByte scans
// (vectorized) beat one strings.ContainsAny, which checks byte by byte.
func hasEscape(s string) bool {
	return strings.IndexByte(s, '%') >= 0 || strings.IndexByte(s, '+') >= 0
}

// appendUnescape appends the form-decoded s to dst, copying the runs
// between escapes in bulk: what url.QueryUnescape returns, without its
// second pass, per-byte writes and allocation. It reports false when s
// holds a '%' not followed by two hex digits.
func appendUnescape(dst []byte, s string) ([]byte, bool) {
	for {
		i := nextEscape(s)
		if i < 0 {
			return append(dst, s...), true
		}
		dst = append(dst, s[:i]...)
		if s[i] == '+' {
			dst = append(dst, ' ')
			s = s[i+1:]

			continue
		}
		if i+2 >= len(s) {
			return dst, false
		}
		hi, lo := unhex[s[i+1]], unhex[s[i+2]]
		if hi|lo == invalidHex {
			return dst, false
		}
		dst = append(dst, hi<<4|lo)
		s = s[i+3:]
	}
}

// nextEscape returns the index of the first '%' or '+' in s, or -1.
func nextEscape(s string) int {
	pct := strings.IndexByte(s, '%')
	end := len(s)
	if pct >= 0 {
		end = pct
	}
	if plus := strings.IndexByte(s[:end], '+'); plus >= 0 {
		return plus
	}

	return pct
}

// invalidHex marks a byte that is not a hex digit in unhex. Valid
// digits are below 16, so OR-ing two lookups is invalidHex only when
// one of them is.
const invalidHex = 0xff

// unhex maps a hex digit to its value and anything else to invalidHex.
//
//nolint:gochecknoglobals // immutable lookup table
var unhex = func() [256]byte {
	var t [256]byte
	for i := range t {
		t[i] = invalidHex
	}
	for c := byte('0'); c <= '9'; c++ {
		t[c] = c - '0'
	}
	for c := byte('a'); c <= 'f'; c++ {
		t[c] = c - 'a' + 10
		t[c-'a'+'A'] = c - 'a' + 10
	}

	return t
}()

// isTrue parses the boolean columns S3 Inventory writes ("true"/"TRUE").
func isTrue(field string) bool {
	return strings.EqualFold(strings.TrimSpace(field), "true")
}

// Anomalies counts the rows read so far that could not be read exactly.
func (r *CSVReader) Anomalies() Anomalies { return r.anomalies }

// Close closes the stream and returns the reader's buffers to the pool,
// after which the rows read no longer hold. A second Close does nothing.
func (r *CSVReader) Close() error {
	var gzErr, streamErr error
	if r.bufs != nil {
		if r.gzipped {
			gzErr = r.bufs.gz.Close()
		}
		r.bufs.release()
		r.bufs, r.lines, r.csvReader = nil, nil, nil
	}
	if r.stream != nil {
		streamErr = r.stream.Close()
		r.stream = nil
	}
	if gzErr != nil {
		return fmt.Errorf("gunzip: %w", gzErr)
	}
	if streamErr != nil {
		return fmt.Errorf("close stream: %w", streamErr)
	}

	return nil
}
