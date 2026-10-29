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
// split in place, the fields viewing the line, and only the fields up to
// the last configured column are kept. The first line that is not of
// that shape (an escaped or stray quote, a quoted field running over
// the line end, a line longer than the buffer) hands that line and
// everything after it to encoding/csv, configured as before, so the
// records are what encoding/csv alone would return.
type CSVReader struct {
	lines           *bufio.Reader // nil once encoding/csv took over
	fields          []string
	maxFields       int // fields kept per line split in place
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

// ErrCSVColumnUnset is returned for a CSVReaderConfig without its key
// or size column.
var ErrCSVColumnUnset = errors.New("required CSV column not set")

// Column is a CSV column. The zero Column is no column, so a config
// that leaves an optional column out reads that field as absent.
type Column struct {
	pos int // index + 1
}

// ColumnAt returns the column at index i, or no column when i is
// negative, as s3fetch.Manifest's column lookups report a missing one.
func ColumnAt(i int) Column {
	if i < 0 {
		return Column{}
	}

	return Column{pos: i + 1}
}

// Index returns the column's index, or -1 for no column.
func (c Column) Index() int { return c.pos - 1 }

// CSVReaderConfig names the columns the CSV reader reads.
type CSVReaderConfig struct {
	// KeyCol is the object key's column (required).
	KeyCol Column

	// SizeCol is the object size's column (required).
	SizeCol Column

	// StorageCol is the storage class's column, if any.
	StorageCol Column

	// AccessTierCol is the Intelligent-Tiering access tier's column, if any.
	AccessTierCol Column

	// DeleteMarkerCol is IsDeleteMarker's column, if any. Only
	// all-versions inventories carry it.
	DeleteMarkerCol Column
}

func (c CSVReaderConfig) validate() error {
	if c.KeyCol.Index() < 0 {
		return fmt.Errorf("%w: key", ErrCSVColumnUnset)
	}
	if c.SizeCol.Index() < 0 {
		return fmt.Errorf("%w: size", ErrCSVColumnUnset)
	}

	return nil
}

// NewCSVReader creates a new CSV inventory reader from an io.Reader.
// The reader should provide the raw CSV data (already decompressed if needed).
// Use NewCSVReaderFromStream for automatic gzip handling.
func NewCSVReader(r io.Reader, cfg CSVReaderConfig) (*CSVReader, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	return newCSVReader(bufio.NewReaderSize(r, csvLineBuffer), cfg), nil
}

// newCSVReader reads lines with the columns of cfg, which is valid.
func newCSVReader(lines *bufio.Reader, cfg CSVReaderConfig) *CSVReader {
	key, size := cfg.KeyCol.Index(), cfg.SizeCol.Index()
	storage, accessTier, deleteMarker := cfg.StorageCol.Index(), cfg.AccessTierCol.Index(), cfg.DeleteMarkerCol.Index()

	return &CSVReader{
		lines:           lines,
		maxFields:       1 + max(key, size, storage, accessTier, deleteMarker),
		keyCol:          key,
		sizeCol:         size,
		storageCol:      storage,
		accessTierCol:   accessTier,
		deleteMarkerCol: deleteMarker,
	}
}

// NewCSVReaderFromStream creates a CSV inventory reader from an S3
// stream, gunzipping it when it starts with the gzip magic bytes. The
// content decides, not the key: a gzip chunk named without ".gz" would
// otherwise be split as text. Gunzip runs on the caller's goroutine:
// the build already reads one chunk per CPU. Close returns the reader's
// buffers for the next chunk to reuse. On error r is closed.
func NewCSVReaderFromStream(r io.ReadCloser, cfg CSVReaderConfig) (*CSVReader, error) {
	if err := cfg.validate(); err != nil {
		r.Close()

		return nil, err
	}
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

	reader := newCSVReader(lines, cfg)
	reader.stream, reader.bufs, reader.gzipped = r, bufs, gzipped

	return reader, nil
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

// record returns the next record's fields, valid until the next call:
// all of them from encoding/csv, the first maxFields from a line split
// in place.
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
			if fields, ok := splitPlainCSV(body, r.fields[:0], r.maxFields); ok {
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
// (no '"') or wholly quoted with no '"' inside, appending its first
// maxFields fields to dst as views of line; it reports false for any
// other line. Knowing where the next quote is, plain fields are split
// on commas alone, and once no quote is left the line needs no more
// scanning than the fields still kept.
func splitPlainCSV(line []byte, dst []string, maxFields int) ([]string, bool) {
	quote := bytes.IndexByte(line, '"')
	start := 0
	for quote >= 0 {
		if start < quote {
			comma := bytes.IndexByte(line[start:quote], ',')
			if comma < 0 {
				return nil, false
			}
			if len(dst) < maxFields {
				dst = append(dst, bytesView(line[start:start+comma]))
			}
			start += comma + 1

			continue
		}
		end := bytes.IndexByte(line[start+1:], '"')
		if end < 0 {
			return nil, false
		}
		end += start + 1
		if end+1 < len(line) && line[end+1] != ',' {
			return nil, false
		}
		if len(dst) < maxFields {
			dst = append(dst, bytesView(line[start+1:end]))
		}
		if end+1 == len(line) {
			return dst, true
		}
		start = end + 2
		quote = nextQuote(line, start)
	}

	return splitCommas(line[start:], dst, maxFields), true
}

// nextQuote returns the index of the first '"' in line at or after
// start, or -1. S3 quotes every field, so it checks start first.
func nextQuote(line []byte, start int) int {
	if start < len(line) && line[start] == '"' {
		return start
	}
	if q := bytes.IndexByte(line[start:], '"'); q >= 0 {
		return start + q
	}

	return -1
}

// splitCommas appends the comma-separated fields of a line holding no
// quote to dst, until dst holds maxFields.
func splitCommas(line []byte, dst []string, maxFields int) []string {
	for len(dst) < maxFields {
		comma := bytes.IndexByte(line, ',')
		if comma < 0 {
			return append(dst, bytesView(line))
		}
		dst = append(dst, bytesView(line[:comma]))
		line = line[comma+1:]
	}

	return dst
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
// next call or Close.
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

		size, ok := parseDigits(fields[r.sizeCol])
		if !ok {
			sizeStr := strings.TrimSpace(fields[r.sizeCol])
			size, err = strconv.ParseUint(sizeStr, 10, 64)
			if err != nil {
				r.anomalies.BadSizes++
				logging.L().Debug().
					Str("key", key).
					Str("size_field", sizeStr).
					Err(err).
					Msg("s3inventory: malformed size, defaulting to 0")
				size = 0
			}
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

// parseDigits parses s when it is 1 to 19 ASCII digits, which cannot
// overflow a uint64 and are what S3 writes for a size. Anything else
// reports false for strconv.ParseUint to judge.
func parseDigits(s string) (uint64, bool) {
	if s == "" || len(s) > maxSafeDigits {
		return 0, false
	}
	var n uint64
	for i := range len(s) {
		c := s[i]
		if c < '0' || c > '9' {
			return 0, false
		}
		n = n*10 + uint64(c-'0')
	}

	return n, true
}

// maxSafeDigits is the most decimal digits that always fit a uint64.
const maxSafeDigits = 19

// decodeKey undoes the URL encoding S3 Inventory applies to keys in CSV
// reports (Parquet keys are raw). It is form decoding — '+' is a space
// and a literal '+' arrives as %2B — matching the url_decode AWS's
// Athena guidance uses. A key with neither '%' nor '+' is returned as
// is; any other decodes into the reader's key buffer, which the
// returned string views until the next call. A key that is not valid
// encoding is kept as-is and counted.
func (r *CSVReader) decodeKey(raw string) string {
	i := nextEscape(raw)
	if i < 0 {
		return raw
	}
	var ok bool
	r.keyBuf, ok = appendUnescape(r.keyBuf[:0], raw, i)
	if !ok {
		r.anomalies.UndecodableKeys++

		return raw
	}

	return bytesView(r.keyBuf)
}

// appendUnescape appends the form-decoded s, whose first escape is at
// i, to dst, copying the runs between escapes in bulk: what
// url.QueryUnescape returns, without its second pass, per-byte writes
// and allocation. It reports false when s holds a '%' not followed by
// two hex digits.
func appendUnescape(dst []byte, s string, i int) ([]byte, bool) {
	for ; i >= 0; i = nextEscape(s) {
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

	return append(dst, s...), true
}

// nextEscape returns the index of the first '%' or '+' in s, or -1.
// Two IndexByte scans (vectorized) beat one strings.IndexAny, which
// checks byte by byte.
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
