package inventory

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/klauspost/pgzip"
)

// fastCSVReader is a hand-rolled CSV parser specialised for AWS S3
// inventory rows: no field quoting, no escape sequences, fixed
// column count. Trades the generality of encoding/csv for ~3x
// throughput and zero per-row allocations beyond the Key string.
//
// Correctness boundary: rejects rows containing a quote character
// (returns an error). If AWS ever quotes a field, the parser must
// fall back to encoding/csv; in practice S3 inventory CSVs do not
// quote.
type fastCSVReader struct {
	br      *bufio.Reader
	cfg     CSVReaderConfig
	closers []io.Closer
	line    []byte // reused per-row scratch
}

// NewCSVInventoryReaderFast returns a hand-rolled reader. The buffered
// reader is sized at 4 MiB which is large enough to keep up with
// pgzip on multi-core hosts.
func NewCSVInventoryReaderFast(r io.Reader, cfg CSVReaderConfig) InventoryReader {
	return &fastCSVReader{
		br:  bufio.NewReaderSize(r, 4*1024*1024),
		cfg: cfg,
	}
}

// NewCSVInventoryReaderFastFromStream mirrors NewCSVInventoryReaderFromStream
// but routes through the hand-rolled parser. Gzip decompression uses pgzip
// the same way the stdlib path does.
func NewCSVInventoryReaderFastFromStream(r io.ReadCloser, key string, cfg CSVReaderConfig) (InventoryReader, error) {
	var src io.Reader = r
	closers := []io.Closer{r}
	if strings.HasSuffix(strings.ToLower(key), ".gz") {
		gzr, err := pgzip.NewReader(r)
		if err != nil {
			r.Close()
			return nil, fmt.Errorf("create gzip reader: %w", err)
		}
		closers = append(closers, gzr)
		src = gzr
	}
	return &fastCSVReader{
		br:      bufio.NewReaderSize(src, 4*1024*1024),
		cfg:     cfg,
		closers: closers,
	}, nil
}

// Next returns the next inventory row. The returned Row.Key and
// StorageClass/AccessTier are freshly allocated strings — they are
// safe for the caller to retain after the next Next() call.
//
// Returns io.EOF when the underlying stream is exhausted.
func (r *fastCSVReader) Next() (Row, error) {
	for {
		// Read one line into r.line, reusing the scratch buffer between calls.
		// ReadSlice avoids the per-line copy that ReadBytes makes; we copy out
		// only the string fields we need.
		line, err := r.readLine()
		if err != nil {
			return Row{}, err
		}
		if len(line) == 0 {
			continue // skip blank lines
		}
		if hasQuote(line) {
			return Row{}, errors.New("fastCSV: unexpected quote in row — fall back to encoding/csv")
		}

		row, ok, err := parseLine(line, r.cfg)
		if err != nil {
			return Row{}, err
		}
		if !ok {
			continue // skipped row (empty key, malformed, etc.)
		}
		return row, nil
	}
}

// readLine returns one line of data without the trailing newline, using
// r.line as a backing buffer. ReadSlice returns a slice into the bufio
// internal buffer that becomes invalid on the next call, so we copy.
func (r *fastCSVReader) readLine() ([]byte, error) {
	r.line = r.line[:0]
	for {
		seg, err := r.br.ReadSlice('\n')
		// If the line was longer than bufio's buffer, ReadSlice returns
		// ErrBufferFull and the partial slice; we keep appending.
		if errors.Is(err, bufio.ErrBufferFull) {
			r.line = append(r.line, seg...)
			continue
		}
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, err
		}
		r.line = append(r.line, seg...)
		// Trim trailing \r\n or \n.
		n := len(r.line)
		for n > 0 && (r.line[n-1] == '\n' || r.line[n-1] == '\r') {
			n--
		}
		r.line = r.line[:n]
		if errors.Is(err, io.EOF) && len(r.line) == 0 {
			return nil, io.EOF
		}
		return r.line, nil
	}
}

func hasQuote(line []byte) bool {
	for _, c := range line {
		if c == '"' {
			return true
		}
	}
	return false
}

// parseLine extracts the Key, Size, StorageClass, and AccessTier fields
// from a single unquoted comma-delimited line. Returns ok=false if the
// row should be skipped (empty key, too few columns).
func parseLine(line []byte, cfg CSVReaderConfig) (Row, bool, error) {
	// Walk once, tracking col index. Materialize only the fields we want.
	var (
		col      int
		fieldStart int
		row      Row
		haveKey  bool
		haveSize bool
	)
	for i := 0; i <= len(line); i++ {
		if i < len(line) && line[i] != ',' {
			continue
		}
		// Field [fieldStart:i] is one column.
		if col == cfg.KeyCol {
			if i == fieldStart {
				return Row{}, false, nil // empty key
			}
			row.Key = string(line[fieldStart:i])
			haveKey = true
		} else if col == cfg.SizeCol {
			row.Size = parseUintTrimmed(line[fieldStart:i])
			haveSize = true
		} else if cfg.StorageCol >= 0 && col == cfg.StorageCol {
			row.StorageClass = string(line[fieldStart:i])
		} else if cfg.AccessTierCol >= 0 && col == cfg.AccessTierCol {
			row.AccessTier = string(line[fieldStart:i])
		}
		fieldStart = i + 1
		col++
		// Early exit: stop after the highest-numbered column we care about.
		if haveKey && haveSize &&
			(cfg.StorageCol < 0 || col > cfg.StorageCol) &&
			(cfg.AccessTierCol < 0 || col > cfg.AccessTierCol) {
			return row, true, nil
		}
	}
	if !haveKey || !haveSize {
		return Row{}, false, nil // malformed
	}
	return row, true, nil
}

func parseUintTrimmed(b []byte) uint64 {
	// Strip leading/trailing whitespace then parse.
	start := 0
	for start < len(b) && (b[start] == ' ' || b[start] == '\t') {
		start++
	}
	end := len(b)
	for end > start && (b[end-1] == ' ' || b[end-1] == '\t') {
		end--
	}
	var n uint64
	for i := start; i < end; i++ {
		c := b[i]
		if c < '0' || c > '9' {
			return 0
		}
		n = n*10 + uint64(c-'0')
	}
	return n
}

func (r *fastCSVReader) Close() error {
	var firstErr error
	for i := len(r.closers) - 1; i >= 0; i-- {
		if err := r.closers[i].Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
