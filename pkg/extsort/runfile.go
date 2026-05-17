package extsort

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
)

// DefaultRunBufferSize is the default I/O buffer for run-file reader
// and writer instances when the caller passes 0. Big enough to
// amortise syscall overhead on sequential append/scan, small enough
// that holding several per worker stays well under any memory cap.
//
// Exported so benches and integration tests can reuse the same default
// without duplicating the literal across files.
const DefaultRunBufferSize = 4 * 1024 * 1024

// Sentinel errors for the extsort package. Wrap with %w when adding
// context via fmt.Errorf so callers can match with errors.Is.
var (
	// ErrInvalidMagic indicates a run file has the wrong magic number.
	ErrInvalidMagic = errors.New("invalid magic")
	// ErrUnsupportedVersion indicates an unsupported run file format version.
	ErrUnsupportedVersion = errors.New("unsupported run file version")
	// ErrNotCompressed indicates the file is not compressed when a
	// compressed reader is required.
	ErrNotCompressed = errors.New("file is not compressed")
	// ErrUnsupportedCompression indicates an unsupported compression type.
	ErrUnsupportedCompression = errors.New("unsupported compression type")
	// ErrNoInputPaths indicates a merge call received no input paths.
	ErrNoInputPaths = errors.New("no input paths provided")
)

// RunFile format:
//
// Header (16 bytes):
//   Magic:   4 bytes  (0x45585453 = "EXTS")
//   Version: 4 bytes  (1)
//   Count:   8 bytes  (number of PrefixRows)
//
// Records (variable length each):
//   PrefixLen:  4 bytes (uint32, length of prefix string)
//   Prefix:     N bytes (prefix string, no null terminator)
//   Depth:      2 bytes (uint16)
//   Count:      8 bytes (uint64)
//   TotalBytes: 8 bytes (uint64)
//   TierCounts: MaxTiers * 8 bytes (uint64 array)
//   TierBytes:  MaxTiers * 8 bytes (uint64 array)
//
// Each record is: 4 + len(prefix) + 2 + 8 + 8 + 12*8 + 12*8 = 22 + len(prefix) + 192 = 214 + len(prefix) bytes

const (
	runFileMagic   = 0x45585453 // "EXTS"
	runFileVersion = 1
	runFileHeader  = 16
)

// RunFileWriter writes sorted PrefixRows to a temporary run file.
type RunFileWriter struct {
	file   *os.File
	writer *bufio.Writer
	count  uint64
	path   string
	buf    []byte // reusable buffer for encoding
	closed bool
}

// NewRunFileWriter creates a new run file writer at the given path.
func NewRunFileWriter(path string, bufferSize int) (*RunFileWriter, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("create run file: %w", err)
	}

	if bufferSize <= 0 {
		bufferSize = DefaultRunBufferSize
	}

	w := &RunFileWriter{
		file:   f,
		writer: bufio.NewWriterSize(f, bufferSize),
		path:   path,
		buf:    make([]byte, 1024), // initial buffer
	}

	header := make([]byte, runFileHeader)
	binary.LittleEndian.PutUint32(header[0:4], runFileMagic)
	binary.LittleEndian.PutUint32(header[4:8], runFileVersion)
	binary.LittleEndian.PutUint64(header[8:16], 0) // count placeholder

	if _, err := w.writer.Write(header); err != nil {
		f.Close()
		os.Remove(path)

		return nil, fmt.Errorf("write header: %w", err)
	}

	return w, nil
}

// Write writes a single PrefixRow to the run file.
func (w *RunFileWriter) Write(row *PrefixRow) error {
	n := encodePrefixRowRecord(&w.buf, row)
	if _, err := w.writer.Write(w.buf[:n]); err != nil {
		return fmt.Errorf("write record: %w", err)
	}
	w.count++

	return nil
}

// WriteAll writes a slice of PrefixRows to the run file.
func (w *RunFileWriter) WriteAll(rows []*PrefixRow) error {
	for _, row := range rows {
		if err := w.Write(row); err != nil {
			return err
		}
	}

	return nil
}

// WriteSorted sorts the rows by prefix and writes them to the run file.
func (w *RunFileWriter) WriteSorted(rows []*PrefixRow) error {
	SortPrefixRows(rows)

	return w.WriteAll(rows)
}

func (w *RunFileWriter) Count() uint64 {
	return w.count
}

func (w *RunFileWriter) Path() string {
	return w.path
}

// Close flushes the buffer, updates the header, and closes the file.
func (w *RunFileWriter) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true

	if err := w.writer.Flush(); err != nil {
		w.file.Close()

		return fmt.Errorf("flush: %w", err)
	}

	if _, err := w.file.Seek(8, 0); err != nil {
		w.file.Close()

		return fmt.Errorf("seek: %w", err)
	}

	var countBuf [8]byte
	binary.LittleEndian.PutUint64(countBuf[:], w.count)
	if _, err := w.file.Write(countBuf[:]); err != nil {
		w.file.Close()

		return fmt.Errorf("update header: %w", err)
	}

	// fsync before close so a crash between Close() returning and the
	// kernel flushing dirty pages can't corrupt or truncate this run.
	// Intermediate run files are only kept until merge completes; the
	// fsync cost (~5-10 ms per file on SSD) is the price of durability.
	if err := w.file.Sync(); err != nil {
		w.file.Close()

		return fmt.Errorf("fsync run file: %w", err)
	}

	if err := w.file.Close(); err != nil {
		return fmt.Errorf("close run file: %w", err)
	}

	return nil
}

// RunFileReader reads PrefixRows from a run file.
type RunFileReader struct {
	file   *os.File
	reader *bufio.Reader
	count  uint64
	read   uint64
	path   string
	buf    []byte // reusable buffer for decoding
	closed bool
}

// OpenRunFile opens a run file for reading.
func OpenRunFile(path string, bufferSize int) (*RunFileReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open run file: %w", err)
	}

	if bufferSize <= 0 {
		bufferSize = DefaultRunBufferSize
	}

	r := &RunFileReader{
		file:   f,
		reader: bufio.NewReaderSize(f, bufferSize),
		path:   path,
		buf:    make([]byte, 1024),
	}

	header := make([]byte, runFileHeader)
	if _, err := io.ReadFull(r.reader, header); err != nil {
		f.Close()

		return nil, fmt.Errorf("read header: %w", err)
	}

	magic := binary.LittleEndian.Uint32(header[0:4])
	if magic != runFileMagic {
		f.Close()

		return nil, fmt.Errorf("%w: got %x, want %x", ErrInvalidMagic, magic, runFileMagic)
	}

	version := binary.LittleEndian.Uint32(header[4:8])
	if version != runFileVersion {
		f.Close()

		return nil, fmt.Errorf("%w: %d", ErrUnsupportedVersion, version)
	}

	r.count = binary.LittleEndian.Uint64(header[8:16])

	return r, nil
}

// Read reads the next PrefixRow from the run file.
// Returns io.EOF when all records have been read.
func (r *RunFileReader) Read() (*PrefixRow, error) {
	if r.read >= r.count {
		return nil, io.EOF
	}

	row, err := readPrefixRowRecord(r.reader, &r.buf)
	if err != nil {
		return nil, err
	}

	r.read++

	return row, nil
}

// ReadInto reads the next PrefixRow into the caller-owned row, avoiding
// the per-row PrefixRow allocation in Read. The caller must ensure the
// previous returned row is no longer in use.
// Returns io.EOF when all records have been read.
func (r *RunFileReader) ReadInto(into *PrefixRow) error {
	if r.read >= r.count {
		return io.EOF
	}
	if _, err := readPrefixRowRecordInto(r.reader, &r.buf, into); err != nil {
		return err
	}
	r.read++

	return nil
}

func (r *RunFileReader) Count() uint64 {
	return r.count
}

func (r *RunFileReader) ReadCount() uint64 {
	return r.read
}

func (r *RunFileReader) Path() string {
	return r.path
}

// Close closes the run file.
func (r *RunFileReader) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	if err := r.file.Close(); err != nil {
		return fmt.Errorf("close run file: %w", err)
	}

	return nil
}

// Remove closes and removes the run file.
func (r *RunFileReader) Remove() error {
	if err := r.Close(); err != nil {
		return err
	}
	if err := os.Remove(r.path); err != nil {
		return fmt.Errorf("remove run file: %w", err)
	}

	return nil
}
