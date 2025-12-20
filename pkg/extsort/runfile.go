package extsort

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"
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
		bufferSize = 4 * 1024 * 1024 // 4MB default
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
	prefixLen := len(row.Prefix)
	recordSize := 4 + prefixLen + 2 + 8 + 8 + MaxTiers*8 + MaxTiers*8

	if len(w.buf) < recordSize {
		w.buf = make([]byte, recordSize*2)
	}

	offset := 0

	binary.LittleEndian.PutUint32(w.buf[offset:], uint32(prefixLen))
	offset += 4
	copy(w.buf[offset:], row.Prefix)
	offset += prefixLen

	binary.LittleEndian.PutUint16(w.buf[offset:], row.Depth)
	offset += 2

	binary.LittleEndian.PutUint64(w.buf[offset:], row.Count)
	offset += 8

	binary.LittleEndian.PutUint64(w.buf[offset:], row.TotalBytes)
	offset += 8

	for i := range MaxTiers {
		binary.LittleEndian.PutUint64(w.buf[offset:], row.TierCounts[i])
		offset += 8
	}

	for i := range MaxTiers {
		binary.LittleEndian.PutUint64(w.buf[offset:], row.TierBytes[i])
		offset += 8
	}

	if _, err := w.writer.Write(w.buf[:offset]); err != nil {
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
	slices.SortFunc(rows, func(a, b *PrefixRow) int {
		return strings.Compare(a.Prefix, b.Prefix)
	})
	return w.WriteAll(rows)
}

// Count returns the number of records written.
func (w *RunFileWriter) Count() uint64 {
	return w.count
}

// Path returns the path to the run file.
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

	return w.file.Close()
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
		bufferSize = 4 * 1024 * 1024 // 4MB default
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
		return nil, fmt.Errorf("invalid magic: got %x, want %x", magic, runFileMagic)
	}

	version := binary.LittleEndian.Uint32(header[4:8])
	if version != runFileVersion {
		f.Close()
		return nil, fmt.Errorf("unsupported version: %d", version)
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

	var lenBuf [4]byte
	if _, err := io.ReadFull(r.reader, lenBuf[:]); err != nil {
		return nil, fmt.Errorf("read prefix length: %w", err)
	}
	prefixLen := int(binary.LittleEndian.Uint32(lenBuf[:]))

	fixedSize := 2 + 8 + 8 + MaxTiers*8 + MaxTiers*8
	recordSize := prefixLen + fixedSize

	if len(r.buf) < recordSize {
		r.buf = make([]byte, recordSize*2)
	}

	if _, err := io.ReadFull(r.reader, r.buf[:recordSize]); err != nil {
		return nil, fmt.Errorf("read record: %w", err)
	}

	row := &PrefixRow{}
	offset := 0

	row.Prefix = string(r.buf[offset : offset+prefixLen])
	offset += prefixLen

	row.Depth = binary.LittleEndian.Uint16(r.buf[offset:])
	offset += 2

	row.Count = binary.LittleEndian.Uint64(r.buf[offset:])
	offset += 8

	row.TotalBytes = binary.LittleEndian.Uint64(r.buf[offset:])
	offset += 8

	for i := range MaxTiers {
		row.TierCounts[i] = binary.LittleEndian.Uint64(r.buf[offset:])
		offset += 8
	}

	for i := range MaxTiers {
		row.TierBytes[i] = binary.LittleEndian.Uint64(r.buf[offset:])
		offset += 8
	}

	r.read++
	return row, nil
}

// Count returns the total number of records in the file.
func (r *RunFileReader) Count() uint64 {
	return r.count
}

// Read returns the number of records read so far.
func (r *RunFileReader) ReadCount() uint64 {
	return r.read
}

// Path returns the path to the run file.
func (r *RunFileReader) Path() string {
	return r.path
}

// Close closes the run file.
func (r *RunFileReader) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	return r.file.Close()
}

// Remove closes and removes the run file.
func (r *RunFileReader) Remove() error {
	if err := r.Close(); err != nil {
		return err
	}
	return os.Remove(r.path)
}
