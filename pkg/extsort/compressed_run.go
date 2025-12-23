package extsort

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"

	"github.com/klauspost/compress/zstd"
)

// Compressed run file format:
//
// Header (32 bytes):
//   Magic:            4 bytes  (0x45585453 = "EXTS")
//   Version:          4 bytes  (2 for compressed format)
//   Flags:            4 bytes  (bit 0: compressed, bits 1-3: compression type)
//   Count:            8 bytes  (number of PrefixRows)
//   UncompressedSize: 8 bytes  (total uncompressed data size, for buffer hints)
//   Reserved:         4 bytes  (future use)
//
// Body: zstd compressed stream of records (same record format as uncompressed)
//
// Compression types:
//   0: None (uncompressed, legacy compatibility)
//   1: Zstd (default, best speed/ratio tradeoff)

const (
	compressedRunFileVersion = 2
	compressedRunFileHeader  = 32

	// Compression type flags (stored in bits 1-3 of flags field).
	compressionTypeNone = 0
	compressionTypeZstd = 1

	// Flags field bit masks.
	flagCompressed      = 1 << 0
	flagCompressionMask = 0x0E // bits 1-3
)

// CompressionLevel defines the compression effort level.
type CompressionLevel int

const (
	// CompressionFastest prioritizes speed over ratio.
	CompressionFastest CompressionLevel = 1
	// CompressionDefault balances speed and ratio.
	CompressionDefault CompressionLevel = 3
	// CompressionBetter prioritizes ratio over speed.
	CompressionBetter CompressionLevel = 6
)

// CompressedRunWriter writes sorted PrefixRows to a compressed run file.
type CompressedRunWriter struct {
	file             *os.File
	compressor       *zstd.Encoder
	writer           *bufio.Writer
	count            uint64
	uncompressedSize uint64
	path             string
	buf              []byte
	closed           bool
	level            CompressionLevel
}

// CompressedRunWriterOptions configures the compressed run writer.
type CompressedRunWriterOptions struct {
	// BufferSize is the size of the write buffer (default 4MB).
	BufferSize int
	// CompressionLevel controls the compression effort (default CompressionDefault).
	CompressionLevel CompressionLevel
}

// NewCompressedRunWriter creates a new compressed run file writer.
func NewCompressedRunWriter(path string, opts CompressedRunWriterOptions) (*CompressedRunWriter, error) {
	if opts.BufferSize <= 0 {
		opts.BufferSize = 4 * 1024 * 1024 // 4MB default
	}
	if opts.CompressionLevel == 0 {
		opts.CompressionLevel = CompressionDefault
	}

	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("create compressed run file: %w", err)
	}

	// Write placeholder header (will be updated on close)
	header := make([]byte, compressedRunFileHeader)
	binary.LittleEndian.PutUint32(header[0:4], runFileMagic)
	binary.LittleEndian.PutUint32(header[4:8], compressedRunFileVersion)
	flags := uint32(flagCompressed | (compressionTypeZstd << 1))
	binary.LittleEndian.PutUint32(header[8:12], flags)
	// Count, UncompressedSize, Reserved will be updated on close

	if _, err := f.Write(header); err != nil {
		f.Close()
		os.Remove(path)
		return nil, fmt.Errorf("write header: %w", err)
	}

	// Create zstd encoder with specified level
	zstdLevel := zstd.SpeedDefault
	switch opts.CompressionLevel {
	case CompressionFastest:
		zstdLevel = zstd.SpeedFastest
	case CompressionDefault:
		zstdLevel = zstd.SpeedDefault
	case CompressionBetter:
		zstdLevel = zstd.SpeedBetterCompression
	}

	enc, err := zstd.NewWriter(f, zstd.WithEncoderLevel(zstdLevel))
	if err != nil {
		f.Close()
		os.Remove(path)
		return nil, fmt.Errorf("create zstd encoder: %w", err)
	}

	return &CompressedRunWriter{
		file:       f,
		compressor: enc,
		writer:     bufio.NewWriterSize(enc, opts.BufferSize),
		path:       path,
		buf:        make([]byte, 1024),
		level:      opts.CompressionLevel,
	}, nil
}

// Write writes a single PrefixRow to the compressed run file.
func (w *CompressedRunWriter) Write(row *PrefixRow) error {
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
	w.uncompressedSize += uint64(offset)
	return nil
}

// WriteAll writes a slice of PrefixRows to the compressed run file.
func (w *CompressedRunWriter) WriteAll(rows []*PrefixRow) error {
	for _, row := range rows {
		if err := w.Write(row); err != nil {
			return err
		}
	}
	return nil
}

// WriteSorted sorts the rows by prefix and writes them to the compressed run file.
func (w *CompressedRunWriter) WriteSorted(rows []*PrefixRow) error {
	slices.SortFunc(rows, func(a, b *PrefixRow) int {
		return strings.Compare(a.Prefix, b.Prefix)
	})
	return w.WriteAll(rows)
}

// Count returns the number of records written.
func (w *CompressedRunWriter) Count() uint64 {
	return w.count
}

// Path returns the path to the run file.
func (w *CompressedRunWriter) Path() string {
	return w.path
}

// Close flushes all buffers, finalizes compression, updates the header, and closes the file.
func (w *CompressedRunWriter) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true

	// Flush the buffered writer
	if err := w.writer.Flush(); err != nil {
		w.compressor.Close()
		w.file.Close()
		return fmt.Errorf("flush buffer: %w", err)
	}

	// Close the compressor (finalizes zstd stream)
	if err := w.compressor.Close(); err != nil {
		w.file.Close()
		return fmt.Errorf("close compressor: %w", err)
	}

	// Update header with final count and uncompressed size
	if _, err := w.file.Seek(12, 0); err != nil {
		w.file.Close()
		return fmt.Errorf("seek to count: %w", err)
	}

	var headerUpdate [20]byte
	binary.LittleEndian.PutUint64(headerUpdate[0:8], w.count)
	binary.LittleEndian.PutUint64(headerUpdate[8:16], w.uncompressedSize)
	// Reserved field (4 bytes) stays zero

	if _, err := w.file.Write(headerUpdate[:]); err != nil {
		w.file.Close()
		return fmt.Errorf("update header: %w", err)
	}

	if err := w.file.Close(); err != nil {
		return fmt.Errorf("close file: %w", err)
	}
	return nil
}

// CompressedRunReader reads PrefixRows from a compressed run file.
type CompressedRunReader struct {
	file         *os.File
	decompressor *zstd.Decoder
	reader       *bufio.Reader
	count        uint64
	read         uint64
	path         string
	buf          []byte
	closed       bool
}

// OpenCompressedRunFile opens a compressed run file for reading.
// It auto-detects whether the file is compressed or uncompressed based on the version.
func OpenCompressedRunFile(path string, bufferSize int) (*CompressedRunReader, error) {
	if bufferSize <= 0 {
		bufferSize = 4 * 1024 * 1024 // 4MB default
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open compressed run file: %w", err)
	}

	// Read header to determine format
	header := make([]byte, compressedRunFileHeader)
	if _, err := io.ReadFull(f, header); err != nil {
		f.Close()
		return nil, fmt.Errorf("read header: %w", err)
	}

	magic := binary.LittleEndian.Uint32(header[0:4])
	if magic != runFileMagic {
		f.Close()
		return nil, fmt.Errorf("invalid magic: got %x, want %x", magic, runFileMagic)
	}

	version := binary.LittleEndian.Uint32(header[4:8])
	if version != compressedRunFileVersion {
		f.Close()
		return nil, fmt.Errorf("unsupported version for compressed reader: %d (want %d)", version, compressedRunFileVersion)
	}

	flags := binary.LittleEndian.Uint32(header[8:12])
	if flags&flagCompressed == 0 {
		f.Close()
		return nil, fmt.Errorf("file is not compressed (flags=%d)", flags)
	}

	compressionType := (flags & flagCompressionMask) >> 1
	if compressionType != compressionTypeZstd {
		f.Close()
		return nil, fmt.Errorf("unsupported compression type: %d", compressionType)
	}

	count := binary.LittleEndian.Uint64(header[12:20])

	// Create zstd decoder
	dec, err := zstd.NewReader(f)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("create zstd decoder: %w", err)
	}

	return &CompressedRunReader{
		file:         f,
		decompressor: dec,
		reader:       bufio.NewReaderSize(dec, bufferSize),
		count:        count,
		path:         path,
		buf:          make([]byte, 1024),
	}, nil
}

// Read reads the next PrefixRow from the compressed run file.
// Returns io.EOF when all records have been read.
func (r *CompressedRunReader) Read() (*PrefixRow, error) {
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

// Count returns the total number of records in the file.
func (r *CompressedRunReader) Count() uint64 {
	return r.count
}

// ReadCount returns the number of records read so far.
func (r *CompressedRunReader) ReadCount() uint64 {
	return r.read
}

// Path returns the path to the run file.
func (r *CompressedRunReader) Path() string {
	return r.path
}

// Close closes the compressed run file.
func (r *CompressedRunReader) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true

	r.decompressor.Close()

	if err := r.file.Close(); err != nil {
		return fmt.Errorf("close compressed run file: %w", err)
	}
	return nil
}

// Remove closes and removes the compressed run file.
func (r *CompressedRunReader) Remove() error {
	if err := r.Close(); err != nil {
		return err
	}
	if err := os.Remove(r.path); err != nil {
		return fmt.Errorf("remove compressed run file: %w", err)
	}
	return nil
}

// RunReader is an interface that abstracts reading from either compressed or uncompressed run files.
type RunReader interface {
	Read() (*PrefixRow, error)
	Count() uint64
	ReadCount() uint64
	Path() string
	Close() error
	Remove() error
}

// Ensure both reader types implement RunReader interface.
var (
	_ RunReader = (*RunFileReader)(nil)
	_ RunReader = (*CompressedRunReader)(nil)
)

// OpenRunFileAuto opens a run file, auto-detecting whether it's compressed or not.
// Returns a RunReader interface that works with either format.
func OpenRunFileAuto(path string, bufferSize int) (RunReader, error) {
	if bufferSize <= 0 {
		bufferSize = 4 * 1024 * 1024
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open run file: %w", err)
	}

	// Read just enough to check magic and version
	header := make([]byte, 8)
	if _, err := io.ReadFull(f, header); err != nil {
		f.Close()
		return nil, fmt.Errorf("read header: %w", err)
	}
	f.Close()

	magic := binary.LittleEndian.Uint32(header[0:4])
	if magic != runFileMagic {
		return nil, fmt.Errorf("invalid magic: got %x, want %x", magic, runFileMagic)
	}

	version := binary.LittleEndian.Uint32(header[4:8])
	switch version {
	case runFileVersion:
		return OpenRunFile(path, bufferSize)
	case compressedRunFileVersion:
		return OpenCompressedRunFile(path, bufferSize)
	default:
		return nil, fmt.Errorf("unsupported run file version: %d", version)
	}
}
