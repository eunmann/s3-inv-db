package extsort

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/klauspost/compress/zstd"
)

// Run file format:
//
// Header (32 bytes):
//   Magic:            4 bytes  (0x45585453 = "EXTS")
//   Version:          4 bytes  (2)
//   Flags:            4 bytes  (bit 0: compressed, bits 1-3: compression type)
//   Count:            8 bytes  (number of PrefixRows)
//   UncompressedSize: 8 bytes  (total uncompressed data size, for buffer hints)
//   Reserved:         4 bytes  (future use)
//
// Body: zstd compressed stream of records in sorted order, each in the
// varint layout encodeRunRecord writes (types.go): the prefix front-coded
// against the previous record's, the depth, and a count and byte total
// for each tier present only.
//
// Compression types:
//   1: Zstd (only supported type)

const (
	runFileMagic             = 0x45585453 // "EXTS"
	compressedRunFileVersion = 3
	compressedRunFileHeader  = 32

	// Compression type flag (stored in bits 1-3 of flags field).
	compressionTypeZstd = 1

	// Flags field bit masks.
	flagCompressed      = 1 << 0
	flagCompressionMask = 0x0E // bits 1-3

	// Byte offset of the Count field in the compressed run file header
	// (4 magic + 4 version + 4 flags).
	compressedRunCountOffset = 12
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
	path             string
	buf              []byte
	prev             string // the last prefix written
	zstdLevel        zstd.EncoderLevel
	count            uint64
	uncompressedSize uint64
	level            CompressionLevel
	closed           bool
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
		opts.BufferSize = DefaultRunBufferSize
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
		return nil, errors.Join(fmt.Errorf("write header: %w", err), f.Close(), os.Remove(path))
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

	enc, err := acquireZstdEncoder(zstdLevel)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("acquire zstd encoder: %w", err), f.Close(), os.Remove(path))
	}
	enc.Reset(f)

	return &CompressedRunWriter{
		file:       f,
		compressor: enc,
		zstdLevel:  zstdLevel,
		writer:     bufio.NewWriterSize(enc, opts.BufferSize),
		path:       path,
		buf:        make([]byte, 0, 1024),
		level:      opts.CompressionLevel,
	}, nil
}

// Write writes a single PrefixRow to the compressed run file.
func (w *CompressedRunWriter) Write(row *PrefixRow) error {
	if err := w.writeRecord(encodeRunRecord(w.buf[:0], w.prev, row)); err != nil {
		return err
	}
	w.prev = row.Prefix

	return nil
}

// WriteEncoded writes row, whose prefix is the previous record's first
// shared bytes followed by suffix, for a caller that knows them without
// the whole prefix; row.Prefix is not read. A later Write front-codes
// against no prefix, which reads back the same.
func (w *CompressedRunWriter) WriteEncoded(shared int, suffix string, row *PrefixRow) error {
	if err := w.writeRecord(encodeRunRecordShared(w.buf[:0], shared, suffix, row)); err != nil {
		return err
	}
	w.prev = ""

	return nil
}

// writeRecord writes an encoded record kept in w.buf.
func (w *CompressedRunWriter) writeRecord(rec []byte) error {
	w.buf = rec
	if _, err := w.writer.Write(rec); err != nil {
		return fmt.Errorf("write record: %w", err)
	}
	w.count++
	w.uncompressedSize += uint64(len(rec))

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
	SortPrefixRows(rows)

	return w.WriteAll(rows)
}

// Close flushes all buffers, finalizes compression, updates the
// header, and closes the file. Idempotent — a second call returns nil
// even if the first call returned an error mid-way (the underlying
// file is always closed exactly once via closeFile).
func (w *CompressedRunWriter) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true

	if err := w.writer.Flush(); err != nil {
		return errors.Join(fmt.Errorf("flush buffer: %w", err), w.closeCompressor(), w.closeFile())
	}

	if err := w.compressor.Close(); err != nil {
		return errors.Join(fmt.Errorf("close compressor: %w", err), w.closeFile())
	}
	releaseZstdEncoder(w.zstdLevel, w.compressor)
	w.compressor = nil

	if _, err := w.file.Seek(compressedRunCountOffset, 0); err != nil {
		return errors.Join(fmt.Errorf("seek to count: %w", err), w.closeFile())
	}

	var headerUpdate [20]byte
	binary.LittleEndian.PutUint64(headerUpdate[0:8], w.count)
	binary.LittleEndian.PutUint64(headerUpdate[8:16], w.uncompressedSize)
	// Reserved field (4 bytes) stays zero

	if _, err := w.file.Write(headerUpdate[:]); err != nil {
		return errors.Join(fmt.Errorf("update header: %w", err), w.closeFile())
	}

	// No fsync: a run is scratch for this build only, and a crash
	// abandons the build, so durability would buy nothing.
	if err := w.closeFile(); err != nil {
		return fmt.Errorf("close file: %w", err)
	}

	return nil
}

// closeFile closes the backing file exactly once; nils the pointer so
// repeat calls are no-ops.
func (w *CompressedRunWriter) closeFile() error {
	if w.file == nil {
		return nil
	}
	f := w.file
	w.file = nil
	if err := f.Close(); err != nil {
		return fmt.Errorf("file close: %w", err)
	}

	return nil
}

// closeCompressor closes the encoder (if still open) and returns the
// resulting error wrapped, or nil if already closed.
func (w *CompressedRunWriter) closeCompressor() error {
	if w.compressor == nil {
		return nil
	}
	c := w.compressor
	w.compressor = nil
	if err := c.Close(); err != nil {
		return fmt.Errorf("close compressor: %w", err)
	}

	return nil
}

// CompressedRunReader reads PrefixRows from a compressed run file.
type CompressedRunReader struct {
	file         *os.File
	decompressor *zstd.Decoder
	records      runRecordDecoder
	path         string
	count        uint64
	read         uint64
	closed       bool
}

// OpenCompressedRunFile opens a run file for reading.
func OpenCompressedRunFile(path string, bufferSize int) (*CompressedRunReader, error) {
	if bufferSize <= 0 {
		bufferSize = DefaultRunBufferSize
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open compressed run file: %w", err)
	}

	header := make([]byte, compressedRunFileHeader)
	if _, err := io.ReadFull(f, header); err != nil {
		f.Close()

		return nil, fmt.Errorf("read header: %w", err)
	}

	magic := binary.LittleEndian.Uint32(header[0:4])
	if magic != runFileMagic {
		f.Close()

		return nil, fmt.Errorf("%w: got %x, want %x", ErrInvalidMagic, magic, runFileMagic)
	}

	version := binary.LittleEndian.Uint32(header[4:8])
	if version != compressedRunFileVersion {
		f.Close()

		return nil, fmt.Errorf("%w (compressed reader): %d (want %d)", ErrUnsupportedVersion, version, compressedRunFileVersion)
	}

	flags := binary.LittleEndian.Uint32(header[8:12])
	if flags&flagCompressed == 0 {
		f.Close()

		return nil, fmt.Errorf("%w (flags=%d)", ErrNotCompressed, flags)
	}

	compressionType := (flags & flagCompressionMask) >> 1
	if compressionType != compressionTypeZstd {
		f.Close()

		return nil, fmt.Errorf("%w: %d", ErrUnsupportedCompression, compressionType)
	}

	count := binary.LittleEndian.Uint64(header[12:20])

	// Create or reuse a pooled zstd decoder.
	dec, err := acquireZstdDecoder()
	if err != nil {
		f.Close()

		return nil, fmt.Errorf("acquire zstd decoder: %w", err)
	}
	if err := dec.Reset(f); err != nil {
		f.Close()
		dec.Close()

		return nil, fmt.Errorf("reset zstd decoder: %w", err)
	}

	return &CompressedRunReader{
		file:         f,
		decompressor: dec,
		records:      runRecordDecoder{in: readWindow{r: bufio.NewReaderSize(dec, bufferSize)}},
		count:        count,
		path:         path,
	}, nil
}

// ReadInto reads the next row into the caller-owned PrefixRow. A
// stream that ends before the header's count is corrupt.
func (r *CompressedRunReader) ReadInto(into *PrefixRow) error {
	if r.read >= r.count {
		return io.EOF
	}
	if err := r.records.next(into); err != nil {
		if errors.Is(err, io.EOF) {
			return fmt.Errorf("%w: run ends after %d of %d records", errRunRecordCorrupt, r.read, r.count)
		}

		return err
	}
	r.read++

	return nil
}

func (r *CompressedRunReader) Count() uint64 {
	return r.count
}

func (r *CompressedRunReader) ReadCount() uint64 {
	return r.read
}

// Close closes the compressed run file.
func (r *CompressedRunReader) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true

	releaseZstdDecoder(r.decompressor)
	r.decompressor = nil

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

// One typed pool per compression level. Zstd encoders allocate
// multi-MB window buffers; reusing them across run files cuts per-flush
// allocations dramatically.
//
//nolint:gochecknoglobals // intentional package-level encoder pools
var zstdEncoderPools = map[zstd.EncoderLevel]*typedPool[zstd.Encoder]{
	zstd.SpeedFastest:           newTypedPool(func() *zstd.Encoder { return nil }),
	zstd.SpeedDefault:           newTypedPool(func() *zstd.Encoder { return nil }),
	zstd.SpeedBetterCompression: newTypedPool(func() *zstd.Encoder { return nil }),
}

//nolint:gochecknoglobals // intentional package-level decoder pool
var zstdDecoderPool = newTypedPool(func() *zstd.Decoder { return nil })

// zstdWindowSize bounds each run stream's match window. Run rows are
// sorted, so most redundancy is between neighbouring rows; a 1 MiB
// window keeps the ratio (run-file bytes are unchanged) at a fraction
// of the default's buffers.
const zstdWindowSize = 1 << 20

// zstdDecoderConcurrency keeps each run decoder asynchronous, decoding
// ahead of the serial final merge, with the fewest block decoders that
// does: 2 merged as fast as the default 4 at ~0.7 MB less heap per open
// run, and 1 (synchronous) merged ~9% slower.
const zstdDecoderConcurrency = 2

// zstdEncoderOptions: one goroutine per stream. Every ingest worker and
// every merge output already runs its own encoder, so the default
// (GOMAXPROCS block encoders per stream) only multiplies buffers by the
// core count.
func zstdEncoderOptions(level zstd.EncoderLevel) []zstd.EOption {
	return []zstd.EOption{
		zstd.WithEncoderLevel(level),
		zstd.WithEncoderConcurrency(1),
		zstd.WithWindowSize(zstdWindowSize),
		zstd.WithLowerEncoderMem(true),
	}
}

// acquireZstdEncoder returns a pooled or freshly-built encoder at the
// requested level. A nil pool (unknown level) or an empty pool both
// fall through to NewWriter; that's by design — callers don't need to
// pre-register every level they use, they just give up pooling at
// other levels.
func acquireZstdEncoder(level zstd.EncoderLevel) (*zstd.Encoder, error) {
	if pool := zstdEncoderPools[level]; pool != nil {
		if enc := pool.Get(); enc != nil {
			return enc, nil
		}
	}
	enc, err := zstd.NewWriter(nil, zstdEncoderOptions(level)...)
	if err != nil {
		return nil, fmt.Errorf("zstd new writer: %w", err)
	}

	return enc, nil
}

func releaseZstdEncoder(level zstd.EncoderLevel, enc *zstd.Encoder) {
	if enc == nil {
		return
	}
	pool := zstdEncoderPools[level]
	if pool == nil {
		return
	}
	// Reset(nil) detaches the encoder so the next user can re-target it
	// without re-allocating window buffers.
	enc.Reset(nil)
	pool.Put(enc)
}

func acquireZstdDecoder() (*zstd.Decoder, error) {
	if dec := zstdDecoderPool.Get(); dec != nil {
		return dec, nil
	}
	dec, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(zstdDecoderConcurrency))
	if err != nil {
		return nil, fmt.Errorf("zstd new reader: %w", err)
	}

	return dec, nil
}

func releaseZstdDecoder(dec *zstd.Decoder) {
	if dec == nil {
		return
	}
	// Reset(nil) detaches from the current source.
	if err := dec.Reset(nil); err != nil {
		dec.Close()

		return
	}
	zstdDecoderPool.Put(dec)
}
