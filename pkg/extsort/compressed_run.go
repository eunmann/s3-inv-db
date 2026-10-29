package extsort

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/klauspost/compress/s2"
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
// Body: a compressed stream (RunCodec, the compression type) of records
// in sorted order, each in the varint layout encodeRunRecord writes
// (types.go): the prefix front-coded against the previous record's, the
// depth, and a count and byte total for each tier present only.

const (
	runFileMagic             = 0x45585453 // "EXTS"
	compressedRunFileVersion = 3
	compressedRunFileHeader  = 32

	// Flags field bit masks.
	flagCompressed      = 1 << 0
	flagCompressionMask = 0x0E // bits 1-3

	// Byte offset of the Count field in the compressed run file header
	// (4 magic + 4 version + 4 flags).
	compressedRunCountOffset = 12
)

// RunCodec is the compression of a run file's body; its value is the
// header's compression type. Readers take the codec from each run's
// header, so runs of different codecs merge together.
type RunCodec uint32

const (
	// RunCodecZstd compresses run bodies with zstd.
	RunCodecZstd RunCodec = 1
	// RunCodecS2 compresses run bodies with S2: faster to write and
	// read than zstd, at more bytes on disk.
	RunCodecS2 RunCodec = 2
)

// defaultRunCodec is the codec every build writes its runs with: the
// one place to switch codecs for a build A/B. A codec writing more
// bytes per row raises the scratch bound docs/operations.md states.
const defaultRunCodec = RunCodecZstd

func (c RunCodec) String() string {
	switch c {
	case RunCodecZstd:
		return "zstd"
	case RunCodecS2:
		return "s2"
	}

	return fmt.Sprintf("RunCodec(%d)", uint32(c))
}

func (c RunCodec) valid() bool {
	return c == RunCodecZstd || c == RunCodecS2
}

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
	file       *os.File
	compressor io.WriteCloser
	// release returns compressor to its pool once it closed cleanly.
	release          func()
	writer           *bufio.Writer
	path             string
	buf              []byte
	prev             string // the last prefix written
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
	// Codec compresses the body (default defaultRunCodec).
	Codec RunCodec
}

// NewCompressedRunWriter creates a new compressed run file writer.
func NewCompressedRunWriter(path string, opts CompressedRunWriterOptions) (*CompressedRunWriter, error) {
	if opts.BufferSize <= 0 {
		opts.BufferSize = DefaultRunBufferSize
	}
	if opts.CompressionLevel == 0 {
		opts.CompressionLevel = CompressionDefault
	}
	if opts.Codec == 0 {
		opts.Codec = defaultRunCodec
	}
	if !opts.Codec.valid() {
		return nil, fmt.Errorf("%w: %d", ErrUnsupportedCompression, opts.Codec)
	}

	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("create compressed run file: %w", err)
	}

	// Write placeholder header (will be updated on close)
	header := make([]byte, compressedRunFileHeader)
	binary.LittleEndian.PutUint32(header[0:4], runFileMagic)
	binary.LittleEndian.PutUint32(header[4:8], compressedRunFileVersion)
	flags := flagCompressed | uint32(opts.Codec)<<1
	binary.LittleEndian.PutUint32(header[8:12], flags)
	// Count, UncompressedSize, Reserved will be updated on close

	if _, err := f.Write(header); err != nil {
		return nil, errors.Join(fmt.Errorf("write header: %w", err), f.Close(), os.Remove(path))
	}

	enc, release, err := acquireRunEncoder(opts.Codec, opts.CompressionLevel, f)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("acquire %s encoder: %w", opts.Codec, err), f.Close(), os.Remove(path))
	}

	return &CompressedRunWriter{
		file:       f,
		compressor: enc,
		release:    release,
		writer:     bufio.NewWriterSize(enc, opts.BufferSize),
		path:       path,
		buf:        make([]byte, 0, 1024),
		level:      opts.CompressionLevel,
	}, nil
}

// Write writes a single PrefixRow to the compressed run file.
func (w *CompressedRunWriter) Write(row *PrefixRow) error {
	w.buf = encodeRunRecord(w.buf[:0], w.prev, row)
	if _, err := w.writer.Write(w.buf); err != nil {
		return fmt.Errorf("write record: %w", err)
	}
	w.prev = row.Prefix
	w.count++
	w.uncompressedSize += uint64(len(w.buf))

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
	w.release()
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
	file *os.File
	// release returns the decoder to its pool.
	release func()
	records runRecordDecoder
	path    string
	count   uint64
	read    uint64
	closed  bool
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

	codec := RunCodec((flags & flagCompressionMask) >> 1)
	if !codec.valid() {
		f.Close()

		return nil, fmt.Errorf("%w: %d", ErrUnsupportedCompression, codec)
	}

	count := binary.LittleEndian.Uint64(header[12:20])

	dec, release, err := acquireRunDecoder(codec, f)
	if err != nil {
		f.Close()

		return nil, fmt.Errorf("acquire %s decoder: %w", codec, err)
	}

	return &CompressedRunReader{
		file:    f,
		release: release,
		records: runRecordDecoder{in: bufio.NewReaderSize(dec, bufferSize)},
		count:   count,
		path:    path,
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

	r.release()
	r.release = nil

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

// acquireRunEncoder returns a pooled codec encoder writing to dst and
// the func that returns it to its pool once it closed cleanly.
func acquireRunEncoder(codec RunCodec, level CompressionLevel, dst io.Writer) (io.WriteCloser, func(), error) {
	if codec == RunCodecS2 {
		s2l := s2LevelFor(level)
		enc := acquireS2Writer(s2l)
		enc.Reset(dst)

		return enc, func() { releaseS2Writer(s2l, enc) }, nil
	}
	zl := zstdLevelFor(level)
	enc, err := acquireZstdEncoder(zl)
	if err != nil {
		return nil, nil, err
	}
	enc.Reset(dst)

	return enc, func() { releaseZstdEncoder(zl, enc) }, nil
}

// acquireRunDecoder returns a pooled codec decoder reading src and the
// func that returns it to its pool.
func acquireRunDecoder(codec RunCodec, src io.Reader) (io.Reader, func(), error) {
	if codec == RunCodecS2 {
		dec := acquireS2Reader()
		dec.Reset(src)

		return dec, func() { releaseS2Reader(dec) }, nil
	}
	dec, err := acquireZstdDecoder()
	if err != nil {
		return nil, nil, err
	}
	if err := dec.Reset(src); err != nil {
		dec.Close()

		return nil, nil, fmt.Errorf("reset zstd decoder: %w", err)
	}

	return dec, func() { releaseZstdDecoder(dec) }, nil
}

func zstdLevelFor(level CompressionLevel) zstd.EncoderLevel {
	switch level {
	case CompressionFastest:
		return zstd.SpeedFastest
	case CompressionBetter:
		return zstd.SpeedBetterCompression
	default:
		return zstd.SpeedDefault
	}
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

// s2Level is an S2 encoder's effort. S2 has no level between its
// default and "better", so CompressionFastest and CompressionDefault
// both take the default.
type s2Level int

const (
	s2LevelFast s2Level = iota
	s2LevelBetter
	numS2Levels
)

func s2LevelFor(level CompressionLevel) s2Level {
	if level == CompressionBetter {
		return s2LevelBetter
	}

	return s2LevelFast
}

// s2BlockSize is each S2 stream's block, the span its matches reach,
// sized like zstdWindowSize. A reader holds one compressed and one
// decoded block: ~2 MiB, within openRunHeapBytes.
const s2BlockSize = 1 << 20

//nolint:gochecknoglobals // intentional package-level encoder pools
var s2WriterPools = [numS2Levels]*typedPool[s2.Writer]{
	newTypedPool(func() *s2.Writer { return nil }),
	newTypedPool(func() *s2.Writer { return nil }),
}

//nolint:gochecknoglobals // intentional package-level decoder pool
var s2ReaderPool = newTypedPool(func() *s2.Reader { return nil })

// acquireS2Writer returns a pooled or new S2 writer. One goroutine per
// stream, as zstdEncoderOptions explains: synchronous encoding holds
// one input and one output block rather than a pair per core.
func acquireS2Writer(level s2Level) *s2.Writer {
	if enc := s2WriterPools[level].Get(); enc != nil {
		return enc
	}
	opts := []s2.WriterOption{
		s2.WriterConcurrency(1),
		s2.WriterBlockSize(s2BlockSize),
	}
	if level == s2LevelBetter {
		opts = append(opts, s2.WriterBetterCompression())
	}

	return s2.NewWriter(nil, opts...)
}

func releaseS2Writer(level s2Level, enc *s2.Writer) {
	enc.Reset(nil)
	s2WriterPools[level].Put(enc)
}

func acquireS2Reader() *s2.Reader {
	if dec := s2ReaderPool.Get(); dec != nil {
		return dec
	}

	return s2.NewReader(nil, s2.ReaderMaxBlockSize(s2BlockSize))
}

func releaseS2Reader(dec *s2.Reader) {
	dec.Reset(nil)
	s2ReaderPool.Put(dec)
}
