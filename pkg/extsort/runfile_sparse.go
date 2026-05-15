package extsort

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"math/bits"
	"os"
)

// Sparse run-file format (variant for benchmark, version 3).
//
// Header (16 bytes): identical to uncompressed run files.
//
// Record (variable):
//   PrefixLen   4 bytes (uint32)
//   Prefix      N bytes
//   Depth       2 bytes
//   Count       8 bytes
//   TotalBytes  8 bytes
//   TierBitmap  2 bytes (bit i set => tier i is present)
//   Per set bit:
//     TierCount 8 bytes
//     TierBytes 8 bytes
//
// For a typical single-tier (STANDARD) row this is
//   4 + N + 2 + 8 + 8 + 2 + 16 = 40 + N
// vs the dense format's 22 + N + 192 = 214 + N — ~5x smaller per row.
const sparseRunFileVersion = 3

// SparseRunFileWriter writes sorted PrefixRows using a per-row tier
// bitmap that skips zero-valued tiers.
type SparseRunFileWriter struct {
	file   *os.File
	writer *bufio.Writer
	count  uint64
	path   string
	buf    []byte
	closed bool
}

func NewSparseRunFileWriter(path string, bufferSize int) (*SparseRunFileWriter, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("create sparse run file: %w", err)
	}
	if bufferSize <= 0 {
		bufferSize = 4 * 1024 * 1024
	}
	w := &SparseRunFileWriter{
		file:   f,
		writer: bufio.NewWriterSize(f, bufferSize),
		path:   path,
		buf:    make([]byte, 1024),
	}
	header := make([]byte, runFileHeader)
	binary.LittleEndian.PutUint32(header[0:4], runFileMagic)
	binary.LittleEndian.PutUint32(header[4:8], sparseRunFileVersion)
	binary.LittleEndian.PutUint64(header[8:16], 0)
	if _, err := w.writer.Write(header); err != nil {
		f.Close()
		os.Remove(path)
		return nil, fmt.Errorf("write header: %w", err)
	}
	return w, nil
}

func (w *SparseRunFileWriter) Write(row *PrefixRow) error {
	// Compute tier bitmap; only tiers with non-zero count OR bytes are emitted.
	var bitmap uint16
	for i := range MaxTiers {
		if row.TierCounts[i] != 0 || row.TierBytes[i] != 0 {
			bitmap |= 1 << uint(i)
		}
	}
	popcount := bits.OnesCount16(bitmap)

	prefixLen := len(row.Prefix)
	recordSize := 4 + prefixLen + 2 + 8 + 8 + 2 + popcount*16
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
	binary.LittleEndian.PutUint16(w.buf[offset:], bitmap)
	offset += 2

	for i := range MaxTiers {
		if bitmap&(1<<uint(i)) == 0 {
			continue
		}
		binary.LittleEndian.PutUint64(w.buf[offset:], row.TierCounts[i])
		offset += 8
		binary.LittleEndian.PutUint64(w.buf[offset:], row.TierBytes[i])
		offset += 8
	}

	if _, err := w.writer.Write(w.buf[:offset]); err != nil {
		return fmt.Errorf("write record: %w", err)
	}
	w.count++
	return nil
}

func (w *SparseRunFileWriter) WriteAll(rows []*PrefixRow) error {
	for _, r := range rows {
		if err := w.Write(r); err != nil {
			return err
		}
	}
	return nil
}

func (w *SparseRunFileWriter) Close() error {
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

// SparseRunFileReader reads PrefixRows from a sparse run file.
type SparseRunFileReader struct {
	file   *os.File
	reader *bufio.Reader
	count  uint64
	read   uint64
	path   string
	buf    []byte
	closed bool
}

func OpenSparseRunFile(path string, bufferSize int) (*SparseRunFileReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open sparse run file: %w", err)
	}
	if bufferSize <= 0 {
		bufferSize = 4 * 1024 * 1024
	}
	r := &SparseRunFileReader{
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
	if binary.LittleEndian.Uint32(header[0:4]) != runFileMagic {
		f.Close()
		return nil, fmt.Errorf("invalid magic")
	}
	if binary.LittleEndian.Uint32(header[4:8]) != sparseRunFileVersion {
		f.Close()
		return nil, fmt.Errorf("unsupported version")
	}
	r.count = binary.LittleEndian.Uint64(header[8:16])
	return r, nil
}

func (r *SparseRunFileReader) Read() (*PrefixRow, error) {
	if r.read >= r.count {
		return nil, io.EOF
	}
	// Read length prefix first.
	var lenBuf [4]byte
	if _, err := io.ReadFull(r.reader, lenBuf[:]); err != nil {
		return nil, fmt.Errorf("read prefix length: %w", err)
	}
	prefixLen := int(binary.LittleEndian.Uint32(lenBuf[:]))

	// Read enough to peek at the bitmap.
	fixedHeadSize := prefixLen + 2 + 8 + 8 + 2
	if len(r.buf) < fixedHeadSize {
		r.buf = make([]byte, fixedHeadSize*2)
	}
	if _, err := io.ReadFull(r.reader, r.buf[:fixedHeadSize]); err != nil {
		return nil, fmt.Errorf("read fixed head: %w", err)
	}

	offset := 0
	row := &PrefixRow{}
	row.Prefix = string(r.buf[offset : offset+prefixLen])
	offset += prefixLen
	row.Depth = binary.LittleEndian.Uint16(r.buf[offset:])
	offset += 2
	row.Count = binary.LittleEndian.Uint64(r.buf[offset:])
	offset += 8
	row.TotalBytes = binary.LittleEndian.Uint64(r.buf[offset:])
	offset += 8
	bitmap := binary.LittleEndian.Uint16(r.buf[offset:])
	popcount := bits.OnesCount16(bitmap)

	if popcount > 0 {
		tierBytes := popcount * 16
		if len(r.buf) < tierBytes {
			r.buf = make([]byte, tierBytes*2)
		}
		if _, err := io.ReadFull(r.reader, r.buf[:tierBytes]); err != nil {
			return nil, fmt.Errorf("read tiers: %w", err)
		}
		toff := 0
		for i := range MaxTiers {
			if bitmap&(1<<uint(i)) == 0 {
				continue
			}
			row.TierCounts[i] = binary.LittleEndian.Uint64(r.buf[toff:])
			toff += 8
			row.TierBytes[i] = binary.LittleEndian.Uint64(r.buf[toff:])
			toff += 8
		}
	}

	r.read++
	return row, nil
}

func (r *SparseRunFileReader) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	return r.file.Close()
}
