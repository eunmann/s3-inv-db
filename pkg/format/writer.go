package format

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
)

// ArrayWriter writes columnar arrays with headers.
type ArrayWriter struct {
	file   *os.File
	writer *bufio.Writer
	count  uint64
	width  uint32
}

// NewArrayWriter creates a writer for a columnar array file.
func NewArrayWriter(path string, width uint32) (*ArrayWriter, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("create array file: %w", err)
	}

	w := bufio.NewWriter(f)

	// Write placeholder header (will be updated on close)
	header := EncodeHeader(Header{
		Magic:   MagicNumber,
		Version: Version,
		Count:   0,
		Width:   width,
	})
	if _, err := w.Write(header); err != nil {
		f.Close()
		os.Remove(path)
		return nil, fmt.Errorf("write header: %w", err)
	}

	return &ArrayWriter{
		file:   f,
		writer: w,
		count:  0,
		width:  width,
	}, nil
}

// WriteU32 writes a uint32 value.
func (w *ArrayWriter) WriteU32(val uint32) error {
	if w.width != 4 {
		return fmt.Errorf("width mismatch: expected 4, got %d", w.width)
	}
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], val)
	if _, err := w.writer.Write(buf[:]); err != nil {
		return fmt.Errorf("write u32: %w", err)
	}
	w.count++
	return nil
}

// WriteU64 writes a uint64 value.
func (w *ArrayWriter) WriteU64(val uint64) error {
	if w.width != 8 {
		return fmt.Errorf("width mismatch: expected 8, got %d", w.width)
	}
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], val)
	if _, err := w.writer.Write(buf[:]); err != nil {
		return fmt.Errorf("write u64: %w", err)
	}
	w.count++
	return nil
}

// WriteU16 writes a uint16 value.
func (w *ArrayWriter) WriteU16(val uint16) error {
	if w.width != 2 {
		return fmt.Errorf("width mismatch: expected 2, got %d", w.width)
	}
	var buf [2]byte
	binary.LittleEndian.PutUint16(buf[:], val)
	if _, err := w.writer.Write(buf[:]); err != nil {
		return fmt.Errorf("write u16: %w", err)
	}
	w.count++
	return nil
}

// Close flushes, updates the header with the correct count, and closes.
func (w *ArrayWriter) Close() error {
	if err := w.writer.Flush(); err != nil {
		w.file.Close()
		return fmt.Errorf("flush: %w", err)
	}

	// Seek back and update header with correct count
	if _, err := w.file.Seek(0, 0); err != nil {
		w.file.Close()
		return fmt.Errorf("seek: %w", err)
	}

	header := EncodeHeader(Header{
		Magic:   MagicNumber,
		Version: Version,
		Count:   w.count,
		Width:   w.width,
	})
	if _, err := w.file.Write(header); err != nil {
		w.file.Close()
		return fmt.Errorf("update header: %w", err)
	}

	if err := w.file.Close(); err != nil {
		return fmt.Errorf("close file: %w", err)
	}
	return nil
}

// Count returns the number of elements written.
func (w *ArrayWriter) Count() uint64 {
	return w.count
}

// BlobWriter writes a prefix blob with offsets.
type BlobWriter struct {
	blobFile   *os.File
	blobWriter *bufio.Writer
	offsets    *ArrayWriter
	offset     uint64
}

// NewBlobWriter creates a writer for prefix strings.
func NewBlobWriter(blobPath, offsetsPath string) (*BlobWriter, error) {
	blobFile, err := os.Create(blobPath)
	if err != nil {
		return nil, fmt.Errorf("create blob file: %w", err)
	}

	offsets, err := NewArrayWriter(offsetsPath, 8)
	if err != nil {
		blobFile.Close()
		os.Remove(blobPath)
		return nil, fmt.Errorf("create offsets: %w", err)
	}

	return &BlobWriter{
		blobFile:   blobFile,
		blobWriter: bufio.NewWriter(blobFile),
		offsets:    offsets,
		offset:     0,
	}, nil
}

// WriteString writes a prefix string and records its offset.
func (w *BlobWriter) WriteString(s string) error {
	// Write offset first
	if err := w.offsets.WriteU64(w.offset); err != nil {
		return fmt.Errorf("write offset: %w", err)
	}

	// Write string bytes
	n, err := w.blobWriter.WriteString(s)
	if err != nil {
		return fmt.Errorf("write string: %w", err)
	}
	w.offset += uint64(n)

	return nil
}

// Close finalizes both files, writing a sentinel offset.
func (w *BlobWriter) Close() error {
	// Write sentinel offset (points past end)
	if err := w.offsets.WriteU64(w.offset); err != nil {
		w.blobWriter.Flush()
		w.blobFile.Close()
		w.offsets.Close()
		return fmt.Errorf("write sentinel offset: %w", err)
	}

	if err := w.blobWriter.Flush(); err != nil {
		w.blobFile.Close()
		w.offsets.Close()
		return fmt.Errorf("flush blob: %w", err)
	}

	if err := w.blobFile.Close(); err != nil {
		w.offsets.Close()
		return fmt.Errorf("close blob: %w", err)
	}

	if err := w.offsets.Close(); err != nil {
		return fmt.Errorf("close offsets: %w", err)
	}
	return nil
}

// Count returns the number of strings written.
func (w *BlobWriter) Count() uint64 {
	// Offsets count is N+1 due to sentinel
	if w.offsets.Count() == 0 {
		return 0
	}
	return w.offsets.Count() - 1
}
