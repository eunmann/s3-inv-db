package format

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
)

// ArrayWriter writes columnar arrays with headers.
type ArrayWriter struct {
	file   *os.File
	writer *bufio.Writer
	path   string
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
		_ = f.Close()
		_ = os.Remove(path)

		return nil, fmt.Errorf("write header: %w", err)
	}

	return &ArrayWriter{
		file:   f,
		writer: w,
		path:   path,
		count:  0,
		width:  width,
	}, nil
}

// WriteU32 writes a uint32 value. Requires the array to have been
// constructed with width=4.
func (w *ArrayWriter) WriteU32(val uint32) error {
	if w.width != 4 {
		return fmt.Errorf("%w: expected 4, got %d", ErrWidthMismatch, w.width)
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
		return fmt.Errorf("%w: expected 8, got %d", ErrWidthMismatch, w.width)
	}
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], val)
	if _, err := w.writer.Write(buf[:]); err != nil {
		return fmt.Errorf("write u64: %w", err)
	}
	w.count++

	return nil
}

// Close flushes, updates the header with the correct count, and closes.
// On any error path the partial file is removed so the failure leaves
// nothing on disk for a later mmap/open to misinterpret as valid data.
func (w *ArrayWriter) Close() error {
	if err := w.writer.Flush(); err != nil {
		return w.cleanupOnErr(fmt.Errorf("flush: %w", err))
	}
	if _, err := w.file.Seek(0, 0); err != nil {
		return w.cleanupOnErr(fmt.Errorf("seek: %w", err))
	}
	header := EncodeHeader(Header{
		Magic:   MagicNumber,
		Version: Version,
		Count:   w.count,
		Width:   w.width,
	})
	if _, err := w.file.Write(header); err != nil {
		return w.cleanupOnErr(fmt.Errorf("update header: %w", err))
	}
	if err := w.file.Close(); err != nil {
		return errors.Join(fmt.Errorf("close file: %w", err), removeIfErr(w.path))
	}

	return nil
}

// cleanupOnErr closes the file and removes the partial output on
// the failure path, joining any secondary errors into the returned
// chain so munmap/remove failures aren't silently dropped.
func (w *ArrayWriter) cleanupOnErr(primary error) error {
	closeErr := w.file.Close()
	removeErr := os.Remove(w.path)

	return errors.Join(primary, closeErr, removeErr)
}

// removeIfErr removes a file and returns the error wrapped, or nil if
// the path didn't exist. Used after a fatal Close error so the partial
// output doesn't linger on disk.
func removeIfErr(path string) error {
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove partial %s: %w", path, err)
	}

	return nil
}

// Count returns the number of elements written.
func (w *ArrayWriter) Count() uint64 {
	return w.count
}

// arrayTailPad is the number of zero bytes a width<8 (or width<4 for
// u32 readers) array carries after the last element so a single LE
// load + mask is always in bounds at the last index. 8 bytes is the
// worst case (a u64 read on a width=1 array).
const arrayTailPad = 8

// repackBufferSize is the bufio.Writer buffer used during the
// streaming repack passes in this package. 1 MiB is large enough
// to amortize syscalls without being conspicuous in RSS.
const repackBufferSize = 1 << 20

// RepackArrayWidthU64 streams srcPath (an ArrayWriter file at any
// width 1..8) into dstPath at the chosen tight width, then writes
// arrayTailPad zero bytes so a u64 mask-load at the last element is
// safe. If srcPath == dstPath the source is overwritten atomically
// via dstPath+".tmp" + rename.
func RepackArrayWidthU64(srcPath, dstPath string, width uint8) error {
	if width < 1 || width > 8 {
		return fmt.Errorf("%w: width=%d", ErrWidthMismatch, width)
	}
	src, err := OpenArray(srcPath)
	if err != nil {
		return fmt.Errorf("open scratch: %w", err)
	}
	defer src.Close()
	count := src.Count()

	tmp := dstPath
	if srcPath == dstPath {
		tmp = dstPath + ".tmp"
	}
	dst, err := os.Create(tmp)
	if err != nil {
		return fmt.Errorf("create dst: %w", err)
	}
	if _, err := dst.Write(EncodeHeader(Header{
		Magic:   MagicNumber,
		Version: Version,
		Count:   count,
		Width:   uint32(width),
	})); err != nil {
		_ = dst.Close()
		_ = os.Remove(tmp)

		return fmt.Errorf("write header: %w", err)
	}

	bw := bufio.NewWriterSize(dst, repackBufferSize)
	var buf [8]byte
	for i := range count {
		binary.LittleEndian.PutUint64(buf[:], src.UnsafeGetU64(i))
		if _, err := bw.Write(buf[:width]); err != nil {
			_ = dst.Close()
			_ = os.Remove(tmp)

			return fmt.Errorf("write u64: %w", err)
		}
	}
	pad := [arrayTailPad]byte{}
	if _, err := bw.Write(pad[:]); err != nil {
		_ = dst.Close()
		_ = os.Remove(tmp)

		return fmt.Errorf("write tail pad: %w", err)
	}
	if err := bw.Flush(); err != nil {
		_ = dst.Close()
		_ = os.Remove(tmp)

		return fmt.Errorf("flush: %w", err)
	}
	if err := dst.Sync(); err != nil {
		_ = dst.Close()
		_ = os.Remove(tmp)

		return fmt.Errorf("sync: %w", err)
	}
	if err := dst.Close(); err != nil {
		_ = os.Remove(tmp)

		return fmt.Errorf("close: %w", err)
	}
	if tmp != dstPath {
		if err := os.Rename(tmp, dstPath); err != nil {
			_ = os.Remove(tmp)

			return fmt.Errorf("rename: %w", err)
		}
	}

	return nil
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

// WriteBytes writes prefix bytes and records its offset.
// This avoids the string conversion overhead of WriteString.
func (w *BlobWriter) WriteBytes(b []byte) error {
	// Write offset first
	if err := w.offsets.WriteU64(w.offset); err != nil {
		return fmt.Errorf("write offset: %w", err)
	}

	// Write bytes
	n, err := w.blobWriter.Write(b)
	if err != nil {
		return fmt.Errorf("write bytes: %w", err)
	}
	w.offset += uint64(n)

	return nil
}

// Close finalizes both files, writing a sentinel offset. On the
// error path the cleanup calls are best-effort — we already have a
// fatal error to return, so secondary close/flush failures during
// cleanup are intentionally discarded via the leading underscores.
func (w *BlobWriter) Close() error {
	// Write sentinel offset (points past end)
	if err := w.offsets.WriteU64(w.offset); err != nil {
		_ = w.blobWriter.Flush()
		_ = w.blobFile.Close()
		_ = w.offsets.Close()

		return fmt.Errorf("write sentinel offset: %w", err)
	}

	if err := w.blobWriter.Flush(); err != nil {
		_ = w.blobFile.Close()
		_ = w.offsets.Close()

		return fmt.Errorf("flush blob: %w", err)
	}

	if err := w.blobFile.Close(); err != nil {
		_ = w.offsets.Close()

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
