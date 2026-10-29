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
	file   *hashedFile
	writer *bufio.Writer
	path   string
	count  uint64
	width  uint32
}

// NewArrayWriter creates a writer for a columnar array file, whose
// checksum it records in sums.
func NewArrayWriter(path string, width uint32, sums *Checksums) (*ArrayWriter, error) {
	f, err := createHashedFile(path, sums)
	if err != nil {
		return nil, fmt.Errorf("create array file: %w", err)
	}
	// The values start after the header, which Close writes with the count.
	if err := f.seek(HeaderSize); err != nil {
		return nil, errors.Join(fmt.Errorf("skip header: %w", err), f.Close(), removeIfErr(path))
	}

	return &ArrayWriter{
		file:   f,
		writer: bufio.NewWriter(f),
		path:   path,
		count:  0,
		width:  width,
	}, nil
}

// WriteU64 writes a uint64 value.
func (w *ArrayWriter) WriteU64(val uint64) error {
	if w.width != 8 {
		return fmt.Errorf("%w: expected 8, got %d", ErrWidthMismatch, w.width)
	}
	if err := putU64(w.writer, val); err != nil {
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
	header := EncodeHeader(Header{
		Magic:   MagicNumber,
		Version: Version,
		Count:   w.count,
		Width:   w.width,
	})
	if _, err := w.file.WriteAt(header, 0); err != nil {
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

// arrayTailPad is the number of zero bytes a width<8 (or width<4 for
// u32 readers) array carries after the last element so a single LE
// load + mask is always in bounds at the last index. 8 bytes is the
// worst case (a u64 read on a width=1 array).
const arrayTailPad = 8

// repackBufferSize is the bufio.Writer buffer used during the
// streaming repack passes in this package. 1 MiB is large enough
// to amortize syscalls without being conspicuous in RSS.
const repackBufferSize = 1 << 20

// writeBufSize is the buffered-writer size of the package's final-file
// writers: core stats, tier stats, prefix tree, core patches and ranked
// bits.
const writeBufSize = 1 << 20

// RepackArrayWidthU64 streams srcPath (an ArrayWriter file at any
// width 1..8) into dstPath at the chosen tight width, then writes
// arrayTailPad zero bytes so a u64 mask-load at the last element is
// safe. If srcPath == dstPath the source is overwritten atomically
// via dstPath+".tmp" + rename. The checksum of dstPath is recorded in
// sums.
func RepackArrayWidthU64(srcPath, dstPath string, width uint8, sums *Checksums) error {
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
	dst, err := createHashedFile(tmp, sums)
	if err != nil {
		return fmt.Errorf("create dst: %w", err)
	}
	cleanup := func(primary error) error {
		return errors.Join(primary, dst.Close(), removeIfErr(tmp))
	}
	if _, err := dst.Write(EncodeHeader(Header{
		Magic:   MagicNumber,
		Version: Version,
		Count:   count,
		Width:   uint32(width),
	})); err != nil {
		return cleanup(fmt.Errorf("write header: %w", err))
	}

	bw := bufio.NewWriterSize(dst, repackBufferSize)
	var buf [8]byte
	for i := range count {
		binary.LittleEndian.PutUint64(buf[:], src.UnsafeGetU64(i))
		if _, err := bw.Write(buf[:width]); err != nil {
			return cleanup(fmt.Errorf("write u64: %w", err))
		}
	}
	pad := [arrayTailPad]byte{}
	if _, err := bw.Write(pad[:]); err != nil {
		return cleanup(fmt.Errorf("write tail pad: %w", err))
	}
	if err := bw.Flush(); err != nil {
		return cleanup(fmt.Errorf("flush: %w", err))
	}
	dst.startWriteback()
	if err := dst.Close(); err != nil {
		return errors.Join(fmt.Errorf("close: %w", err), removeIfErr(tmp))
	}
	if tmp != dstPath {
		if err := os.Rename(tmp, dstPath); err != nil {
			return errors.Join(fmt.Errorf("rename: %w", err), removeIfErr(tmp))
		}
		sums.move(tmp, dstPath)
	}

	return nil
}

// BlobWriter writes a prefix blob with offsets.
type BlobWriter struct {
	blobFile   *hashedFile
	blobWriter *bufio.Writer
	offsets    *ArrayWriter
	sums       *Checksums
	offset     uint64
}

// NewBlobWriter creates a writer for prefix strings, whose files'
// checksums it records in sums.
func NewBlobWriter(blobPath, offsetsPath string, sums *Checksums) (*BlobWriter, error) {
	blobFile, err := createHashedFile(blobPath, sums)
	if err != nil {
		return nil, fmt.Errorf("create blob file: %w", err)
	}

	// Close repacks the offsets, so only the repacked file is hashed.
	offsets, err := NewArrayWriter(offsetsPath, 8, nil)
	if err != nil {
		blobFile.Close()
		os.Remove(blobPath)

		return nil, fmt.Errorf("create offsets: %w", err)
	}

	return &BlobWriter{
		blobFile:   blobFile,
		blobWriter: bufio.NewWriter(blobFile),
		offsets:    offsets,
		sums:       sums,
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

// Close finalizes both files, writing a sentinel offset. Secondary
// close/flush errors during cleanup are joined into the returned chain
// so failures aren't silently dropped.
func (w *BlobWriter) Close() error {
	if err := w.offsets.WriteU64(w.offset); err != nil {
		return errors.Join(
			fmt.Errorf("write sentinel offset: %w", err),
			w.blobWriter.Flush(),
			w.blobFile.Close(),
			w.offsets.Close(),
		)
	}
	if err := w.blobWriter.Flush(); err != nil {
		return errors.Join(
			fmt.Errorf("flush blob: %w", err),
			w.blobFile.Close(),
			w.offsets.Close(),
		)
	}
	w.blobFile.startWriteback()
	if err := w.blobFile.Close(); err != nil {
		return errors.Join(fmt.Errorf("close blob: %w", err), w.offsets.Close())
	}
	if err := w.offsets.Close(); err != nil {
		return fmt.Errorf("close offsets: %w", err)
	}
	// The offsets file is written 8 bytes wide; keep the bytes the blob
	// size needs, as the other offset arrays do.
	if err := RepackArrayWidthU64(w.offsets.path, w.offsets.path, byteWidthOf(w.offset), w.sums); err != nil {
		return fmt.Errorf("repack offsets: %w", err)
	}

	return nil
}
