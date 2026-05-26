package format

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

// MmapFile represents a memory-mapped file.
type MmapFile struct {
	path string
	data []byte
	size int64
}

// AccessHint advises the kernel about expected access pattern.
// Used by OpenMmapWithHint so callers can split files by access shape:
// MPHF arrays are random-access; depth/columnar arrays are sequential.
type AccessHint int

// Access hint values for OpenMmapWithHint. The default (AccessHintNone)
// matches the kernel's own default behaviour — no madvise call.
const (
	AccessHintNone AccessHint = iota
	AccessHintRandom
	AccessHintSequential
)

// OpenMmap opens a file and maps it into memory with no access hint.
// Equivalent to OpenMmapWithHint(path, AccessHintNone) — preserved for
// callers that don't care to specify a pattern.
func OpenMmap(path string) (*MmapFile, error) {
	return OpenMmapWithHint(path, AccessHintNone)
}

// OpenMmapWithHint opens a file and mmap's it, optionally hinting the
// kernel about the expected access pattern via posix_madvise. Random
// hint is right for MPHF lookup arrays where each query touches one
// page in an arbitrary location; sequential is right for depth /
// columnar / blob scans during iteration. The wrong hint just costs a
// little bit of extra readahead — never correctness.
func OpenMmapWithHint(path string, hint AccessHint) (*MmapFile, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open file: %w", err)
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat file: %w", err)
	}

	size := info.Size()
	if size == 0 {
		return &MmapFile{path: path, data: nil, size: 0}, nil
	}

	data, err := unix.Mmap(int(f.Fd()), 0, int(size), unix.PROT_READ, unix.MAP_SHARED)
	if err != nil {
		return nil, fmt.Errorf("mmap: %w", err)
	}

	switch hint {
	case AccessHintRandom:
		_ = unix.Madvise(data, unix.MADV_RANDOM)
	case AccessHintSequential:
		_ = unix.Madvise(data, unix.MADV_SEQUENTIAL)
	case AccessHintNone:
		// no advice — leave kernel default
	}

	return &MmapFile{
		path: path,
		data: data,
		size: size,
	}, nil
}

// Close unmaps the file. Idempotent.
func (m *MmapFile) Close() error {
	if m.data == nil {
		return nil
	}
	data := m.data
	m.data = nil
	if err := unix.Munmap(data); err != nil {
		return fmt.Errorf("munmap: %w", err)
	}

	return nil
}

// Data returns the raw memory-mapped bytes.
func (m *MmapFile) Data() []byte {
	return m.data
}

// Size returns the file size.
func (m *MmapFile) Size() int64 {
	return m.size
}

// ArrayReader provides read access to a columnar array via mmap.
//
// Thread Safety: ArrayReader is safe for concurrent read access from multiple
// goroutines. All read methods can be called concurrently. Close should only
// be called once, after all read operations have completed.
type ArrayReader struct {
	mmap   *MmapFile
	data   []byte
	header Header
}

// OpenArray opens a columnar array file with no access hint. See
// OpenArrayWithHint for the hinted form.
func OpenArray(path string) (*ArrayReader, error) {
	return OpenArrayWithHint(path, AccessHintNone)
}

// OpenArrayWithHint opens a columnar array file and applies the given
// madvise hint to its mmap region. Use AccessHintRandom for arrays
// indexed by hash position (MPHF fp / pos, per-prefix stats); use
// AccessHintSequential for arrays scanned in ranges (depth posting
// lists, segment dictionary).
func OpenArrayWithHint(path string, hint AccessHint) (*ArrayReader, error) {
	mmap, err := OpenMmapWithHint(path, hint)
	if err != nil {
		return nil, fmt.Errorf("mmap file: %w", err)
	}

	if mmap.Size() < int64(HeaderSize) {
		mmap.Close()

		return nil, ErrInvalidHeader
	}

	header, err := DecodeHeader(mmap.Data()[:HeaderSize])
	if err != nil {
		mmap.Close()

		return nil, fmt.Errorf("decode header: %w", err)
	}

	if header.Magic != MagicNumber {
		mmap.Close()

		return nil, ErrMagicMismatch
	}

	if header.Version != Version {
		mmap.Close()

		return nil, ErrVersionMismatch
	}

	expectedSize := int64(HeaderSize) + int64(header.Count)*int64(header.Width)
	if mmap.Size() < expectedSize {
		mmap.Close()

		return nil, fmt.Errorf("%w: %d < %d", ErrFileTooSmall, mmap.Size(), expectedSize)
	}

	return &ArrayReader{
		mmap:   mmap,
		header: header,
		data:   mmap.Data()[HeaderSize:],
	}, nil
}

// Close releases the memory mapping. Idempotent and nil-safe.
func (r *ArrayReader) Close() error {
	if r == nil || r.mmap == nil {
		return nil
	}

	return r.mmap.Close()
}

// Count returns the number of elements.
func (r *ArrayReader) Count() uint64 {
	return r.header.Count
}

// Width returns the element width in bytes.
func (r *ArrayReader) Width() uint32 {
	return r.header.Width
}

// GetU32 returns the uint32 value at the given index. Requires the
// array to have been written with width=4.
func (r *ArrayReader) GetU32(idx uint64) (uint32, error) {
	if idx >= r.header.Count {
		return 0, ErrBoundsCheck
	}
	if r.header.Width != 4 {
		return 0, fmt.Errorf("%w: expected 4, got %d", ErrWidthMismatch, r.header.Width)
	}
	offset := idx * 4

	return binary.LittleEndian.Uint32(r.data[offset:]), nil
}

// GetU64 returns the uint64 value at the given index.
func (r *ArrayReader) GetU64(idx uint64) (uint64, error) {
	if idx >= r.header.Count {
		return 0, ErrBoundsCheck
	}
	if r.header.Width != 8 {
		return 0, fmt.Errorf("%w: expected 8, got %d", ErrWidthMismatch, r.header.Width)
	}
	offset := idx * 8

	return binary.LittleEndian.Uint64(r.data[offset:]), nil
}

// UnsafeGetU32 returns the value without bounds checking. Caller must
// have validated idx < Count(); out-of-range reads are undefined.
func (r *ArrayReader) UnsafeGetU32(idx uint64) uint32 {
	return binary.LittleEndian.Uint32(r.data[idx*4:])
}

// UnsafeGetU64 returns the value without bounds checking. Caller must
// have validated idx < Count(); out-of-range reads are undefined.
func (r *ArrayReader) UnsafeGetU64(idx uint64) uint64 {
	return binary.LittleEndian.Uint64(r.data[idx*8:])
}

// BlobReader provides read access to prefix strings via mmap.
//
// Thread Safety: BlobReader is safe for concurrent read access from multiple
// goroutines. All read methods can be called concurrently. Close should only
// be called once, after all read operations have completed.
type BlobReader struct {
	blobMmap    *MmapFile
	offsetsMmap *ArrayReader
}

// OpenBlob opens a prefix blob with its offsets file.
func OpenBlob(blobPath, offsetsPath string) (*BlobReader, error) {
	blobMmap, err := OpenMmap(blobPath)
	if err != nil {
		return nil, fmt.Errorf("open blob: %w", err)
	}

	offsets, err := OpenArray(offsetsPath)
	if err != nil {
		blobMmap.Close()

		return nil, fmt.Errorf("open offsets: %w", err)
	}

	return &BlobReader{
		blobMmap:    blobMmap,
		offsetsMmap: offsets,
	}, nil
}

// Close releases resources.
func (r *BlobReader) Close() error {
	return errors.Join(r.blobMmap.Close(), r.offsetsMmap.Close())
}

// Count returns the number of strings (N, not N+1).
func (r *BlobReader) Count() uint64 {
	if r.offsetsMmap.Count() == 0 {
		return 0
	}

	return r.offsetsMmap.Count() - 1
}

// Get returns the string at the given index.
func (r *BlobReader) Get(idx uint64) (string, error) {
	if idx >= r.Count() {
		return "", ErrBoundsCheck
	}

	start, err := r.offsetsMmap.GetU64(idx)
	if err != nil {
		return "", fmt.Errorf("get start offset: %w", err)
	}

	end, err := r.offsetsMmap.GetU64(idx + 1)
	if err != nil {
		return "", fmt.Errorf("get end offset: %w", err)
	}

	if end > uint64(r.blobMmap.Size()) || start > end {
		return "", ErrBoundsCheck
	}

	return string(r.blobMmap.Data()[start:end]), nil
}

// UnsafeGet returns the string at idx without bounds checking. Caller
// must have validated idx < Count(); out-of-range reads are
// undefined.
func (r *BlobReader) UnsafeGet(idx uint64) string {
	start := r.offsetsMmap.UnsafeGetU64(idx)
	end := r.offsetsMmap.UnsafeGetU64(idx + 1)

	return string(r.blobMmap.Data()[start:end])
}
