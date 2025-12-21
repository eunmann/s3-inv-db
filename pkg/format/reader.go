package format

import (
	"encoding/binary"
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

// OpenMmap opens a file and maps it into memory.
func OpenMmap(path string) (*MmapFile, error) {
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

	return &MmapFile{
		path: path,
		data: data,
		size: size,
	}, nil
}

// Close unmaps the file.
func (m *MmapFile) Close() error {
	if m.data == nil {
		return nil
	}
	if err := unix.Munmap(m.data); err != nil {
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
	header Header
	data   []byte
}

// OpenArray opens a columnar array file.
func OpenArray(path string) (*ArrayReader, error) {
	mmap, err := OpenMmap(path)
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
		return nil, fmt.Errorf("file too small: %d < %d", mmap.Size(), expectedSize)
	}

	return &ArrayReader{
		mmap:   mmap,
		header: header,
		data:   mmap.Data()[HeaderSize:],
	}, nil
}

// Close releases the memory mapping.
func (r *ArrayReader) Close() error {
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

// GetU32 returns the uint32 value at the given index.
func (r *ArrayReader) GetU32(idx uint64) (uint32, error) {
	if idx >= r.header.Count {
		return 0, ErrBoundsCheck
	}
	if r.header.Width != 4 {
		return 0, fmt.Errorf("width mismatch: expected 4, got %d", r.header.Width)
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
		return 0, fmt.Errorf("width mismatch: expected 8, got %d", r.header.Width)
	}
	offset := idx * 8
	return binary.LittleEndian.Uint64(r.data[offset:]), nil
}

// GetU16 returns the uint16 value at the given index.
func (r *ArrayReader) GetU16(idx uint64) (uint16, error) {
	if idx >= r.header.Count {
		return 0, ErrBoundsCheck
	}
	if r.header.Width != 2 {
		return 0, fmt.Errorf("width mismatch: expected 2, got %d", r.header.Width)
	}
	offset := idx * 2
	return binary.LittleEndian.Uint16(r.data[offset:]), nil
}

// UnsafeGetU32 returns the value without bounds checking.
//
// WARNING: This method performs NO bounds checking for performance.
// Passing an idx >= Count() will cause undefined behavior (likely a panic
// or memory corruption). Only use this in hot paths where the caller has
// already validated the index. For safe access, use GetU32 instead.
func (r *ArrayReader) UnsafeGetU32(idx uint64) uint32 {
	return binary.LittleEndian.Uint32(r.data[idx*4:])
}

// UnsafeGetU64 returns the value without bounds checking.
//
// WARNING: This method performs NO bounds checking for performance.
// Passing an idx >= Count() will cause undefined behavior (likely a panic
// or memory corruption). Only use this in hot paths where the caller has
// already validated the index. For safe access, use GetU64 instead.
func (r *ArrayReader) UnsafeGetU64(idx uint64) uint64 {
	return binary.LittleEndian.Uint64(r.data[idx*8:])
}

// UnsafeGetU16 returns the value without bounds checking.
//
// WARNING: This method performs NO bounds checking for performance.
// Passing an idx >= Count() will cause undefined behavior (likely a panic
// or memory corruption). Only use this in hot paths where the caller has
// already validated the index. For safe access, use GetU16 instead.
func (r *ArrayReader) UnsafeGetU16(idx uint64) uint16 {
	return binary.LittleEndian.Uint16(r.data[idx*2:])
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
	err1 := r.blobMmap.Close()
	err2 := r.offsetsMmap.Close()
	if err1 != nil {
		return err1
	}
	return err2
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

// UnsafeGet returns the string without bounds checking.
//
// WARNING: This method performs NO bounds checking for performance.
// Passing an idx >= Count() will cause undefined behavior (likely a panic
// or memory corruption). Only use this in hot paths where the caller has
// already validated the index. For safe access, use Get instead.
func (r *BlobReader) UnsafeGet(idx uint64) string {
	start := r.offsetsMmap.UnsafeGetU64(idx)
	end := r.offsetsMmap.UnsafeGetU64(idx + 1)
	return string(r.blobMmap.Data()[start:end])
}
