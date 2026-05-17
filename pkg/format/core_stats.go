package format

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

// CoreStatsFile is the row-major per-prefix core stats file.
const CoreStatsFile = "core_stats.bin"

// CoreStatsRowStride is the fixed byte stride per prefix row.
// Layout (little-endian):
//
//	offset  0: object_count           uint64
//	offset  8: total_bytes            uint64
//	offset 16: subtree_end            uint64
//	offset 24: depth                  uint16
//	offset 26: max_depth_in_subtree   uint16
const CoreStatsRowStride = 28

const (
	coreStatsOffObjectCount = 0
	coreStatsOffTotalBytes  = 8
	coreStatsOffSubtreeEnd  = 16
	coreStatsOffDepth       = 24
	coreStatsOffMaxDepth    = 26
)

// Sentinel errors for err113 / linter.
var (
	errCoreStatsRowPosOOB = errors.New("core stats row position out of range")
	errCoreStatsRowWidth  = errors.New("core stats row width mismatch")
)

// CoreStatsBuilder writes the row-major core stats file in-place
// during a build. Each prefix gets one row at its preorder position:
// object_count + total_bytes + depth are known at Add() time;
// subtree_end + max_depth_in_subtree are populated later when the
// subtree closes (out-of-preorder random writes).
//
// Backed by a single mmap'd file at the final output path. The file
// grows by ftruncate+remap when the capacity hint is exceeded, just
// like u64RandomDiskArray. At Finalize the header is rewritten with
// the actual row count and the mmap is msync'd to disk.
type CoreStatsBuilder struct {
	path    string
	file    *os.File
	data    []byte // mmap region (PROT_READ|PROT_WRITE)
	dataOff int    // == HeaderSize; first row starts here
	size    int64  // current mmap'd byte length
	count   int    // number of rows committed (Add calls)
}

// NewCoreStatsBuilder creates a core stats file at outDir/core_stats.bin
// sized to fit capacity rows (grown on demand if exceeded). The header
// is written immediately so the file is valid mid-build.
func NewCoreStatsBuilder(outDir string, capacity uint64) (*CoreStatsBuilder, error) {
	path := filepath.Join(outDir, CoreStatsFile)
	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("create core stats file: %w", err)
	}
	b := &CoreStatsBuilder{
		path:    path,
		file:    f,
		dataOff: HeaderSize,
	}
	initial := max(int64(HeaderSize)+int64(capacity)*int64(CoreStatsRowStride), int64(unix.Getpagesize()))
	if err := b.grow(initial); err != nil {
		_ = b.Close()
		os.Remove(path)

		return nil, err
	}
	// Write placeholder header at offset 0; final Count is patched
	// in Close().
	header := EncodeHeader(Header{
		Magic:   MagicNumber,
		Version: Version,
		Count:   0,
		Width:   uint32(CoreStatsRowStride),
	})
	copy(b.data[:HeaderSize], header)

	return b, nil
}

func (b *CoreStatsBuilder) grow(newSize int64) error {
	if newSize <= b.size {
		return nil
	}
	if b.data != nil {
		if err := unix.Munmap(b.data); err != nil {
			return fmt.Errorf("core stats munmap: %w", err)
		}
		b.data = nil
	}
	if err := b.file.Truncate(newSize); err != nil {
		return fmt.Errorf("core stats truncate: %w", err)
	}
	data, err := unix.Mmap(int(b.file.Fd()), 0, int(newSize), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		return fmt.Errorf("core stats mmap: %w", err)
	}
	b.data = data
	b.size = newSize

	return nil
}

// rowSlice returns the byte slice for row idx (must be < b.count).
func (b *CoreStatsBuilder) rowSlice(idx int) []byte {
	start := b.dataOff + idx*CoreStatsRowStride

	return b.data[start : start+CoreStatsRowStride]
}

// Add appends one row for the next prefix position with the fields
// known at Add time. SetSubtree must be called later (during subtree
// close) to fill in subtree_end and max_depth_in_subtree at the same
// row position.
func (b *CoreStatsBuilder) Add(objectCount, totalBytes uint64, depth uint16) error {
	needed := int64(b.dataOff + (b.count+1)*CoreStatsRowStride)
	if needed > b.size {
		newSize := max(b.size*2, needed)
		if err := b.grow(newSize); err != nil {
			return err
		}
	}
	row := b.rowSlice(b.count)
	binary.LittleEndian.PutUint64(row[coreStatsOffObjectCount:], objectCount)
	binary.LittleEndian.PutUint64(row[coreStatsOffTotalBytes:], totalBytes)
	binary.LittleEndian.PutUint16(row[coreStatsOffDepth:], depth)
	// subtree_end (uint64 at offset 16) and max_depth_in_subtree
	// (uint16 at offset 26) stay zero until SetSubtree.
	b.count++

	return nil
}

// SetSubtree fills in subtree_end and max_depth_in_subtree at the
// row for position pos. Pos must be < Count().
func (b *CoreStatsBuilder) SetSubtree(pos, subtreeEnd uint64, maxDepth uint16) error {
	if pos >= uint64(b.count) {
		return fmt.Errorf("%w: pos=%d count=%d", errCoreStatsRowPosOOB, pos, b.count)
	}
	row := b.rowSlice(int(pos))
	binary.LittleEndian.PutUint64(row[coreStatsOffSubtreeEnd:], subtreeEnd)
	binary.LittleEndian.PutUint16(row[coreStatsOffMaxDepth:], maxDepth)

	return nil
}

// Count returns the number of rows committed.
func (b *CoreStatsBuilder) Count() int { return b.count }

// Finalize patches the final row count into the header, truncates
// the file to the exact byte length, msyncs, and closes. The file
// remains at its final path (no temp-rename dance) — it was already
// mmap'd to its final location during the build.
func (b *CoreStatsBuilder) Finalize() error {
	finalSize := int64(b.dataOff + b.count*CoreStatsRowStride)
	header := EncodeHeader(Header{
		Magic:   MagicNumber,
		Version: Version,
		Count:   uint64(b.count),
		Width:   uint32(CoreStatsRowStride),
	})
	copy(b.data[:HeaderSize], header)
	if err := unix.Msync(b.data, unix.MS_SYNC); err != nil {
		return fmt.Errorf("core stats msync: %w", err)
	}
	if err := unix.Munmap(b.data); err != nil {
		return fmt.Errorf("core stats munmap: %w", err)
	}
	b.data = nil
	if err := b.file.Truncate(finalSize); err != nil {
		return fmt.Errorf("core stats final truncate: %w", err)
	}
	if err := b.file.Close(); err != nil {
		return fmt.Errorf("core stats close: %w", err)
	}
	b.file = nil

	return nil
}

// Close releases the mmap and closes the file without finalizing.
// Used for error paths; the caller should remove the file separately.
func (b *CoreStatsBuilder) Close() error {
	var firstErr error
	if b.data != nil {
		if err := unix.Munmap(b.data); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("core stats munmap: %w", err)
		}
		b.data = nil
	}
	if b.file != nil {
		if err := b.file.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("core stats close: %w", err)
		}
		b.file = nil
	}

	return firstErr
}

// CoreStatsReader reads the row-major core stats file. One mmap'd
// region; per-prefix calls (Stats, Depth, SubtreeEnd, MaxDepth) all
// hit the same 28-byte row, so a single page covers ~146 prefixes'
// worth of data on a 4 KiB page.
type CoreStatsReader struct {
	mmap     *MmapFile
	dataOff  int64
	rowCount uint64
	stride   int
}

// OpenCoreStats opens the row-major core stats file. Returns
// (nil, nil) when the file is absent — callers fall back to the
// legacy per-column ArrayReader path in that case (back-compat with
// indexes built before the row-major layout existed).
func OpenCoreStats(indexDir string) (*CoreStatsReader, error) {
	path := filepath.Join(indexDir, CoreStatsFile)
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			return nil, nil //nolint:nilnil // legacy callers fall back to per-column reader
		}

		return nil, fmt.Errorf("stat core stats: %w", err)
	}
	// core_stats.bin serves both random-access (StatsForPrefix on
	// individual prefixes from HTTP requests) AND sequential-access
	// (subtree walks, browse children). MADV_RANDOM disables kernel
	// readahead and hurts sequential walks; MADV_SEQUENTIAL hurts
	// random per-prefix lookups. Leave the hint as default so the
	// kernel adapts to the observed access pattern.
	mmap, err := OpenMmapWithHint(path, AccessHintNone)
	if err != nil {
		return nil, fmt.Errorf("open core stats mmap: %w", err)
	}
	if mmap.Size() < int64(HeaderSize) {
		mmap.Close()

		return nil, fmt.Errorf("core stats: %w", ErrInvalidHeader)
	}
	header, err := DecodeHeader(mmap.Data()[:HeaderSize])
	if err != nil {
		mmap.Close()

		return nil, fmt.Errorf("decode core stats header: %w", err)
	}
	if header.Magic != MagicNumber {
		mmap.Close()

		return nil, ErrMagicMismatch
	}
	if header.Version != Version {
		mmap.Close()

		return nil, ErrVersionMismatch
	}
	stride := int(header.Width)
	if stride != CoreStatsRowStride {
		mmap.Close()

		return nil, fmt.Errorf("%w: got %d expected %d", errCoreStatsRowWidth, stride, CoreStatsRowStride)
	}
	expected := int64(HeaderSize) + int64(header.Count)*int64(stride)
	if mmap.Size() < expected {
		mmap.Close()

		return nil, fmt.Errorf("%w: %d < %d", ErrFileTooSmall, mmap.Size(), expected)
	}

	return &CoreStatsReader{
		mmap:     mmap,
		dataOff:  int64(HeaderSize),
		rowCount: header.Count,
		stride:   stride,
	}, nil
}

// Count returns the number of rows.
func (r *CoreStatsReader) Count() uint64 { return r.rowCount }

// rowSlice returns the byte slice for row idx; no bounds check —
// hot-path callers validate idx separately.
func (r *CoreStatsReader) rowSlice(idx uint64) []byte {
	start := r.dataOff + int64(idx)*int64(r.stride)

	return r.mmap.Data()[start : start+int64(r.stride)]
}

// UnsafeObjectCount returns the object count at pos. No bounds check.
func (r *CoreStatsReader) UnsafeObjectCount(pos uint64) uint64 {
	return binary.LittleEndian.Uint64(r.rowSlice(pos)[coreStatsOffObjectCount:])
}

// UnsafeTotalBytes returns the total bytes at pos. No bounds check.
func (r *CoreStatsReader) UnsafeTotalBytes(pos uint64) uint64 {
	return binary.LittleEndian.Uint64(r.rowSlice(pos)[coreStatsOffTotalBytes:])
}

// UnsafeSubtreeEnd returns the subtree end position at pos. No bounds check.
func (r *CoreStatsReader) UnsafeSubtreeEnd(pos uint64) uint64 {
	return binary.LittleEndian.Uint64(r.rowSlice(pos)[coreStatsOffSubtreeEnd:])
}

// UnsafeDepth returns the depth at pos as uint32. No bounds check.
func (r *CoreStatsReader) UnsafeDepth(pos uint64) uint32 {
	return uint32(binary.LittleEndian.Uint16(r.rowSlice(pos)[coreStatsOffDepth:]))
}

// UnsafeMaxDepth returns the max depth in subtree at pos as uint32.
// No bounds check.
func (r *CoreStatsReader) UnsafeMaxDepth(pos uint64) uint32 {
	return uint32(binary.LittleEndian.Uint16(r.rowSlice(pos)[coreStatsOffMaxDepth:]))
}

// UnsafeStats returns (objectCount, totalBytes) at pos in one row
// read — the hottest StatsForPrefix path.
//
//nolint:gocritic // gocritic wants named returns, nonamedreturns forbids them
func (r *CoreStatsReader) UnsafeStats(pos uint64) (uint64, uint64) {
	row := r.rowSlice(pos)

	return binary.LittleEndian.Uint64(row[coreStatsOffObjectCount:]),
		binary.LittleEndian.Uint64(row[coreStatsOffTotalBytes:])
}

// Close releases the mmap.
func (r *CoreStatsReader) Close() error {
	if r == nil || r.mmap == nil {
		return nil
	}
	err := r.mmap.Close()
	r.mmap = nil

	return err
}
