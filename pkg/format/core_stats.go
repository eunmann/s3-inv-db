package format

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

// CoreStatsFile is the row-major per-prefix core stats file.
const CoreStatsFile = "core_stats.bin"

// coreStatsBuildStride is the fixed safe stride used during the
// build, before per-field widths are known. At Finalize the file is
// repacked to a smaller stride if observed maxes allow.
//
//	offset  0: object_count           uint64
//	offset  8: total_bytes            uint64
//	offset 16: subtree_end            uint64
//	offset 24: depth                  uint16
//	offset 26: max_depth_in_subtree   uint16
const coreStatsBuildStride = 28

const (
	bldOffCount      = 0
	bldOffBytes      = 8
	bldOffSubtreeEnd = 16
	bldOffDepth      = 24
	bldOffMaxDepth   = 26
)

// CoreStatsStride is the per-index field-width schema decided at
// Finalize from the observed value ranges. Each field stores the
// number of bytes that field occupies in the row. Fields are written
// in this fixed order; field offset in a row is the running sum.
type CoreStatsStride struct {
	Count      uint8 // 1..8
	Bytes      uint8 // 1..8
	SubtreeEnd uint8 // 1..8
	Depth      uint8 // 1..2
	MaxDepth   uint8 // 1..2
}

// CoreStatsStrideBytes is the on-disk size of the encoded stride
// descriptor that follows the file Header.
const CoreStatsStrideBytes = 5

// RowBytes is the total width of a row at this stride.
func (s CoreStatsStride) RowBytes() int {
	return int(s.Count) + int(s.Bytes) + int(s.SubtreeEnd) + int(s.Depth) + int(s.MaxDepth)
}

// Encode serialises the descriptor to 5 raw bytes.
func (s CoreStatsStride) Encode() [CoreStatsStrideBytes]byte {
	return [CoreStatsStrideBytes]byte{s.Count, s.Bytes, s.SubtreeEnd, s.Depth, s.MaxDepth}
}

// DecodeCoreStatsStride parses the 5-byte descriptor.
func DecodeCoreStatsStride(buf []byte) (CoreStatsStride, error) {
	if len(buf) < CoreStatsStrideBytes {
		return CoreStatsStride{}, ErrInvalidHeader
	}
	s := CoreStatsStride{Count: buf[0], Bytes: buf[1], SubtreeEnd: buf[2], Depth: buf[3], MaxDepth: buf[4]}
	if err := s.validate(); err != nil {
		return CoreStatsStride{}, err
	}

	return s, nil
}

func (s CoreStatsStride) validate() error {
	if s.Count < 1 || s.Count > 8 ||
		s.Bytes < 1 || s.Bytes > 8 ||
		s.SubtreeEnd < 1 || s.SubtreeEnd > 8 ||
		s.Depth < 1 || s.Depth > 2 ||
		s.MaxDepth < 1 || s.MaxDepth > 2 {
		return errCoreStatsStrideInvalid
	}

	return nil
}

// byteWidthOf returns the smallest n in [1,8] such that v < 2^(8n).
// For v==0 returns 1. The numeric returns are the byte widths
// themselves — disable the magic-number check on this function only.
//
//nolint:mnd // each return is the literal byte width the case picks
func byteWidthOf(v uint64) uint8 {
	switch {
	case v < 1<<8:
		return 1
	case v < 1<<16:
		return 2
	case v < 1<<24:
		return 3
	case v < 1<<32:
		return 4
	case v < 1<<40:
		return 5
	case v < 1<<48:
		return 6
	case v < 1<<56:
		return 7
	default:
		return 8
	}
}

// widthMask masks off bytes above width n. Used by the reader to
// extract a uint64 from a wider load. Const-like lookup table; Go
// doesn't allow const arrays so it lives as a package-level var.
//
//nolint:gochecknoglobals // immutable lookup table
var widthMask = [9]uint64{
	0,
	0xFF,
	0xFFFF,
	0xFFFFFF,
	0xFFFFFFFF,
	0xFFFFFFFFFF,
	0xFFFFFFFFFFFF,
	0xFFFFFFFFFFFFFF,
	0xFFFFFFFFFFFFFFFF,
}

// coreStatsReadTailPad is the number of zero bytes appended after
// the last row so the reader can do a single 8-byte LE load + mask
// even for narrow fields on the last row without faulting past the
// mmap.
const coreStatsReadTailPad = 8

// Sentinel errors for err113 / linter.
var (
	errCoreStatsRowPosOOB     = errors.New("core stats row position out of range")
	errCoreStatsRowWidth      = errors.New("core stats row width mismatch")
	errCoreStatsStrideInvalid = errors.New("core stats stride descriptor invalid")
)

// CoreStatsBuilder writes the row-major core stats file in-place
// during a build. Each prefix gets one row at its preorder position:
// object_count + total_bytes + depth are known at Add() time;
// subtree_end + max_depth_in_subtree are populated later when the
// subtree closes (out-of-preorder random writes).
//
// During the build the file uses a safe-wide stride
// (coreStatsBuildStride); at Finalize the observed maxes determine a
// tight per-field schema and the rows are repacked into the final
// file with a CoreStatsStride descriptor recorded after the Header.
type CoreStatsBuilder struct {
	path    string
	file    *os.File
	data    []byte // mmap region (PROT_READ|PROT_WRITE)
	dataOff int    // == HeaderSize; first row starts here
	size    int64  // current mmap'd byte length
	count   int    // number of rows committed (Add calls)

	// Observed maxes — drive the tight per-field schema at Finalize.
	maxCount      uint64
	maxBytes      uint64
	maxSubtreeEnd uint64
	maxDepth      uint16
	maxMaxDepth   uint16
}

// NewCoreStatsBuilder creates a core stats file at outDir/core_stats.bin
// sized to fit capacity rows (grown on demand if exceeded). The header
// is written at the build-stride placeholder; Finalize rewrites it to
// the final tight stride.
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
	initial := max(int64(HeaderSize)+int64(capacity)*int64(coreStatsBuildStride), int64(unix.Getpagesize()))
	if err := b.grow(initial); err != nil {
		_ = b.Close()
		os.Remove(path)

		return nil, err
	}
	header := EncodeHeader(Header{
		Magic:   MagicNumber,
		Version: Version,
		Count:   0,
		Width:   uint32(coreStatsBuildStride),
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

// buildRowSlice returns the byte slice for build-stride row idx
// (must be < b.count).
func (b *CoreStatsBuilder) buildRowSlice(idx int) []byte {
	start := b.dataOff + idx*coreStatsBuildStride

	return b.data[start : start+coreStatsBuildStride]
}

// Add appends one row for the next prefix position with the fields
// known at Add time. SetSubtree must be called later (during subtree
// close) to fill in subtree_end and max_depth_in_subtree.
func (b *CoreStatsBuilder) Add(objectCount, totalBytes uint64, depth uint16) error {
	needed := int64(b.dataOff + (b.count+1)*coreStatsBuildStride)
	if needed > b.size {
		newSize := max(b.size*2, needed)
		if err := b.grow(newSize); err != nil {
			return err
		}
	}
	row := b.buildRowSlice(b.count)
	binary.LittleEndian.PutUint64(row[bldOffCount:], objectCount)
	binary.LittleEndian.PutUint64(row[bldOffBytes:], totalBytes)
	binary.LittleEndian.PutUint16(row[bldOffDepth:], depth)
	// subtree_end and max_depth_in_subtree stay zero until SetSubtree.

	if objectCount > b.maxCount {
		b.maxCount = objectCount
	}
	if totalBytes > b.maxBytes {
		b.maxBytes = totalBytes
	}
	if depth > b.maxDepth {
		b.maxDepth = depth
	}
	b.count++

	return nil
}

// SetSubtree fills in subtree_end and max_depth_in_subtree at the
// row for position pos. Pos must be < Count().
func (b *CoreStatsBuilder) SetSubtree(pos, subtreeEnd uint64, maxDepth uint16) error {
	if pos >= uint64(b.count) {
		return fmt.Errorf("%w: pos=%d count=%d", errCoreStatsRowPosOOB, pos, b.count)
	}
	row := b.buildRowSlice(int(pos))
	binary.LittleEndian.PutUint64(row[bldOffSubtreeEnd:], subtreeEnd)
	binary.LittleEndian.PutUint16(row[bldOffMaxDepth:], maxDepth)

	if subtreeEnd > b.maxSubtreeEnd {
		b.maxSubtreeEnd = subtreeEnd
	}
	if maxDepth > b.maxMaxDepth {
		b.maxMaxDepth = maxDepth
	}

	return nil
}

// Count returns the number of rows committed.
func (b *CoreStatsBuilder) Count() int { return b.count }

// ObservedStride returns the per-field byte widths derived from the
// observed maxes. Exposed for tests + visibility benches.
func (b *CoreStatsBuilder) ObservedStride() CoreStatsStride {
	return CoreStatsStride{
		Count:      byteWidthOf(b.maxCount),
		Bytes:      byteWidthOf(b.maxBytes),
		SubtreeEnd: byteWidthOf(b.maxSubtreeEnd),
		Depth:      byteWidth16(b.maxDepth),
		MaxDepth:   byteWidth16(b.maxMaxDepth),
	}
}

// byteWidth16 returns 1 for v < 256 else 2.
func byteWidth16(v uint16) uint8 {
	if v < 1<<8 {
		return 1
	}

	return 2
}

// Finalize chooses a tight per-field stride from the observed maxes,
// repacks rows into the final file (with the stride descriptor
// written right after the header), msyncs, and closes.
func (b *CoreStatsBuilder) Finalize() error {
	stride := b.ObservedStride()
	rowBytes := stride.RowBytes()

	// Allocate a tight buffer for the repacked data. Memory cost is
	// count * rowBytes plus the read tail pad.
	out := make([]byte, b.count*rowBytes+coreStatsReadTailPad)
	for i := range b.count {
		src := b.buildRowSlice(i)
		dst := out[i*rowBytes : (i+1)*rowBytes]
		repackRow(dst, src, stride)
	}

	// Drop the build-stride file and write the final tight file in
	// its place.
	if err := unix.Msync(b.data, unix.MS_SYNC); err != nil {
		return fmt.Errorf("core stats msync: %w", err)
	}
	if err := unix.Munmap(b.data); err != nil {
		return fmt.Errorf("core stats munmap: %w", err)
	}
	b.data = nil
	if err := b.file.Close(); err != nil {
		return fmt.Errorf("core stats close (build): %w", err)
	}
	b.file = nil
	if err := os.Remove(b.path); err != nil {
		return fmt.Errorf("core stats remove build file: %w", err)
	}

	final, err := os.Create(b.path)
	if err != nil {
		return fmt.Errorf("core stats create final: %w", err)
	}
	header := EncodeHeader(Header{
		Magic:   MagicNumber,
		Version: Version,
		Count:   uint64(b.count),
		Width:   uint32(rowBytes),
	})
	if _, err := final.Write(header); err != nil {
		_ = final.Close()

		return fmt.Errorf("core stats write final header: %w", err)
	}
	desc := stride.Encode()
	if _, err := final.Write(desc[:]); err != nil {
		_ = final.Close()

		return fmt.Errorf("core stats write stride desc: %w", err)
	}
	if _, err := final.Write(out); err != nil {
		_ = final.Close()

		return fmt.Errorf("core stats write rows: %w", err)
	}
	if err := final.Sync(); err != nil {
		_ = final.Close()

		return fmt.Errorf("core stats sync final: %w", err)
	}
	if err := final.Close(); err != nil {
		return fmt.Errorf("core stats close final: %w", err)
	}

	return nil
}

// repackRow narrows the build-stride row into the tight stride.
func repackRow(dst, src []byte, s CoreStatsStride) {
	off := 0
	writeLE(dst[off:], binary.LittleEndian.Uint64(src[bldOffCount:]), int(s.Count))
	off += int(s.Count)
	writeLE(dst[off:], binary.LittleEndian.Uint64(src[bldOffBytes:]), int(s.Bytes))
	off += int(s.Bytes)
	writeLE(dst[off:], binary.LittleEndian.Uint64(src[bldOffSubtreeEnd:]), int(s.SubtreeEnd))
	off += int(s.SubtreeEnd)
	writeLE(dst[off:], uint64(binary.LittleEndian.Uint16(src[bldOffDepth:])), int(s.Depth))
	off += int(s.Depth)
	writeLE(dst[off:], uint64(binary.LittleEndian.Uint16(src[bldOffMaxDepth:])), int(s.MaxDepth))
}

// writeLE writes the low n bytes of v at dst[0..n].
func writeLE(dst []byte, v uint64, n int) {
	for i := range n {
		dst[i] = byte(v >> (8 * i))
	}
}

// Close releases the mmap and closes the file without finalizing.
// Used for error paths; the caller should remove the file separately.
func (b *CoreStatsBuilder) Close() error {
	var errs []error
	if b.data != nil {
		if err := unix.Munmap(b.data); err != nil {
			errs = append(errs, fmt.Errorf("core stats munmap: %w", err))
		}
		b.data = nil
	}
	if b.file != nil {
		if err := b.file.Close(); err != nil {
			errs = append(errs, fmt.Errorf("core stats close: %w", err))
		}
		b.file = nil
	}

	return errors.Join(errs...)
}

// CoreStatsReader reads the row-major core stats file with per-index
// adaptive field widths. Field offsets within a row are precomputed
// at Open so the hot accessors are branch-free LE loads + masks.
type CoreStatsReader struct {
	mmap     *MmapFile
	dataOff  int64
	rowCount uint64
	stride   int
	desc     CoreStatsStride

	offCount      int
	offBytes      int
	offSubtreeEnd int
	offDepth      int
	offMaxDepth   int
}

// OpenCoreStats opens the row-major core stats file. Returns an
// error if the file is missing — every supported index format
// includes core_stats.bin.
func OpenCoreStats(indexDir string) (*CoreStatsReader, error) {
	path := filepath.Join(indexDir, CoreStatsFile)
	mmap, err := OpenMmapWithHint(path, AccessHintNone)
	if err != nil {
		return nil, fmt.Errorf("open core stats mmap: %w", err)
	}
	if mmap.Size() < int64(HeaderSize+CoreStatsStrideBytes) {
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
	desc, err := DecodeCoreStatsStride(mmap.Data()[HeaderSize : HeaderSize+CoreStatsStrideBytes])
	if err != nil {
		mmap.Close()

		return nil, fmt.Errorf("decode core stats stride: %w", err)
	}
	stride := desc.RowBytes()
	if stride != int(header.Width) {
		mmap.Close()

		return nil, fmt.Errorf("%w: header.Width=%d sum=%d", errCoreStatsRowWidth, header.Width, stride)
	}
	expected := int64(HeaderSize+CoreStatsStrideBytes) + int64(header.Count)*int64(stride)
	if mmap.Size() < expected {
		mmap.Close()

		return nil, fmt.Errorf("%w: %d < %d", ErrFileTooSmall, mmap.Size(), expected)
	}

	r := &CoreStatsReader{
		mmap:     mmap,
		dataOff:  int64(HeaderSize + CoreStatsStrideBytes),
		rowCount: header.Count,
		stride:   stride,
		desc:     desc,
	}
	r.offCount = 0
	r.offBytes = int(desc.Count)
	r.offSubtreeEnd = r.offBytes + int(desc.Bytes)
	r.offDepth = r.offSubtreeEnd + int(desc.SubtreeEnd)
	r.offMaxDepth = r.offDepth + int(desc.Depth)

	return r, nil
}

// Count returns the number of rows.
func (r *CoreStatsReader) Count() uint64 { return r.rowCount }

// Stride exposes the per-field width schema used by this index.
func (r *CoreStatsReader) Stride() CoreStatsStride { return r.desc }

// rowOff returns the absolute byte offset for row idx.
func (r *CoreStatsReader) rowOff(idx uint64) int64 {
	return r.dataOff + int64(idx)*int64(r.stride)
}

// readU64 reads `n` little-endian bytes at off as a uint64. The
// file has a coreStatsReadTailPad pad after the last row so a
// single 8-byte load + mask is always safe.
func (r *CoreStatsReader) readU64(off int64, n uint8) uint64 {
	v := binary.LittleEndian.Uint64(r.mmap.Data()[off : off+8])

	return v & widthMask[n]
}

// UnsafeObjectCount returns the object count at pos. No bounds check.
func (r *CoreStatsReader) UnsafeObjectCount(pos uint64) uint64 {
	return r.readU64(r.rowOff(pos)+int64(r.offCount), r.desc.Count)
}

// UnsafeTotalBytes returns the total bytes at pos. No bounds check.
func (r *CoreStatsReader) UnsafeTotalBytes(pos uint64) uint64 {
	return r.readU64(r.rowOff(pos)+int64(r.offBytes), r.desc.Bytes)
}

// UnsafeSubtreeEnd returns the subtree end position at pos. No bounds check.
func (r *CoreStatsReader) UnsafeSubtreeEnd(pos uint64) uint64 {
	return r.readU64(r.rowOff(pos)+int64(r.offSubtreeEnd), r.desc.SubtreeEnd)
}

// UnsafeDepth returns the depth at pos as uint32. No bounds check.
func (r *CoreStatsReader) UnsafeDepth(pos uint64) uint32 {
	return uint32(r.readU64(r.rowOff(pos)+int64(r.offDepth), r.desc.Depth))
}

// UnsafeMaxDepth returns the max depth in subtree at pos as uint32.
// No bounds check.
func (r *CoreStatsReader) UnsafeMaxDepth(pos uint64) uint32 {
	return uint32(r.readU64(r.rowOff(pos)+int64(r.offMaxDepth), r.desc.MaxDepth))
}

// UnsafeStats returns (objectCount, totalBytes) at pos in one row
// read — the hottest StatsForPrefix path.
//
//nolint:gocritic // gocritic wants named returns, nonamedreturns forbids them
func (r *CoreStatsReader) UnsafeStats(pos uint64) (uint64, uint64) {
	base := r.rowOff(pos)
	data := r.mmap.Data()
	cnt := binary.LittleEndian.Uint64(data[base+int64(r.offCount):base+int64(r.offCount)+8]) & widthMask[r.desc.Count]
	bv := binary.LittleEndian.Uint64(data[base+int64(r.offBytes):base+int64(r.offBytes)+8]) & widthMask[r.desc.Bytes]

	return cnt, bv
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

// silence unused import in case io isn't otherwise referenced.
var _ = io.EOF
