package format

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
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
//	offset 16: subtree_size           uint64 (subtree_end - pos)
//	offset 24: depth                  uint16
//	offset 26: tier_mask              uint16
const coreStatsBuildStride = 28

const (
	bldOffCount       = 0
	bldOffBytes       = 8
	bldOffSubtreeSize = 16
	bldOffDepth       = 24
	bldOffTierMask    = 26
)

// coreStatsDataOff is where the first row starts in the final file,
// after the Header and the stride descriptor. Rows are packed: padding
// them to cache-line boundaries measured no faster (#51).
const coreStatsDataOff = HeaderSize + CoreStatsStrideBytes

// CoreStatsStride is the per-index field-width schema decided at
// Finalize from the observed value ranges. Each field stores the
// number of bytes that field occupies in the row. Fields are written
// in this fixed order; field offset in a row is the running sum.
type CoreStatsStride struct {
	Count       uint8 // 1..8
	Bytes       uint8 // 1..8
	SubtreeSize uint8 // 1..8; the field holds subtree_end - pos
	Depth       uint8 // 1..2
	// TierMask is 0 when no row has data outside tier slot 0, so the
	// mask is implied (slot 0 for a row with data), else 1..2. Bit i is
	// tier slot i (tiers.json order) holding data in this row.
	TierMask uint8
}

// CoreStatsStrideBytes is the on-disk size of the encoded stride
// descriptor that follows the file Header.
const CoreStatsStrideBytes = 5

// RowBytes is the total width of a row at this stride.
func (s CoreStatsStride) RowBytes() int {
	return int(s.Count) + int(s.Bytes) + int(s.SubtreeSize) + int(s.Depth) + int(s.TierMask)
}

// Encode serialises the descriptor to 5 raw bytes.
func (s CoreStatsStride) Encode() [CoreStatsStrideBytes]byte {
	return [CoreStatsStrideBytes]byte{s.Count, s.Bytes, s.SubtreeSize, s.Depth, s.TierMask}
}

// DecodeCoreStatsStride parses the 5-byte descriptor.
func DecodeCoreStatsStride(buf []byte) (CoreStatsStride, error) {
	if len(buf) < CoreStatsStrideBytes {
		return CoreStatsStride{}, ErrInvalidHeader
	}
	s := CoreStatsStride{Count: buf[0], Bytes: buf[1], SubtreeSize: buf[2], Depth: buf[3], TierMask: buf[4]}
	if err := s.validate(); err != nil {
		return CoreStatsStride{}, err
	}

	return s, nil
}

func (s CoreStatsStride) validate() error {
	if s.Count < 1 || s.Count > 8 ||
		s.Bytes < 1 || s.Bytes > 8 ||
		s.SubtreeSize < 1 || s.SubtreeSize > 8 ||
		s.Depth < 1 || s.Depth > 2 ||
		s.TierMask > 2 {
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
// object_count, total_bytes, depth and tier_mask are known at Add()
// time; subtree_size is filled in later when the subtree closes (an
// out-of-preorder random write).
//
// During the build the file uses a safe-wide stride
// (coreStatsBuildStride); at Finalize the rows' widths determine a
// tight per-field schema (see Finalize) and the rows are repacked into
// the final file with a CoreStatsStride descriptor recorded after the
// Header.
type CoreStatsBuilder struct {
	path    string
	file    *os.File
	data    []byte // mmap region (PROT_READ|PROT_WRITE)
	dataOff int    // == HeaderSize; first row starts here
	size    int64  // current mmap'd byte length
	count   int    // number of rows committed (Add calls)

	// Observed maxes — drive the tight per-field schema at Finalize.
	maxDepth    uint16
	maxTierMask uint16
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
// close) to fill in subtree_size.
func (b *CoreStatsBuilder) Add(objectCount, totalBytes uint64, depth, tierMask uint16) error {
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
	binary.LittleEndian.PutUint16(row[bldOffTierMask:], tierMask)

	b.maxDepth = max(b.maxDepth, depth)
	b.maxTierMask = max(b.maxTierMask, tierMask)
	b.count++

	return nil
}

// SetSubtree records the subtree of the row at position pos as ending
// at subtreeEnd (inclusive; >= pos). Pos must be < Count().
func (b *CoreStatsBuilder) SetSubtree(pos, subtreeEnd uint64) error {
	if pos >= uint64(b.count) || subtreeEnd < pos {
		return fmt.Errorf("%w: pos=%d end=%d count=%d", errCoreStatsRowPosOOB, pos, subtreeEnd, b.count)
	}
	row := b.buildRowSlice(int(pos))
	binary.LittleEndian.PutUint64(row[bldOffSubtreeSize:], subtreeEnd-pos)

	return nil
}

// Count returns the number of rows committed.
func (b *CoreStatsBuilder) Count() int { return b.count }

// chooseStride picks the row widths: the patched fields from the joint
// distribution of the widths each row needs (see choosePatch), depth
// and tier mask from their maxes. It reads every build row once.
func (b *CoreStatsBuilder) chooseStride() (CoreStatsStride, patchChoice) {
	var hist patchHistogram
	var maxCount, maxBytes, maxSize uint64
	for i := range b.count {
		row := b.buildRowSlice(i)
		c, by, sz := binary.LittleEndian.Uint64(row[bldOffCount:]),
			binary.LittleEndian.Uint64(row[bldOffBytes:]),
			binary.LittleEndian.Uint64(row[bldOffSubtreeSize:])
		hist.add(c, by, sz)
		maxCount, maxBytes, maxSize = max(maxCount, c), max(maxBytes, by), max(maxSize, sz)
	}
	// Without exceptions there are no sentinels, so every field can take
	// the plain width of its largest value, all-ones included: one byte
	// narrower than choosePatch allows when a maximum is all ones.
	plain := patchChoice{count: byteWidthOf(maxCount), bytes: byteWidthOf(maxBytes), size: byteWidthOf(maxSize)}
	plain.cost = uint64(b.count) * uint64(plain.count+plain.bytes+plain.size)
	choice := hist.choosePatch(uint64(b.count))
	if choice.exceptions == 0 || plain.cost <= choice.cost {
		choice = plain
	}

	return CoreStatsStride{
		Count:       choice.count,
		Bytes:       choice.bytes,
		SubtreeSize: choice.size,
		Depth:       byteWidth16(b.maxDepth),
		TierMask:    tierMaskWidth(b.maxTierMask),
	}, choice
}

// byteWidth16 returns 1 for v < 256 else 2.
func byteWidth16(v uint16) uint8 {
	if v < 1<<8 {
		return 1
	}

	return 2
}

// tierMaskWidth is 0 when every mask is 0 or slot 0 alone (implied by
// the row having data), else the bytes that hold the largest mask.
func tierMaskWidth(maxMask uint16) uint8 {
	if maxMask <= 1 {
		return 0
	}

	return byteWidth16(maxMask)
}

// Finalize chooses the stride (count, bytes and subtree size from the
// width histogram, with patch files for the rows that don't fit; depth
// and tier mask from their maxes), streams the repacked rows into a
// fresh file at the same path (Header + stride descriptor first), and
// closes both files. Memory
// during the repack is bounded by the bufio buffer (1 MiB) — at the
// project's stated 10B-prefix scale a per-row in-memory buffer would
// be hundreds of GB.
func (b *CoreStatsBuilder) Finalize() error {
	desc, choice := b.chooseStride()
	stride := desc.RowBytes()
	var patch *patchWriter
	if choice.exceptions > 0 {
		var err error
		if patch, err = newPatchWriter(filepath.Dir(b.path)); err != nil {
			return err
		}
	}
	abortPatch := func(err error) error {
		if patch != nil {
			return errors.Join(err, patch.abort())
		}

		return err
	}

	// Write the tight final file to a sibling tmp path, then rename
	// over the build file. We keep the build mmap live throughout the
	// row-streaming loop so each row read is a memory copy from
	// page-cached pages.
	tmp := b.path + ".tmp"
	final, err := os.Create(tmp)
	if err != nil {
		return abortPatch(fmt.Errorf("core stats create tmp: %w", err))
	}
	var head [coreStatsDataOff]byte
	copy(head[:], EncodeHeader(Header{
		Magic:   MagicNumber,
		Version: Version,
		Count:   uint64(b.count),
		Width:   uint32(stride),
	}))
	enc := desc.Encode()
	copy(head[HeaderSize:], enc[:])
	if _, err := final.Write(head[:]); err != nil {
		return abortPatch(cleanupTmp(final, tmp, fmt.Errorf("write header: %w", err)))
	}

	bw := bufio.NewWriterSize(final, repackBufferSize)
	rowBuf := make([]byte, stride)
	for i := range b.count {
		exception := repackRow(rowBuf, b.buildRowSlice(i), desc)
		if _, err := bw.Write(rowBuf); err != nil {
			return abortPatch(cleanupTmp(final, tmp, fmt.Errorf("write row %d: %w", i, err)))
		}
		if patch != nil {
			row := b.buildRowSlice(i)
			err := patch.add(exception, binary.LittleEndian.Uint64(row[bldOffCount:]),
				binary.LittleEndian.Uint64(row[bldOffBytes:]), binary.LittleEndian.Uint64(row[bldOffSubtreeSize:]))
			if err != nil {
				return abortPatch(cleanupTmp(final, tmp, err))
			}
		}
	}
	if patch != nil {
		if err := patch.close(); err != nil {
			return cleanupTmp(final, tmp, err)
		}
	}
	pad := [coreStatsReadTailPad]byte{}
	if _, err := bw.Write(pad[:]); err != nil {
		return cleanupTmp(final, tmp, fmt.Errorf("write tail pad: %w", err))
	}
	if err := bw.Flush(); err != nil {
		return cleanupTmp(final, tmp, fmt.Errorf("flush: %w", err))
	}
	if err := final.Sync(); err != nil {
		_ = final.Close()
		_ = os.Remove(tmp)

		return fmt.Errorf("sync final: %w", err)
	}
	if err := final.Close(); err != nil {
		_ = os.Remove(tmp)

		return fmt.Errorf("close final: %w", err)
	}

	// Drop the build mmap before the rename so the file we replace
	// has no live references.
	if err := unix.Munmap(b.data); err != nil {
		_ = os.Remove(tmp)

		return fmt.Errorf("munmap build: %w", err)
	}
	b.data = nil
	if err := b.file.Close(); err != nil {
		_ = os.Remove(tmp)

		return fmt.Errorf("close build: %w", err)
	}
	b.file = nil
	if err := os.Rename(tmp, b.path); err != nil {
		_ = os.Remove(tmp)

		return fmt.Errorf("rename tmp into place: %w", err)
	}

	return nil
}

// cleanupTmp closes f and removes path, returning the wrapped err.
// Used on every error branch inside Finalize so cleanup is uniform
// and the partial file never lingers.
func cleanupTmp(f *os.File, path string, err error) error {
	_ = f.Close()
	_ = os.Remove(path)

	return err
}

// repackRow narrows the build-stride row into the tight stride. A
// patched field whose value doesn't fit stores its sentinel; repackRow
// reports whether any did, so the row's exact values go to the patch.
func repackRow(dst, src []byte, s CoreStatsStride) bool {
	exception := false
	off := 0
	for _, f := range [patchedFields]struct {
		at    int
		width uint8
	}{{bldOffCount, s.Count}, {bldOffBytes, s.Bytes}, {bldOffSubtreeSize, s.SubtreeSize}} {
		v := binary.LittleEndian.Uint64(src[f.at:])
		if fieldNeed(v) > f.width {
			v, exception = widthMask[f.width], true
		}
		writeLE(dst[off:], v, int(f.width))
		off += int(f.width)
	}
	writeLE(dst[off:], uint64(binary.LittleEndian.Uint16(src[bldOffDepth:])), int(s.Depth))
	off += int(s.Depth)
	writeLE(dst[off:], uint64(binary.LittleEndian.Uint16(src[bldOffTierMask:])), int(s.TierMask))

	return exception
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
	patch    *corePatch
	rowCount uint64
	stride   int
	desc     CoreStatsStride

	offCount       int
	offBytes       int
	offSubtreeSize int
	offDepth       int
	offTierMask    int
	// patched[f] is set when field f (count, bytes, subtree size) can
	// hold its sentinel: the index has exceptions and f is under 8 bytes.
	patched [patchedFields]bool
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
	r, err := newCoreStatsReader(mmap)
	if err != nil {
		mmap.Close()

		return nil, err
	}
	if r.patch, err = openCorePatch(indexDir, r.rowCount); err != nil {
		mmap.Close()

		return nil, err
	}
	if r.patch != nil {
		for f, w := range [patchedFields]uint8{r.desc.Count, r.desc.Bytes, r.desc.SubtreeSize} {
			r.patched[f] = w < 8
		}
	}

	return r, nil
}

func newCoreStatsReader(mmap *MmapFile) (*CoreStatsReader, error) {
	if mmap.Size() < coreStatsDataOff {
		return nil, fmt.Errorf("core stats: %w", ErrInvalidHeader)
	}
	header, err := DecodeHeader(mmap.Data()[:HeaderSize])
	if err != nil {
		return nil, fmt.Errorf("decode core stats header: %w", err)
	}
	if header.Magic != MagicNumber {
		return nil, ErrMagicMismatch
	}
	if header.Version != Version {
		return nil, ErrVersionMismatch
	}
	desc, err := DecodeCoreStatsStride(mmap.Data()[HeaderSize : HeaderSize+CoreStatsStrideBytes])
	if err != nil {
		return nil, fmt.Errorf("decode core stats stride: %w", err)
	}
	stride := desc.RowBytes()
	if stride != int(header.Width) {
		return nil, fmt.Errorf("%w: header.Width=%d sum=%d", errCoreStatsRowWidth, header.Width, stride)
	}
	expected := int64(coreStatsDataOff) + int64(header.Count)*int64(stride) + coreStatsReadTailPad
	if mmap.Size() < expected {
		return nil, fmt.Errorf("%w: %d < %d", ErrFileTooSmall, mmap.Size(), expected)
	}

	r := &CoreStatsReader{
		mmap:     mmap,
		rowCount: header.Count,
		stride:   stride,
		desc:     desc,
	}
	r.offCount = coreStatsDataOff
	r.offBytes = r.offCount + int(desc.Count)
	r.offSubtreeSize = r.offBytes + int(desc.Bytes)
	r.offDepth = r.offSubtreeSize + int(desc.SubtreeSize)
	r.offTierMask = r.offDepth + int(desc.Depth)

	return r, nil
}

// Count returns the number of rows.
func (r *CoreStatsReader) Count() uint64 { return r.rowCount }

// Stride exposes the per-field width schema used by this index.
func (r *CoreStatsReader) Stride() CoreStatsStride { return r.desc }

// field reads the n-byte little-endian field at offset fieldOff (which
// includes the data offset) of row pos. The file has a
// coreStatsReadTailPad pad after the last row so a single 8-byte load
// + mask is always safe.
func (r *CoreStatsReader) field(pos uint64, fieldOff int, n uint8) uint64 {
	off := int(pos)*r.stride + fieldOff
	v := binary.LittleEndian.Uint64(r.mmap.Data()[off : off+8])

	return v & widthMask[n]
}

// Patched field indexes, in corePatch row order.
const (
	patchCount = iota
	patchBytes
	patchSize
)

// patchedField reads patched field f at fieldOff: the stored value, or
// the exact one from the patch when the field holds its sentinel.
func (r *CoreStatsReader) patchedField(pos uint64, f, fieldOff int, n uint8) uint64 {
	v := r.field(pos, fieldOff, n)
	if r.patched[f] && v == widthMask[n] {
		return r.patch.value(pos, f)
	}

	return v
}

// UnsafeObjectCount returns the object count at pos. No bounds check.
func (r *CoreStatsReader) UnsafeObjectCount(pos uint64) uint64 {
	return r.patchedField(pos, patchCount, r.offCount, r.desc.Count)
}

// UnsafeTotalBytes returns the total bytes at pos. No bounds check.
func (r *CoreStatsReader) UnsafeTotalBytes(pos uint64) uint64 {
	return r.patchedField(pos, patchBytes, r.offBytes, r.desc.Bytes)
}

// UnsafeSubtreeEnd returns the subtree end position at pos. No bounds check.
func (r *CoreStatsReader) UnsafeSubtreeEnd(pos uint64) uint64 {
	return pos + r.patchedField(pos, patchSize, r.offSubtreeSize, r.desc.SubtreeSize)
}

// UnsafeDepth returns the depth at pos as uint32. No bounds check.
func (r *CoreStatsReader) UnsafeDepth(pos uint64) uint32 {
	return uint32(r.field(pos, r.offDepth, r.desc.Depth))
}

// UnsafeTierMask returns the tier slots holding data at pos (bit i =
// slot i). With a zero-width mask field it is slot 0 for a row with
// data and 0 otherwise. No bounds check.
func (r *CoreStatsReader) UnsafeTierMask(pos uint64) uint16 {
	if r.desc.TierMask == 0 {
		cnt, bv := r.UnsafeStats(pos)
		if cnt == 0 && bv == 0 {
			return 0
		}

		return 1
	}

	return uint16(r.field(pos, r.offTierMask, r.desc.TierMask))
}

// UnsafeStats returns (objectCount, totalBytes) at pos in one row
// read — the hottest StatsForPrefix path.
//
//nolint:gocritic // gocritic wants named returns, nonamedreturns forbids them
func (r *CoreStatsReader) UnsafeStats(pos uint64) (uint64, uint64) {
	base := int(pos) * r.stride
	data := r.mmap.Data()
	cnt := binary.LittleEndian.Uint64(data[base+r.offCount:base+r.offCount+8]) & widthMask[r.desc.Count]
	bv := binary.LittleEndian.Uint64(data[base+r.offBytes:base+r.offBytes+8]) & widthMask[r.desc.Bytes]
	if r.patched[patchCount] && cnt == widthMask[r.desc.Count] {
		cnt = r.patch.value(pos, patchCount)
	}
	if r.patched[patchBytes] && bv == widthMask[r.desc.Bytes] {
		bv = r.patch.value(pos, patchBytes)
	}

	return cnt, bv
}

// Close releases the mmap.
func (r *CoreStatsReader) Close() error {
	if r == nil || r.mmap == nil {
		return nil
	}
	err := errors.Join(r.mmap.Close(), r.patch.close())
	r.mmap, r.patch = nil, nil

	return err
}
