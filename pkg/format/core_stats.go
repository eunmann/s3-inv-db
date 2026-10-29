package format

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"math/bits"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

// CoreStatsFile is the row-major per-prefix core stats file.
const CoreStatsFile = "core_stats.bin"

// coreStatsBuildStride is the fixed safe stride used during the
// build, before per-field widths are known. Finalize repacks the rows
// to the stride it chooses: count, bytes and child start from the
// width histogram (rows that don't fit go to patch files), the tier
// mask from its max.
//
//	offset  0: object_count           uint64
//	offset  8: total_bytes            uint64
//	offset 16: child_start            uint64 (row of the first child)
//	offset 24: tier_mask              uint16
const coreStatsBuildStride = 26

// coreStatsMaxInitialRows caps the rows a CoreStatsBuilder reserves
// before any are added. The capacity hint can over-count many times
// and every reserved block is allocated on disk, so the file starts no
// larger than this and doubles as rows arrive.
const coreStatsMaxInitialRows = 1 << 16

const (
	bldOffCount      = 0
	bldOffBytes      = 8
	bldOffChildStart = 16
	bldOffTierMask   = 24
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
	Count uint8 // 1..8
	Bytes uint8 // 1..8
	// ChildStart is 1..8; the field holds the row of the prefix's first
	// child (rows are in level order, so its children are the rows from
	// it up to the next row's child start).
	ChildStart uint8
	// TierMask is 0 when no row has data outside tier slot 0, so the
	// mask is implied (slot 0 for a row with data), else 1..2. Bit i is
	// tier slot i (tiers.json order) holding data in this row.
	TierMask uint8
}

// CoreStatsStrideBytes is the on-disk size of the encoded stride
// descriptor that follows the file Header.
const CoreStatsStrideBytes = 4

// RowBytes is the total width of a row at this stride.
func (s CoreStatsStride) RowBytes() int {
	return int(s.Count) + int(s.Bytes) + int(s.ChildStart) + int(s.TierMask)
}

// Encode serialises the descriptor to 4 raw bytes.
func (s CoreStatsStride) Encode() [CoreStatsStrideBytes]byte {
	return [CoreStatsStrideBytes]byte{s.Count, s.Bytes, s.ChildStart, s.TierMask}
}

// DecodeCoreStatsStride parses the 4-byte descriptor.
func DecodeCoreStatsStride(buf []byte) (CoreStatsStride, error) {
	if len(buf) < CoreStatsStrideBytes {
		return CoreStatsStride{}, ErrInvalidHeader
	}
	s := CoreStatsStride{Count: buf[0], Bytes: buf[1], ChildStart: buf[2], TierMask: buf[3]}
	if err := s.validate(); err != nil {
		return CoreStatsStride{}, err
	}

	return s, nil
}

func (s CoreStatsStride) validate() error {
	if s.Count < 1 || s.Count > 8 ||
		s.Bytes < 1 || s.Bytes > 8 ||
		s.ChildStart < 1 || s.ChildStart > 8 ||
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
	errCoreStatsRowWidth      = errors.New("core stats row width mismatch")
	errCoreStatsStrideInvalid = errors.New("core stats stride descriptor invalid")
)

// CoreStatsBuilder writes the row-major core stats file during a
// build, one row per prefix in level order (Add order).
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

	// Observed max — drives the tier field's width at Finalize.
	maxTierMask uint16
	// tierEnc is how the tier field encodes the mask (SetTierMaskEncoding).
	tierEnc TierMaskEncoding
}

// NewCoreStatsBuilder creates a core stats file at outDir/core_stats.bin
// sized to fit capacity rows, at most coreStatsMaxInitialRows (grown on
// demand if exceeded). The header
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
	initial := max(int64(HeaderSize)+int64(min(capacity, coreStatsMaxInitialRows))*int64(coreStatsBuildStride), int64(unix.Getpagesize()))
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
	if err := preallocate(b.file, b.size, newSize); err != nil {
		return fmt.Errorf("core stats grow: %w", err)
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

// Add appends the next row: the prefix's object count, total bytes,
// the row of its first child and its tier mask.
func (b *CoreStatsBuilder) Add(objectCount, totalBytes, childStart uint64, tierMask uint16) error {
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
	binary.LittleEndian.PutUint64(row[bldOffChildStart:], childStart)
	binary.LittleEndian.PutUint16(row[bldOffTierMask:], tierMask)

	b.maxTierMask = max(b.maxTierMask, tierMask)
	b.count++

	return nil
}

// SetTierMaskEncoding sets how Finalize stores each row's tier mask: the
// tier stats writer's MaskEncoding for the layout it writes.
func (b *CoreStatsBuilder) SetTierMaskEncoding(enc TierMaskEncoding) {
	b.tierEnc = enc
}

// Count returns the number of rows committed.
func (b *CoreStatsBuilder) Count() int { return b.count }

// chooseStride picks the row widths: the patched fields from the joint
// distribution of the widths each row needs (see choosePatch), the
// tier mask from its max. It reads every build row once.
func (b *CoreStatsBuilder) chooseStride() (CoreStatsStride, patchChoice) {
	var hist patchHistogram
	var maxCount, maxBytes, maxSize uint64
	for i := range b.count {
		row := b.buildRowSlice(i)
		c, by, sz := binary.LittleEndian.Uint64(row[bldOffCount:]),
			binary.LittleEndian.Uint64(row[bldOffBytes:]),
			binary.LittleEndian.Uint64(row[bldOffChildStart:])
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
		Count:      choice.count,
		Bytes:      choice.bytes,
		ChildStart: choice.size,
		TierMask:   tierFieldWidth(b.maxTierMask, b.tierEnc),
	}, choice
}

// byteWidth16 returns 1 for v < 256 else 2.
func byteWidth16(v uint16) uint8 {
	if v < 1<<8 {
		return 1
	}

	return 2
}

// tierFieldWidth is the tier field's width under enc.
func tierFieldWidth(maxMask uint16, enc TierMaskEncoding) uint8 {
	switch enc {
	case TierMaskDerived:
		return 0
	case TierMaskCode:
		return 1
	default:
		return tierMaskWidth(maxMask)
	}
}

// encodeTierField is the tier field's value for mask under enc.
func encodeTierField(mask uint16, enc TierMaskEncoding) uint64 {
	if enc != TierMaskCode {
		return uint64(mask)
	}
	switch bits.OnesCount16(mask) {
	case 0:
		return 0
	case 1:
		return uint64(bits.TrailingZeros16(mask)) + 1
	default:
		return tierCodeSeveral
	}
}

// tierMaskWidth is 0 when every mask is 0 or slot 0 alone (implied by
// the row having data), else the bytes that hold the largest mask.
func tierMaskWidth(maxMask uint16) uint8 {
	if maxMask <= 1 {
		return 0
	}

	return byteWidth16(maxMask)
}

// Finalize chooses the stride (count, bytes and child start from the
// width histogram, with patch files for the rows that don't fit; the
// tier mask from its max), streams the repacked rows into a
// fresh file at the same path (Header + stride descriptor first), and
// closes both files. Memory during the repack is bounded by the bufio
// buffer (1 MiB) — at the project's stated 10B-prefix scale a per-row
// in-memory buffer would be hundreds of GB.
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
	// Each field is stored as a whole word over the bytes after it, so
	// the row buffer has a word of slack past the row.
	rowBuf := make([]byte, stride+8)
	for i := range b.count {
		exception := repackRow(rowBuf, b.buildRowSlice(i), desc, b.tierEnc)
		if _, err := bw.Write(rowBuf[:stride]); err != nil {
			return abortPatch(cleanupTmp(final, tmp, fmt.Errorf("write row %d: %w", i, err)))
		}
		if patch != nil {
			row := b.buildRowSlice(i)
			err := patch.add(exception, binary.LittleEndian.Uint64(row[bldOffCount:]),
				binary.LittleEndian.Uint64(row[bldOffBytes:]), binary.LittleEndian.Uint64(row[bldOffChildStart:]))
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
func repackRow(dst, src []byte, s CoreStatsStride, enc TierMaskEncoding) bool {
	exception := false
	off := 0
	for _, f := range [patchedFields]struct {
		at    int
		width uint8
	}{{bldOffCount, s.Count}, {bldOffBytes, s.Bytes}, {bldOffChildStart, s.ChildStart}} {
		v := binary.LittleEndian.Uint64(src[f.at:])
		if fieldNeed(v) > f.width {
			v, exception = widthMask[f.width], true
		}
		binary.LittleEndian.PutUint64(dst[off:], v)
		off += int(f.width)
	}
	binary.LittleEndian.PutUint64(dst[off:], encodeTierField(binary.LittleEndian.Uint16(src[bldOffTierMask:]), enc))

	return exception
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

	offCount      int
	offBytes      int
	offChildStart int
	offTierMask   int
	// patched[f] is set when field f (count, bytes, child start) can
	// hold its sentinel: the index has exceptions and f is under 8 bytes.
	patched [patchedFields]bool
}

// OpenCoreStats opens the row-major core stats file. Returns an
// error if the file is missing — every supported index format
// includes core_stats.bin.
func OpenCoreStats(indexDir string) (*CoreStatsReader, error) {
	path := filepath.Join(indexDir, CoreStatsFile)
	mmap, err := OpenMmap(path)
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
		for f, w := range [patchedFields]uint8{r.desc.Count, r.desc.Bytes, r.desc.ChildStart} {
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
	r.offChildStart = r.offBytes + int(desc.Bytes)
	r.offTierMask = r.offChildStart + int(desc.ChildStart)

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
	patchChildStart
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

// UnsafeChildStart returns the row of pos's first child: pos's
// children are the rows from it up to UnsafeChildStart(pos+1), the
// row count past the last row. No bounds check.
func (r *CoreStatsReader) UnsafeChildStart(pos uint64) uint64 {
	if pos == r.rowCount {
		return r.rowCount
	}

	return r.patchedField(pos, patchChildStart, r.offChildStart, r.desc.ChildStart)
}

// UnsafeTierField returns pos's tier field, which the tier stats reader
// decodes by the index's TierMaskEncoding (as a plain mask, bit i is
// slot i holding data). A zero-width field reads 1 for a row with data
// and 0 otherwise. No bounds check.
func (r *CoreStatsReader) UnsafeTierField(pos uint64) uint16 {
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
