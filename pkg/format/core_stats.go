package format

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"math/bits"
	"os"
	"path/filepath"
)

// CoreStatsFile is the row-major per-prefix core stats file.
const CoreStatsFile = "core_stats.bin"

// coreStatsDataOff is where the first row starts in the final file,
// after the Header and the stride descriptor. Rows are packed: padding
// them to cache-line boundaries measured no faster (#51).
const coreStatsDataOff = HeaderSize + CoreStatsStrideBytes

// CoreStatsStride is the per-index field-width schema a CoreStatsPlan
// picks from the value ranges. Each field stores the
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

// CoreStatsPlan gathers, from every row in any order, what fixes the
// core row widths before a row is written: the widths each row's count
// and bytes need, their maxima and the largest tier mask. The zero
// value is an empty plan.
//
// Rows are in level order, so the child starts are not known until
// every depth's size is: row i's child start is above i, never below the
// row before's, and the last row's is the row count. That bounds them
// enough to choose the child start's width without them, except within
// 0.5% above a power of 256 (see ChildStartTail).
type CoreStatsPlan struct {
	// hist counts rows by the widths their count and bytes need (index
	// 0 unused); tail does for the rows ChildStartTail selects.
	hist, tail         [9][9]uint64
	rows, tailRows     uint64
	maxCount, maxBytes uint64
	maxMask            uint16
}

// Add records the next row's count, bytes and tier mask.
func (p *CoreStatsPlan) Add(count, bytes uint64, tierMask uint16) {
	p.hist[fieldNeed(count)][fieldNeed(bytes)]++
	p.rows++
	p.maxCount, p.maxBytes = max(p.maxCount, count), max(p.maxBytes, bytes)
	p.maxMask = max(p.maxMask, tierMask)
}

// ChildStartTail reports whether choosing the widths needs the rows
// whose child start is at least from (AddTail), and from.
//
// The child start's width is at least k-1 bytes, where k is the width
// the row count needs: a width w leaves every row from widthMask[w]-1 on
// an exception (its child start is above it), which for w <= k-2 is
// over 99% of rows. At k-1 the exceptions are the rows whose child start
// needs k bytes, a run at the end at least rows-widthMask[k-1]+1 long:
// under the 0.5% cap only when the row count is within 0.5% above a
// power of 256. Then the choice needs those rows' count and bytes
// widths; otherwise it needs none.
func (p *CoreStatsPlan) ChildStartTail() (uint64, bool) {
	k := fieldNeed(p.rows)
	if k == 1 {
		return 0, false
	}

	return widthMask[k-1], patchFeasible(p.childStartFloor(k-1), p.rows)
}

// AddTail records a row whose child start is at least ChildStartTail's
// from, in any order.
func (p *CoreStatsPlan) AddTail(count, bytes uint64) {
	p.tail[fieldNeed(count)][fieldNeed(bytes)]++
	p.tailRows++
}

// childStartFloor is the fewest rows whose child start needs more than
// ws bytes: every row from widthMask[ws]-1 on.
func (p *CoreStatsPlan) childStartFloor(ws uint8) uint64 {
	return p.rows - min(p.rows, widthMask[ws]-1)
}

// countBytesExceptions counts the rows of h whose count or bytes need
// more than wc or wb bytes.
func countBytesExceptions(h *[9][9]uint64, wc, wb uint8) uint64 {
	var e uint64
	for c := uint8(1); c <= 8; c++ {
		for b := uint8(1); b <= 8; b++ {
			if c > wc || b > wb {
				e += h[c][b]
			}
		}
	}

	return e
}

// errCorePlan means the rows given to a core stats writer differ from
// the plan it was made from, or the plan lacks the tail it needs.
var errCorePlan = errors.New("core stats rows differ from their plan")

// stride picks the row widths, as a 3-way histogram of every row's
// count, bytes and child start widths would (see choosePatch): the
// count and bytes from the plan's histogram, the child start from the
// bound in ChildStartTail and, near a power of 256, the tail.
func (p *CoreStatsPlan) stride(enc TierMaskEncoding) (CoreStatsStride, patchChoice, error) {
	k := fieldNeed(p.rows)
	from, needTail := p.ChildStartTail()
	if needTail && p.tailRows < p.childStartFloor(k-1) {
		return CoreStatsStride{}, patchChoice{}, fmt.Errorf("%w: %d rows with a child start from %d, at least %d expected",
			errCorePlan, p.tailRows, from, p.childStartFloor(k-1))
	}
	var bounded bool
	choice := choosePatch(p.rows, func(wc, wb, ws uint8) uint64 {
		switch {
		case ws >= k:
			return countBytesExceptions(&p.hist, wc, wb)
		case ws == k-1 && needTail:
			return countBytesExceptions(&p.hist, wc, wb) - countBytesExceptions(&p.tail, wc, wb) + p.tailRows
		}
		e := p.childStartFloor(ws)
		bounded = bounded || patchFeasible(e, p.rows)

		return e
	})
	if bounded {
		return CoreStatsStride{}, patchChoice{}, fmt.Errorf("%w: child start bound within the exception cap", errCorePlan)
	}
	// Without exceptions there are no sentinels, so every field can take
	// the plain width of its largest value, all-ones included: one byte
	// narrower than choosePatch allows when a maximum is all ones.
	plain := patchChoice{count: byteWidthOf(p.maxCount), bytes: byteWidthOf(p.maxBytes), size: byteWidthOf(p.rows)}
	plain.cost = p.rows * uint64(plain.count+plain.bytes+plain.size)
	if choice.exceptions == 0 || plain.cost <= choice.cost {
		choice = plain
	}

	return CoreStatsStride{
		Count:      choice.count,
		Bytes:      choice.bytes,
		ChildStart: choice.size,
		TierMask:   tierFieldWidth(p.maxMask, enc),
	}, choice, nil
}

// CoreStatsWriter writes core_stats.bin, and its patch files when the
// plan's widths leave exceptions, one row at a time in position order,
// straight to their final form.
type CoreStatsWriter struct {
	path  string
	f     *os.File
	bw    *bufio.Writer
	patch *patchWriter
	// row is the row being packed, with a word of slack: each field is
	// stored as a whole word over the bytes after it.
	row  []byte
	desc CoreStatsStride
	enc  TierMaskEncoding
	plan *CoreStatsPlan
	// The rows, exceptions and last child start written.
	n, exceptions, lastChild uint64
	wantExceptions           uint64
	closed                   bool
}

// NewCoreStatsWriter creates outDir's core stats file, at the widths
// plan picks with the tier field encoded as enc (the tier plan's
// MaskEncoding), and its patch files when those widths leave
// exceptions. Near a power of 256 the plan must hold its tail first.
func NewCoreStatsWriter(outDir string, plan *CoreStatsPlan, enc TierMaskEncoding) (*CoreStatsWriter, error) {
	desc, choice, err := plan.stride(enc)
	if err != nil {
		return nil, err
	}
	w := &CoreStatsWriter{
		path:           filepath.Join(outDir, CoreStatsFile),
		row:            make([]byte, desc.RowBytes()+8),
		desc:           desc,
		enc:            enc,
		plan:           plan,
		wantExceptions: choice.exceptions,
	}
	if w.f, err = os.Create(w.path); err != nil {
		return nil, fmt.Errorf("create core stats file: %w", err)
	}
	w.bw = bufio.NewWriterSize(w.f, writeBufSize)
	head := EncodeHeader(Header{Magic: MagicNumber, Version: Version, Count: plan.rows, Width: uint32(desc.RowBytes())})
	enc4 := desc.Encode()
	if _, err := w.bw.Write(append(head, enc4[:]...)); err != nil {
		return nil, errors.Join(fmt.Errorf("write core stats header: %w", err), w.Abort())
	}
	if choice.exceptions > 0 {
		if w.patch, err = newPatchWriter(outDir); err != nil {
			return nil, errors.Join(err, w.Abort())
		}
	}

	return w, nil
}

// Stride returns the widths the rows are written at.
func (w *CoreStatsWriter) Stride() CoreStatsStride { return w.desc }

// Add writes the next row: the prefix's object count, total bytes, the
// row of its first child and its tier mask. It fails on a row the plan
// doesn't hold: past its row count, a child start out of level order, a
// value or mask wider than its field allows, or one exception more.
func (w *CoreStatsWriter) Add(count, bytes, childStart uint64, tierMask uint16) error {
	if w.n == w.plan.rows || childStart <= w.n || childStart < w.lastChild || childStart > w.plan.rows || tierMask > w.plan.maxMask {
		return fmt.Errorf("%w: row %d of %d (child start %d after %d, tier mask %#x of at most %#x)",
			errCorePlan, w.n, w.plan.rows, childStart, w.lastChild, tierMask, w.plan.maxMask)
	}
	exception := false
	off := 0
	for _, f := range [patchedFields]struct {
		v     uint64
		width uint8
	}{{count, w.desc.Count}, {bytes, w.desc.Bytes}, {childStart, w.desc.ChildStart}} {
		v := f.v
		if fieldNeed(v) > f.width {
			if w.patch == nil && v > widthMask[f.width] {
				return fmt.Errorf("%w: row %d value %d wider than %d bytes", errCorePlan, w.n, v, f.width)
			}
			v, exception = widthMask[f.width], true
		}
		binary.LittleEndian.PutUint64(w.row[off:], v)
		off += int(f.width)
	}
	binary.LittleEndian.PutUint64(w.row[off:], encodeTierField(tierMask, w.enc))
	if _, err := w.bw.Write(w.row[:w.desc.RowBytes()]); err != nil {
		return fmt.Errorf("write core stats row %d: %w", w.n, err)
	}
	if w.patch != nil {
		if exception {
			if w.exceptions == w.wantExceptions {
				return fmt.Errorf("%w: more than the %d exceptions planned", errCorePlan, w.wantExceptions)
			}
			w.exceptions++
		}
		if err := w.patch.add(exception, count, bytes, childStart); err != nil {
			return err
		}
	}
	w.n++
	w.lastChild = childStart

	return nil
}

// Close finishes the files once every row the plan counted is written.
// On an error every file it started is removed.
func (w *CoreStatsWriter) Close() error {
	if w.n != w.plan.rows || w.exceptions != w.wantExceptions || (w.n > 0 && w.lastChild != w.n) {
		return errors.Join(fmt.Errorf("%w: wrote %d rows with %d exceptions, last child start %d; planned %d rows with %d",
			errCorePlan, w.n, w.exceptions, w.lastChild, w.plan.rows, w.wantExceptions), w.Abort())
	}
	var pad [coreStatsReadTailPad]byte
	if _, err := w.bw.Write(pad[:]); err != nil {
		return errors.Join(fmt.Errorf("write core stats tail pad: %w", err), w.Abort())
	}
	if err := w.bw.Flush(); err != nil {
		return errors.Join(fmt.Errorf("flush core stats: %w", err), w.Abort())
	}
	startWriteback(w.f)
	err := w.f.Close()
	w.f = nil
	if err != nil {
		return errors.Join(fmt.Errorf("close core stats: %w", err), w.Abort())
	}
	if w.patch != nil {
		err := w.patch.close()
		w.patch = nil
		if err != nil {
			return errors.Join(err, w.Abort())
		}
	}
	w.closed = true

	return nil
}

// Abort removes every file the writer started, unless Close finished
// them. Idempotent.
func (w *CoreStatsWriter) Abort() error {
	if w.closed {
		return nil
	}
	var errs []error
	if w.f != nil {
		errs = append(errs, w.f.Close())
		w.f = nil
	}
	if w.patch != nil {
		errs = append(errs, w.patch.abort())
		w.patch = nil
	}

	return errors.Join(append(errs, removeIfErr(w.path))...)
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
