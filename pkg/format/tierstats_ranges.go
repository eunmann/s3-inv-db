package format

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math/bits"
	"os"
	"path/filepath"
)

// TierStatsRanges writes the files a TierStatsWriter does, byte for
// byte, from ranges of rows several goroutines write at once
// (TierStatsRange). A row's dense slots go to its place; a multi-tier
// row's stored slots, offset and mask go to the places the multi-tier
// rows and stored slots before its range fix, which the range is given.
// The multi layout's bit vector, whose rank directory counts across
// ranges, and the checks across ranges go in each range's part, which
// Merge takes in row order.
type TierStatsRanges struct {
	plan *TierStatsPlan
	// The layout's files, as in TierStatsWriter.
	dense, multi, offsets, masks *hashedFile
	rowBits                      *rankedBitsWriter
	denseWidth, multiWidth       tierSlotWidths
	offsetWidth                  uint8
	// The rows, multi-tier rows and stored slots merged.
	rows, multiRows, stored uint64
}

// NewTierStatsRanges creates the tier files plan's layout needs in
// outDir at their final sizes, as NewTierStatsWriter does.
func NewTierStatsRanges(outDir string, plan *TierStatsPlan, sums *Checksums) (*TierStatsRanges, error) {
	w := &TierStatsRanges{plan: plan}
	if plan.slots == 1 {
		return w, nil
	}
	dir := filepath.Join(outDir, TierStatsDir)
	if err := os.MkdirAll(dir, DirPerm); err != nil {
		return nil, fmt.Errorf("create tier_stats dir: %w", err)
	}
	var err error
	if plan.choose() == tierLayoutDense {
		w.denseWidth = plan.denseBits.widths()
		w.dense, err = createSlotRanges(filepath.Join(dir, TierDenseFile), plan.rows, (plan.slots-1)*w.denseWidth.slotBytes(), w.denseWidth, sums)
		if err != nil {
			return nil, err
		}

		return w, nil
	}
	w.multiWidth, w.offsetWidth = plan.multiBits.widths(), byteWidthOf(plan.multiSlots)
	if w.multi, err = createSlotRanges(filepath.Join(dir, TierMultiFile), plan.multiSlots, w.multiWidth.slotBytes(), w.multiWidth, sums); err != nil {
		return nil, err
	}
	if w.offsets, err = createArrayRanges(filepath.Join(dir, TierMultiOffsetsFile), plan.multiRows+1, w.offsetWidth, sums); err != nil {
		return nil, errors.Join(err, w.Abort())
	}
	if w.rowBits, err = newRankedBitsWriter(filepath.Join(dir, TierMultiRowsFile), sums); err != nil {
		return nil, errors.Join(err, w.Abort())
	}
	if plan.slots >= tierCodeMinSlots {
		if w.masks, err = createArrayRanges(filepath.Join(dir, TierMultiMaskFile), plan.multiRows, tierMultiMaskWidth, sums); err != nil {
			return nil, errors.Join(err, w.Abort())
		}
	}

	return w, nil
}

// createSlotRanges creates a tier_multi.bin / tier_dense.bin of count
// rows of rowWidth bytes at its final size.
func createSlotRanges(path string, count uint64, rowWidth int, width tierSlotWidths, sums *Checksums) (*hashedFile, error) {
	head := EncodeHeader(Header{Magic: MagicNumber, Version: Version, Count: count, Width: uint32(rowWidth)})
	size := tierSlotDataOff + int64(count)*int64(rowWidth) + coreStatsReadTailPad

	return createSizedFile(path, append(head, width.count, width.bytes), size, sums)
}

// createArrayRanges creates an array file of count values of width bytes,
// as narrowArrayWriter writes it, at its final size.
func createArrayRanges(path string, count uint64, width uint8, sums *Checksums) (*hashedFile, error) {
	head := EncodeHeader(Header{Magic: MagicNumber, Version: Version, Count: count, Width: uint32(width)})

	return createSizedFile(path, head, HeaderSize+int64(count)*int64(width)+arrayTailPad, sums)
}

// TierStatsPart is what a range leaves for Merge.
type TierStatsPart struct {
	// bits holds each row's multi-tier bit, from the range's first row at
	// bit 0, in the multi layout.
	bits []uint64
	// The range's first row, multi-tier row and stored slot, and how many
	// of each it wrote.
	first, rows         uint64
	multiFirst, multi   uint64
	storedFirst, stored uint64
}

// TierStatsRange writes ranges of tier rows, one goroutine's. Its
// buffers are rangeBufSize per file of the layout.
type TierStatsRange struct {
	w                            *TierStatsRanges
	part                         *TierStatsPart
	dense, multi, offsets, masks rangeBuf
}

// NewRange returns a range writer for one goroutine.
func (w *TierStatsRanges) NewRange() *TierStatsRange {
	r := &TierStatsRange{w: w}
	for _, f := range []struct {
		f   *hashedFile
		buf *rangeBuf
	}{{w.dense, &r.dense}, {w.multi, &r.multi}, {w.offsets, &r.offsets}, {w.masks, &r.masks}} {
		if f.f != nil {
			*f.buf = newRangeBuf(f.f)
		}
	}

	return r
}

// Start begins a range at row, after multiRows multi-tier rows that
// store storedSlots slots, which part describes.
func (r *TierStatsRange) Start(row, multiRows, storedSlots uint64, part *TierStatsPart) error {
	w := r.w
	if row > w.plan.rows || multiRows > w.plan.multiRows || storedSlots > w.plan.multiSlots {
		return fmt.Errorf("%w: range at row %d, multi-tier row %d, slot %d; planned %d, %d, %d",
			errTierPlanMismatch, row, multiRows, storedSlots, w.plan.rows, w.plan.multiRows, w.plan.multiSlots)
	}
	*part = TierStatsPart{bits: part.bits[:0], first: row, multiFirst: multiRows, storedFirst: storedSlots}
	r.part = part
	switch {
	case w.dense != nil:
		return r.dense.seek(tierSlotDataOff + int64(row)*int64((w.plan.slots-1)*w.denseWidth.slotBytes()))
	case w.multi != nil:
		err := errors.Join(
			r.multi.seek(tierSlotDataOff+int64(storedSlots)*int64(w.multiWidth.slotBytes())),
			r.offsets.seek(HeaderSize+int64(multiRows)*int64(w.offsetWidth)))
		if w.masks != nil {
			err = errors.Join(err, r.masks.seek(HeaderSize+int64(multiRows)*tierMultiMaskWidth))
		}

		return err
	}

	return nil
}

// AddSlots writes the range's next row's tier slots, as the plan's Add
// returned them, with the checks TierStatsWriter.AddSlots makes.
func (r *TierStatsRange) AddSlots(mask uint16, slots []uint64) error {
	w, p := r.w, r.part
	if p.first+p.rows == w.plan.rows {
		return fmt.Errorf("%w: more than %d rows", errTierPlanMismatch, w.plan.rows)
	}
	if mask>>w.plan.slots != 0 || len(slots) != 2*bits.OnesCount16(mask) {
		return fmt.Errorf("%w: mask %#x with %d values, of %d slots", errTierPlanMismatch, mask, len(slots), w.plan.slots)
	}
	var err error
	switch {
	case w.dense != nil:
		err = r.addDense(mask, slots)
	case w.multi != nil:
		err = r.addMulti(mask, slots)
	}
	p.rows++

	return err
}

// addDense writes every slot but the last, zeros for those without
// data.
func (r *TierStatsRange) addDense(mask uint16, slots []uint64) error {
	width := r.w.denseWidth
	dst, err := r.dense.next((r.w.plan.slots - 1) * width.slotBytes())
	if err != nil {
		return err
	}
	for slot := range r.w.plan.slots - 1 {
		var count, bytes uint64
		if mask&(1<<slot) != 0 {
			count, bytes, slots = slots[0], slots[1], slots[2:]
		}
		if err := putSlot(dst[slot*width.slotBytes():], width, count, bytes); err != nil {
			return err
		}
	}

	return nil
}

// addMulti writes a multi-tier row's slots but its highest, its offset
// and its mask.
func (r *TierStatsRange) addMulti(mask uint16, slots []uint64) error {
	w, p := r.w, r.part
	multi := len(slots) >= 4
	p.bits = setBit(p.bits, p.rows, multi)
	if !multi {
		return nil
	}
	stored := uint64(len(slots)/2 - 1)
	if p.multiFirst+p.multi == w.plan.multiRows || p.storedFirst+p.stored+stored > w.plan.multiSlots {
		return fmt.Errorf("%w: more than %d multi-tier rows or %d slots", errTierPlanMismatch, w.plan.multiRows, w.plan.multiSlots)
	}
	if err := putNarrow(&r.offsets, w.offsetWidth, p.storedFirst+p.stored); err != nil {
		return err
	}
	if w.masks != nil {
		if err := putNarrow(&r.masks, tierMultiMaskWidth, uint64(mask)); err != nil {
			return err
		}
	}
	width := w.multiWidth
	dst, err := r.multi.next(int(stored) * width.slotBytes())
	if err != nil {
		return err
	}
	for k := range int(stored) {
		if err := putSlot(dst[k*width.slotBytes():], width, slots[2*k], slots[2*k+1]); err != nil {
			return err
		}
	}
	p.multi++
	p.stored += stored

	return nil
}

// putSlot stores a slot's count then bytes at width into dst, which has
// 8 bytes of room past the slot.
func putSlot(dst []byte, width tierSlotWidths, count, bytes uint64) error {
	if count > widthMask[width.count] || bytes > widthMask[width.bytes] {
		return fmt.Errorf("%w: slot (%d, %d) wider than %d+%d bytes",
			errTierPlanMismatch, count, bytes, width.count, width.bytes)
	}
	binary.LittleEndian.PutUint64(dst, count)
	binary.LittleEndian.PutUint64(dst[width.count:], bytes)

	return nil
}

// putNarrow stores v, which fits width bytes, as b's next value.
func putNarrow(b *rangeBuf, width uint8, v uint64) error {
	dst, err := b.next(int(width))
	if err != nil {
		return err
	}
	binary.LittleEndian.PutUint64(dst, v)

	return nil
}

// Flush writes the slots still buffered.
func (r *TierStatsRange) Flush() error {
	return errors.Join(r.dense.flush(), r.multi.flush(), r.offsets.flush(), r.masks.flush())
}

// Merge takes the next range's part, in row order: it checks the range
// starts where the ones before end and appends its multi-tier bits.
func (w *TierStatsRanges) Merge(p *TierStatsPart) error {
	if p.first != w.rows || (w.multi != nil && (p.multiFirst != w.multiRows || p.storedFirst != w.stored)) {
		return fmt.Errorf("%w: tier range at row %d, multi-tier row %d, slot %d after %d, %d, %d",
			errRangeOrder, p.first, p.multiFirst, p.storedFirst, w.rows, w.multiRows, w.stored)
	}
	if w.rowBits != nil {
		if err := w.rowBits.addBits(p.bits, p.rows); err != nil {
			return err
		}
	}
	w.rows += p.rows
	w.multiRows += p.multi
	w.stored += p.stored

	return nil
}

// Close finishes the files once every row the plan counted is merged, as
// TierStatsWriter.Close does. On an error every file it started is
// removed.
func (w *TierStatsRanges) Close() error {
	if w.rows != w.plan.rows || (w.multi != nil && (w.multiRows != w.plan.multiRows || w.stored != w.plan.multiSlots)) {
		return errors.Join(fmt.Errorf("%w: wrote %d rows and %d slots, planned %d and %d",
			errTierPlanMismatch, w.rows, w.stored, w.plan.rows, w.plan.multiSlots), w.Abort())
	}
	if w.offsets != nil {
		last := binary.LittleEndian.AppendUint64(nil, w.stored)[:w.offsetWidth]
		if _, err := w.offsets.WriteAt(last, HeaderSize+int64(w.multiRows)*int64(w.offsetWidth)); err != nil {
			return errors.Join(fmt.Errorf("write %s: %w", TierMultiOffsetsFile, err), w.Abort())
		}
	}
	var errs []error
	var paths []string
	for _, f := range []**hashedFile{&w.dense, &w.multi, &w.offsets, &w.masks} {
		if *f != nil {
			paths = append(paths, (*f).path)
			errs = append(errs, closeSized(*f))
			*f = nil
		}
	}
	if w.rowBits != nil {
		paths = append(paths, w.rowBits.path)
		errs = append(errs, w.rowBits.close())
		w.rowBits = nil
	}
	err := errors.Join(errs...)
	if err != nil {
		for _, path := range paths {
			err = errors.Join(err, removeIfErr(path))
		}
	}

	return err
}

// Abort removes every file the writer started and has not finished.
// Idempotent.
func (w *TierStatsRanges) Abort() error {
	var errs []error
	for _, f := range []**hashedFile{&w.dense, &w.multi, &w.offsets, &w.masks} {
		errs = append(errs, abortSized(*f))
		*f = nil
	}
	if w.rowBits != nil {
		errs = append(errs, w.rowBits.abort())
		w.rowBits = nil
	}

	return errors.Join(errs...)
}
