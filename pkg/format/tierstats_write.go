package format

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"math/bits"
	"os"
	"path/filepath"
	"slices"

	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

// Tier stats store only what a prefix's core row doesn't already give.
// The core row carries the prefix's totals and its tier mask (the tier
// slots holding data), and every prefix's per-tier counts and bytes sum
// to its totals. So a prefix with one tier needs nothing more, and in a
// prefix with several, the last tier is the totals minus the others.
// The builder picks the smaller of two layouts for the rest, from exact
// sizes computed over the whole index:
//
//   - multi (tier_multi.bin + .rows + .off.u64): for every prefix with
//     two or more tiers, the slots of every tier in its mask except the
//     highest, in slot order. A bit vector marks those prefixes; its
//     rank locates a prefix's run of slots through the offsets array.
//   - dense (tier_dense.bin): every prefix stores slots 0..T-2 of the T
//     present tiers, zeros included; slot T-1 is derived.
//
// An index with one present tier writes no tier files. Each stored slot
// is a count then bytes, little-endian, at the widths the layout's
// largest stored values need, recorded in a 2-byte descriptor.
//
// The core row's tier field follows from the layout (TierMaskEncoding):
// the dense layout gives the mask itself, so the field is empty; the
// multi layout stores the mask, or past 8 slots, where the mask takes 2
// bytes, a 1-byte code with the masks of the multi-tier prefixes in
// tier_multi.mask.
const (
	TierMultiFile        = "tier_multi.bin"
	TierMultiRowsFile    = "tier_multi.rows"
	TierMultiOffsetsFile = "tier_multi.off.u64"
	TierMultiMaskFile    = "tier_multi.mask"
	TierDenseFile        = "tier_dense.bin"
)

// TierMaskEncoding is how a core row's tier field holds the prefix's
// tier mask.
type TierMaskEncoding uint8

const (
	// TierMaskPlain stores the mask: bit i is slot i holding data.
	TierMaskPlain TierMaskEncoding = iota
	// TierMaskDerived stores nothing: the dense layout's slots give it.
	TierMaskDerived
	// TierMaskCode stores one byte: 0 for no data, slot+1 for a prefix
	// with one tier, tierCodeSeveral for two or more, whose mask is at
	// the prefix's rank in tier_multi.mask.
	TierMaskCode
)

// tierCodeSeveral is the TierMaskCode value of a multi-tier prefix.
const tierCodeSeveral = 0xFF

// tierCodeMinSlots is the fewest slots that take TierMaskCode: below it
// the plain mask fits one byte.
const tierCodeMinSlots = 9

// tierMultiMaskWidth is a tier_multi.mask entry's width.
const tierMultiMaskWidth = 2

// tierSlotDescBytes is the slot-width descriptor after the Header of
// tier_multi.bin and tier_dense.bin: the count width, then the bytes
// width.
const tierSlotDescBytes = 2

// tierSlotDataOff is where slot data starts in those files.
const tierSlotDataOff = HeaderSize + tierSlotDescBytes

// maxTierSlots is the most tier slots a mask can describe.
const maxTierSlots = 16

// wordBits is the bits in one bit-vector word.
const wordBits = 64

// bitWords is the uint64 words a bit vector of n bits takes.
func bitWords(n uint64) uint64 { return (n + wordBits - 1) / wordBits }

// Sentinel errors.
var (
	errTierStatsLayout    = errors.New("tier stats layout invalid")
	errNoTiersDeclared    = errors.New("tier stats writer requires at least one present tier")
	errUndeclaredTierData = errors.New("prefix has data for a tier absent from the declared present set")
	errTierSumMismatch    = errors.New("prefix tier stats don't sum to its totals")
)

// tierSlotWidths is the byte width of a stored slot's count and bytes.
type tierSlotWidths struct{ count, bytes uint8 }

func (w tierSlotWidths) slotBytes() int { return int(w.count) + int(w.bytes) }

// slotBits is the bitwise OR of every stored slot's count and of its
// bytes: a value's byte width depends only on its highest set bit, so
// the OR's width is the widest value's. One OR per slot replaced a
// width computation per slot that was half of Add's time.
type slotBits struct{ count, bytes uint64 }

func (b *slotBits) add(count, bytes uint64) {
	b.count |= count
	b.bytes |= bytes
}

// widths are the byte widths that hold every value added, at least one
// byte each, even when every value is zero (or nothing was added).
func (b *slotBits) widths() tierSlotWidths {
	return tierSlotWidths{count: byteWidthOf(b.count), bytes: byteWidthOf(b.bytes)}
}

// tierLayout names a stored layout.
type tierLayout int

const (
	tierLayoutAuto tierLayout = iota
	tierLayoutDense
	tierLayoutMulti
)

// writeBufSize is the buffered-writer size of the tier files.
const writeBufSize = 1 << 20

// TierStatsPlan gathers, from every prefix's tier stats in any order,
// what fixes the tier layout before a row is written: the row and slot
// counts and the widest stored values of each layout, and the largest
// mask. Each is a sum, an OR or a max, so the order rows arrive in
// doesn't change it.
type TierStatsPlan struct {
	// slotByTier maps a tier ID to its slot, or -1 when the tier is
	// absent from the declared present set.
	slotByTier [tiers.NumTiers]int
	slots      int
	rows       uint64
	// For the multi layout: prefixes with two or more tiers, and the
	// slots they store.
	multiRows  uint64
	multiSlots uint64
	multiBits  slotBits
	denseBits  slotBits
	maxMask    uint16
	row        tierRow
	// force overrides the size-based choice; tests only.
	force tierLayout
}

// NewTierStatsPlan starts a plan for the given present tiers (the tiers
// with data anywhere in the index), which become tier slots in tier-ID
// order.
func NewTierStatsPlan(present []tiers.ID) (*TierStatsPlan, error) {
	if len(present) == 0 {
		return nil, errNoTiersDeclared
	}
	sorted := slices.Compact(slices.Sorted(slices.Values(present)))
	if len(sorted) > maxTierSlots {
		return nil, fmt.Errorf("%w: %d tiers", errTierStatsLayout, len(sorted))
	}
	for _, id := range sorted {
		if int(id) >= int(tiers.NumTiers) {
			return nil, fmt.Errorf("%w: tier id %d out of range", errTierStatsLayout, id)
		}
	}
	p := &TierStatsPlan{slots: len(sorted)}
	for tid := range p.slotByTier {
		p.slotByTier[tid] = slices.Index(sorted, tiers.ID(tid))
	}

	return p, nil
}

// tierRow is one prefix's tier stats by slot: mask holds the slots with
// data, and the others' count and bytes are zero.
type tierRow struct {
	count, bytes [maxTierSlots]uint64
	mask         uint16
}

// fill sets row to the prefix's stats, indexed by tier ID. It fails when
// a tier outside the declared set has data: its counts would be lost.
func (p *TierStatsPlan) fill(row *tierRow, counts, bytes *[tiers.NumTiers]uint64) error {
	for set := row.mask; set != 0; set &= set - 1 {
		slot := bits.TrailingZeros16(set)
		row.count[slot], row.bytes[slot] = 0, 0
	}
	row.mask = 0
	for id := range tiers.NumTiers {
		c, b := counts[id], bytes[id]
		if c == 0 && b == 0 {
			continue
		}
		slot := p.slotByTier[id]
		if slot < 0 {
			return fmt.Errorf("%w: tier %d", errUndeclaredTierData, id)
		}
		row.mask |= 1 << slot
		row.count[slot], row.bytes[slot] = c, b
	}

	return nil
}

// Add records the next prefix's tier stats, indexed by tier ID, and
// returns its tier mask. It fails when a tier outside the declared set
// has data or when the tiers don't sum to the prefix's totals (a
// derived tier would be wrong).
func (p *TierStatsPlan) Add(counts, bytes *[tiers.NumTiers]uint64, totalCount, totalBytes uint64) (uint16, error) {
	row := &p.row
	if err := p.fill(row, counts, bytes); err != nil {
		return 0, err
	}
	var sumCount, sumBytes, carryCount, carryBytes uint64
	for set := row.mask; set != 0; set &= set - 1 {
		slot := bits.TrailingZeros16(set)
		var cc, cb uint64
		sumCount, cc = bits.Add64(sumCount, row.count[slot], 0)
		sumBytes, cb = bits.Add64(sumBytes, row.bytes[slot], 0)
		carryCount |= cc
		carryBytes |= cb
	}
	if carryCount != 0 || carryBytes != 0 || sumCount != totalCount || sumBytes != totalBytes {
		return 0, fmt.Errorf("%w: tiers %d objects / %d bytes, totals %d / %d",
			errTierSumMismatch, sumCount, sumBytes, totalCount, totalBytes)
	}
	p.rows++
	if p.slots == 1 {
		return row.mask, nil
	}
	p.maxMask = max(p.maxMask, row.mask)
	if n := bits.OnesCount16(row.mask); n >= 2 {
		p.multiRows++
		p.multiSlots += uint64(n - 1)
		stored := row.mask &^ (1 << (bits.Len16(row.mask) - 1))
		for stored != 0 {
			slot := bits.TrailingZeros16(stored)
			stored &= stored - 1
			p.multiBits.add(row.count[slot], row.bytes[slot])
		}
	}
	for slot := range p.slots - 1 {
		p.denseBits.add(row.count[slot], row.bytes[slot])
	}

	return row.mask, nil
}

// choose returns the layout whose files and core tier fields together
// are smaller; dense on a tie (no indirection).
func (p *TierStatsPlan) choose() tierLayout {
	if p.force != tierLayoutAuto {
		return p.force
	}
	if p.denseSize() <= p.multiSize()+p.multiMaskSize() {
		return tierLayoutDense
	}

	return tierLayoutMulti
}

// MaskEncoding returns how the core rows' tier field encodes the mask
// for the layout the plan picks. Call it after the last Add.
func (p *TierStatsPlan) MaskEncoding() TierMaskEncoding {
	switch {
	case p.slots <= 1:
		return TierMaskPlain
	case p.choose() == tierLayoutDense:
		return TierMaskDerived
	case p.slots >= tierCodeMinSlots:
		return TierMaskCode
	default:
		return TierMaskPlain
	}
}

// multiMaskSize is what the multi layout's masks cost: every core row's
// tier field, plus tier_multi.mask when the field is a code.
func (p *TierStatsPlan) multiMaskSize() uint64 {
	if p.slots < tierCodeMinSlots {
		return p.rows * uint64(tierMaskWidth(p.maxMask))
	}

	return p.rows + uint64(HeaderSize) + p.multiRows*tierMultiMaskWidth + arrayTailPad
}

// multiSize and denseSize are the exact on-disk sizes of each layout.
func (p *TierStatsPlan) multiSize() uint64 {
	slotsFile := uint64(tierSlotDataOff) + p.multiSlots*uint64(p.multiBits.widths().slotBytes()) + coreStatsReadTailPad
	words := bitWords(p.rows)
	rowsFile := uint64(HeaderSize) + 8*words + 8*((words+rankBlockWords-1)/rankBlockWords)
	offsetsFile := uint64(HeaderSize) + (p.multiRows+1)*uint64(byteWidthOf(p.multiSlots)) + arrayTailPad

	return slotsFile + rowsFile + offsetsFile
}

func (p *TierStatsPlan) denseSize() uint64 {
	return uint64(tierSlotDataOff) + p.rows*uint64(p.slots-1)*uint64(p.denseBits.widths().slotBytes()) + coreStatsReadTailPad
}

// errTierPlanMismatch means the rows written differ from the rows the
// plan was made from.
var errTierPlanMismatch = errors.New("tier stats rows differ from their plan")

// TierStatsWriter writes the tier files in the layout its plan picks,
// one prefix at a time in position order, straight to their final
// form: every count and width their headers hold is in the plan.
type TierStatsWriter struct {
	plan *TierStatsPlan
	row  tierRow
	rows uint64
	// The layout's files: dense, or the multi layout's four (masks only
	// past 8 slots). All nil with one present tier.
	dense   *slotFile
	multi   *slotFile
	offsets *narrowArrayWriter
	rowBits *rankedBitsWriter
	masks   *narrowArrayWriter
	stored  uint64
}

// NewTierStatsWriter creates the tier files plan's layout needs in
// outDir. With one present tier it creates none.
func NewTierStatsWriter(outDir string, plan *TierStatsPlan) (*TierStatsWriter, error) {
	w := &TierStatsWriter{plan: plan}
	if plan.slots == 1 {
		return w, nil
	}
	dir := filepath.Join(outDir, TierStatsDir)
	if err := os.MkdirAll(dir, DirPerm); err != nil {
		return nil, fmt.Errorf("create tier_stats dir: %w", err)
	}
	var err error
	if plan.choose() == tierLayoutDense {
		width := plan.denseBits.widths()
		if w.dense, err = createSlotFile(filepath.Join(dir, TierDenseFile), plan.rows, (plan.slots-1)*width.slotBytes(), width); err != nil {
			return nil, err
		}

		return w, nil
	}
	width := plan.multiBits.widths()
	if w.multi, err = createSlotFile(filepath.Join(dir, TierMultiFile), plan.multiSlots, width.slotBytes(), width); err != nil {
		return nil, err
	}
	if w.offsets, err = newNarrowArrayWriter(filepath.Join(dir, TierMultiOffsetsFile), byteWidthOf(plan.multiSlots)); err != nil {
		return nil, errors.Join(err, w.Abort())
	}
	if w.rowBits, err = newRankedBitsWriter(filepath.Join(dir, TierMultiRowsFile)); err != nil {
		return nil, errors.Join(err, w.Abort())
	}
	if plan.slots >= tierCodeMinSlots {
		if w.masks, err = newNarrowArrayWriter(filepath.Join(dir, TierMultiMaskFile), tierMultiMaskWidth); err != nil {
			return nil, errors.Join(err, w.Abort())
		}
	}

	return w, nil
}

// Add writes the next prefix's tier stats, indexed by tier ID, and
// returns its tier mask for its core row. The plan has checked them.
func (w *TierStatsWriter) Add(counts, bytes *[tiers.NumTiers]uint64) (uint16, error) {
	if w.rows == w.plan.rows {
		return 0, fmt.Errorf("%w: more than %d rows", errTierPlanMismatch, w.plan.rows)
	}
	w.rows++
	row := &w.row
	if err := w.plan.fill(row, counts, bytes); err != nil {
		return 0, err
	}
	switch {
	case w.dense != nil:
		for slot := range w.plan.slots - 1 {
			if err := w.dense.put(row.count[slot], row.bytes[slot]); err != nil {
				return 0, err
			}
		}
	case w.multi != nil:
		if err := w.addMulti(row); err != nil {
			return 0, err
		}
	}

	return row.mask, nil
}

func (w *TierStatsWriter) addMulti(row *tierRow) error {
	multi := bits.OnesCount16(row.mask) >= 2
	if err := w.rowBits.add(multi); err != nil {
		return err
	}
	if !multi {
		return nil
	}
	if err := w.offsets.put(w.stored); err != nil {
		return err
	}
	if w.masks != nil {
		if err := w.masks.put(uint64(row.mask)); err != nil {
			return err
		}
	}
	rest := row.mask &^ (1 << (bits.Len16(row.mask) - 1))
	for rest != 0 {
		slot := bits.TrailingZeros16(rest)
		rest &= rest - 1
		if err := w.multi.put(row.count[slot], row.bytes[slot]); err != nil {
			return err
		}
		w.stored++
	}

	return nil
}

// Close finishes the files once every row the plan counted is written.
// On an error every file it started is removed.
func (w *TierStatsWriter) Close() error {
	if w.rows != w.plan.rows || (w.multi != nil && w.stored != w.plan.multiSlots) {
		return errors.Join(fmt.Errorf("%w: wrote %d rows and %d slots, planned %d and %d",
			errTierPlanMismatch, w.rows, w.stored, w.plan.rows, w.plan.multiSlots), w.Abort())
	}
	switch {
	case w.dense != nil:
		err := w.dense.close()
		w.dense = nil

		return err
	case w.multi != nil:
		if err := w.offsets.put(w.stored); err != nil {
			return errors.Join(err, w.Abort())
		}
		paths := []string{w.multi.path, w.offsets.path, w.rowBits.path}
		if w.masks != nil {
			paths = append(paths, w.masks.path)
		}
		err := errors.Join(w.multi.close(), w.offsets.close(), w.rowBits.close(), w.masks.close())
		w.multi, w.offsets, w.rowBits, w.masks = nil, nil, nil, nil
		if err != nil {
			for _, path := range paths {
				err = errors.Join(err, removeIfErr(path))
			}
		}

		return err
	}

	return nil
}

// Abort removes every file the writer started. Idempotent.
func (w *TierStatsWriter) Abort() error {
	var errs []error
	if w.dense != nil {
		errs = append(errs, w.dense.abort())
	}
	if w.multi != nil {
		errs = append(errs, w.multi.abort())
	}
	if w.rowBits != nil {
		errs = append(errs, w.rowBits.abort())
	}
	errs = append(errs, w.offsets.abort(), w.masks.abort())
	w.dense, w.multi, w.offsets, w.rowBits, w.masks = nil, nil, nil, nil, nil

	return errors.Join(errs...)
}

// slotFile writes a tier_multi.bin / tier_dense.bin: Header, slot-width
// descriptor, slots, tail pad.
type slotFile struct {
	path  string
	f     *os.File
	bw    *bufio.Writer
	width tierSlotWidths
}

func createSlotFile(path string, count uint64, rowWidth int, width tierSlotWidths) (*slotFile, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("create %s: %w", filepath.Base(path), err)
	}
	s := &slotFile{path: path, f: f, bw: bufio.NewWriterSize(f, writeBufSize), width: width}
	head := EncodeHeader(Header{Magic: MagicNumber, Version: Version, Count: count, Width: uint32(rowWidth)})
	head = append(head, width.count, width.bytes)
	if _, err := s.bw.Write(head); err != nil {
		return nil, errors.Join(fmt.Errorf("write %s header: %w", filepath.Base(path), err), s.abort())
	}

	return s, nil
}

func (s *slotFile) put(count, bytes uint64) error {
	if count > widthMask[s.width.count] || bytes > widthMask[s.width.bytes] {
		return fmt.Errorf("%w: slot (%d, %d) wider than %d+%d bytes",
			errTierPlanMismatch, count, bytes, s.width.count, s.width.bytes)
	}
	// Each field goes in as a whole word, cut back to its width.
	b := binary.LittleEndian.AppendUint64(s.bw.AvailableBuffer(), count)[:s.width.count]
	b = binary.LittleEndian.AppendUint64(b, bytes)[:int(s.width.count)+int(s.width.bytes)]
	if _, err := s.bw.Write(b); err != nil {
		return fmt.Errorf("write slot: %w", err)
	}

	return nil
}

func (s *slotFile) close() error {
	var pad [coreStatsReadTailPad]byte
	if _, err := s.bw.Write(pad[:]); err != nil {
		return errors.Join(fmt.Errorf("write tail pad: %w", err), s.abort())
	}
	if err := s.bw.Flush(); err != nil {
		return errors.Join(fmt.Errorf("flush %s: %w", filepath.Base(s.path), err), s.abort())
	}
	startWriteback(s.f)
	if err := s.f.Close(); err != nil {
		return errors.Join(fmt.Errorf("close %s: %w", filepath.Base(s.path), err), removeIfErr(s.path))
	}

	return nil
}

func (s *slotFile) abort() error {
	return errors.Join(s.f.Close(), removeIfErr(s.path))
}

// narrowArrayWriter writes an array file of unsigned values at a fixed
// width of 1..8 bytes, with the tail pad readers need.
type narrowArrayWriter struct {
	path  string
	f     *os.File
	bw    *bufio.Writer
	count uint64
	width uint8
	buf   [8]byte
}

func newNarrowArrayWriter(path string, width uint8) (*narrowArrayWriter, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("create %s: %w", filepath.Base(path), err)
	}
	w := &narrowArrayWriter{path: path, f: f, bw: bufio.NewWriterSize(f, writeBufSize), width: width}
	// Placeholder header; close rewrites it with the count.
	if _, err := w.bw.Write(make([]byte, HeaderSize)); err != nil {
		return nil, errors.Join(fmt.Errorf("write header: %w", err), w.abort())
	}

	return w, nil
}

func (w *narrowArrayWriter) put(v uint64) error {
	binary.LittleEndian.PutUint64(w.buf[:], v)
	if _, err := w.bw.Write(w.buf[:w.width]); err != nil {
		return fmt.Errorf("write %s: %w", filepath.Base(w.path), err)
	}
	w.count++

	return nil
}

// close is a no-op on a nil writer, as is abort.
func (w *narrowArrayWriter) close() error {
	if w == nil {
		return nil
	}
	var pad [arrayTailPad]byte
	if _, err := w.bw.Write(pad[:]); err != nil {
		return errors.Join(fmt.Errorf("write tail pad: %w", err), w.abort())
	}
	if err := w.bw.Flush(); err != nil {
		return errors.Join(fmt.Errorf("flush %s: %w", filepath.Base(w.path), err), w.abort())
	}
	head := EncodeHeader(Header{Magic: MagicNumber, Version: Version, Count: w.count, Width: uint32(w.width)})
	if _, err := w.f.WriteAt(head, 0); err != nil {
		return errors.Join(fmt.Errorf("write header: %w", err), w.abort())
	}
	startWriteback(w.f)
	if err := w.f.Close(); err != nil {
		return errors.Join(fmt.Errorf("close %s: %w", filepath.Base(w.path), err), removeIfErr(w.path))
	}

	return nil
}

func (w *narrowArrayWriter) abort() error {
	if w == nil {
		return nil
	}

	return errors.Join(w.f.Close(), removeIfErr(w.path))
}
