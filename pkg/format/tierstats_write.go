package format

import (
	"bufio"
	"cmp"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
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

	// Every prefix's slots sit in tierScratchFile while the build
	// streams (its slot mask, then a uvarint count and bytes for each set
	// slot); Close converts it to the chosen layout and removes it.
	tierScratchFile = "tier_scratch.bin"
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

// maxScratchRowBytes bounds one scratch row: the mask and two uvarints
// per slot.
const maxScratchRowBytes = 2 + 2*maxTierSlots*binary.MaxVarintLen64

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

// TierStatsWriter streams per-prefix tier stats during a build and, on
// Close, writes them in the smaller layout. Add returns the prefix's
// tier mask for its core row.
type TierStatsWriter struct {
	dir         string
	scratchPath string
	scratch     *os.File
	w           *bufio.Writer
	rowBuf      []byte
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
	// force overrides the size-based choice; tests only.
	force tierLayout
}

// tierLayout names a stored layout.
type tierLayout int

const (
	tierLayoutAuto tierLayout = iota
	tierLayoutDense
	tierLayoutMulti
)

// choose returns the layout whose files and core tier fields together
// are smaller; dense on a tie (no indirection).
func (w *TierStatsWriter) choose() tierLayout {
	if w.force != tierLayoutAuto {
		return w.force
	}
	if w.denseSize() <= w.multiSize()+w.multiMaskSize() {
		return tierLayoutDense
	}

	return tierLayoutMulti
}

// MaskEncoding returns how the core rows' tier field encodes the mask
// for the layout Close will write. Call it after the last Add.
func (w *TierStatsWriter) MaskEncoding() TierMaskEncoding {
	switch {
	case w.slots <= 1:
		return TierMaskPlain
	case w.choose() == tierLayoutDense:
		return TierMaskDerived
	case w.slots >= tierCodeMinSlots:
		return TierMaskCode
	default:
		return TierMaskPlain
	}
}

// multiMaskSize is what the multi layout's masks cost: every core row's
// tier field, plus tier_multi.mask when the field is a code.
func (w *TierStatsWriter) multiMaskSize() uint64 {
	if w.slots < tierCodeMinSlots {
		return w.rows * uint64(tierMaskWidth(w.maxMask))
	}

	return w.rows + uint64(HeaderSize) + w.multiRows*tierMultiMaskWidth + arrayTailPad
}

// NewTierStatsWriter creates a writer for the given present tiers (the
// tiers with data anywhere in the index), which become tier slots in
// tier-ID order. With one present tier nothing is written to disk.
func NewTierStatsWriter(outDir string, present []tiers.ID) (*TierStatsWriter, error) {
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
	w := &TierStatsWriter{
		dir:   filepath.Join(outDir, TierStatsDir),
		slots: len(sorted),
	}
	for tid := range w.slotByTier {
		w.slotByTier[tid] = slices.Index(sorted, tiers.ID(tid))
	}
	if w.slots == 1 {
		return w, nil
	}
	if err := os.MkdirAll(w.dir, DirPerm); err != nil {
		return nil, fmt.Errorf("create tier_stats dir: %w", err)
	}
	w.scratchPath = filepath.Join(w.dir, tierScratchFile)
	f, err := os.Create(w.scratchPath)
	if err != nil {
		return nil, fmt.Errorf("create tier stats scratch: %w", err)
	}
	w.scratch = f
	w.w = bufio.NewWriterSize(f, writeBufSize)
	w.rowBuf = make([]byte, 0, maxScratchRowBytes)

	return w, nil
}

// writeBufSize is the buffered-writer size for the scratch file.
const writeBufSize = 1 << 20

// Add records the next prefix's tier stats, indexed by tier ID, and
// returns its slot mask. Only the tiers in tierMask (bit i for tier i)
// are read. It fails when a tier outside the declared set has data (the
// counts would be lost) or when the tiers don't sum to the prefix's
// totals (a derived tier would be wrong, or a tier with data was left
// out of tierMask).
func (w *TierStatsWriter) Add(tierMask uint16, counts, bytes *[tiers.NumTiers]uint64, totalCount, totalBytes uint64) (uint16, error) {
	if tierMask>>tiers.NumTiers != 0 {
		return 0, fmt.Errorf("%w: tier mask %#x", errUndeclaredTierData, tierMask)
	}
	var mask uint16
	var slotCount, slotBytes [maxTierSlots]uint64
	var sumCount, sumBytes, carryCount, carryBytes uint64
	for set := tierMask; set != 0; set &= set - 1 {
		id := bits.TrailingZeros16(set)
		c, b := counts[id], bytes[id]
		if c == 0 && b == 0 {
			continue
		}
		slot := w.slotByTier[id]
		if slot < 0 {
			return 0, fmt.Errorf("%w: tier %d", errUndeclaredTierData, id)
		}
		mask |= 1 << slot
		slotCount[slot], slotBytes[slot] = c, b
		var cc, cb uint64
		sumCount, cc = bits.Add64(sumCount, c, 0)
		sumBytes, cb = bits.Add64(sumBytes, b, 0)
		carryCount |= cc
		carryBytes |= cb
	}
	if carryCount != 0 || carryBytes != 0 || sumCount != totalCount || sumBytes != totalBytes {
		return 0, fmt.Errorf("%w: tiers %d objects / %d bytes, totals %d / %d",
			errTierSumMismatch, sumCount, sumBytes, totalCount, totalBytes)
	}
	w.rows++
	if w.slots == 1 {
		return mask, nil
	}
	w.maxMask = max(w.maxMask, mask)

	if n := bits.OnesCount16(mask); n >= 2 {
		w.multiRows++
		w.multiSlots += uint64(n - 1)
		stored := mask &^ (1 << (bits.Len16(mask) - 1))
		for stored != 0 {
			slot := bits.TrailingZeros16(stored)
			stored &= stored - 1
			w.multiBits.add(slotCount[slot], slotBytes[slot])
		}
	}
	for slot := range w.slots - 1 {
		w.denseBits.add(slotCount[slot], slotBytes[slot])
	}
	row := binary.LittleEndian.AppendUint16(w.rowBuf[:0], mask)
	for set := mask; set != 0; set &= set - 1 {
		slot := bits.TrailingZeros16(set)
		row = binary.AppendUvarint(row, slotCount[slot])
		row = binary.AppendUvarint(row, slotBytes[slot])
	}
	if _, err := w.w.Write(row); err != nil {
		return 0, fmt.Errorf("write tier stats scratch: %w", err)
	}

	return mask, nil
}

// multiSize and denseSize are the exact on-disk sizes of each layout.
func (w *TierStatsWriter) multiSize() uint64 {
	slotsFile := uint64(tierSlotDataOff) + w.multiSlots*uint64(w.multiBits.widths().slotBytes()) + coreStatsReadTailPad
	words := bitWords(w.rows)
	rowsFile := uint64(HeaderSize) + 8*words + 8*((words+rankBlockWords-1)/rankBlockWords)
	offsetsFile := uint64(HeaderSize) + (w.multiRows+1)*uint64(byteWidthOf(w.multiSlots)) + arrayTailPad

	return slotsFile + rowsFile + offsetsFile
}

func (w *TierStatsWriter) denseSize() uint64 {
	return uint64(tierSlotDataOff) + w.rows*uint64(w.slots-1)*uint64(w.denseBits.widths().slotBytes()) + coreStatsReadTailPad
}

// Close writes the smaller layout and removes the scratch file. On an
// error every file it started is removed.
func (w *TierStatsWriter) Close() error {
	if w.scratch == nil {
		return nil
	}
	err := w.w.Flush()
	err = errors.Join(err, w.scratch.Close())
	w.scratch = nil
	if err == nil {
		if w.choose() == tierLayoutDense {
			err = w.writeDense()
		} else {
			err = w.writeMulti()
		}
	}

	return errors.Join(err, removeIfErr(w.scratchPath))
}

// Abort removes the scratch file without writing a layout.
func (w *TierStatsWriter) Abort() error {
	if w.scratch == nil {
		return nil
	}
	err := w.scratch.Close()
	w.scratch = nil

	return errors.Join(err, removeIfErr(w.scratchPath))
}

// scratchRow is one prefix's slots read back from the scratch file:
// mask holds its set slots, and the others' count and bytes are zero.
type scratchRow struct {
	count, bytes [maxTierSlots]uint64
	mask         uint16
}

// scratchRows calls fn with each prefix's slots from the scratch file.
func (w *TierStatsWriter) scratchRows(fn func(row *scratchRow) error) error {
	f, err := os.Open(w.scratchPath)
	if err != nil {
		return fmt.Errorf("open tier stats scratch: %w", err)
	}
	defer f.Close()
	r := bufio.NewReaderSize(f, writeBufSize)
	var row scratchRow
	for range w.rows {
		for set := row.mask; set != 0; set &= set - 1 {
			slot := bits.TrailingZeros16(set)
			row.count[slot], row.bytes[slot] = 0, 0
		}
		// Decode the row in place from the read buffer: varints read a
		// byte per call through the reader were most of this pass.
		buf, err := r.Peek(min(maxScratchRowBytes, r.Size()))
		if len(buf) < 2 {
			return fmt.Errorf("read tier stats scratch: %w", cmp.Or(err, io.ErrUnexpectedEOF))
		}
		row.mask = binary.LittleEndian.Uint16(buf)
		off := 2
		for set := row.mask; set != 0; set &= set - 1 {
			slot := bits.TrailingZeros16(set)
			c, n := binary.Uvarint(buf[off:])
			if n <= 0 {
				return fmt.Errorf("read tier stats scratch: %w", errTierScratchCorrupt)
			}
			b, m := binary.Uvarint(buf[off+n:])
			if m <= 0 {
				return fmt.Errorf("read tier stats scratch: %w", errTierScratchCorrupt)
			}
			row.count[slot], row.bytes[slot] = c, b
			off += n + m
		}
		if _, err := r.Discard(off); err != nil {
			return fmt.Errorf("read tier stats scratch: %w", err)
		}
		if err := fn(&row); err != nil {
			return err
		}
	}

	return nil
}

// errTierScratchCorrupt means a scratch row is cut short or holds an
// overlong varint.
var errTierScratchCorrupt = errors.New("tier stats scratch row corrupt")

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
	if err := s.f.Close(); err != nil {
		return errors.Join(fmt.Errorf("close %s: %w", filepath.Base(s.path), err), removeIfErr(s.path))
	}

	return nil
}

func (s *slotFile) abort() error {
	return errors.Join(s.f.Close(), removeIfErr(s.path))
}

func (w *TierStatsWriter) writeDense() error {
	path := filepath.Join(w.dir, TierDenseFile)
	out, err := createSlotFile(path, w.rows, (w.slots-1)*w.denseBits.widths().slotBytes(), w.denseBits.widths())
	if err != nil {
		return err
	}
	err = w.scratchRows(func(row *scratchRow) error {
		for slot := range w.slots - 1 {
			if err := out.put(row.count[slot], row.bytes[slot]); err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil {
		return errors.Join(err, out.abort())
	}

	return out.close()
}

func (w *TierStatsWriter) writeMulti() error {
	slotsPath := filepath.Join(w.dir, TierMultiFile)
	out, err := createSlotFile(slotsPath, w.multiSlots, w.multiBits.widths().slotBytes(), w.multiBits.widths())
	if err != nil {
		return err
	}
	offsetsPath := filepath.Join(w.dir, TierMultiOffsetsFile)
	offW := byteWidthOf(w.multiSlots)
	offsets, err := newNarrowArrayWriter(offsetsPath, offW)
	if err != nil {
		return errors.Join(err, out.abort())
	}
	rowsPath := filepath.Join(w.dir, TierMultiRowsFile)
	rowBits, err := newRankedBitsWriter(rowsPath)
	if err != nil {
		return errors.Join(err, out.abort(), offsets.abort())
	}
	masksPath := filepath.Join(w.dir, TierMultiMaskFile)
	var masks *narrowArrayWriter
	if w.slots >= tierCodeMinSlots {
		if masks, err = newNarrowArrayWriter(masksPath, tierMultiMaskWidth); err != nil {
			return errors.Join(err, out.abort(), offsets.abort(), rowBits.abort())
		}
	}
	var stored uint64
	err = w.scratchRows(func(r *scratchRow) error {
		mask := r.mask
		multi := bits.OnesCount16(mask) >= 2
		if err := rowBits.add(multi); err != nil {
			return err
		}
		if multi {
			if err := offsets.put(stored); err != nil {
				return err
			}
			if masks != nil {
				if err := masks.put(uint64(mask)); err != nil {
					return err
				}
			}
			rest := mask &^ (1 << (bits.Len16(mask) - 1))
			for rest != 0 {
				slot := bits.TrailingZeros16(rest)
				rest &= rest - 1
				if err := out.put(r.count[slot], r.bytes[slot]); err != nil {
					return err
				}
				stored++
			}
		}

		return nil
	})
	if err == nil {
		err = offsets.put(stored)
	}
	if err != nil {
		return errors.Join(err, out.abort(), offsets.abort(), rowBits.abort(), masks.abort())
	}
	if err := errors.Join(out.close(), offsets.close(), rowBits.close(), masks.close()); err != nil {
		return errors.Join(err, removeIfErr(slotsPath), removeIfErr(offsetsPath), removeIfErr(rowsPath), removeIfErr(masksPath))
	}

	return nil
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
