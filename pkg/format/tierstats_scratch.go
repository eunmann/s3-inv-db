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

// Every prefix's slots sit in tierScratchFile while the build streams
// (its slot mask, then a uvarint count and bytes for each set slot);
// Close converts it to the chosen layout and removes it.
const tierScratchFile = "tier_scratch.bin"

// maxScratchRowBytes bounds one scratch row: the mask and two uvarints
// per slot.
const maxScratchRowBytes = 2 + 2*maxTierSlots*binary.MaxVarintLen64

// TierStatsScratchWriter streams per-prefix tier stats during a build and, on
// Close, writes them in the smaller layout. Add returns the prefix's
// tier mask for its core row.
type TierStatsScratchWriter struct {
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

// choose returns the layout whose files and core tier fields together
// are smaller; dense on a tie (no indirection).
func (w *TierStatsScratchWriter) choose() tierLayout {
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
func (w *TierStatsScratchWriter) MaskEncoding() TierMaskEncoding {
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
func (w *TierStatsScratchWriter) multiMaskSize() uint64 {
	if w.slots < tierCodeMinSlots {
		return w.rows * uint64(tierMaskWidth(w.maxMask))
	}

	return w.rows + uint64(HeaderSize) + w.multiRows*tierMultiMaskWidth + arrayTailPad
}

// NewTierStatsScratchWriter creates a writer for the given present tiers (the
// tiers with data anywhere in the index), which become tier slots in
// tier-ID order. With one present tier nothing is written to disk.
func NewTierStatsScratchWriter(outDir string, present []tiers.ID) (*TierStatsScratchWriter, error) {
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
	w := &TierStatsScratchWriter{
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

// Add records the next prefix's tier stats, indexed by tier ID, and
// returns its tier mask. It fails when a tier outside the declared set
// has data (the counts would be lost) or when the tiers don't sum to
// the prefix's totals (a derived tier would be wrong).
func (w *TierStatsScratchWriter) Add(counts, bytes *[tiers.NumTiers]uint64, totalCount, totalBytes uint64) (uint16, error) {
	var mask uint16
	var slotCount, slotBytes [maxTierSlots]uint64
	var sumCount, sumBytes, carryCount, carryBytes uint64
	for id := range tiers.NumTiers {
		c, b := counts[id], bytes[id]
		if c == 0 && b == 0 {
			continue
		}
		slot := w.slotByTier[id]
		if slot < 0 {
			return 0, fmt.Errorf("%w: tier %d", ErrUndeclaredTierData, id)
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
func (w *TierStatsScratchWriter) multiSize() uint64 {
	slotsFile := uint64(tierSlotDataOff) + w.multiSlots*uint64(w.multiBits.widths().slotBytes()) + coreStatsReadTailPad
	words := bitWords(w.rows)
	rowsFile := uint64(HeaderSize) + 8*words + 8*((words+rankBlockWords-1)/rankBlockWords)
	offsetsFile := uint64(HeaderSize) + (w.multiRows+1)*uint64(byteWidthOf(w.multiSlots)) + arrayTailPad

	return slotsFile + rowsFile + offsetsFile
}

func (w *TierStatsScratchWriter) denseSize() uint64 {
	return uint64(tierSlotDataOff) + w.rows*uint64(w.slots-1)*uint64(w.denseBits.widths().slotBytes()) + coreStatsReadTailPad
}

// Close writes the smaller layout and removes the scratch file. On an
// error every file it started is removed.
func (w *TierStatsScratchWriter) Close() error {
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
func (w *TierStatsScratchWriter) Abort() error {
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
func (w *TierStatsScratchWriter) scratchRows(fn func(row *scratchRow) error) error {
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

func (w *TierStatsScratchWriter) writeDense() error {
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

func (w *TierStatsScratchWriter) writeMulti() error {
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
