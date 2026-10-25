package format

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math/bits"
	"os"
	"path/filepath"

	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

// TierStatsReader reads per-prefix tier breakdowns. It derives what
// the layout doesn't store (see tierstats_write.go) from the prefix's
// core row: its totals and tier mask.
type TierStatsReader struct {
	manifest *tiers.Manifest
	core     *CoreStatsReader
	// One of: nothing (a single tier), dense, or multi + rows + offsets.
	dense   *slotFileReader
	multi   *slotFileReader
	rows    *rankedBitsReader
	offsets *ArrayReader
	// masks holds the multi-tier prefixes' masks when the core tier
	// field is a code (TierMaskCode).
	masks *ArrayReader
	slots int
	enc   TierMaskEncoding
}

// OpenTierStats opens the tier stats of the index in indexDir, whose
// core stats core is already open (the reader keeps using it). Returns
// an empty reader (no tiers) when the index has no tier data, so
// callers can dispatch on HasTierData rather than nil-checking.
func OpenTierStats(indexDir string, core *CoreStatsReader) (*TierStatsReader, error) {
	manifest, err := tiers.ReadManifest(indexDir)
	if err != nil {
		return nil, fmt.Errorf("read tier manifest: %w", err)
	}
	if manifest == nil || len(manifest.Tiers) == 0 {
		return &TierStatsReader{manifest: &tiers.Manifest{}}, nil
	}
	r := &TierStatsReader{manifest: manifest, core: core, slots: len(manifest.Tiers)}
	if r.slots > maxTierSlots {
		return nil, fmt.Errorf("%w: %d tiers", errTierStatsLayout, r.slots)
	}
	if r.slots == 1 {
		return r, nil
	}
	dir := filepath.Join(indexDir, TierStatsDir)
	if _, err := os.Stat(filepath.Join(dir, TierDenseFile)); err == nil {
		r.dense, err = openSlotFile(filepath.Join(dir, TierDenseFile))
		if err != nil {
			return nil, err
		}
		r.enc = TierMaskDerived
		if err := r.checkDense(); err != nil {
			return nil, errors.Join(err, r.Close())
		}

		if err := r.checkTierField(); err != nil {
			return nil, errors.Join(err, r.Close())
		}

		return r, nil
	}
	if err := r.openMulti(dir); err != nil {
		return nil, errors.Join(err, r.Close())
	}
	if err := r.checkTierField(); err != nil {
		return nil, errors.Join(err, r.Close())
	}

	return r, nil
}

func (r *TierStatsReader) checkDense() error {
	wantRow := (r.slots - 1) * r.dense.width.slotBytes()
	if r.dense.count != r.core.Count() || r.dense.rowWidth != wantRow {
		return fmt.Errorf("%w: dense has %d rows of %d bytes, want %d of %d",
			errTierStatsLayout, r.dense.count, r.dense.rowWidth, r.core.Count(), wantRow)
	}

	return nil
}

// checkTierField checks that the core rows' tier field has the width
// the layout's encoding writes.
func (r *TierStatsReader) checkTierField() error {
	width := r.core.Stride().TierMask
	if (r.enc == TierMaskDerived && width != 0) || (r.enc == TierMaskCode && width != 1) {
		return fmt.Errorf("%w: a %d-byte core tier field for encoding %d", errTierStatsLayout, width, r.enc)
	}

	return nil
}

func (r *TierStatsReader) openMulti(dir string) error {
	var err error
	if r.slots >= tierCodeMinSlots {
		r.enc = TierMaskCode
		if r.masks, err = OpenArray(filepath.Join(dir, TierMultiMaskFile)); err != nil {
			return fmt.Errorf("open %s: %w", TierMultiMaskFile, err)
		}
	}
	if r.multi, err = openSlotFile(filepath.Join(dir, TierMultiFile)); err != nil {
		return err
	}
	if r.rows, err = openRankedBits(filepath.Join(dir, TierMultiRowsFile)); err != nil {
		return err
	}
	if r.offsets, err = OpenArray(filepath.Join(dir, TierMultiOffsetsFile)); err != nil {
		return fmt.Errorf("open %s: %w", TierMultiOffsetsFile, err)
	}
	if r.rows.n != r.core.Count() {
		return fmt.Errorf("%w: %s has %d rows, index has %d", errTierStatsLayout, TierMultiRowsFile, r.rows.n, r.core.Count())
	}
	multiRows := r.rows.rank(r.rows.n)
	if r.offsets.Count() != multiRows+1 {
		return fmt.Errorf("%w: %d offsets for %d multi-tier rows", errTierStatsLayout, r.offsets.Count(), multiRows)
	}
	if last := r.offsets.UnsafeGetU64(multiRows); last != r.multi.count {
		return fmt.Errorf("%w: offsets end at slot %d of %d", errTierStatsLayout, last, r.multi.count)
	}
	if r.masks != nil && r.masks.Count() != multiRows {
		return fmt.Errorf("%w: %d masks for %d multi-tier rows", errTierStatsLayout, r.masks.Count(), multiRows)
	}

	return nil
}

// TierBreakdown represents statistics for a single tier.
type TierBreakdown struct {
	TierName    string
	Bytes       uint64
	ObjectCount uint64
	TierID      tiers.ID
}

// Breakdown returns the tier breakdown for the given preorder
// position, including only tiers with non-zero data.
func (r *TierStatsReader) Breakdown(pos uint64) []TierBreakdown {
	return r.appendBreakdown(nil, pos, true)
}

// AppendBreakdown appends Breakdown(pos) to dst and returns the
// extended slice, so a caller reading many positions can reuse one
// buffer instead of allocating per position.
func (r *TierStatsReader) AppendBreakdown(dst []TierBreakdown, pos uint64) []TierBreakdown {
	return r.appendBreakdown(dst, pos, true)
}

// BreakdownAll returns the tier breakdown for all present tiers
// at the given preorder position, including zeros.
func (r *TierStatsReader) BreakdownAll(pos uint64) []TierBreakdown {
	return r.appendBreakdown(nil, pos, false)
}

// appendBreakdown appends pos's tiers to dst. A nil dst gets one
// allocation of the exact size; the slots decode into arrays on the
// stack.
func (r *TierStatsReader) appendBreakdown(dst []TierBreakdown, pos uint64, nonZeroOnly bool) []TierBreakdown {
	if !r.HasTierData() {
		return dst
	}
	if pos >= r.core.Count() {
		if dst == nil {
			dst = make([]TierBreakdown, 0, r.slots)
		}

		return dst
	}
	var counts, bytesArr [maxTierSlots]uint64
	mask := r.fill(pos, &counts, &bytesArr)
	if dst == nil {
		n := r.slots
		if nonZeroOnly {
			n = bits.OnesCount16(mask)
		}
		dst = make([]TierBreakdown, 0, n)
	}
	for slot, tier := range r.manifest.Tiers {
		if nonZeroOnly && mask&(1<<slot) == 0 {
			continue
		}
		dst = append(dst, TierBreakdown{
			TierID:      tier.ID,
			TierName:    tier.Name,
			Bytes:       bytesArr[slot],
			ObjectCount: counts[slot],
		})
	}

	return dst
}

// fill decodes pos's per-slot stats and returns its tier mask.
func (r *TierStatsReader) fill(pos uint64, counts, bytesArr *[maxTierSlots]uint64) uint16 {
	field := r.core.UnsafeTierField(pos)
	if field == 0 {
		return 0
	}
	if r.dense != nil {
		return r.fillDense(pos, counts, bytesArr)
	}
	totalCount, totalBytes := r.core.UnsafeStats(pos)
	mask, rank, ranked := field, uint64(0), false
	if r.enc == TierMaskCode {
		if field != tierCodeSeveral {
			mask = 1 << (field - 1)
		} else {
			rank, ranked = r.rows.rank(pos), true
			mask = uint16(r.masks.UnsafeGetU64(rank))
		}
	}
	derived := bits.Len16(mask) - 1
	var sumCount, sumBytes uint64
	if bits.OnesCount16(mask) >= 2 {
		if !ranked {
			rank = r.rows.rank(pos)
		}
		i := r.offsets.UnsafeGetU64(rank)
		stored := mask &^ (1 << derived)
		for stored != 0 {
			slot := bits.TrailingZeros16(stored)
			stored &= stored - 1
			c, b := r.multi.slot(i * uint64(r.multi.width.slotBytes()))
			counts[slot], bytesArr[slot] = c, b
			sumCount += c
			sumBytes += b
			i++
		}
	}
	counts[derived], bytesArr[derived] = totalCount-sumCount, totalBytes-sumBytes

	return mask
}

// fillDense decodes a dense row, whose mask is the slots holding data.
func (r *TierStatsReader) fillDense(pos uint64, counts, bytesArr *[maxTierSlots]uint64) uint16 {
	totalCount, totalBytes := r.core.UnsafeStats(pos)
	var mask uint16
	var sumCount, sumBytes uint64
	last := r.slots - 1
	for slot := range last {
		c, b := r.dense.slot(pos*uint64(r.dense.rowWidth) + uint64(slot*r.dense.width.slotBytes()))
		counts[slot], bytesArr[slot] = c, b
		if c != 0 || b != 0 {
			mask |= 1 << slot
		}
		sumCount += c
		sumBytes += b
	}
	counts[last], bytesArr[last] = totalCount-sumCount, totalBytes-sumBytes
	if counts[last] != 0 || bytesArr[last] != 0 {
		mask |= 1 << last
	}

	return mask
}

// HasTierData reports whether any tier data is available.
func (r *TierStatsReader) HasTierData() bool {
	return r != nil && r.manifest != nil && len(r.manifest.Tiers) > 0
}

// Close releases the mapped files. The core stats reader is the
// caller's to close.
func (r *TierStatsReader) Close() error {
	if r == nil {
		return nil
	}
	err := errors.Join(r.dense.close(), r.multi.close(), r.rows.close(), r.offsets.Close(), r.masks.Close())
	r.dense, r.multi, r.rows, r.offsets, r.masks = nil, nil, nil, nil, nil

	return err
}

// slotFileReader reads tier_dense.bin or tier_multi.bin.
type slotFileReader struct {
	mmap     *MmapFile
	count    uint64 // rows (dense) or slots (multi)
	rowWidth int
	width    tierSlotWidths
}

func openSlotFile(path string) (*slotFileReader, error) {
	name := filepath.Base(path)
	mmap, err := OpenMmap(path)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", name, err)
	}
	r, err := newSlotFileReader(mmap)
	if err != nil {
		mmap.Close()

		return nil, fmt.Errorf("%s: %w", name, err)
	}

	return r, nil
}

func newSlotFileReader(mmap *MmapFile) (*slotFileReader, error) {
	data := mmap.Data()
	if len(data) < tierSlotDataOff {
		return nil, ErrInvalidHeader
	}
	header, err := DecodeHeader(data[:HeaderSize])
	if err != nil {
		return nil, fmt.Errorf("decode header: %w", err)
	}
	if header.Magic != MagicNumber {
		return nil, ErrMagicMismatch
	}
	if header.Version != Version {
		return nil, ErrVersionMismatch
	}
	w := tierSlotWidths{count: data[HeaderSize], bytes: data[HeaderSize+1]}
	if w.count < 1 || w.count > 8 || w.bytes < 1 || w.bytes > 8 {
		return nil, fmt.Errorf("%w: slot widths %d, %d", errTierStatsLayout, w.count, w.bytes)
	}
	want := int64(tierSlotDataOff) + int64(header.Count)*int64(header.Width) + coreStatsReadTailPad
	if int64(len(data)) < want {
		return nil, fmt.Errorf("%w: %d < %d", ErrFileTooSmall, len(data), want)
	}

	return &slotFileReader{mmap: mmap, count: header.Count, rowWidth: int(header.Width), width: w}, nil
}

// slot decodes the slot at byte offset off of the slot data.
//
//nolint:gocritic // gocritic wants named returns, nonamedreturns forbids them
func (r *slotFileReader) slot(off uint64) (uint64, uint64) {
	data := r.mmap.Data()[tierSlotDataOff+off:]
	c := binary.LittleEndian.Uint64(data) & widthMask[r.width.count]
	b := binary.LittleEndian.Uint64(data[r.width.count:]) & widthMask[r.width.bytes]

	return c, b
}

func (r *slotFileReader) close() error {
	if r == nil {
		return nil
	}

	return r.mmap.Close()
}
