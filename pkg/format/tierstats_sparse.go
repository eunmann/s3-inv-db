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

// TierStatsSparseFile holds the per-row variable-length tier stats
// data when the builder picks the sparse layout. Each row is:
//
//	2 bytes  presence bitmap (uint16; bit i set ⇒ manifest slot i
//	         carries a non-zero (count, bytes) pair in this row)
//	16×popcnt(bitmap)  packed (count u64, bytes u64) pairs in
//	                   manifest order — only the populated ones
const TierStatsSparseFile = "tier_stats_sparse.bin"

// TierStatsSparseOffsetsFile is the N+1 array of byte offsets into
// TierStatsSparseFile. Offsets[i] is the start of row i's data;
// offsets[N] is the trailing sentinel = total row bytes. Honours the
// adaptive-width path of ArrayWriter so the file shrinks for small
// indexes.
const TierStatsSparseOffsetsFile = "tier_stats_sparse.off.u64"

// tierStatsSparseBitmapBytes is the byte width of the per-row
// presence bitmap. Uint16 supports up to 16 present-tier slots; the
// project's maximum (13 storage classes) fits.
const tierStatsSparseBitmapBytes = 2

// Sentinel errors.
var (
	errSparseBitmapWidth  = errors.New("tier stats sparse: presence bitmap too narrow for present-tier count")
	errSparseOffsetsLen   = errors.New("tier stats sparse: offsets file length mismatch")
	errSparseSizeMismatch = errors.New("tier stats sparse: written size mismatch")
)

// tierStatsSparseShouldUse decides whether the sparse layout beats
// the dense one for an inventory with the given totals.
//
//	dense cost:  presentTiers × 16 × N
//	row body:    2 × N + 16 × populatedSum     (bitmap + populated cells)
//	max offset:  row body bytes                 (sentinel at end)
//	sparse cost: row body + (N+1) × byteWidthOf(row body)
//
// Using the EXACT offset width — RepackArrayWidthU64 will shrink the
// offsets file to byteWidthOf(maxOffset) at Close, so the heuristic
// should reflect what's actually going to land on disk. An earlier
// version of this function used a conservative 8-byte offset width,
// which biased the decision toward dense and left disk savings on
// the table for shapes near the break-even line.
func tierStatsSparseShouldUse(presentTiers int, n, populatedSum uint64) bool {
	if presentTiers <= 1 {
		// One-slot rows: dense has no waste, sparse adds ≥2B bitmap
		// per row and an offsets file — always worse.
		return false
	}
	rowBodyBytes := uint64(tierStatsSparseBitmapBytes)*n + tierStatsSlotBytes64*populatedSum
	offsetWidth := uint64(byteWidthOf(rowBodyBytes))
	denseCost := uint64(presentTiers) * tierStatsSlotBytes64 * n
	sparseCost := rowBodyBytes + (n+1)*offsetWidth

	return sparseCost < denseCost
}

const tierStatsSlotBytes64 = uint64(TierStatsSlotBytes)

// convertDenseToSparse streams the dense tier-stats file at densePath
// into a sparse pair under tier_stats/ and removes the dense file on
// success.
//
// Crash safety: the sparse rows file is renamed to its final name
// LAST among the things a reader looks at, after the offsets file is
// already in place. TierStatsSparsePresent only returns true once
// the rows file is at the canonical path, and the only path through
// OpenTierStats that selects sparse requires the rows file to be
// present. The reader's invariant therefore holds even if the
// process is killed at any point during conversion:
//
//	state                           reader chooses
//	─────────────────────────────────────────────
//	rows .tmp only                  dense (rows.bin missing)
//	offsets final, rows .tmp        dense
//	offsets final + rows final      sparse  ✓
//	offsets final + rows final, no dense   sparse  ✓
//
// The dense file is only removed after the sparse pair is fully in
// place; a crash before that step leaves an orphan dense file (the
// reader still picks sparse, no correctness issue, just wasted bytes
// the next build will overwrite).
func convertDenseToSparse(densePath string, slotCount int, n, populatedSum uint64) error {
	if slotCount > tierStatsSparseBitmapBytes*8 {
		return fmt.Errorf("%w: %d slots", errSparseBitmapWidth, slotCount)
	}

	tierDir := filepath.Dir(densePath)
	sparsePath := filepath.Join(tierDir, TierStatsSparseFile)
	offsetsPath := filepath.Join(tierDir, TierStatsSparseOffsetsFile)
	sparseTmp := sparsePath + ".tmp"
	offsetsTmp := offsetsPath + ".tmp"

	written, err := streamSparseFromDense(densePath, sparseTmp, offsetsTmp, slotCount, n)
	if err != nil {
		_ = os.Remove(sparseTmp)
		_ = os.Remove(offsetsTmp)

		return err
	}
	expected := uint64(tierStatsSparseBitmapBytes)*n + tierStatsSlotBytes64*populatedSum
	if written != expected {
		_ = os.Remove(sparseTmp)
		_ = os.Remove(offsetsTmp)

		return fmt.Errorf("%w: wrote=%d expected=%d", errSparseSizeMismatch, written, expected)
	}

	// Step 1: repack offsets into the FINAL path. The sparse rows
	// file is still at sparseTmp; tierStatsSparsePresent therefore
	// returns false, and the reader keeps using dense.
	offWidth := byteWidthOf(written)
	if err := RepackArrayWidthU64(offsetsTmp, offsetsPath, offWidth); err != nil {
		_ = os.Remove(sparseTmp)
		_ = os.Remove(offsetsTmp)

		return fmt.Errorf("repack offsets to width %d: %w", offWidth, err)
	}
	if err := os.Remove(offsetsTmp); err != nil && !os.IsNotExist(err) {
		_ = os.Remove(sparseTmp)
		_ = os.Remove(offsetsPath)

		return fmt.Errorf("remove offsets scratch: %w", err)
	}

	// Step 2: rename the rows file into its final path. This is the
	// switch-over point — once it returns, tierStatsSparsePresent is
	// true and the reader will route to the sparse path. The
	// offsets file is already at its final path, so OpenTierStatsSparse
	// has everything it needs.
	if err := os.Rename(sparseTmp, sparsePath); err != nil {
		_ = os.Remove(sparseTmp)
		_ = os.Remove(offsetsPath)

		return fmt.Errorf("rename sparse: %w", err)
	}

	// Step 3: drop the dense file. A crash here leaves an orphan
	// dense file but the reader correctly chooses sparse.
	if err := os.Remove(densePath); err != nil {
		return fmt.Errorf("remove dense: %w", err)
	}

	return nil
}

// streamSparseFromDense reads densePath row-by-row and emits the
// sparse-rows file (sparseTmp) and the wide offsets scratch file
// (offsetsTmp). Returns the total byte length of the rows region (so
// the caller can sanity-check against the expected populatedSum
// budget and pick an offset-array width).
func streamSparseFromDense(densePath, sparseTmp, offsetsTmp string, slotCount int, n uint64) (uint64, error) {
	dense, err := os.Open(densePath)
	if err != nil {
		return 0, fmt.Errorf("open dense: %w", err)
	}
	defer dense.Close()
	if _, err := dense.Seek(int64(HeaderSize), 0); err != nil {
		return 0, fmt.Errorf("seek dense data: %w", err)
	}

	sf, err := os.Create(sparseTmp)
	if err != nil {
		return 0, fmt.Errorf("create sparse: %w", err)
	}
	if _, err := sf.Write(EncodeHeader(Header{
		Magic:   MagicNumber,
		Version: Version,
		Count:   n,
		Width:   0,
	})); err != nil {
		_ = sf.Close()

		return 0, fmt.Errorf("write sparse header: %w", err)
	}
	bw := bufio.NewWriterSize(sf, writeBufSize)

	ow, err := NewArrayWriter(offsetsTmp, 8)
	if err != nil {
		_ = sf.Close()

		return 0, fmt.Errorf("create offsets scratch: %w", err)
	}

	denseRow := make([]byte, slotCount*TierStatsSlotBytes)
	off := uint64(0)
	for row := range n {
		if err := ow.WriteU64(off); err != nil {
			_ = sf.Close()
			_ = ow.Close()

			return 0, fmt.Errorf("write offset row %d: %w", row, err)
		}
		if _, err := dense.Read(denseRow); err != nil {
			_ = sf.Close()
			_ = ow.Close()

			return 0, fmt.Errorf("read dense row %d: %w", row, err)
		}
		written, err := writeSparseRow(bw, denseRow, slotCount)
		if err != nil {
			_ = sf.Close()
			_ = ow.Close()

			return 0, fmt.Errorf("write sparse row %d: %w", row, err)
		}
		off += written
	}
	if err := ow.WriteU64(off); err != nil {
		_ = sf.Close()
		_ = ow.Close()

		return 0, fmt.Errorf("write sentinel offset: %w", err)
	}
	if err := bw.Flush(); err != nil {
		_ = sf.Close()
		_ = ow.Close()

		return 0, fmt.Errorf("flush sparse: %w", err)
	}
	if err := sf.Sync(); err != nil {
		_ = sf.Close()
		_ = ow.Close()

		return 0, fmt.Errorf("sync sparse: %w", err)
	}
	if err := sf.Close(); err != nil {
		_ = ow.Close()

		return 0, fmt.Errorf("close sparse: %w", err)
	}
	if err := ow.Close(); err != nil {
		return 0, fmt.Errorf("close offsets scratch: %w", err)
	}

	return off, nil
}

// writeSparseRow emits one row's bitmap + populated (count, bytes)
// pairs to bw. Returns the byte count written.
func writeSparseRow(bw *bufio.Writer, denseRow []byte, slotCount int) (uint64, error) {
	bitmap := uint16(0)
	for slot := range slotCount {
		base := slot * TierStatsSlotBytes
		count := binary.LittleEndian.Uint64(denseRow[base : base+8])
		b := binary.LittleEndian.Uint64(denseRow[base+8 : base+16])
		if count != 0 || b != 0 {
			bitmap |= 1 << slot
		}
	}
	var bitmapBuf [tierStatsSparseBitmapBytes]byte
	binary.LittleEndian.PutUint16(bitmapBuf[:], bitmap)
	if _, err := bw.Write(bitmapBuf[:]); err != nil {
		return 0, fmt.Errorf("bitmap: %w", err)
	}
	written := uint64(tierStatsSparseBitmapBytes)
	for slot := range slotCount {
		if bitmap&(1<<slot) == 0 {
			continue
		}
		base := slot * TierStatsSlotBytes
		if _, err := bw.Write(denseRow[base : base+TierStatsSlotBytes]); err != nil {
			return 0, fmt.Errorf("slot %d: %w", slot, err)
		}
		written += tierStatsSlotBytes64
	}

	return written, nil
}

// TierStatsSparseReader reads the bitmap + variable-length per-row
// layout selected when sparse beats dense at build time.
type TierStatsSparseReader struct {
	mmap     *MmapFile
	dataOff  int64
	offsets  *ArrayReader
	rowCount uint64
	// slotCount must be supplied by OpenTierStats from the manifest.
	slotCount int
}

// OpenTierStatsSparse opens the sparse pair if both files are
// present. Caller supplies slotCount because it comes from the tier
// manifest, not from anything stored in the sparse file (the bitmap
// width is fixed at 2 bytes and is independent of how many of those
// bits are actually meaningful).
func OpenTierStatsSparse(indexDir string, slotCount int) (*TierStatsSparseReader, error) {
	if slotCount > tierStatsSparseBitmapBytes*8 {
		return nil, fmt.Errorf("%w: %d slots", errSparseBitmapWidth, slotCount)
	}

	rowsPath := filepath.Join(indexDir, TierStatsDir, TierStatsSparseFile)
	offsetsPath := filepath.Join(indexDir, TierStatsDir, TierStatsSparseOffsetsFile)

	mmap, err := OpenMmapWithHint(rowsPath, AccessHintRandom)
	if err != nil {
		return nil, fmt.Errorf("open sparse rows mmap: %w", err)
	}
	if mmap.Size() < int64(HeaderSize) {
		mmap.Close()

		return nil, fmt.Errorf("sparse rows: %w", ErrInvalidHeader)
	}
	header, err := DecodeHeader(mmap.Data()[:HeaderSize])
	if err != nil {
		mmap.Close()

		return nil, fmt.Errorf("decode sparse rows header: %w", err)
	}
	if header.Magic != MagicNumber {
		mmap.Close()

		return nil, ErrMagicMismatch
	}
	if header.Version != Version {
		mmap.Close()

		return nil, ErrVersionMismatch
	}

	offsets, err := OpenArrayWithHint(offsetsPath, AccessHintRandom)
	if err != nil {
		mmap.Close()

		return nil, fmt.Errorf("open sparse offsets: %w", err)
	}
	if offsets.Count() != header.Count+1 {
		offsets.Close()
		mmap.Close()

		return nil, fmt.Errorf("%w: rows=%d, offsets=%d", errSparseOffsetsLen, header.Count, offsets.Count())
	}

	return &TierStatsSparseReader{
		mmap:      mmap,
		dataOff:   int64(HeaderSize),
		offsets:   offsets,
		rowCount:  header.Count,
		slotCount: slotCount,
	}, nil
}

// Count returns the number of rows.
func (r *TierStatsSparseReader) Count() uint64 { return r.rowCount }

// SlotCount returns the configured manifest slot count.
func (r *TierStatsSparseReader) SlotCount() int { return r.slotCount }

// fillRow decodes the row at pos into the caller-supplied per-slot
// arrays and returns the presence bitmap. Absent slots are left at
// their zero value. Counts and bytesArr must be large enough to hold
// every present-tier slot — the reader's slotCount upper bound is
// 8*tierStatsSparseBitmapBytes = 16, so a [16]uint64 on the caller
// stack suffices.
func (r *TierStatsSparseReader) fillRow(pos uint64, counts, bytesArr *[tierStatsSparseBitmapBytes * 8]uint64) uint16 {
	if pos >= r.rowCount {
		return 0
	}
	start := r.offsets.UnsafeGetU64(pos)
	data := r.mmap.Data()[r.dataOff:]
	bitmap := binary.LittleEndian.Uint16(data[start : start+tierStatsSparseBitmapBytes])
	off := start + tierStatsSparseBitmapBytes
	bm := bitmap
	for bm != 0 {
		slot := bits.TrailingZeros16(bm)
		bm &= bm - 1
		counts[slot] = binary.LittleEndian.Uint64(data[off : off+8])
		bytesArr[slot] = binary.LittleEndian.Uint64(data[off+8 : off+16])
		off += TierStatsSlotBytes
	}

	return bitmap
}

// Close releases mmap'd regions.
func (r *TierStatsSparseReader) Close() error {
	if r == nil {
		return nil
	}
	var mmapErr, offsetsErr error
	if r.mmap != nil {
		mmapErr = r.mmap.Close()
		r.mmap = nil
	}
	if r.offsets != nil {
		offsetsErr = r.offsets.Close()
		r.offsets = nil
	}

	return errors.Join(mmapErr, offsetsErr)
}

// tierStatsSparsePresent reports whether the sparse pair exists on
// disk under indexDir/tier_stats/. Used by OpenTierStats to pick a
// reader.
func tierStatsSparsePresent(indexDir string) bool {
	_, err := os.Stat(filepath.Join(indexDir, TierStatsDir, TierStatsSparseFile))

	return err == nil
}
