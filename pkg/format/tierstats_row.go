package format

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

// TierStatsRowFile is the on-disk filename for the row-major
// per-prefix tier-stats layout. Lives inside tier_stats/ alongside
// the legacy per-tier columnar files (which are no longer written
// when row-major is enabled but may still exist on older indexes).
const TierStatsRowFile = "tier_stats_row.bin"

// TierStatsRowStride is the fixed byte stride per prefix row.
// Layout (little-endian): for each tier ID in 0..NumTiers-1,
//
//	count (8 bytes), bytes (8 bytes) — total 16 bytes per tier
//
// Slot index for tier T is T*16; this is independent of the
// tier manifest. The manifest is read separately to know which
// tier IDs actually have data so callers can skip empty slots
// during iteration.
//
// Fixed layout keeps Add()-time work O(NumTiers) bytes regardless
// of which tiers are populated for a given prefix, eliminates the
// per-tier discovery branch on the hot writer path, and makes the
// reader's GetBreakdown(pos) a single page fault on a cold index
// (vs 2×NumTiers page faults across the legacy per-tier columnar
// files — the dominant TierBreakdown cost at 11+ active tiers).
const TierStatsRowStride = int(tiers.NumTiers) * 16

// Sentinel errors for err113.
var (
	errTierStatsRowPosOOB = errors.New("tier stats row position out of range")
	errTierStatsRowWidth  = errors.New("tier stats row width mismatch")
)

// writeBufSize is the buffered-writer size for the streaming
// tier-stats row file. Sized big enough that fsync amortises across
// many rows without holding too many dirty pages mid-build.
const writeBufSize = 1 << 20

// TierStatsRowWriter writes per-prefix all-tier stats row-major to a
// single file. Each Add appends one fixed-stride row in tier-ID
// order, regardless of which tiers have data for that prefix.
// Sequential append matches the IndexBuilder's preorder Add stream
// so a buffered writer is sufficient — no mmap on the write side.
type TierStatsRowWriter struct {
	file   *os.File
	writer *bufio.Writer
	path   string
	count  uint64
}

// NewTierStatsRowWriter creates a row-major tier-stats writer under
// outDir/tier_stats/. The directory is created if missing.
func NewTierStatsRowWriter(outDir string) (*TierStatsRowWriter, error) {
	tierDir := filepath.Join(outDir, tierStatsDir)
	if err := os.MkdirAll(tierDir, DirPerm); err != nil {
		return nil, fmt.Errorf("create tier_stats dir: %w", err)
	}
	path := filepath.Join(tierDir, TierStatsRowFile)
	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("create tier stats row file: %w", err)
	}
	w := bufio.NewWriterSize(f, writeBufSize)
	// Placeholder header; rewritten at Close with the final count.
	header := EncodeHeader(Header{
		Magic:   MagicNumber,
		Version: Version,
		Count:   0,
		Width:   uint32(TierStatsRowStride),
	})
	if _, err := w.Write(header); err != nil {
		f.Close()
		os.Remove(path)

		return nil, fmt.Errorf("write tier stats row header: %w", err)
	}

	return &TierStatsRowWriter{
		file:   f,
		writer: w,
		path:   path,
	}, nil
}

// Add writes one row for the next prefix position. Counts and bytes
// are indexed by tier ID. Both arrays must be the same length;
// tier IDs out of [0, NumTiers) are not written. Counts[i] is the
// object count for tier i; bytes[i] is the total bytes.
func (w *TierStatsRowWriter) Add(counts, bytes *[tiers.NumTiers]uint64) error {
	var row [TierStatsRowStride]byte
	for id := range tiers.NumTiers {
		off := int(id) * 16
		binary.LittleEndian.PutUint64(row[off:off+8], counts[id])
		binary.LittleEndian.PutUint64(row[off+8:off+16], bytes[id])
	}
	if _, err := w.writer.Write(row[:]); err != nil {
		return fmt.Errorf("write tier stats row: %w", err)
	}
	w.count++

	return nil
}

// Count returns the number of rows written so far.
func (w *TierStatsRowWriter) Count() uint64 { return w.count }

// Close flushes, rewrites the header with the final row count, and
// closes the file.
func (w *TierStatsRowWriter) Close() error {
	if err := w.writer.Flush(); err != nil {
		w.file.Close()

		return fmt.Errorf("flush tier stats row: %w", err)
	}
	if _, err := w.file.Seek(0, 0); err != nil {
		w.file.Close()

		return fmt.Errorf("seek tier stats row: %w", err)
	}
	header := EncodeHeader(Header{
		Magic:   MagicNumber,
		Version: Version,
		Count:   w.count,
		Width:   uint32(TierStatsRowStride),
	})
	if _, err := w.file.Write(header); err != nil {
		w.file.Close()

		return fmt.Errorf("update tier stats row header: %w", err)
	}
	if err := w.file.Close(); err != nil {
		return fmt.Errorf("close tier stats row: %w", err)
	}

	return nil
}

// TierStatsRowReader reads the row-major tier-stats file. One mmap'd
// region; GetBreakdown(pos) is a single page fault on a cold index.
type TierStatsRowReader struct {
	mmap     *MmapFile
	dataOff  int64 // offset to first row (== HeaderSize)
	rowCount uint64
	stride   int
}

// OpenTierStatsRow opens the row-major tier stats file. Returns
// (nil, nil) when the file is absent — callers should fall back to
// the legacy per-tier columnar reader in that case (back-compat).
func OpenTierStatsRow(indexDir string) (*TierStatsRowReader, error) {
	path := filepath.Join(indexDir, tierStatsDir, TierStatsRowFile)
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			return nil, nil //nolint:nilnil // legacy callers fall back to per-tier reader
		}

		return nil, fmt.Errorf("stat tier stats row: %w", err)
	}
	mmap, err := OpenMmapWithHint(path, AccessHintRandom)
	if err != nil {
		return nil, fmt.Errorf("open tier stats row mmap: %w", err)
	}
	if mmap.Size() < int64(HeaderSize) {
		mmap.Close()

		return nil, fmt.Errorf("tier stats row: %w", ErrInvalidHeader)
	}
	header, err := DecodeHeader(mmap.Data()[:HeaderSize])
	if err != nil {
		mmap.Close()

		return nil, fmt.Errorf("decode tier stats row header: %w", err)
	}
	if header.Magic != MagicNumber {
		mmap.Close()

		return nil, ErrMagicMismatch
	}
	if header.Version != Version {
		mmap.Close()

		return nil, ErrVersionMismatch
	}
	stride := int(header.Width)
	if stride != TierStatsRowStride {
		mmap.Close()

		return nil, fmt.Errorf("%w: got %d expected %d", errTierStatsRowWidth, stride, TierStatsRowStride)
	}
	expected := int64(HeaderSize) + int64(header.Count)*int64(stride)
	if mmap.Size() < expected {
		mmap.Close()

		return nil, fmt.Errorf("%w: %d < %d", ErrFileTooSmall, mmap.Size(), expected)
	}

	return &TierStatsRowReader{
		mmap:     mmap,
		dataOff:  int64(HeaderSize),
		rowCount: header.Count,
		stride:   stride,
	}, nil
}

// Count returns the number of rows.
func (r *TierStatsRowReader) Count() uint64 { return r.rowCount }

// CountBytesAtTier returns (count, bytes) at the given position for
// the given tier ID. Branchless inside the hot caller — no map
// lookup, no slice search.
//
//nolint:gocritic // gocritic wants named returns, nonamedreturns forbids them
func (r *TierStatsRowReader) CountBytesAtTier(pos uint64, tier tiers.ID) (uint64, uint64, error) {
	if pos >= r.rowCount {
		return 0, 0, fmt.Errorf("%w: pos=%d count=%d", errTierStatsRowPosOOB, pos, r.rowCount)
	}
	rowStart := r.dataOff + int64(pos)*int64(r.stride)
	slot := rowStart + int64(tier)*16
	data := r.mmap.Data()

	return binary.LittleEndian.Uint64(data[slot : slot+8]),
		binary.LittleEndian.Uint64(data[slot+8 : slot+16]),
		nil
}

// UnsafeRow returns the raw row bytes for pos as a slice aliased over
// the mmap'd region. NumTiers consecutive (count, bytes) uint64 pairs
// in tier-ID order. Caller MUST NOT mutate; the slice is invalid
// after Close.
func (r *TierStatsRowReader) UnsafeRow(pos uint64) []byte {
	rowStart := r.dataOff + int64(pos)*int64(r.stride)

	return r.mmap.Data()[rowStart : rowStart+int64(r.stride)]
}

// Close releases the mmap'd region.
func (r *TierStatsRowReader) Close() error {
	if r == nil || r.mmap == nil {
		return nil
	}
	err := r.mmap.Close()
	r.mmap = nil

	return err
}
