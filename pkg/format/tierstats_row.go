package format

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"

	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

// TierStatsRowFile is the row-major per-prefix tier-stats file.
const TierStatsRowFile = "tier_stats_row.bin"

// TierStatsSlotBytes is the byte width of one (count, bytes) tier slot:
// uint64 count + uint64 bytes.
const TierStatsSlotBytes = 16

// Sentinel errors for err113.
var (
	errTierStatsRowWidth  = errors.New("tier stats row width invalid")
	errNoTiersDeclared    = errors.New("tier stats row writer requires at least one present tier")
	errUndeclaredTierData = errors.New("prefix has data for a tier absent from the declared present set")
)

// writeBufSize is the buffered-writer size for the streaming
// tier-stats row file. Sized big enough that fsync amortises across
// many rows without holding too many dirty pages mid-build.
const writeBufSize = 1 << 20

// TierStatsRowWriter writes per-prefix tier stats row-major to a single
// file. Each Add appends one fixed-stride row holding only the tiers
// declared present at construction, in tier-ID order. The present set
// is known up front (from the ingest tier mask), so the sparse layout
// is written directly — no dense intermediate, no compaction pass.
// Sequential append matches the IndexBuilder's preorder Add stream so a
// buffered writer is sufficient — no mmap on the write side.
type TierStatsRowWriter struct {
	file   *os.File
	writer *bufio.Writer
	path   string
	rowBuf []byte
	// slotByTier maps a tier ID to its slot in the packed row, or -1
	// when the tier is absent from the declared present set.
	slotByTier [tiers.NumTiers]int
	stride     int
	count      uint64
}

// NewTierStatsRowWriter creates a row-major tier-stats writer under
// outDir/tier_stats/. Present lists the tiers that have data globally;
// the row stride is len(present) slots in tier-ID order. Present must
// be non-empty — an index with no tier data writes no row file. The
// directory is created if missing.
func NewTierStatsRowWriter(outDir string, present []tiers.ID) (*TierStatsRowWriter, error) {
	if len(present) == 0 {
		return nil, errNoTiersDeclared
	}
	sorted := slices.Compact(slices.Sorted(slices.Values(present)))
	for _, id := range sorted {
		if int(id) >= int(tiers.NumTiers) {
			return nil, fmt.Errorf("%w: tier id %d out of range", errTierStatsRowWidth, id)
		}
	}

	// slotByTier[t] is tier t's slot in the packed row, or -1 when t is
	// absent. Built by scanning the array's own index range so the write
	// is provably in-bounds; slices.Index yields the sorted slot or -1.
	var slotByTier [tiers.NumTiers]int
	for tid := range slotByTier {
		slotByTier[tid] = slices.Index(sorted, tiers.ID(tid))
	}
	stride := len(sorted) * TierStatsSlotBytes

	tierDir := filepath.Join(outDir, TierStatsDir)
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
		Width:   uint32(stride),
	})
	if _, err := w.Write(header); err != nil {
		f.Close()
		os.Remove(path)

		return nil, fmt.Errorf("write tier stats row header: %w", err)
	}

	return &TierStatsRowWriter{
		file:       f,
		writer:     w,
		path:       path,
		rowBuf:     make([]byte, stride),
		slotByTier: slotByTier,
		stride:     stride,
	}, nil
}

// Add writes one packed row for the next prefix position. Counts and
// bytes are indexed by tier ID. Only declared-present tiers occupy a
// slot; a tier with data that was not declared present is a mask bug
// that would silently drop counts, so Add fails with
// errUndeclaredTierData rather than discarding it.
func (w *TierStatsRowWriter) Add(counts, bytes *[tiers.NumTiers]uint64) error {
	row := w.rowBuf
	for id := range tiers.NumTiers {
		slot := w.slotByTier[id]
		if slot < 0 {
			if counts[id] != 0 || bytes[id] != 0 {
				return fmt.Errorf("%w: tier %d", errUndeclaredTierData, id)
			}

			continue
		}
		off := slot * TierStatsSlotBytes
		binary.LittleEndian.PutUint64(row[off:off+8], counts[id])
		binary.LittleEndian.PutUint64(row[off+8:off+16], bytes[id])
	}
	if _, err := w.writer.Write(row); err != nil {
		return fmt.Errorf("write tier stats row: %w", err)
	}
	w.count++

	return nil
}

// Count returns the number of rows written so far.
func (w *TierStatsRowWriter) Count() uint64 { return w.count }

// Close flushes, rewrites the header with the final row count, and
// closes the file. On any error path the partial file is removed so a
// later mmap/open doesn't misinterpret it as valid data.
func (w *TierStatsRowWriter) Close() error {
	if err := w.writer.Flush(); err != nil {
		return w.cleanupOnErr(fmt.Errorf("flush tier stats row: %w", err))
	}
	if _, err := w.file.Seek(0, 0); err != nil {
		return w.cleanupOnErr(fmt.Errorf("seek tier stats row: %w", err))
	}
	header := EncodeHeader(Header{
		Magic:   MagicNumber,
		Version: Version,
		Count:   w.count,
		Width:   uint32(w.stride),
	})
	if _, err := w.file.Write(header); err != nil {
		return w.cleanupOnErr(fmt.Errorf("update tier stats row header: %w", err))
	}
	if err := w.file.Close(); err != nil {
		return errors.Join(fmt.Errorf("close tier stats row: %w", err), removeIfErr(w.path))
	}

	return nil
}

func (w *TierStatsRowWriter) cleanupOnErr(primary error) error {
	closeErr := w.file.Close()
	removeErr := os.Remove(w.path)

	return errors.Join(primary, closeErr, removeErr)
}

// TierStatsRowReader reads the row-major tier-stats file. One mmap'd
// region; Breakdown(pos) is a single page fault on a cold index.
type TierStatsRowReader struct {
	mmap     *MmapFile
	dataOff  int64 // offset to first row (== HeaderSize)
	rowCount uint64
	stride   int
}

// OpenTierStatsRow opens the row-major tier stats file. Returns an
// error if the file is missing or unreadable.
func OpenTierStatsRow(indexDir string) (*TierStatsRowReader, error) {
	path := filepath.Join(indexDir, TierStatsDir, TierStatsRowFile)
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
	if stride <= 0 || stride%TierStatsSlotBytes != 0 {
		mmap.Close()

		return nil, fmt.Errorf("%w: stride %d not a positive multiple of %d", errTierStatsRowWidth, stride, TierStatsSlotBytes)
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

// SlotCount returns the number of (count, bytes) slots per row —
// equivalently, the number of present tiers recorded in this file's
// manifest. Caller can cross-check against len(manifest.Tiers).
func (r *TierStatsRowReader) SlotCount() int { return r.stride / TierStatsSlotBytes }

// UnsafeRow returns the raw row bytes for pos as a slice aliased over
// the mmap'd region. The row holds SlotCount() consecutive
// (count, bytes) uint64 pairs in manifest order (NOT tier-ID order):
// slot i corresponds to manifest.Tiers[i]. Caller MUST NOT mutate;
// the slice is invalid after Close.
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
