package format

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sync"

	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

// TierStatsRowFile is the row-major per-prefix tier-stats file.
const TierStatsRowFile = "tier_stats_row.bin"

// TierStatsSlotBytes is the byte width of one (count, bytes) tier slot:
// uint64 count + uint64 bytes.
const TierStatsSlotBytes = 16

// denseTierStatsRowStride is the byte stride of the dense intermediate
// written during ingest, before PackTierStatsRow rewrites the file
// down to present-tier slots in manifest order.
const denseTierStatsRowStride = int(tiers.NumTiers) * TierStatsSlotBytes

// Sentinel error for err113.
var errTierStatsRowWidth = errors.New("tier stats row width invalid")

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
		Width:   uint32(denseTierStatsRowStride),
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
	var row [denseTierStatsRowStride]byte
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
		Width:   uint32(denseTierStatsRowStride),
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
	path := filepath.Join(indexDir, tierStatsDir, TierStatsRowFile)
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

// PackTierStatsRow rewrites tier_stats_row.bin from the dense
// NumTiers-slot stride emitted during ingest to a packed stride that
// holds only slots for tiers in presentTiers, in manifest order
// (sorted by tier ID, same as tiers.WriteManifest).
//
// Implementation: writes the packed result to an adjacent .tmp file in
// parallel, then renames over the dense file. An earlier attempt to
// repack in place was unsafe — row r's packed-write region at byte
// offset r×packedStride lies inside row 0's dense-read region at
// [0, denseStride), so workers concurrently mid-pack could overwrite
// dense bytes another worker hasn't read yet.
//
// Disk during finalize peaks at (1 + presentTiers/NumTiers) × dense
// size; after rename, steady-state is presentTiers/NumTiers of dense.
//
// When len(presentTiers) == NumTiers the dense file is already optimal
// and the function returns without I/O. When len(presentTiers) == 0
// the file is removed (no tier data → OpenTierStats's manifest guard
// already returns an empty reader in that case).
func PackTierStatsRow(outDir string, presentTiers []tiers.ID) error {
	path := filepath.Join(outDir, tierStatsDir, TierStatsRowFile)

	if len(presentTiers) == 0 {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("remove tier stats row: %w", err)
		}

		return nil
	}

	src, header, err := openDenseTierStatsRow(path)
	if err != nil {
		return err
	}
	if src == nil {
		// File is absent or already packed — no work to do.
		return nil
	}
	defer src.Close()

	if len(presentTiers) == int(tiers.NumTiers) {
		// Dense file already covers every tier; packing is a no-op.
		return nil
	}

	// Sort present tiers so slot order is deterministic and matches the
	// manifest writer (which also sorts by tier ID).
	sortedTiers := slices.Clone(presentTiers)
	slices.Sort(sortedTiers)
	packedStride := len(sortedTiers) * TierStatsSlotBytes

	// Map each output slot to its byte offset in a dense row.
	denseOffByPackedSlot := make([]int, len(sortedTiers))
	for i, id := range sortedTiers {
		denseOffByPackedSlot[i] = int(id) * TierStatsSlotBytes
	}

	if err := writePackedTierStatsRow(src, path, int64(header.Count), header.Count, packedStride, denseOffByPackedSlot); err != nil {
		return err
	}
	// fsync the subdir so the rename dirent is durable. The outer
	// SyncDir(outDir) at the end of Finalize covers the index root but
	// not tier_stats/; without this, a crash after Finalize but before
	// the next writeback could leave the dense file in place, then
	// OpenTierStats would reject the index when SlotCount() mismatches
	// the manifest tier count.
	if err := SyncDir(filepath.Join(outDir, tierStatsDir)); err != nil {
		return fmt.Errorf("sync tier_stats dir: %w", err)
	}

	return nil
}

// openDenseTierStatsRow opens path and validates its header is in the
// dense pre-pack form. Returns (nil, _, nil) when the file is absent
// (no-op for re-finalize) or already non-dense (already packed or
// written by a different version) — caller treats either as nothing
// to do. Caller is responsible for closing src when it is non-nil.
func openDenseTierStatsRow(path string) (*os.File, Header, error) {
	src, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, Header{}, nil
		}

		return nil, Header{}, fmt.Errorf("open dense tier stats row: %w", err)
	}
	headerBuf := make([]byte, HeaderSize)
	if _, err := src.ReadAt(headerBuf, 0); err != nil {
		src.Close()

		return nil, Header{}, fmt.Errorf("read tier stats row header: %w", err)
	}
	header, err := DecodeHeader(headerBuf)
	if err != nil {
		src.Close()

		return nil, Header{}, fmt.Errorf("decode tier stats row header: %w", err)
	}
	if int(header.Width) != denseTierStatsRowStride {
		src.Close()

		return nil, Header{}, nil
	}

	return src, header, nil
}

// writePackedTierStatsRow creates path+".tmp", writes the packed
// header + n packed rows in parallel from src, fsyncs, closes src/dst,
// and renames the tmp over path. On any error the .tmp is removed.
func writePackedTierStatsRow(src *os.File, path string, n int64, count uint64, packedStride int, denseOffByPackedSlot []int) error {
	tmpPath := path + ".tmp"
	dst, err := os.Create(tmpPath)
	if err != nil {
		return fmt.Errorf("create packed tier stats row: %w", err)
	}
	cleanupTmp := func() {
		dst.Close()
		os.Remove(tmpPath)
	}

	newSize := int64(HeaderSize) + n*int64(packedStride)
	if err := dst.Truncate(newSize); err != nil {
		cleanupTmp()

		return fmt.Errorf("truncate packed tier stats row: %w", err)
	}
	newHeader := EncodeHeader(Header{
		Magic:   MagicNumber,
		Version: Version,
		Count:   count,

		Width: uint32(packedStride),
	})
	if _, err := dst.WriteAt(newHeader, 0); err != nil {
		cleanupTmp()

		return fmt.Errorf("write packed tier stats row header: %w", err)
	}

	if n > 0 {
		if err := packRowsParallel(src, dst, n, packedStride, denseOffByPackedSlot); err != nil {
			cleanupTmp()

			return err
		}
	}

	if err := dst.Sync(); err != nil {
		cleanupTmp()

		return fmt.Errorf("sync packed tier stats row: %w", err)
	}
	if err := dst.Close(); err != nil {
		os.Remove(tmpPath)

		return fmt.Errorf("close packed tier stats row: %w", err)
	}
	if err := src.Close(); err != nil {
		os.Remove(tmpPath)

		return fmt.Errorf("close dense tier stats row: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		os.Remove(tmpPath)

		return fmt.Errorf("rename packed tier stats row: %w", err)
	}

	return nil
}

// packRowsParallel reads dense rows from src and writes packed rows to
// dst in parallel. Src and dst are disjoint files, so workers can
// freely operate on their own row ranges without read/write hazards.
func packRowsParallel(src, dst *os.File, n int64, packedStride int, denseOffByPackedSlot []int) error {
	const rowsPerChunk = 1 << 14 // 16 K rows: ~3 MiB dense buf, ~512 KiB packed buf @ 5 tiers
	numWorkers := max(min(runtime.NumCPU(), int(n)), 1)
	rowsPerWorker := (n + int64(numWorkers) - 1) / int64(numWorkers)

	errs := make([]error, numWorkers)
	var wg sync.WaitGroup
	for w := range numWorkers {
		start := int64(w) * rowsPerWorker
		end := min(start+rowsPerWorker, n)
		if start >= end {
			continue
		}
		wg.Add(1)
		go func(w int, start, end int64) {
			defer wg.Done()
			denseBuf := make([]byte, rowsPerChunk*denseTierStatsRowStride)
			packedBuf := make([]byte, rowsPerChunk*packedStride)
			for chunk := start; chunk < end; chunk += rowsPerChunk {
				cEnd := min(chunk+rowsPerChunk, end)
				rows := int(cEnd - chunk)
				dlen := rows * denseTierStatsRowStride
				plen := rows * packedStride

				denseOff := int64(HeaderSize) + chunk*int64(denseTierStatsRowStride)
				if _, err := src.ReadAt(denseBuf[:dlen], denseOff); err != nil {
					errs[w] = fmt.Errorf("read dense tier rows at %d: %w", chunk, err)

					return
				}
				for r := range rows {
					denseRow := denseBuf[r*denseTierStatsRowStride : (r+1)*denseTierStatsRowStride]
					packedRow := packedBuf[r*packedStride : (r+1)*packedStride]
					for slot, denseSlotOff := range denseOffByPackedSlot {
						copy(packedRow[slot*TierStatsSlotBytes:(slot+1)*TierStatsSlotBytes],
							denseRow[denseSlotOff:denseSlotOff+TierStatsSlotBytes])
					}
				}

				packedOff := int64(HeaderSize) + chunk*int64(packedStride)
				if _, err := dst.WriteAt(packedBuf[:plen], packedOff); err != nil {
					errs[w] = fmt.Errorf("write packed tier rows at %d: %w", chunk, err)

					return
				}
			}
		}(w, start, end)
	}
	wg.Wait()

	return errors.Join(errs...)
}
