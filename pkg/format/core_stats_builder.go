package format

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

// coreStatsBuildStride is the fixed safe stride used during the
// build, before per-field widths are known. Finalize repacks the rows
// to the stride it chooses: count, bytes and child start from the
// width histogram (rows that don't fit go to patch files), the tier
// mask from its max.
//
//	offset  0: object_count           uint64
//	offset  8: total_bytes            uint64
//	offset 16: child_start            uint64 (row of the first child)
//	offset 24: tier_mask              uint16
const coreStatsBuildStride = 26

// coreStatsMaxInitialRows caps the rows a CoreStatsBuilder reserves
// before any are added. The capacity hint can over-count many times
// and every reserved block is allocated on disk, so the file starts no
// larger than this and doubles as rows arrive.
const coreStatsMaxInitialRows = 1 << 16

const (
	bldOffCount      = 0
	bldOffBytes      = 8
	bldOffChildStart = 16
	bldOffTierMask   = 24
)

// CoreStatsBuilder writes the row-major core stats file during a
// build, one row per prefix in level order (Add order).
//
// During the build the file uses a safe-wide stride
// (coreStatsBuildStride); at Finalize the rows' widths determine a
// tight per-field schema (see Finalize) and the rows are repacked into
// the final file with a CoreStatsStride descriptor recorded after the
// Header.
type CoreStatsBuilder struct {
	path    string
	file    *os.File
	data    []byte // mmap region (PROT_READ|PROT_WRITE)
	dataOff int    // == HeaderSize; first row starts here
	size    int64  // current mmap'd byte length
	count   int    // number of rows committed (Add calls)

	// Observed max — drives the tier field's width at Finalize.
	maxTierMask uint16
	// tierEnc is how the tier field encodes the mask (SetTierMaskEncoding).
	tierEnc TierMaskEncoding
}

// NewCoreStatsBuilder creates a core stats file at outDir/core_stats.bin
// sized to fit capacity rows, at most coreStatsMaxInitialRows (grown on
// demand if exceeded). The header
// is written at the build-stride placeholder; Finalize rewrites it to
// the final tight stride.
func NewCoreStatsBuilder(outDir string, capacity uint64) (*CoreStatsBuilder, error) {
	path := filepath.Join(outDir, CoreStatsFile)
	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("create core stats file: %w", err)
	}
	b := &CoreStatsBuilder{
		path:    path,
		file:    f,
		dataOff: HeaderSize,
	}
	initial := max(int64(HeaderSize)+int64(min(capacity, coreStatsMaxInitialRows))*int64(coreStatsBuildStride), int64(unix.Getpagesize()))
	if err := b.grow(initial); err != nil {
		_ = b.Close()
		os.Remove(path)

		return nil, err
	}
	header := EncodeHeader(Header{
		Magic:   MagicNumber,
		Version: Version,
		Count:   0,
		Width:   uint32(coreStatsBuildStride),
	})
	copy(b.data[:HeaderSize], header)

	return b, nil
}

func (b *CoreStatsBuilder) grow(newSize int64) error {
	if newSize <= b.size {
		return nil
	}
	if b.data != nil {
		if err := unix.Munmap(b.data); err != nil {
			return fmt.Errorf("core stats munmap: %w", err)
		}
		b.data = nil
	}
	if err := preallocate(b.file, b.size, newSize); err != nil {
		return fmt.Errorf("core stats grow: %w", err)
	}
	data, err := unix.Mmap(int(b.file.Fd()), 0, int(newSize), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		return fmt.Errorf("core stats mmap: %w", err)
	}
	b.data = data
	b.size = newSize

	return nil
}

// buildRowSlice returns the byte slice for build-stride row idx
// (must be < b.count).
func (b *CoreStatsBuilder) buildRowSlice(idx int) []byte {
	start := b.dataOff + idx*coreStatsBuildStride

	return b.data[start : start+coreStatsBuildStride]
}

// Add appends the next row: the prefix's object count, total bytes,
// the row of its first child and its tier mask.
func (b *CoreStatsBuilder) Add(objectCount, totalBytes, childStart uint64, tierMask uint16) error {
	needed := int64(b.dataOff + (b.count+1)*coreStatsBuildStride)
	if needed > b.size {
		newSize := max(b.size*2, needed)
		if err := b.grow(newSize); err != nil {
			return err
		}
	}
	row := b.buildRowSlice(b.count)
	binary.LittleEndian.PutUint64(row[bldOffCount:], objectCount)
	binary.LittleEndian.PutUint64(row[bldOffBytes:], totalBytes)
	binary.LittleEndian.PutUint64(row[bldOffChildStart:], childStart)
	binary.LittleEndian.PutUint16(row[bldOffTierMask:], tierMask)

	b.maxTierMask = max(b.maxTierMask, tierMask)
	b.count++

	return nil
}

// SetTierMaskEncoding sets how Finalize stores each row's tier mask: the
// tier stats writer's MaskEncoding for the layout it writes.
func (b *CoreStatsBuilder) SetTierMaskEncoding(enc TierMaskEncoding) {
	b.tierEnc = enc
}

// Count returns the number of rows committed.
func (b *CoreStatsBuilder) Count() int { return b.count }

// chooseStride picks the row widths: the patched fields from the joint
// distribution of the widths each row needs (see choosePatch), the
// tier mask from its max. It reads every build row once.
func (b *CoreStatsBuilder) chooseStride() (CoreStatsStride, patchChoice) {
	var hist patchHistogram
	var maxCount, maxBytes, maxSize uint64
	for i := range b.count {
		row := b.buildRowSlice(i)
		c, by, sz := binary.LittleEndian.Uint64(row[bldOffCount:]),
			binary.LittleEndian.Uint64(row[bldOffBytes:]),
			binary.LittleEndian.Uint64(row[bldOffChildStart:])
		hist.add(c, by, sz)
		maxCount, maxBytes, maxSize = max(maxCount, c), max(maxBytes, by), max(maxSize, sz)
	}
	// Without exceptions there are no sentinels, so every field can take
	// the plain width of its largest value, all-ones included: one byte
	// narrower than choosePatch allows when a maximum is all ones.
	plain := patchChoice{count: byteWidthOf(maxCount), bytes: byteWidthOf(maxBytes), size: byteWidthOf(maxSize)}
	plain.cost = uint64(b.count) * uint64(plain.count+plain.bytes+plain.size)
	choice := hist.choosePatch(uint64(b.count))
	if choice.exceptions == 0 || plain.cost <= choice.cost {
		choice = plain
	}

	return CoreStatsStride{
		Count:      choice.count,
		Bytes:      choice.bytes,
		ChildStart: choice.size,
		TierMask:   tierFieldWidth(b.maxTierMask, b.tierEnc),
	}, choice
}

// Finalize chooses the stride (count, bytes and child start from the
// width histogram, with patch files for the rows that don't fit; the
// tier mask from its max), streams the repacked rows into a
// fresh file at the same path (Header + stride descriptor first), and
// closes both files. Memory during the repack is bounded by the bufio
// buffer (1 MiB) — at the project's stated 10B-prefix scale a per-row
// in-memory buffer would be hundreds of GB.
func (b *CoreStatsBuilder) Finalize() error {
	desc, choice := b.chooseStride()
	stride := desc.RowBytes()
	var patch *patchWriter
	if choice.exceptions > 0 {
		var err error
		if patch, err = newPatchWriter(filepath.Dir(b.path)); err != nil {
			return err
		}
	}
	abortPatch := func(err error) error {
		if patch != nil {
			return errors.Join(err, patch.abort())
		}

		return err
	}

	// Write the tight final file to a sibling tmp path, then rename
	// over the build file. We keep the build mmap live throughout the
	// row-streaming loop so each row read is a memory copy from
	// page-cached pages.
	tmp := b.path + ".tmp"
	final, err := os.Create(tmp)
	if err != nil {
		return abortPatch(fmt.Errorf("core stats create tmp: %w", err))
	}
	var head [coreStatsDataOff]byte
	copy(head[:], EncodeHeader(Header{
		Magic:   MagicNumber,
		Version: Version,
		Count:   uint64(b.count),
		Width:   uint32(stride),
	}))
	enc := desc.Encode()
	copy(head[HeaderSize:], enc[:])
	if _, err := final.Write(head[:]); err != nil {
		return abortPatch(cleanupTmp(final, tmp, fmt.Errorf("write header: %w", err)))
	}

	bw := bufio.NewWriterSize(final, repackBufferSize)
	// Each field is stored as a whole word over the bytes after it, so
	// the row buffer has a word of slack past the row.
	rowBuf := make([]byte, stride+8)
	for i := range b.count {
		exception := repackRow(rowBuf, b.buildRowSlice(i), desc, b.tierEnc)
		if _, err := bw.Write(rowBuf[:stride]); err != nil {
			return abortPatch(cleanupTmp(final, tmp, fmt.Errorf("write row %d: %w", i, err)))
		}
		if patch != nil {
			row := b.buildRowSlice(i)
			err := patch.add(exception, binary.LittleEndian.Uint64(row[bldOffCount:]),
				binary.LittleEndian.Uint64(row[bldOffBytes:]), binary.LittleEndian.Uint64(row[bldOffChildStart:]))
			if err != nil {
				return abortPatch(cleanupTmp(final, tmp, err))
			}
		}
	}
	if patch != nil {
		if err := patch.close(); err != nil {
			return cleanupTmp(final, tmp, err)
		}
	}
	pad := [coreStatsReadTailPad]byte{}
	if _, err := bw.Write(pad[:]); err != nil {
		return cleanupTmp(final, tmp, fmt.Errorf("write tail pad: %w", err))
	}
	if err := bw.Flush(); err != nil {
		return cleanupTmp(final, tmp, fmt.Errorf("flush: %w", err))
	}
	startWriteback(final)
	if err := final.Close(); err != nil {
		_ = os.Remove(tmp)

		return fmt.Errorf("close final: %w", err)
	}

	// Drop the build mmap before the rename so the file we replace
	// has no live references.
	if err := unix.Munmap(b.data); err != nil {
		_ = os.Remove(tmp)

		return fmt.Errorf("munmap build: %w", err)
	}
	b.data = nil
	if err := b.file.Close(); err != nil {
		_ = os.Remove(tmp)

		return fmt.Errorf("close build: %w", err)
	}
	b.file = nil
	if err := os.Rename(tmp, b.path); err != nil {
		_ = os.Remove(tmp)

		return fmt.Errorf("rename tmp into place: %w", err)
	}

	return nil
}

// cleanupTmp closes f and removes path, returning the wrapped err.
// Used on every error branch inside Finalize so cleanup is uniform
// and the partial file never lingers.
func cleanupTmp(f *os.File, path string, err error) error {
	_ = f.Close()
	_ = os.Remove(path)

	return err
}

// repackRow narrows the build-stride row into the tight stride. A
// patched field whose value doesn't fit stores its sentinel; repackRow
// reports whether any did, so the row's exact values go to the patch.
func repackRow(dst, src []byte, s CoreStatsStride, enc TierMaskEncoding) bool {
	exception := false
	off := 0
	for _, f := range [patchedFields]struct {
		at    int
		width uint8
	}{{bldOffCount, s.Count}, {bldOffBytes, s.Bytes}, {bldOffChildStart, s.ChildStart}} {
		v := binary.LittleEndian.Uint64(src[f.at:])
		if fieldNeed(v) > f.width {
			v, exception = widthMask[f.width], true
		}
		binary.LittleEndian.PutUint64(dst[off:], v)
		off += int(f.width)
	}
	binary.LittleEndian.PutUint64(dst[off:], encodeTierField(binary.LittleEndian.Uint16(src[bldOffTierMask:]), enc))

	return exception
}

// Close releases the mmap and closes the file without finalizing.
// Used for error paths; the caller should remove the file separately.
func (b *CoreStatsBuilder) Close() error {
	var errs []error
	if b.data != nil {
		if err := unix.Munmap(b.data); err != nil {
			errs = append(errs, fmt.Errorf("core stats munmap: %w", err))
		}
		b.data = nil
	}
	if b.file != nil {
		if err := b.file.Close(); err != nil {
			errs = append(errs, fmt.Errorf("core stats close: %w", err))
		}
		b.file = nil
	}

	return errors.Join(errs...)
}
