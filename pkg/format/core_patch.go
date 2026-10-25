package format

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// Patched core fields. A field's width is set by the values most rows
// need, not by its largest value: the root and the top levels carry
// bucket-wide totals that would otherwise widen every row. A value that
// doesn't fit its field is stored as the field's all-ones sentinel, the
// row's bit is set in core_patch.rows, and its exact count, bytes and
// subtree size go to core_patch.bin at the rank of that bit. The builder
// picks the widths that make the core files smallest; when no narrower
// width pays for its exceptions, there are none and no patch files.
const (
	CorePatchFile     = "core_patch.bin"
	CorePatchRowsFile = "core_patch.rows"

	// Each exception is three uint64s: count, bytes, subtree size.
	corePatchRowBytes = 24
)

// patchedFields are the core fields that can be patched.
const patchedFields = 3

// fieldNeed is the narrowest width, 1..8, that stores v without an
// exception. Below 8 bytes the all-ones value is the sentinel, so it
// needs the next width up; at 8 bytes every value fits.
func fieldNeed(v uint64) uint8 {
	for w := uint8(1); w < 8; w++ {
		if v < widthMask[w] {
			return w
		}
	}

	return 8
}

// patchHistogram counts rows by the widths their count, bytes and
// subtree size need: hist[c][b][s] for widths 1..8 (index 0 unused).
type patchHistogram [9][9][9]uint64

func (h *patchHistogram) add(count, bytes, size uint64) {
	h[fieldNeed(count)][fieldNeed(bytes)][fieldNeed(size)]++
}

// patchChoice is a set of field widths, the exceptions they cause and
// the bytes they cost.
type patchChoice struct {
	count, bytes, size uint8
	exceptions         uint64
	cost               uint64
}

// exceptions counts the rows whose count, bytes or subtree size need
// more than the given widths.
func (h *patchHistogram) exceptions(wc, wb, ws uint8) uint64 {
	var e uint64
	for c := uint8(1); c <= 8; c++ {
		for b := uint8(1); b <= 8; b++ {
			for s := uint8(1); s <= 8; s++ {
				if c > wc || b > wb || s > ws {
					e += h[c][b][s]
				}
			}
		}
	}

	return e
}

// patchCost is the bytes rows take with the given widths: the
// patched fields of every row, plus, if any row is an exception, its
// patch row and the per-row bit vector.
func patchCost(rows uint64, wc, wb, ws uint8, exceptions uint64) uint64 {
	cost := rows * uint64(wc+wb+ws)
	if exceptions > 0 {
		words := bitWords(rows)
		cost += exceptions*corePatchRowBytes + HeaderSize*2 + 8*words + 8*((words+rankBlockWords-1)/rankBlockWords)
	}

	return cost
}

// choosePatch returns the widths that minimise patchCost for rows rows.
// Ties keep the wider widths, which have fewer exceptions to look up.
func (h *patchHistogram) choosePatch(rows uint64) patchChoice {
	best := patchChoice{count: 8, bytes: 8, size: 8, cost: ^uint64(0)}
	for wc := uint8(8); wc >= 1; wc-- {
		for wb := uint8(8); wb >= 1; wb-- {
			for ws := uint8(8); ws >= 1; ws-- {
				e := h.exceptions(wc, wb, ws)
				if cost := patchCost(rows, wc, wb, ws, e); cost < best.cost {
					best = patchChoice{count: wc, bytes: wb, size: ws, exceptions: e, cost: cost}
				}
			}
		}
	}

	return best
}

// patchWriter writes the exceptions of a patched core file.
type patchWriter struct {
	path string
	f    *os.File
	bw   *bufio.Writer
	rows *rankedBitsWriter
	n    uint64
}

func newPatchWriter(dir string) (*patchWriter, error) {
	path := filepath.Join(dir, CorePatchFile)
	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("create %s: %w", CorePatchFile, err)
	}
	rows, err := newRankedBitsWriter(filepath.Join(dir, CorePatchRowsFile))
	if err != nil {
		return nil, errors.Join(err, f.Close(), removeIfErr(path))
	}
	w := &patchWriter{path: path, f: f, bw: bufio.NewWriterSize(f, writeBufSize), rows: rows}
	// Placeholder header; close rewrites it with the exception count.
	if _, err := w.bw.Write(make([]byte, HeaderSize)); err != nil {
		return nil, errors.Join(fmt.Errorf("write %s header: %w", CorePatchFile, err), w.abort())
	}

	return w, nil
}

// add records whether the next row is an exception, and if it is, its
// exact values.
func (w *patchWriter) add(exception bool, count, bytes, size uint64) error {
	if err := w.rows.add(exception); err != nil {
		return err
	}
	if !exception {
		return nil
	}
	for _, v := range [patchedFields]uint64{count, bytes, size} {
		if err := putU64(w.bw, v); err != nil {
			return fmt.Errorf("write %s: %w", CorePatchFile, err)
		}
	}
	w.n++

	return nil
}

func (w *patchWriter) close() error {
	if err := w.bw.Flush(); err != nil {
		return errors.Join(fmt.Errorf("flush %s: %w", CorePatchFile, err), w.abort())
	}
	head := EncodeHeader(Header{Magic: MagicNumber, Version: Version, Count: w.n, Width: corePatchRowBytes})
	if _, err := w.f.WriteAt(head, 0); err != nil {
		return errors.Join(fmt.Errorf("write %s header: %w", CorePatchFile, err), w.abort())
	}
	if err := w.f.Close(); err != nil {
		return errors.Join(fmt.Errorf("close %s: %w", CorePatchFile, err), w.rows.abort(), removeIfErr(w.path))
	}
	if err := w.rows.close(); err != nil {
		return errors.Join(err, removeIfErr(w.path))
	}

	return nil
}

func (w *patchWriter) abort() error {
	return errors.Join(w.f.Close(), removeIfErr(w.path), w.rows.abort())
}

// corePatch reads a core file's exceptions.
type corePatch struct {
	table *MmapFile
	rows  *rankedBitsReader
}

// openCorePatch opens the exceptions of the core file in dir, or
// returns nil when the index has none.
func openCorePatch(dir string, rowCount uint64) (*corePatch, error) {
	path := filepath.Join(dir, CorePatchFile)
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		return nil, nil //nolint:nilnil // no patch files: no exceptions
	}
	table, err := OpenMmap(path)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", CorePatchFile, err)
	}
	rows, err := openRankedBits(filepath.Join(dir, CorePatchRowsFile))
	if err != nil {
		return nil, errors.Join(err, table.Close())
	}
	p := &corePatch{table: table, rows: rows}
	if err := p.check(rowCount); err != nil {
		return nil, errors.Join(err, p.close())
	}

	return p, nil
}

func (p *corePatch) check(rowCount uint64) error {
	data := p.table.Data()
	if len(data) < HeaderSize {
		return fmt.Errorf("%s: %w", CorePatchFile, ErrInvalidHeader)
	}
	header, err := DecodeHeader(data[:HeaderSize])
	if err != nil {
		return fmt.Errorf("decode %s header: %w", CorePatchFile, err)
	}
	if header.Magic != MagicNumber {
		return ErrMagicMismatch
	}
	if header.Version != Version {
		return ErrVersionMismatch
	}
	// Check the bit vector's length before ranking it: rank past its end
	// would read past the file.
	if p.rows.n != rowCount {
		return fmt.Errorf("%w: %s has %d rows, core stats %d", errCoreStatsRowWidth, CorePatchRowsFile, p.rows.n, rowCount)
	}
	if marked := p.rows.rank(rowCount); header.Width != corePatchRowBytes || marked != header.Count {
		return fmt.Errorf("%w: %s has %d exceptions of %d bytes for %d marked rows",
			errCoreStatsRowWidth, CorePatchFile, header.Count, header.Width, marked)
	}
	if want := uint64(HeaderSize) + header.Count*corePatchRowBytes; uint64(len(data)) < want {
		return fmt.Errorf("%w: %s %d < %d", ErrFileTooSmall, CorePatchFile, len(data), want)
	}

	return nil
}

// value returns field f (0 count, 1 bytes, 2 subtree size) of the
// exception at row pos, whose bit must be set.
func (p *corePatch) value(pos uint64, f int) uint64 {
	off := HeaderSize + p.rows.rank(pos)*corePatchRowBytes + uint64(f)*8

	return binary.LittleEndian.Uint64(p.table.Data()[off:])
}

func (p *corePatch) close() error {
	if p == nil {
		return nil
	}

	return errors.Join(p.table.Close(), p.rows.close())
}
