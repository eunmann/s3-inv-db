package format

import (
	"errors"
	"fmt"
	"path/filepath"
)

// CoreStatsRanges writes the files a CoreStatsWriter does, byte for
// byte, from ranges of rows several goroutines write at once
// (CoreStatsRange): each row goes straight to its place in
// core_stats.bin. The exceptions, whose place in core_patch.bin is their
// rank, and the checks across ranges go in each range's part, which
// Merge takes in row order.
type CoreStatsRanges struct {
	path  string
	f     *hashedFile
	patch *patchWriter
	plan  *CoreStatsPlan
	desc  CoreStatsStride
	enc   TierMaskEncoding
	// The rows, exceptions and last child start merged.
	n, exceptions, lastChild uint64
	wantExceptions           uint64
	closed                   bool
}

// NewCoreStatsRanges creates outDir's core stats file at its final
// size, at the widths plan picks with the tier field encoded as enc,
// and its patch files when those widths leave exceptions, as
// NewCoreStatsWriter does.
func NewCoreStatsRanges(outDir string, plan *CoreStatsPlan, enc TierMaskEncoding, sums *Checksums) (*CoreStatsRanges, error) {
	desc, choice, err := plan.stride(enc)
	if err != nil {
		return nil, err
	}
	head := EncodeHeader(Header{Magic: MagicNumber, Version: Version, Count: plan.rows, Width: uint32(desc.RowBytes())})
	enc4 := desc.Encode()
	size := coreStatsDataOff + int64(plan.rows)*int64(desc.RowBytes()) + coreStatsReadTailPad
	path := filepath.Join(outDir, CoreStatsFile)
	f, err := createSizedFile(path, append(head, enc4[:]...), size, sums)
	if err != nil {
		return nil, err
	}
	w := &CoreStatsRanges{path: path, f: f, plan: plan, desc: desc, enc: enc, wantExceptions: choice.exceptions}
	if choice.exceptions > 0 {
		if w.patch, err = newPatchWriter(outDir, sums); err != nil {
			return nil, errors.Join(err, w.Abort())
		}
	}

	return w, nil
}

// CoreStatsPart is what a range leaves for Merge.
type CoreStatsPart struct {
	// vals holds each exception's count, bytes and child start.
	vals []uint64
	// bits holds each row's exception bit, from the range's first row at
	// bit 0, when the files have exceptions.
	bits                  []uint64
	first, rows           uint64
	firstChild, lastChild uint64
}

// CoreStatsRange writes ranges of core rows, one goroutine's. Its buffer
// is rangeBufSize.
type CoreStatsRange struct {
	w    *CoreStatsRanges
	part *CoreStatsPart
	buf  rangeBuf
}

// NewRange returns a range writer for one goroutine.
func (w *CoreStatsRanges) NewRange() *CoreStatsRange {
	return &CoreStatsRange{w: w, buf: newRangeBuf(w.f)}
}

// Start begins a range at row, which part describes.
func (r *CoreStatsRange) Start(row uint64, part *CoreStatsPart) error {
	if row > r.w.plan.rows {
		return fmt.Errorf("%w: range at row %d of %d", errCorePlan, row, r.w.plan.rows)
	}
	*part = CoreStatsPart{vals: part.vals[:0], bits: part.bits[:0], first: row}
	r.part = part

	return r.buf.seek(coreStatsDataOff + int64(row)*int64(r.w.desc.RowBytes()))
}

// Add writes the range's next row, with the checks CoreStatsWriter.Add
// makes within the range.
func (r *CoreStatsRange) Add(count, bytes, childStart uint64, tierMask uint16) error {
	w, p := r.w, r.part
	row := p.first + p.rows
	if row == w.plan.rows || childStart <= row || (p.rows > 0 && childStart < p.lastChild) || childStart > w.plan.rows || tierMask > w.plan.maxMask {
		return fmt.Errorf("%w: row %d of %d (child start %d after %d, tier mask %#x of at most %#x)",
			errCorePlan, row, w.plan.rows, childStart, p.lastChild, tierMask, w.plan.maxMask)
	}
	dst, err := r.buf.next(w.desc.RowBytes())
	if err != nil {
		return err
	}
	exception, err := packCoreRow(dst, w.desc, w.enc, w.patch != nil, row, count, bytes, childStart, tierMask)
	if err != nil {
		return err
	}
	if w.patch != nil {
		if exception {
			if uint64(len(p.vals)) == patchedFields*w.wantExceptions {
				return fmt.Errorf("%w: more than the %d exceptions planned", errCorePlan, w.wantExceptions)
			}
			p.vals = append(p.vals, count, bytes, childStart)
		}
		p.bits = setBit(p.bits, p.rows, exception)
	}
	if p.rows == 0 {
		p.firstChild = childStart
	}
	p.lastChild = childStart
	p.rows++

	return nil
}

// Flush writes the rows still buffered.
func (r *CoreStatsRange) Flush() error { return r.buf.flush() }

// Merge takes the next range's part, in row order: it checks the child
// starts run on from the range before and writes the range's exceptions.
func (w *CoreStatsRanges) Merge(p *CoreStatsPart) error {
	if p.first != w.n {
		return fmt.Errorf("%w: core range at row %d after %d rows", errRangeOrder, p.first, w.n)
	}
	if p.rows == 0 {
		return nil
	}
	exceptions := uint64(len(p.vals) / patchedFields)
	if p.firstChild < w.lastChild || w.exceptions+exceptions > w.wantExceptions {
		return fmt.Errorf("%w: row %d (child start %d after %d, %d exceptions after %d of %d planned)",
			errCorePlan, p.first, p.firstChild, w.lastChild, exceptions, w.exceptions, w.wantExceptions)
	}
	if w.patch != nil {
		if err := w.patch.addRange(p.bits, p.rows, p.vals); err != nil {
			return err
		}
	}
	w.n += p.rows
	w.exceptions += exceptions
	w.lastChild = p.lastChild

	return nil
}

// Close finishes the files once every row the plan counted is merged, as
// CoreStatsWriter.Close does. On an error every file it started is
// removed.
func (w *CoreStatsRanges) Close() error {
	if w.n != w.plan.rows || w.exceptions != w.wantExceptions || (w.n > 0 && w.lastChild != w.n) {
		return errors.Join(fmt.Errorf("%w: wrote %d rows with %d exceptions, last child start %d; planned %d rows with %d",
			errCorePlan, w.n, w.exceptions, w.lastChild, w.plan.rows, w.wantExceptions), w.Abort())
	}
	err := closeSized(w.f)
	w.f = nil
	if err != nil {
		return errors.Join(err, w.Abort())
	}
	if w.patch != nil {
		err := w.patch.close()
		w.patch = nil
		if err != nil {
			return errors.Join(err, w.Abort())
		}
	}
	w.closed = true

	return nil
}

// Abort removes every file the writer started, unless Close finished
// them. Idempotent.
func (w *CoreStatsRanges) Abort() error {
	if w.closed {
		return nil
	}
	err := errors.Join(abortSized(w.f), removeIfErr(w.path))
	w.f = nil
	if w.patch != nil {
		err = errors.Join(err, w.patch.abort())
		w.patch = nil
	}

	return err
}
