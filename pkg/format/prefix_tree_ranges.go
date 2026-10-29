package format

import (
	"encoding/binary"
	"errors"
	"fmt"
	"path/filepath"
)

// PrefixTreeRanges writes the file a PrefixTreeWriter does, byte for
// byte, from ranges of rows several goroutines write at once
// (PrefixTreeRange), each row straight to its place. The largest segment
// ID and parent distance each range wrote go in its part, which Merge
// takes in row order for Close to check against the widths.
type PrefixTreeRanges struct {
	path     string
	f        *hashedFile
	count    uint64
	maxDist  uint64
	segments uint32
	segW     uint8
	distW    uint8
	// The rows merged, and their largest segment ID and distance.
	n       uint64
	topDist uint64
	topSeg  uint32
	anySeg  bool
	closed  bool
}

// NewPrefixTreeRanges creates the prefix tree of count prefixes in
// outDir at its final size, as NewPrefixTreeWriter does.
func NewPrefixTreeRanges(outDir string, count uint64, segments uint32, maxDist uint64, sums *Checksums) (*PrefixTreeRanges, error) {
	w := &PrefixTreeRanges{
		path:     filepath.Join(outDir, PrefixTreeFile),
		count:    count,
		segments: segments,
		maxDist:  maxDist,
		segW:     byteWidthOf(uint64(max(segments, 1) - 1)),
		distW:    byteWidthOf(maxDist),
	}
	rowW := uint32(w.segW) + uint32(w.distW)
	head := EncodeHeader(Header{Magic: MagicNumber, Version: Version, Count: count, Width: rowW})
	size := prefixTreeDataOff + int64(count)*int64(rowW) + prefixTreeTailPad
	f, err := createSizedFile(w.path, append(head, w.segW, w.distW), size, sums)
	if err != nil {
		return nil, err
	}
	w.f = f

	return w, nil
}

// PrefixTreePart is what a range leaves for Merge.
type PrefixTreePart struct {
	first, rows uint64
	topDist     uint64
	topSeg      uint32
	anySeg      bool
}

// PrefixTreeRange writes ranges of prefix tree rows, one goroutine's.
// Its buffer is rangeBufSize.
type PrefixTreeRange struct {
	w    *PrefixTreeRanges
	part *PrefixTreePart
	buf  rangeBuf
}

// NewRange returns a range writer for one goroutine.
func (w *PrefixTreeRanges) NewRange() *PrefixTreeRange {
	return &PrefixTreeRange{w: w, buf: newRangeBuf(w.f)}
}

// Start begins a range at row, which part describes.
func (r *PrefixTreeRange) Start(row uint64, part *PrefixTreePart) error {
	if row > r.w.count {
		return fmt.Errorf("%w: range at row %d of %d", errPrefixTreePlan, row, r.w.count)
	}
	*part = PrefixTreePart{first: row}
	r.part = part

	return r.buf.seek(prefixTreeDataOff + int64(row)*int64(r.w.segW+r.w.distW))
}

// AddRoot writes the root prefix "", which must be row 0.
func (r *PrefixTreeRange) AddRoot() error {
	if row := r.part.first + r.part.rows; row != 0 {
		return fmt.Errorf("%w: root at position %d", errPrefixTreeOrder, row)
	}

	return r.writeRow(0, 0)
}

// Add writes the range's next prefix: the prefix at parentPos, then
// segment segID, then a slash, with the checks PrefixTreeWriter.Add
// makes.
func (r *PrefixTreeRange) Add(parentPos uint64, segID uint32) error {
	w, p := r.w, r.part
	row := p.first + p.rows
	if row == 0 || parentPos >= row {
		return fmt.Errorf("%w: parent %d for position %d", errPrefixTreeOrder, parentPos, row)
	}
	dist := row - parentPos
	if segID >= w.segments || dist > w.maxDist {
		return fmt.Errorf("%w: segment %d of %d, parent distance %d of at most %d",
			errPrefixTreePlan, segID, w.segments, dist, w.maxDist)
	}
	p.topSeg, p.topDist, p.anySeg = max(p.topSeg, segID), max(p.topDist, dist), true

	return r.writeRow(segID, dist)
}

func (r *PrefixTreeRange) writeRow(segID uint32, dist uint64) error {
	if r.part.first+r.part.rows == r.w.count {
		return fmt.Errorf("%w: more than %d rows", errPrefixTreePlan, r.w.count)
	}
	dst, err := r.buf.next(int(r.w.segW + r.w.distW))
	if err != nil {
		return err
	}
	binary.LittleEndian.PutUint64(dst, uint64(segID))
	binary.LittleEndian.PutUint64(dst[r.w.segW:], dist)
	r.part.rows++

	return nil
}

// Flush writes the rows still buffered.
func (r *PrefixTreeRange) Flush() error { return r.buf.flush() }

// Merge takes the next range's part, in row order.
func (w *PrefixTreeRanges) Merge(p *PrefixTreePart) error {
	if p.first != w.n {
		return fmt.Errorf("%w: prefix tree range at row %d after %d rows", errRangeOrder, p.first, w.n)
	}
	w.n += p.rows
	w.topSeg, w.topDist = max(w.topSeg, p.topSeg), max(w.topDist, p.topDist)
	w.anySeg = w.anySeg || p.anySeg

	return nil
}

// Close finishes the file once every row is merged and the largest
// segment ID and distance are the ones the widths were chosen for, as
// PrefixTreeWriter.Close does. On an error it removes the file.
func (w *PrefixTreeRanges) Close() error {
	segments := uint32(0)
	if w.anySeg {
		segments = w.topSeg + 1
	}
	if w.n != w.count || segments != w.segments || w.topDist != w.maxDist {
		return errors.Join(fmt.Errorf("%w: %d rows, %d segments, parent distance %d; planned %d, %d, %d",
			errPrefixTreePlan, w.n, segments, w.topDist, w.count, w.segments, w.maxDist), w.Abort())
	}
	err := closeSized(w.f)
	w.f = nil
	if err != nil {
		return fmt.Errorf("close prefix tree: %w", err)
	}
	w.closed = true

	return nil
}

// Abort removes the file unless Close finished it. Idempotent.
func (w *PrefixTreeRanges) Abort() error {
	if w.closed {
		return nil
	}
	err := errors.Join(abortSized(w.f), removeIfErr(w.path))
	w.f = nil

	return err
}
