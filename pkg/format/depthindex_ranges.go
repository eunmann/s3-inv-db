package format

import (
	"encoding/binary"
	"errors"
	"fmt"
	"path/filepath"
)

// DepthIndexRanges writes the files a DepthIndexWriter does, byte for
// byte, from the depths' sizes and ranges of their rows several
// goroutines write at once (DepthIndexRange). The sizes give the depth
// offsets, which it writes up front, and every block's place: a range
// starts on a block and ends on one or at its depth's end, so each
// writes whole blocks.
type DepthIndexRanges struct {
	blocks *hashedFile
	// sizes holds each depth's rows; blockBase each depth's first block,
	// one past the last depth the total.
	sizes, blockBase []uint64
	offsetsPath      string
	blocksPath       string
	// The depth and the row within it the parts merged reach.
	depth  int
	local  uint64
	closed bool
}

// NewDepthIndexRanges writes outDir's depth offsets for depths of the
// given sizes, which end at the first empty one, and creates the depth
// blocks at their final size.
func NewDepthIndexRanges(outDir string, sizes []uint64, sums *Checksums) (*DepthIndexRanges, error) {
	n := 0
	for n < len(sizes) && sizes[n] > 0 {
		n++
	}
	for _, size := range sizes[n:] {
		if size > 0 {
			return nil, fmt.Errorf("%w: a depth with rows below one without", ErrDepthOrder)
		}
	}
	w := &DepthIndexRanges{
		sizes:       sizes[:n],
		blockBase:   make([]uint64, n+1),
		offsetsPath: filepath.Join(outDir, DepthOffsetsFile),
		blocksPath:  filepath.Join(outDir, DepthBlocksFile),
	}
	var rows uint64
	for d, size := range w.sizes {
		w.blockBase[d+1] = w.blockBase[d] + (size+DepthBlockSize-1)/DepthBlockSize
		rows += size
	}
	if err := writeDepthOffsets(w.offsetsPath, w.sizes, rows, sums); err != nil {
		return nil, err
	}
	blocks := w.blockBase[n]
	head := EncodeHeader(Header{Magic: MagicNumber, Version: Version, Count: blocks, Width: 8})
	f, err := createSizedFile(w.blocksPath, head, HeaderSize+8*int64(blocks), sums)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("create depth blocks: %w", err), removeIfErr(w.offsetsPath))
	}
	w.blocks = f

	return w, nil
}

// writeDepthOffsets writes where each depth starts and, last, the row
// count, at the width the row count needs, as DepthIndexWriter.Close
// leaves them; no depths, no offsets.
func writeDepthOffsets(path string, sizes []uint64, rows uint64, sums *Checksums) error {
	count := 0
	if len(sizes) > 0 {
		count = len(sizes) + 1
	}
	width := byteWidthOf(rows)
	data := EncodeHeader(Header{Magic: MagicNumber, Version: Version, Count: uint64(count), Width: uint32(width)})
	var start uint64
	for d := range count {
		data = binary.LittleEndian.AppendUint64(data, start)[:len(data)+int(width)]
		if d < len(sizes) {
			start += sizes[d]
		}
	}
	data = append(data, make([]byte, arrayTailPad)...)
	if err := WriteFile(path, data, sums); err != nil {
		return errors.Join(fmt.Errorf("write depth offsets: %w", err), removeIfErr(path))
	}

	return nil
}

// DepthIndexPart is what a range leaves for Merge: its depth, its first
// row within the depth and its rows.
type DepthIndexPart struct {
	depth       int
	first, rows uint64
}

// DepthIndexRange writes ranges of depth blocks, one goroutine's. Its
// buffer is rangeBufSize.
type DepthIndexRange struct {
	w    *DepthIndexRanges
	part *DepthIndexPart
	buf  rangeBuf
	open blockAcc
}

// NewRange returns a range writer for one goroutine.
func (w *DepthIndexRanges) NewRange() *DepthIndexRange {
	return &DepthIndexRange{w: w, buf: newRangeBuf(w.blocks)}
}

// Start begins a range at row local of depth, a multiple of
// DepthBlockSize, which part describes.
func (r *DepthIndexRange) Start(depth int, local uint64, part *DepthIndexPart) error {
	if depth >= len(r.w.sizes) || local > r.w.sizes[depth] || local%DepthBlockSize != 0 {
		return fmt.Errorf("%w: range at row %d of depth %d", ErrDepthOrder, local, depth)
	}
	*part = DepthIndexPart{depth: depth, first: local}
	r.part, r.open = part, blockAcc{}

	return r.buf.seek(HeaderSize + 8*int64(r.w.blockBase[depth]+local/DepthBlockSize))
}

// Add adds the range's next row: a prefix with count objects of bytes
// total.
func (r *DepthIndexRange) Add(count, bytes uint64) error {
	p := r.part
	if p.first+p.rows == r.w.sizes[p.depth] {
		return fmt.Errorf("%w: more than %d rows at depth %d", ErrDepthOrder, r.w.sizes[p.depth], p.depth)
	}
	r.open.add(count, bytes)
	p.rows++
	if r.open.n == DepthBlockSize {
		return r.writeBlock()
	}

	return nil
}

func (r *DepthIndexRange) writeBlock() error {
	dst, err := r.buf.next(8)
	if err != nil {
		return err
	}
	binary.LittleEndian.PutUint64(dst, packBlockBounds(r.open.bounds))
	r.open = blockAcc{}

	return nil
}

// Flush ends the range, which must end on a block or at its depth's
// end, and writes the blocks still buffered.
func (r *DepthIndexRange) Flush() error {
	if r.open.n > 0 {
		if p := r.part; p.first+p.rows != r.w.sizes[p.depth] {
			return fmt.Errorf("%w: range ends at row %d of depth %d, inside a block", ErrDepthOrder, p.first+p.rows, p.depth)
		}
		if err := r.writeBlock(); err != nil {
			return err
		}
	}

	return r.buf.flush()
}

// Merge takes the next range's part, in level order.
func (w *DepthIndexRanges) Merge(p *DepthIndexPart) error {
	if p.rows == 0 {
		return nil
	}
	for w.depth < len(w.sizes) && w.local == w.sizes[w.depth] {
		w.depth, w.local = w.depth+1, 0
	}
	if p.depth != w.depth || p.first != w.local {
		return fmt.Errorf("%w: depth range at row %d of depth %d after row %d of depth %d",
			errRangeOrder, p.first, p.depth, w.local, w.depth)
	}
	w.local += p.rows

	return nil
}

// Close finishes the files once every row is merged. On an error it
// removes them.
func (w *DepthIndexRanges) Close() error {
	for w.depth < len(w.sizes) && w.local == w.sizes[w.depth] {
		w.depth, w.local = w.depth+1, 0
	}
	if w.depth != len(w.sizes) {
		return errors.Join(fmt.Errorf("%w: rows merged up to row %d of depth %d of %d",
			ErrDepthOrder, w.local, w.depth, len(w.sizes)), w.Abort())
	}
	err := closeSized(w.blocks)
	w.blocks = nil
	if err != nil {
		return errors.Join(fmt.Errorf("close depth index: %w", err), w.Abort())
	}
	w.closed = true

	return nil
}

// Abort removes the files unless Close finished them. Idempotent.
func (w *DepthIndexRanges) Abort() error {
	if w.closed {
		return nil
	}
	err := errors.Join(abortSized(w.blocks), removeIfErr(w.blocksPath), removeIfErr(w.offsetsPath))
	w.blocks = nil

	return err
}
