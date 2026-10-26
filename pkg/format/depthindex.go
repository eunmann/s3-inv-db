package format

import (
	"errors"
	"fmt"
	"path/filepath"
)

// Rows are numbered in level order: the root, then every depth-1
// prefix, then every depth-2 prefix, each depth in key order. A
// depth's rows are therefore one contiguous range, and the depth
// index is just where each range starts (depth_offsets.u64), plus the
// stats bounds of each block of DepthBlockSize rows (depth_blocks.u64).

// DepthIndexWriter writes the depth offsets and block bounds from the
// rows in level order. Memory is constant: both files stream.
type DepthIndexWriter struct {
	offsetsPath string
	blocksPath  string
	offsets     *ArrayWriter
	blocks      *ArrayWriter
	rows        uint64
	depth       uint32
	open        blockAcc // the current depth's block still filling
	started     bool
}

// NewDepthIndexWriter creates the depth index files in outDir.
func NewDepthIndexWriter(outDir string) (*DepthIndexWriter, error) {
	w := &DepthIndexWriter{
		offsetsPath: filepath.Join(outDir, DepthOffsetsFile),
		blocksPath:  filepath.Join(outDir, DepthBlocksFile),
	}
	var err error
	if w.offsets, err = NewArrayWriter(w.offsetsPath, 8); err != nil {
		return nil, fmt.Errorf("create depth offsets: %w", err)
	}
	if w.blocks, err = NewArrayWriter(w.blocksPath, 8); err != nil {
		return nil, errors.Join(fmt.Errorf("create depth blocks: %w", err), w.Abort())
	}

	return w, nil
}

// ErrDepthOrder is returned when rows don't arrive in level order: the
// root at depth 0 first, then depths one at a time without gaps.
var ErrDepthOrder = errors.New("depth index: rows out of level order")

// Add adds the next row in level order: a prefix at depth with count
// objects of bytes total.
func (w *DepthIndexWriter) Add(depth uint32, count, bytes uint64) error {
	switch {
	case !w.started:
		if depth != 0 {
			return fmt.Errorf("%w: first row at depth %d", ErrDepthOrder, depth)
		}
		if err := w.offsets.WriteU64(0); err != nil {
			return fmt.Errorf("write depth offset: %w", err)
		}
		w.started = true
	case depth == w.depth+1:
		if err := w.flushBlock(); err != nil {
			return err
		}
		if err := w.offsets.WriteU64(w.rows); err != nil {
			return fmt.Errorf("write depth offset: %w", err)
		}
		w.depth = depth
	case depth != w.depth:
		return fmt.Errorf("%w: depth %d after depth %d", ErrDepthOrder, depth, w.depth)
	}
	w.open.add(count, bytes)
	w.rows++
	if w.open.n == DepthBlockSize {
		return w.flushBlock()
	}

	return nil
}

func (w *DepthIndexWriter) flushBlock() error {
	if w.open.n == 0 {
		return nil
	}
	if err := w.blocks.WriteU64(packBlockBounds(w.open.bounds)); err != nil {
		return fmt.Errorf("write depth block: %w", err)
	}
	w.open = blockAcc{}

	return nil
}

// Close writes the last block and the closing offset (the row count)
// and narrows the offsets to the width the row count needs.
func (w *DepthIndexWriter) Close() error {
	err := w.flushBlock()
	if err == nil && w.started {
		if err = w.offsets.WriteU64(w.rows); err != nil {
			err = fmt.Errorf("write sentinel offset: %w", err)
		}
	}
	if err != nil {
		return errors.Join(err, w.Abort())
	}
	offsets, blocks := w.offsets, w.blocks
	w.offsets, w.blocks = nil, nil
	if err := errors.Join(offsets.Close(), blocks.Close()); err != nil {
		return errors.Join(fmt.Errorf("close depth index: %w", err), removeIfErr(w.offsetsPath), removeIfErr(w.blocksPath))
	}
	if err := RepackArrayWidthU64(w.offsetsPath, w.offsetsPath, byteWidthOf(w.rows)); err != nil {
		return errors.Join(fmt.Errorf("repack depth offsets: %w", err), removeIfErr(w.offsetsPath), removeIfErr(w.blocksPath))
	}

	return nil
}

// Abort removes the files without finishing them. Idempotent.
func (w *DepthIndexWriter) Abort() error {
	var errs []error
	for _, a := range []**ArrayWriter{&w.offsets, &w.blocks} {
		if *a != nil {
			errs = append(errs, (*a).Close())
			*a = nil
		}
	}

	return errors.Join(append(errs, removeIfErr(w.offsetsPath), removeIfErr(w.blocksPath))...)
}

// DepthIndex reads where each depth's rows start and their block
// bounds.
//
// Thread Safety: DepthIndex is safe for concurrent read access from multiple
// goroutines. All read methods can be called concurrently. Close should only
// be called once, after all read operations have completed.
type DepthIndex struct {
	offsets   *ArrayReader
	blocks    *ArrayReader
	starts    []uint64 // each depth's first row; one past the last depth holds the row count
	blockBase []uint64 // each depth's first block in blocks; one past the last depth holds the total
	maxDepth  uint32
}

// OpenDepthIndex opens a depth index from files.
func OpenDepthIndex(outDir string) (*DepthIndex, error) {
	// Both arrays keep the kernel's default readahead; see OpenMmap.
	offsets, err := OpenArray(filepath.Join(outDir, DepthOffsetsFile))
	if err != nil {
		return nil, fmt.Errorf("open offsets: %w", err)
	}
	blocks, err := OpenArray(filepath.Join(outDir, DepthBlocksFile))
	if err != nil {
		return nil, errors.Join(fmt.Errorf("open blocks: %w", err), offsets.Close())
	}
	d := &DepthIndex{offsets: offsets, blocks: blocks}
	if err := d.load(); err != nil {
		return nil, errors.Join(err, d.Close())
	}

	return d, nil
}

// load reads the offsets (one per depth) into memory and checks the
// blocks file holds exactly one entry per block. O(depths).
func (d *DepthIndex) load() error {
	n := d.offsets.Count()
	if n == 1 {
		return fmt.Errorf("%w: one depth offset", ErrSizeMismatch)
	}
	d.starts = make([]uint64, n)
	d.blockBase = make([]uint64, max(n, 1))
	for i := range n {
		d.starts[i] = d.offsets.UnsafeGetU64(i)
		if i == 0 {
			continue
		}
		size := d.starts[i] - d.starts[i-1]
		if d.starts[i] <= d.starts[i-1] {
			return fmt.Errorf("%w: depth %d has no rows (offsets %d, %d)", ErrSizeMismatch, i-1, d.starts[i-1], d.starts[i])
		}
		d.blockBase[i] = d.blockBase[i-1] + (size+DepthBlockSize-1)/DepthBlockSize
	}
	if n > 0 {
		if d.starts[0] != 0 {
			return fmt.Errorf("%w: first depth starts at row %d", ErrSizeMismatch, d.starts[0])
		}
		d.maxDepth = uint32(n - 2)
	}
	if want := d.blockBase[len(d.blockBase)-1]; d.blocks.Count() != want {
		return fmt.Errorf("%w: %d depth blocks, the offsets need %d", ErrSizeMismatch, d.blocks.Count(), want)
	}

	return nil
}

// Count is the number of rows the offsets cover.
func (d *DepthIndex) Count() uint64 {
	if len(d.starts) == 0 {
		return 0
	}

	return d.starts[len(d.starts)-1]
}

// Close releases resources.
func (d *DepthIndex) Close() error {
	return errors.Join(d.offsets.Close(), d.blocks.Close())
}

// MaxDepth returns the maximum depth in the index.
func (d *DepthIndex) MaxDepth() uint32 {
	return d.maxDepth
}

// Start returns depth's first row; Start(MaxDepth()+1) is the row
// count.
func (d *DepthIndex) Start(depth uint32) uint64 {
	return d.starts[depth]
}

// DepthOf returns the depth of row, which must be below Count.
func (d *DepthIndex) DepthOf(row uint64) uint32 {
	// The first start above row ends its depth.
	lo, hi := 0, len(d.starts)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if d.starts[mid] > row {
			hi = mid
		} else {
			lo = mid + 1
		}
	}

	return uint32(lo - 1)
}

// Span is a half-open range [Lo, Hi) of rows, all at one depth.
type Span struct {
	Lo, Hi uint64
}

// Len is the number of rows in the span.
func (s Span) Len() uint64 { return s.Hi - s.Lo }
