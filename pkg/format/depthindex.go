package format

import (
	"errors"
	"fmt"
	"path/filepath"
)

// DepthIndexBuilder accumulates positions per depth and writes the
// depth posting lists at Finalize. Storage is **disk-backed per
// depth** via u64StreamArray (zstd-compressed): at billion-prefix
// scale the positions would otherwise be ~8 GiB of heap (1B × 8 B).
// Each per-depth array is append-only and positions are added in
// preorder (pos = 0, 1, 2, ...), so within any one depth they are
// already sorted — no post-add sort needed.
//
// The "monotonically increasing u64s in append order, read back once
// sequentially" access pattern is what makes the compressed-stream
// primitive the right fit here (vs the mmap-backed u64DiskArray the
// MPHF builder needs).
//
// Alongside the positions it bounds each depth's stats per block of
// DepthBlockSize entries (BlockBounds), so ranked and filtered queries
// can skip blocks that can't qualify. A full block's packed bounds go
// into the depth's stream right after its last position, so the bounds
// cost no stream, file or encoder of their own; Build splits them out.
type DepthIndexBuilder struct {
	tempDir  string
	buckets  []*u64StreamArray // index = depth
	open     []blockAcc        // each depth's block still filling
	maxDepth uint32
}

// NewDepthIndexBuilder creates a depth index builder backed by
// per-depth disk arrays. The tempDir is where per-depth scratch
// files live; cleaned up by Build (after the final arrays are
// written) or by Close on the error path.
func NewDepthIndexBuilder(tempDir string) *DepthIndexBuilder {
	return &DepthIndexBuilder{tempDir: tempDir}
}

// Add appends pos, a prefix with count objects of bytes total, to the
// per-depth posting list. Caller must add in pos order — positions
// within a depth are written in arrival order and Build relies on that
// order being already sorted.
func (b *DepthIndexBuilder) Add(pos uint64, depth uint32, count, bytes uint64) error {
	for uint32(len(b.buckets)) <= depth {
		d := len(b.buckets)
		a, err := newU64StreamArray(b.tempDir, fmt.Sprintf(DepthBucketNameFmt, d))
		if err != nil {
			return fmt.Errorf("create depth bucket %d: %w", d, err)
		}
		b.buckets = append(b.buckets, a)
		b.open = append(b.open, blockAcc{})
	}
	if err := b.buckets[depth].Append(pos); err != nil {
		return fmt.Errorf("append depth %d pos: %w", depth, err)
	}
	acc := &b.open[depth]
	acc.add(count, bytes)
	if acc.n == DepthBlockSize {
		if err := b.buckets[depth].Append(packBlockBounds(acc.bounds)); err != nil {
			return fmt.Errorf("append depth %d block: %w", depth, err)
		}
		*acc = blockAcc{}
	}
	if depth > b.maxDepth {
		b.maxDepth = depth
	}

	return nil
}

func (b *DepthIndexBuilder) Build(outDir string) error {
	offsetsPath := filepath.Join(outDir, DepthOffsetsFile)
	positionsPath := filepath.Join(outDir, DepthPositionsFile)
	blocksPath := filepath.Join(outDir, DepthBlocksFile)

	offsetsWriter, err := NewArrayWriter(offsetsPath, 8)
	if err != nil {
		return fmt.Errorf("create offsets writer: %w", err)
	}
	positionsWriter, err := NewArrayWriter(positionsPath, 8)
	if err != nil {
		offsetsWriter.Close()

		return fmt.Errorf("create positions writer: %w", err)
	}
	blocksWriter, err := NewArrayWriter(blocksPath, 8)
	if err != nil {
		offsetsWriter.Close()
		positionsWriter.Close()

		return fmt.Errorf("create blocks writer: %w", err)
	}

	offset := uint64(0)
	maxPos := uint64(0)
	var errs []error
	for d := uint32(0); d <= b.maxDepth; d++ {
		if err := offsetsWriter.WriteU64(offset); err != nil {
			errs = append(errs, fmt.Errorf("write offset depth %d: %w", d, err))

			break
		}
		written, top, err := b.writeDepth(d, positionsWriter, blocksWriter)
		if err != nil {
			errs = append(errs, err)

			break
		}
		offset += written
		maxPos = max(maxPos, top)
	}

	if err := offsetsWriter.WriteU64(offset); err != nil {
		errs = append(errs, fmt.Errorf("write sentinel offset: %w", err))
	}
	if err := positionsWriter.Close(); err != nil {
		errs = append(errs, fmt.Errorf("close positions: %w", err))
	}
	if err := blocksWriter.Close(); err != nil {
		errs = append(errs, fmt.Errorf("close blocks: %w", err))
	}
	if err := offsetsWriter.Close(); err != nil {
		errs = append(errs, fmt.Errorf("close offsets: %w", err))
	}

	if len(errs) > 0 {
		if err := removeIfErr(positionsPath); err != nil {
			errs = append(errs, err)
		}
		if err := removeIfErr(offsetsPath); err != nil {
			errs = append(errs, err)
		}
		if err := removeIfErr(blocksPath); err != nil {
			errs = append(errs, err)
		}

		return errors.Join(errs...)
	}

	// Adaptive width on both files: positions cap at the observed max
	// preorder pos; offsets cap at total positions (== offset). Both
	// can be considerably narrower than the 8 B/elt build stride.
	posWidth := byteWidthOf(maxPos)
	if err := RepackArrayWidthU64(positionsPath, positionsPath, posWidth); err != nil {
		return fmt.Errorf("repack depth_positions to width %d: %w", posWidth, err)
	}
	offWidth := byteWidthOf(offset)
	if err := RepackArrayWidthU64(offsetsPath, offsetsPath, offWidth); err != nil {
		return fmt.Errorf("repack depth_offsets to width %d: %w", offWidth, err)
	}

	return nil
}

// writeDepth splits depth d's stream into its positions, appended to
// positions, and its block bounds, the last partial block's included,
// appended to blocks. It releases the depth's stream and returns how
// many positions it wrote and the largest.
func (b *DepthIndexBuilder) writeDepth(d uint32, positions, blocks *ArrayWriter) (uint64, uint64, error) { //nolint:gocritic // gocritic wants named returns, nonamedreturns forbids them
	if int(d) >= len(b.buckets) || b.buckets[d] == nil {
		return 0, 0, nil
	}
	bucket := b.buckets[d]
	defer func() {
		bucket.Close()
		b.buckets[d] = nil
	}()
	var written, top uint64
	inBlock := 0
	if err := bucket.Iterate(func(v uint64) error {
		if inBlock == DepthBlockSize {
			inBlock = 0
			if err := blocks.WriteU64(v); err != nil {
				return fmt.Errorf("write block depth %d: %w", d, err)
			}

			return nil
		}
		if err := positions.WriteU64(v); err != nil {
			return fmt.Errorf("write position depth %d: %w", d, err)
		}
		inBlock++
		top = max(top, v)
		written++

		return nil
	}); err != nil {
		return 0, 0, fmt.Errorf("iterate depth %d: %w", d, err)
	}
	if acc := b.open[d]; acc.n > 0 {
		if err := blocks.WriteU64(packBlockBounds(acc.bounds)); err != nil {
			return 0, 0, fmt.Errorf("write depth %d last block: %w", d, err)
		}
	}

	return written, top, nil
}

// Close releases any open per-depth buckets without writing the
// output. Used on the error path; idempotent.
func (b *DepthIndexBuilder) Close() error {
	var errs []error
	for i, s := range b.buckets {
		if s == nil {
			continue
		}
		if err := s.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close depth %d stream: %w", i, err))
		}
		b.buckets[i] = nil
	}

	return errors.Join(errs...)
}

// DepthIndex provides read access to depth posting lists.
//
// Thread Safety: DepthIndex is safe for concurrent read access from multiple
// goroutines. All read methods can be called concurrently. Close should only
// be called once, after all read operations have completed.
type DepthIndex struct {
	offsets   *ArrayReader
	positions *ArrayReader
	blocks    *ArrayReader
	blockBase []uint64 // each depth's first block in blocks; one past the last depth holds the total
	maxDepth  uint32
}

// OpenDepthIndex opens a depth index from files.
func OpenDepthIndex(outDir string) (*DepthIndex, error) {
	offsetsPath := filepath.Join(outDir, DepthOffsetsFile)
	positionsPath := filepath.Join(outDir, DepthPositionsFile)

	// Both arrays keep the kernel's default readahead; see OpenMmap.
	offsets, err := OpenArray(offsetsPath)
	if err != nil {
		return nil, fmt.Errorf("open offsets: %w", err)
	}

	positions, err := OpenArray(positionsPath)
	if err != nil {
		offsets.Close()

		return nil, fmt.Errorf("open positions: %w", err)
	}

	blocks, err := OpenArray(filepath.Join(outDir, DepthBlocksFile))
	if err != nil {
		_ = errors.Join(offsets.Close(), positions.Close())

		return nil, fmt.Errorf("open blocks: %w", err)
	}

	// maxDepth = number of offsets - 2 (because we have maxDepth+2 entries)
	maxDepth := uint32(0)
	if offsets.Count() > 1 {
		maxDepth = uint32(offsets.Count() - 2)
	}

	d := &DepthIndex{
		offsets:   offsets,
		positions: positions,
		blocks:    blocks,
		maxDepth:  maxDepth,
	}
	if err := d.indexBlocks(); err != nil {
		return nil, errors.Join(err, d.Close())
	}

	return d, nil
}

// indexBlocks sets each depth's first block and checks the blocks file
// holds exactly one entry per block. The offsets are one per depth,
// so this is O(depths).
func (d *DepthIndex) indexBlocks() error {
	n := d.offsets.Count()
	if n < 2 {
		if d.blocks.Count() != 0 {
			return fmt.Errorf("%w: %d depth blocks for an empty depth index", ErrSizeMismatch, d.blocks.Count())
		}

		return nil
	}
	d.blockBase = make([]uint64, n)
	for i := uint64(1); i < n; i++ {
		lo, hi := d.offsets.UnsafeGetU64(i-1), d.offsets.UnsafeGetU64(i)
		if hi < lo {
			return fmt.Errorf("%w: depth %d offsets go backwards (%d after %d)", ErrSizeMismatch, i-1, hi, lo)
		}
		d.blockBase[i] = d.blockBase[i-1] + (hi-lo+DepthBlockSize-1)/DepthBlockSize
	}
	if want := d.blockBase[n-1]; d.blocks.Count() != want {
		return fmt.Errorf("%w: %d depth blocks, the offsets need %d", ErrSizeMismatch, d.blocks.Count(), want)
	}

	return nil
}

// Count is the number of positions the index holds across all depths.
func (d *DepthIndex) Count() uint64 { return d.positions.Count() }

// End is the last offset: where the deepest level's positions end. It
// equals Count for a well-formed index.
func (d *DepthIndex) End() uint64 {
	if d.offsets.Count() == 0 {
		return 0
	}

	return d.offsets.UnsafeGetU64(d.offsets.Count() - 1)
}

// Close releases resources.
func (d *DepthIndex) Close() error {
	return errors.Join(d.offsets.Close(), d.positions.Close(), d.blocks.Close())
}

// MaxDepth returns the maximum depth in the index.
func (d *DepthIndex) MaxDepth() uint32 {
	return d.maxDepth
}

// PositionsInSubtree returns positions at the given depth that fall within
// the subtree [subtreeStart, subtreeEnd]. Uses binary search.
func (d *DepthIndex) PositionsInSubtree(depth uint32, subtreeStart, subtreeEnd uint64) ([]uint64, error) {
	span, err := d.SubtreeSpan(depth, subtreeStart, subtreeEnd)
	if err != nil || span.Len() == 0 {
		return nil, err
	}

	positions := make([]uint64, span.Len())
	for i := range positions {
		positions[i] = d.positions.UnsafeGetU64(span.Lo + uint64(i))
	}

	return positions, nil
}

// Span is a half-open range [Lo, Hi) of depth posting-list indices.
type Span struct {
	Lo, Hi uint64
}

// Len is the number of positions in the span.
func (s Span) Len() uint64 { return s.Hi - s.Lo }

// SubtreeSpan returns the posting-list indices whose positions at depth
// fall within the subtree [subtreeStart, subtreeEnd], so callers can
// count or window the matches without materialising them.
func (d *DepthIndex) SubtreeSpan(depth uint32, subtreeStart, subtreeEnd uint64) (Span, error) {
	if depth > d.maxDepth {
		return Span{}, nil
	}

	start, err := d.offsets.GetU64(uint64(depth))
	if err != nil {
		return Span{}, fmt.Errorf("get start offset for depth %d: %w", depth, err)
	}

	end, err := d.offsets.GetU64(uint64(depth + 1))
	if err != nil {
		return Span{}, fmt.Errorf("get end offset for depth %d: %w", depth, err)
	}

	if start >= end {
		return Span{}, nil
	}

	lo := d.binarySearchLower(start, end, subtreeStart)
	// Positions strictly increase, so the subtree holds at most
	// subtreeEnd-subtreeStart+1 of this level's entries: search only
	// those, not the rest of the level.
	hi := end
	if span := subtreeEnd - subtreeStart + 1; subtreeEnd >= subtreeStart && lo+span < end {
		hi = lo + span
	}

	return Span{Lo: lo, Hi: d.binarySearchUpper(lo, hi, subtreeEnd)}, nil
}

// PositionAt returns the position at posting-list index i. The index
// must come from a SubtreeSpan range.
func (d *DepthIndex) PositionAt(i uint64) uint64 {
	return d.positions.UnsafeGetU64(i)
}

// binarySearchLower finds the first index where positions[idx] >= target.
func (d *DepthIndex) binarySearchLower(start, end, target uint64) uint64 {
	lo, hi := start, end
	for lo < hi {
		mid := lo + (hi-lo)/2
		val := d.positions.UnsafeGetU64(mid)
		if val < target {
			lo = mid + 1
		} else {
			hi = mid
		}
	}

	return lo
}

// binarySearchUpper finds the first index where positions[idx] > target.
func (d *DepthIndex) binarySearchUpper(start, end, target uint64) uint64 {
	lo, hi := start, end
	for lo < hi {
		mid := lo + (hi-lo)/2
		val := d.positions.UnsafeGetU64(mid)
		if val <= target {
			lo = mid + 1
		} else {
			hi = mid
		}
	}

	return lo
}
