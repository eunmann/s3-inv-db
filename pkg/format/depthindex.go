package format

import (
	"fmt"
	"path/filepath"
	"sort"
)

// DepthIndexBuilder builds depth posting lists from depth array.
type DepthIndexBuilder struct {
	buckets  map[uint32][]uint64 // depth -> positions
	maxDepth uint32
}

// NewDepthIndexBuilder creates a new depth index builder.
func NewDepthIndexBuilder() *DepthIndexBuilder {
	return &DepthIndexBuilder{
		buckets: make(map[uint32][]uint64),
	}
}

// Add adds a position at the given depth.
func (b *DepthIndexBuilder) Add(pos uint64, depth uint32) {
	b.buckets[depth] = append(b.buckets[depth], pos)
	if depth > b.maxDepth {
		b.maxDepth = depth
	}
}

// Build writes the depth index files.
func (b *DepthIndexBuilder) Build(outDir string) error {
	offsetsPath := filepath.Join(outDir, "depth_offsets.u64")
	positionsPath := filepath.Join(outDir, "depth_positions.u64")

	// Create offsets writer (maxDepth+2 entries: one for each depth + sentinel)
	offsetsWriter, err := NewArrayWriter(offsetsPath, 8)
	if err != nil {
		return fmt.Errorf("create offsets writer: %w", err)
	}

	positionsWriter, err := NewArrayWriter(positionsPath, 8)
	if err != nil {
		offsetsWriter.Close()
		return fmt.Errorf("create positions writer: %w", err)
	}

	offset := uint64(0)

	// Write positions for each depth, track offsets
	for d := uint32(0); d <= b.maxDepth; d++ {
		if err := offsetsWriter.WriteU64(offset); err != nil {
			offsetsWriter.Close()
			positionsWriter.Close()
			return fmt.Errorf("write offset: %w", err)
		}

		positions := b.buckets[d]
		// Positions should already be sorted since we add in pos order,
		// but verify/sort for safety
		if !sort.SliceIsSorted(positions, func(i, j int) bool {
			return positions[i] < positions[j]
		}) {
			sort.Slice(positions, func(i, j int) bool {
				return positions[i] < positions[j]
			})
		}

		for _, pos := range positions {
			if err := positionsWriter.WriteU64(pos); err != nil {
				offsetsWriter.Close()
				positionsWriter.Close()
				return fmt.Errorf("write position: %w", err)
			}
		}
		offset += uint64(len(positions))
	}

	// Write sentinel offset
	if err := offsetsWriter.WriteU64(offset); err != nil {
		offsetsWriter.Close()
		positionsWriter.Close()
		return fmt.Errorf("write sentinel offset: %w", err)
	}

	if err := positionsWriter.Close(); err != nil {
		offsetsWriter.Close()
		return fmt.Errorf("close positions: %w", err)
	}

	if err := offsetsWriter.Close(); err != nil {
		return fmt.Errorf("close offsets: %w", err)
	}

	return nil
}

// MaxDepth returns the maximum depth seen.
func (b *DepthIndexBuilder) MaxDepth() uint32 {
	return b.maxDepth
}

// DepthIndex provides read access to depth posting lists.
type DepthIndex struct {
	offsets   *ArrayReader
	positions *ArrayReader
	maxDepth  uint32
}

// OpenDepthIndex opens a depth index from files.
func OpenDepthIndex(outDir string) (*DepthIndex, error) {
	offsetsPath := filepath.Join(outDir, "depth_offsets.u64")
	positionsPath := filepath.Join(outDir, "depth_positions.u64")

	offsets, err := OpenArray(offsetsPath)
	if err != nil {
		return nil, fmt.Errorf("open offsets: %w", err)
	}

	positions, err := OpenArray(positionsPath)
	if err != nil {
		offsets.Close()
		return nil, fmt.Errorf("open positions: %w", err)
	}

	// maxDepth = number of offsets - 2 (because we have maxDepth+2 entries)
	maxDepth := uint32(0)
	if offsets.Count() > 1 {
		maxDepth = uint32(offsets.Count() - 2)
	}

	return &DepthIndex{
		offsets:   offsets,
		positions: positions,
		maxDepth:  maxDepth,
	}, nil
}

// Close releases resources.
func (d *DepthIndex) Close() error {
	err1 := d.offsets.Close()
	err2 := d.positions.Close()
	if err1 != nil {
		return err1
	}
	return err2
}

// MaxDepth returns the maximum depth in the index.
func (d *DepthIndex) MaxDepth() uint32 {
	return d.maxDepth
}

// GetPositionsAtDepth returns all positions at the given depth.
func (d *DepthIndex) GetPositionsAtDepth(depth uint32) ([]uint64, error) {
	if depth > d.maxDepth {
		return nil, nil
	}

	start, err := d.offsets.GetU64(uint64(depth))
	if err != nil {
		return nil, err
	}

	end, err := d.offsets.GetU64(uint64(depth + 1))
	if err != nil {
		return nil, err
	}

	count := end - start
	if count == 0 {
		return nil, nil
	}

	positions := make([]uint64, count)
	for i := uint64(0); i < count; i++ {
		pos, err := d.positions.GetU64(start + i)
		if err != nil {
			return nil, err
		}
		positions[i] = pos
	}

	return positions, nil
}

// GetPositionsInSubtree returns positions at the given depth that fall within
// the subtree [subtreeStart, subtreeEnd]. Uses binary search.
func (d *DepthIndex) GetPositionsInSubtree(depth uint32, subtreeStart, subtreeEnd uint64) ([]uint64, error) {
	if depth > d.maxDepth {
		return nil, nil
	}

	start, err := d.offsets.GetU64(uint64(depth))
	if err != nil {
		return nil, err
	}

	end, err := d.offsets.GetU64(uint64(depth + 1))
	if err != nil {
		return nil, err
	}

	if start >= end {
		return nil, nil
	}

	// Binary search for lower bound (first pos >= subtreeStart)
	lo := d.binarySearchLower(start, end, subtreeStart)

	// Binary search for upper bound (first pos > subtreeEnd)
	hi := d.binarySearchUpper(lo, end, subtreeEnd)

	count := hi - lo
	if count == 0 {
		return nil, nil
	}

	positions := make([]uint64, count)
	for i := uint64(0); i < count; i++ {
		positions[i] = d.positions.UnsafeGetU64(lo + i)
	}

	return positions, nil
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

// DepthIterator iterates over positions at a specific depth within a subtree.
type DepthIterator struct {
	index   *DepthIndex
	start   uint64 // slice start in positions array
	end     uint64 // slice end in positions array
	current uint64 // current index in positions array
	pos     uint64 // current position value
}

// NewDepthIterator creates an iterator for a subtree at a specific depth.
func (d *DepthIndex) NewDepthIterator(depth uint32, subtreeStart, subtreeEnd uint64) (*DepthIterator, error) {
	if depth > d.maxDepth {
		return &DepthIterator{current: 0, end: 0}, nil
	}

	sliceStart, err := d.offsets.GetU64(uint64(depth))
	if err != nil {
		return nil, err
	}

	sliceEnd, err := d.offsets.GetU64(uint64(depth + 1))
	if err != nil {
		return nil, err
	}

	// Binary search for bounds
	lo := d.binarySearchLower(sliceStart, sliceEnd, subtreeStart)
	hi := d.binarySearchUpper(lo, sliceEnd, subtreeEnd)

	return &DepthIterator{
		index:   d,
		start:   lo,
		end:     hi,
		current: lo,
	}, nil
}

// Next advances to the next position. Returns false when done.
func (it *DepthIterator) Next() bool {
	if it.current >= it.end {
		return false
	}
	it.pos = it.index.positions.UnsafeGetU64(it.current)
	it.current++
	return true
}

// Pos returns the current position.
func (it *DepthIterator) Pos() uint64 {
	return it.pos
}

// Count returns the number of remaining positions.
func (it *DepthIterator) Count() uint64 {
	return it.end - it.current
}
