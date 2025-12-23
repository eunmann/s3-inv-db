// Package indexread provides read-only access to S3 inventory indexes.
package indexread

import (
	"fmt"
	"path/filepath"

	"github.com/eunmann/s3-inv-db/pkg/format"
)

// Index provides low-latency access to an S3 inventory index via mmap.
//
// Thread Safety: Index is safe for concurrent read access from multiple
// goroutines. All read methods (Lookup, Stats, TierBreakdown, etc.) can be
// called concurrently. Close should only be called once, after all read
// operations have completed.
type Index struct {
	subtreeEnd        *format.ArrayReader
	depth             *format.ArrayReader
	objectCount       *format.ArrayReader
	totalBytes        *format.ArrayReader
	maxDepthInSubtree *format.ArrayReader
	depthIndex        *format.DepthIndex
	mphf              *format.MPHF
	tierStats         *format.TierStatsReader
	count             uint64
	maxDepth          uint32
}

// Open opens an index from the given directory.
func Open(dir string) (*Index, error) {
	var idx Index
	var err error

	idx.subtreeEnd, err = format.OpenArray(filepath.Join(dir, "subtree_end.u64"))
	if err != nil {
		return nil, fmt.Errorf("open subtree_end: %w", err)
	}

	idx.depth, err = format.OpenArray(filepath.Join(dir, "depth.u32"))
	if err != nil {
		idx.Close()
		return nil, fmt.Errorf("open depth: %w", err)
	}

	idx.objectCount, err = format.OpenArray(filepath.Join(dir, "object_count.u64"))
	if err != nil {
		idx.Close()
		return nil, fmt.Errorf("open object_count: %w", err)
	}

	idx.totalBytes, err = format.OpenArray(filepath.Join(dir, "total_bytes.u64"))
	if err != nil {
		idx.Close()
		return nil, fmt.Errorf("open total_bytes: %w", err)
	}

	idx.maxDepthInSubtree, err = format.OpenArray(filepath.Join(dir, "max_depth_in_subtree.u32"))
	if err != nil {
		idx.Close()
		return nil, fmt.Errorf("open max_depth_in_subtree: %w", err)
	}

	idx.depthIndex, err = format.OpenDepthIndex(dir)
	if err != nil {
		idx.Close()
		return nil, fmt.Errorf("open depth index: %w", err)
	}

	idx.mphf, err = format.OpenMPHF(dir)
	if err != nil {
		idx.Close()
		return nil, fmt.Errorf("open MPHF: %w", err)
	}

	idx.tierStats, err = format.OpenTierStats(dir)
	if err != nil {
		idx.Close()
		return nil, fmt.Errorf("open tier stats: %w", err)
	}

	idx.count = idx.subtreeEnd.Count()
	idx.maxDepth = idx.depthIndex.MaxDepth()

	return &idx, nil
}

// closer is an interface for types with a Close method.
type closer interface {
	Close() error
}

// closeAll closes multiple resources and returns the first error encountered.
func closeAll(closers ...closer) error {
	var firstErr error
	for _, c := range closers {
		if c == nil {
			continue
		}
		if err := c.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// Close releases all resources.
func (idx *Index) Close() error {
	return closeAll(
		idx.subtreeEnd,
		idx.depth,
		idx.objectCount,
		idx.totalBytes,
		idx.maxDepthInSubtree,
		idx.depthIndex,
		idx.mphf,
		idx.tierStats,
	)
}

// Stats holds aggregated statistics for a prefix.
type Stats struct {
	ObjectCount uint64
	TotalBytes  uint64
}

// Lookup returns the preorder position for a prefix, or ok=false if not found.
func (idx *Index) Lookup(prefix string) (pos uint64, ok bool) {
	return idx.mphf.Lookup(prefix)
}

// Stats returns the object count and total bytes for the node at pos.
// Returns zero-valued Stats if pos is out of bounds. Use StatsForPrefix
// if you need to distinguish between "not found" and "found with zero stats".
func (idx *Index) Stats(pos uint64) Stats {
	if pos >= idx.count {
		return Stats{}
	}
	return Stats{
		ObjectCount: idx.objectCount.UnsafeGetU64(pos),
		TotalBytes:  idx.totalBytes.UnsafeGetU64(pos),
	}
}

// StatsForPrefix returns stats for a prefix string.
func (idx *Index) StatsForPrefix(prefix string) (Stats, bool) {
	pos, ok := idx.Lookup(prefix)
	if !ok {
		return Stats{}, false
	}
	return idx.Stats(pos), true
}

// Depth returns the depth of the node at pos.
func (idx *Index) Depth(pos uint64) uint32 {
	if pos >= idx.count {
		return 0
	}
	return idx.depth.UnsafeGetU32(pos)
}

// SubtreeEnd returns the end position of the subtree rooted at pos.
func (idx *Index) SubtreeEnd(pos uint64) uint64 {
	if pos >= idx.count {
		return 0
	}
	return idx.subtreeEnd.UnsafeGetU64(pos)
}

// MaxDepthInSubtree returns the maximum depth in the subtree rooted at pos.
func (idx *Index) MaxDepthInSubtree(pos uint64) uint32 {
	if pos >= idx.count {
		return 0
	}
	return idx.maxDepthInSubtree.UnsafeGetU32(pos)
}

// PrefixString returns the prefix string for the node at pos.
func (idx *Index) PrefixString(pos uint64) (string, error) {
	s, err := idx.mphf.GetPrefix(pos)
	if err != nil {
		return "", fmt.Errorf("get prefix for pos %d: %w", pos, err)
	}
	return s, nil
}

// Count returns the total number of prefix nodes.
func (idx *Index) Count() uint64 {
	return idx.count
}

// MaxDepth returns the maximum depth in the trie.
func (idx *Index) MaxDepth() uint32 {
	return idx.maxDepth
}

// DescendantsAtDepth returns positions of descendants at exactly the given
// relative depth, in alphabetical order.
//
// Returns (nil, nil) for invalid inputs:
//   - prefixPos >= Count() (out of bounds)
//   - relDepth < 0 (negative depth)
//   - targetDepth > MaxDepth() (no nodes exist at that depth)
//
// An empty slice indicates no descendants at that depth (valid but empty result).
func (idx *Index) DescendantsAtDepth(prefixPos uint64, relDepth int) ([]uint64, error) {
	if prefixPos >= idx.count {
		return nil, nil
	}
	if relDepth < 0 {
		return nil, nil
	}

	baseDepth := idx.Depth(prefixPos)
	targetDepth := baseDepth + uint32(relDepth)

	if targetDepth > idx.maxDepth {
		return nil, nil
	}

	subtreeStart := prefixPos
	subtreeEnd := idx.SubtreeEnd(prefixPos)

	positions, err := idx.depthIndex.GetPositionsInSubtree(targetDepth, subtreeStart, subtreeEnd)
	if err != nil {
		return nil, fmt.Errorf("get positions at depth %d: %w", targetDepth, err)
	}
	return positions, nil
}

// DescendantsUpToDepth returns positions of descendants up to the given
// relative depth, grouped by depth then alphabetical.
//
// Returns (nil, nil) for invalid inputs:
//   - prefixPos >= Count() (out of bounds)
//   - maxRelDepth < 0 (negative depth)
//
// An empty slice indicates no descendants up to that depth (valid but empty result).
func (idx *Index) DescendantsUpToDepth(prefixPos uint64, maxRelDepth int) ([][]uint64, error) {
	if prefixPos >= idx.count {
		return nil, nil
	}
	if maxRelDepth < 0 {
		return nil, nil
	}

	baseDepth := idx.Depth(prefixPos)
	maxSubtreeDepth := idx.MaxDepthInSubtree(prefixPos)
	subtreeStart := prefixPos
	subtreeEnd := idx.SubtreeEnd(prefixPos)

	// Pre-allocate result slice with exact capacity
	depthLevels := min(int(maxSubtreeDepth-baseDepth), maxRelDepth)
	if depthLevels <= 0 {
		return nil, nil
	}
	result := make([][]uint64, 0, depthLevels)

	for d := baseDepth + 1; d <= baseDepth+uint32(maxRelDepth) && d <= maxSubtreeDepth; d++ {
		positions, err := idx.depthIndex.GetPositionsInSubtree(d, subtreeStart, subtreeEnd)
		if err != nil {
			return nil, fmt.Errorf("get positions at depth %d: %w", d, err)
		}
		result = append(result, positions)
	}

	return result, nil
}

// Filter specifies criteria for filtering results.
type Filter struct {
	MinCount uint64
	MinBytes uint64
}

// DescendantsAtDepthFiltered returns filtered descendants at a specific depth.
func (idx *Index) DescendantsAtDepthFiltered(prefixPos uint64, relDepth int, filter Filter) ([]uint64, error) {
	positions, err := idx.DescendantsAtDepth(prefixPos, relDepth)
	if err != nil {
		return nil, fmt.Errorf("get descendants at depth %d: %w", relDepth, err)
	}

	if filter.MinCount == 0 && filter.MinBytes == 0 {
		return positions, nil
	}

	// Pre-allocate with input length (worst case: all positions pass filter)
	filtered := make([]uint64, 0, len(positions))
	for _, pos := range positions {
		stats := idx.Stats(pos)
		if stats.ObjectCount >= filter.MinCount && stats.TotalBytes >= filter.MinBytes {
			filtered = append(filtered, pos)
		}
	}

	return filtered, nil
}

// Iterator provides sequential access to node positions.
type Iterator interface {
	// Next advances to the next position. Returns false when done.
	Next() bool
	// Pos returns the current position. Valid after Next() returns true.
	Pos() uint64
	// Depth returns the depth of the current position.
	Depth() uint32
}

// depthIteratorWrapper wraps format.DepthIterator to implement Iterator.
type depthIteratorWrapper struct {
	it    *format.DepthIterator
	depth uint32
}

func (w *depthIteratorWrapper) Next() bool {
	return w.it.Next()
}

func (w *depthIteratorWrapper) Pos() uint64 {
	return w.it.Pos()
}

func (w *depthIteratorWrapper) Depth() uint32 {
	return w.depth
}

// NewDescendantIterator returns an iterator over descendants at a specific depth.
func (idx *Index) NewDescendantIterator(prefixPos uint64, relDepth int) (Iterator, error) {
	if prefixPos >= idx.count {
		return &emptyIterator{}, nil
	}
	if relDepth < 0 {
		return &emptyIterator{}, nil
	}

	baseDepth := idx.Depth(prefixPos)
	targetDepth := baseDepth + uint32(relDepth)

	subtreeStart := prefixPos
	subtreeEnd := idx.SubtreeEnd(prefixPos)

	it, err := idx.depthIndex.NewDepthIterator(targetDepth, subtreeStart, subtreeEnd)
	if err != nil {
		return nil, fmt.Errorf("create depth iterator at depth %d: %w", targetDepth, err)
	}

	return &depthIteratorWrapper{it: it, depth: targetDepth}, nil
}

type emptyIterator struct{}

func (e *emptyIterator) Next() bool    { return false }
func (e *emptyIterator) Pos() uint64   { return 0 }
func (e *emptyIterator) Depth() uint32 { return 0 }

// TierBreakdown is an alias for format.TierBreakdown.
type TierBreakdown = format.TierBreakdown

// HasTierData returns whether the index has tier statistics.
func (idx *Index) HasTierData() bool {
	return idx.tierStats != nil && idx.tierStats.HasTierData()
}

// TierBreakdown returns the per-tier statistics for the node at pos.
// Returns nil if:
//   - No tier data was collected during index build (check HasTierData first)
//   - The pos is out of bounds
//
// Use HasTierData to distinguish "no tier data in index" from "empty breakdown".
func (idx *Index) TierBreakdown(pos uint64) []TierBreakdown {
	if idx.tierStats == nil {
		return nil
	}
	return idx.tierStats.GetBreakdown(pos)
}

// TierBreakdownAll returns the per-tier statistics for all present tiers (including zeros).
// Returns nil if no tier data was collected during index build.
// Use HasTierData to check if tier data is available.
func (idx *Index) TierBreakdownAll(pos uint64) []TierBreakdown {
	if idx.tierStats == nil {
		return nil
	}
	return idx.tierStats.GetBreakdownAll(pos)
}

// TierBreakdownForPrefix returns the per-tier statistics for a prefix string.
// Returns nil if:
//   - The prefix is not found in the index
//   - No tier data was collected during index build
//
// Use Lookup to check if a prefix exists, and HasTierData to check for tier data.
func (idx *Index) TierBreakdownForPrefix(prefix string) []TierBreakdown {
	pos, ok := idx.Lookup(prefix)
	if !ok {
		return nil
	}
	return idx.TierBreakdown(pos)
}

// TierBreakdownMap returns the per-tier statistics as a map keyed by tier name.
// Returns nil if no tier data was collected during index build.
// Use HasTierData to check if tier data is available.
func (idx *Index) TierBreakdownMap(pos uint64) map[string]TierBreakdown {
	breakdown := idx.TierBreakdown(pos)
	if breakdown == nil {
		return nil
	}
	result := make(map[string]TierBreakdown, len(breakdown))
	for _, tb := range breakdown {
		result[tb.TierName] = tb
	}
	return result
}
