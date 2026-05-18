// Package indexread provides read-only access to S3 inventory indexes.
package indexread

import (
	"errors"
	"fmt"

	"github.com/eunmann/s3-inv-db/pkg/format"
)

// Index provides low-latency access to an S3 inventory index via
// mmap. Safe for concurrent read access; Close once after all reads.
type Index struct {
	coreStats  *format.CoreStatsReader
	depthIndex *format.DepthIndex
	mphf       *format.MPHF
	tierStats  *format.TierStatsReader
	count      uint64
	maxDepth   uint32
}

// Open opens an index from the given directory.
func Open(dir string) (*Index, error) {
	coreStats, err := format.OpenCoreStats(dir)
	if err != nil {
		return nil, fmt.Errorf("open core stats: %w", err)
	}
	depthIndex, err := format.OpenDepthIndex(dir)
	if err != nil {
		coreStats.Close()

		return nil, fmt.Errorf("open depth index: %w", err)
	}

	mphf, err := format.OpenMPHF(dir)
	if err != nil {
		_ = errors.Join(coreStats.Close(), depthIndex.Close())

		return nil, fmt.Errorf("open MPHF: %w", err)
	}

	tierStats, err := format.OpenTierStats(dir)
	if err != nil {
		_ = errors.Join(coreStats.Close(), depthIndex.Close(), mphf.Close())

		return nil, fmt.Errorf("open tier stats: %w", err)
	}

	return &Index{
		coreStats:  coreStats,
		depthIndex: depthIndex,
		mphf:       mphf,
		tierStats:  tierStats,
		count:      coreStats.Count(),
		maxDepth:   depthIndex.MaxDepth(),
	}, nil
}

// ErrMissingCoreStats is returned by Open when the index directory
// has no core_stats.bin (legacy per-column layout is no longer
// supported; rebuild required).
var ErrMissingCoreStats = errors.New("index missing core_stats.bin (rebuild required)")

// Close releases all resources, joining any errors.
func (idx *Index) Close() error {
	return errors.Join(
		idx.coreStats.Close(),
		idx.depthIndex.Close(),
		idx.mphf.Close(),
		idx.tierStats.Close(),
	)
}

// Stats holds aggregated statistics for a prefix.
type Stats struct {
	ObjectCount uint64
	TotalBytes  uint64
}

// Lookup returns the preorder position for a prefix, or ok=false if not found.
func (idx *Index) Lookup(prefix string) (uint64, bool) {
	return idx.mphf.Lookup(prefix)
}

// Stats returns the per-prefix object count and total bytes at pos.
// Returns zero-valued Stats when pos is out of bounds — use
// StatsForPrefix to distinguish "not found" from "found with zero stats".
func (idx *Index) Stats(pos uint64) Stats {
	if pos >= idx.count {
		return Stats{}
	}
	objCount, totalBytes := idx.coreStats.UnsafeStats(pos)

	return Stats{ObjectCount: objCount, TotalBytes: totalBytes}
}

// StatsForPrefix is Lookup + Stats; ok=false on miss.
func (idx *Index) StatsForPrefix(prefix string) (Stats, bool) {
	pos, ok := idx.Lookup(prefix)
	if !ok {
		return Stats{}, false
	}

	return idx.Stats(pos), true
}

func (idx *Index) Depth(pos uint64) uint32 {
	if pos >= idx.count {
		return 0
	}

	return idx.coreStats.UnsafeDepth(pos)
}

func (idx *Index) SubtreeEnd(pos uint64) uint64 {
	if pos >= idx.count {
		return 0
	}

	return idx.coreStats.UnsafeSubtreeEnd(pos)
}

func (idx *Index) MaxDepthInSubtree(pos uint64) uint32 {
	if pos >= idx.count {
		return 0
	}

	return idx.coreStats.UnsafeMaxDepth(pos)
}

func (idx *Index) PrefixString(pos uint64) (string, error) {
	s, err := idx.mphf.Prefix(pos)
	if err != nil {
		return "", fmt.Errorf("get prefix for pos %d: %w", pos, err)
	}

	return s, nil
}

func (idx *Index) Count() uint64    { return idx.count }
func (idx *Index) MaxDepth() uint32 { return idx.maxDepth }

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

	positions, err := idx.depthIndex.PositionsInSubtree(targetDepth, subtreeStart, subtreeEnd)
	if err != nil {
		return nil, fmt.Errorf("get positions at depth %d: %w", targetDepth, err)
	}

	return positions, nil
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

// TierBreakdown is an alias for format.TierBreakdown.
type TierBreakdown = format.TierBreakdown

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
	if !idx.HasTierData() {
		return nil
	}

	return idx.tierStats.Breakdown(pos)
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
