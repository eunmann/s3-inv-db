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
	prefixes   *format.PrefixTreeReader
	tierStats  *format.TierStatsReader
	count      uint64
	maxDepth   uint32
}

// Options tune how an index is opened.
type Options struct {
	// Prefault reads every index file into the page cache before Open
	// returns (format.Prefault): slower to open, but the first queries
	// don't wait on storage. For a server loading an index it will
	// serve; a one-off query opens without it.
	Prefault bool
}

// Open opens an index from the given directory.
func Open(dir string) (*Index, error) {
	return OpenWith(dir, Options{})
}

// OpenWith opens an index from the given directory with options.
func OpenWith(dir string, opts Options) (*Index, error) {
	if opts.Prefault {
		if err := format.Prefault(dir); err != nil {
			return nil, fmt.Errorf("prefault index: %w", err)
		}
	}

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

	prefixes, err := format.OpenPrefixTreeReader(dir)
	if err != nil {
		_ = errors.Join(coreStats.Close(), depthIndex.Close(), mphf.Close())

		return nil, fmt.Errorf("open prefix tree: %w", err)
	}

	tierStats, err := format.OpenTierStats(dir, coreStats)
	if err != nil {
		_ = errors.Join(coreStats.Close(), depthIndex.Close(), mphf.Close(), prefixes.Close())

		return nil, fmt.Errorf("open tier stats: %w", err)
	}

	return &Index{
		coreStats:  coreStats,
		depthIndex: depthIndex,
		mphf:       mphf,
		prefixes:   prefixes,
		tierStats:  tierStats,
		count:      coreStats.Count(),
		maxDepth:   depthIndex.MaxDepth(),
	}, nil
}

// Close releases all resources, joining any errors.
func (idx *Index) Close() error {
	return errors.Join(
		idx.coreStats.Close(),
		idx.depthIndex.Close(),
		idx.mphf.Close(),
		idx.prefixes.Close(),
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
// Fast path: pos is expected to be a valid position obtained from
// Lookup, DescendantsAtDepth, or another *Index method. Out-of-bounds
// pos returns zero-valued Stats as a non-fatal safety net; if the
// caller has not validated pos, use StatsForPrefix to distinguish
// "prefix missing" from "prefix present with zero stats".
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

// HasChildren reports whether the prefix at pos has a child prefix.
func (idx *Index) HasChildren(pos uint64) bool {
	return pos < idx.count && idx.coreStats.UnsafeSubtreeEnd(pos) > pos
}

func (idx *Index) PrefixString(pos uint64) (string, error) {
	s, err := idx.prefixes.Prefix(pos)
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
	span, err := idx.descendantSpan(prefixPos, relDepth)
	if err != nil || span.Len() == 0 {
		return nil, err
	}

	positions := make([]uint64, span.Len())
	for i := range positions {
		positions[i] = idx.depthIndex.PositionAt(span.Lo + uint64(i))
	}

	return positions, nil
}

// descendantSpan returns the depth-index span holding the descendants
// of prefixPos at relDepth; it is empty when there are none or the
// input is invalid (see DescendantsAtDepth).
func (idx *Index) descendantSpan(prefixPos uint64, relDepth int) (format.Span, error) {
	if prefixPos >= idx.count || relDepth < 0 {
		return format.Span{}, nil
	}
	targetDepth := idx.Depth(prefixPos) + uint32(relDepth)
	if targetDepth > idx.maxDepth {
		return format.Span{}, nil
	}

	span, err := idx.depthIndex.SubtreeSpan(targetDepth, prefixPos, idx.SubtreeEnd(prefixPos))
	if err != nil {
		return format.Span{}, fmt.Errorf("get positions at depth %d: %w", targetDepth, err)
	}

	return span, nil
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

// Page is one window of descendant positions and the total number of
// matches across all windows.
type Page struct {
	Positions []uint64
	Total     int
}

// DescendantsPage returns the matches of DescendantsAtDepthFiltered
// at [offset, offset+limit) together with the total match count,
// holding only the page in memory. Without a filter the total is the
// size of the depth-index span and only the page is read; with one,
// every descendant's stats are read to count the matches.
func (idx *Index) DescendantsPage(prefixPos uint64, relDepth int, filter Filter, offset, limit int) (Page, error) {
	span, err := idx.descendantSpan(prefixPos, relDepth)
	if err != nil {
		return Page{}, fmt.Errorf("get descendants at depth %d: %w", relDepth, err)
	}
	offset, limit = max(offset, 0), max(limit, 0)
	if filter.MinCount == 0 && filter.MinBytes == 0 {
		total := int(span.Len())
		start := min(offset, total)
		page := Page{Positions: make([]uint64, min(limit, total-start)), Total: total}
		for i := range page.Positions {
			page.Positions[i] = idx.depthIndex.PositionAt(span.Lo + uint64(start+i))
		}

		return page, nil
	}

	var page Page
	for i := span.Lo; i < span.Hi; i++ {
		pos := idx.depthIndex.PositionAt(i)
		stats := idx.Stats(pos)
		if stats.ObjectCount < filter.MinCount || stats.TotalBytes < filter.MinBytes {
			continue
		}
		if page.Total >= offset && len(page.Positions) < limit {
			page.Positions = append(page.Positions, pos)
		}
		page.Total++
	}

	return page, nil
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
