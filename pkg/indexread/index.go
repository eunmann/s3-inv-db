// Package indexread provides read-only access to S3 inventory indexes.
package indexread

import (
	"errors"
	"fmt"
	"math/bits"
	"slices"

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
	identity   *Identity
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

	idx := &Index{
		identity:   new(Identity),
		coreStats:  coreStats,
		depthIndex: depthIndex,
		mphf:       mphf,
		prefixes:   prefixes,
		tierStats:  tierStats,
		count:      coreStats.Count(),
		maxDepth:   depthIndex.MaxDepth(),
	}
	if err := idx.checkCounts(); err != nil {
		return nil, errors.Join(err, idx.Close())
	}

	return idx, nil
}

// Identity is a token unique to one opened index. A cache keyed by it
// can't mistake one index for another: while the cache holds the token
// it can't be freed, so no later Open can be handed the same address.
// It holds nothing else, so it doesn't keep a closed index's memory
// alive.
type Identity struct{ _ byte }

// Identity returns the index's identity token.
func (idx *Index) Identity() *Identity { return idx.identity }

// ErrIndexInconsistent is returned when the index's files disagree: by
// Open when they hold different prefix counts, and by the descendant
// queries when a row's child range leaves the next depth's rows. A
// mixed or damaged file set would otherwise answer with zeros or wrong
// rows, or panic.
var ErrIndexInconsistent = errors.New("index files are inconsistent")

// checkCounts requires every per-prefix structure to hold the same
// number of prefixes. Each count is a header or an array length, so
// this is O(1).
func (idx *Index) checkCounts() error {
	counts := []struct {
		name string
		n    uint64
	}{
		{"MPHF", idx.mphf.Count()},
		{"prefix tree", idx.prefixes.Count()},
		{"depth offsets end", idx.depthIndex.Count()},
	}
	for _, c := range counts {
		if c.n != idx.count {
			return fmt.Errorf("%w: prefix counts differ, core stats has %d, %s has %d", ErrIndexInconsistent, idx.count, c.name, c.n)
		}
	}

	return nil
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

// Lookup returns the position (row) of a prefix, or ok=false if not
// found. Rows are in level order: the root, then every depth-1 prefix,
// then every depth-2 prefix, each depth in key order.
func (idx *Index) Lookup(prefix string) (uint64, bool) {
	pos, ok := idx.mphf.Lookup(prefix)
	if pos >= idx.count {
		return 0, false // a damaged slot is a miss, not a zero row
	}

	return pos, ok
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

// Depth returns the depth of the prefix at pos: 0 for the root.
func (idx *Index) Depth(pos uint64) uint32 {
	if pos >= idx.count {
		return 0
	}

	return idx.depthIndex.DepthOf(pos)
}

// HasChildren reports whether the prefix at pos has a child prefix.
func (idx *Index) HasChildren(pos uint64) bool {
	return pos < idx.count && idx.coreStats.UnsafeChildStart(pos+1) > idx.coreStats.UnsafeChildStart(pos)
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
	span, _, err := idx.descendantSpanAt(prefixPos, relDepth)
	if err != nil || span.Len() == 0 {
		return nil, err
	}

	positions := make([]uint64, span.Len())
	for i := range positions {
		positions[i] = span.Lo + uint64(i)
	}

	return positions, nil
}

// DescendantSpan returns the rows of prefixPos's descendants at
// relDepth, in key order: Len() of them, read with PositionsInSpan. It
// is what DescendantsAtDepth starts with, for callers that need the
// count before choosing which positions to read.
func (idx *Index) DescendantSpan(prefixPos uint64, relDepth int) (format.Span, error) {
	span, _, err := idx.descendantSpanAt(prefixPos, relDepth)

	return span, err
}

// SpanPosition returns the position of span's i-th entry, for walking a
// span in place; i must be below span.Len().
func (idx *Index) SpanPosition(span format.Span, i uint64) uint64 {
	return span.Lo + i
}

// PositionsInSpan returns the positions of span's entries
// [offset, offset+limit), clamped to the span.
func (idx *Index) PositionsInSpan(span format.Span, offset, limit int) []uint64 {
	total := int(span.Len())
	start := min(max(offset, 0), total)
	positions := make([]uint64, min(max(limit, 0), total-start))
	for i := range positions {
		positions[i] = span.Lo + uint64(start+i)
	}

	return positions
}

// descendantSpanAt returns the rows of prefixPos's descendants at
// relDepth and the depth they lie at. The span is empty when there are
// none or the input is invalid (see DescendantsAtDepth); the error is
// ErrIndexInconsistent when a child range on disk leaves its depth.
func (idx *Index) descendantSpanAt(prefixPos uint64, relDepth int) (format.Span, uint32, error) {
	if prefixPos >= idx.count || relDepth < 0 {
		return format.Span{}, 0, nil
	}
	depth := idx.Depth(prefixPos)
	targetDepth := depth + uint32(relDepth)
	if targetDepth > idx.maxDepth {
		return format.Span{}, 0, nil
	}

	// Level order keeps each level's descendants contiguous: the
	// children of rows [lo, hi) are rows [childStart(lo), childStart(hi)),
	// all within the next depth's rows.
	span := format.Span{Lo: prefixPos, Hi: prefixPos + 1}
	for d := depth + 1; d <= targetDepth; d++ {
		parents := span
		span = format.Span{Lo: idx.coreStats.UnsafeChildStart(parents.Lo), Hi: idx.coreStats.UnsafeChildStart(parents.Hi)}
		if span.Lo < idx.depthIndex.Start(d) || span.Hi < span.Lo || span.Hi > idx.depthIndex.Start(d+1) {
			return format.Span{}, 0, fmt.Errorf("%w: rows %d to %d have children at rows %d to %d, outside depth %d",
				ErrIndexInconsistent, parents.Lo, parents.Hi, span.Lo, span.Hi, d)
		}
		if span.Len() == 0 {
			return format.Span{}, targetDepth, nil
		}
	}

	return span, targetDepth, nil
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
// size of the depth-index span and only the page is read. With one,
// the depth-block bounds settle whole blocks where they can (none of
// a block matches, or all of it does) and the rest are read entry by
// entry.
func (idx *Index) DescendantsPage(prefixPos uint64, relDepth int, filter Filter, offset, limit int) (Page, error) {
	span, depth, err := idx.descendantSpanAt(prefixPos, relDepth)
	if err != nil {
		return Page{}, err
	}
	offset, limit = max(offset, 0), max(limit, 0)
	if filter.MinCount == 0 && filter.MinBytes == 0 {
		return Page{Positions: idx.PositionsInSpan(span, offset, limit), Total: int(span.Len())}, nil
	}

	var page Page
	blocks := idx.depthIndex.SpanBlocks(depth, span)
	if blocks.Len() < rankBlockMin {
		idx.filterInto(&page, span, filter, offset, limit)

		return page, nil
	}
	for i := range blocks.Len() {
		b, e := blocks.Bounds(i), blocks.Entries(i)
		switch {
		case b.MaxCount < filter.MinCount || b.MaxBytes < filter.MinBytes:
		case b.MinCount >= filter.MinCount && b.MinBytes >= filter.MinBytes:
			// Every entry matches: read only those the window takes.
			from := max(offset, page.Total)
			to := min(offset+limit, page.Total+int(e.Len()))
			for k := from; k < to; k++ {
				page.Positions = append(page.Positions, e.Lo+uint64(k-page.Total))
			}
			page.Total += int(e.Len())
		default:
			idx.filterInto(&page, e, filter, offset, limit)
		}
	}

	return page, nil
}

// filterInto counts span's entries that pass filter into page, keeping
// those at match index [offset, offset+limit).
//
// It runs only with a filter, mostly over the blocks the bounds couldn't
// settle, where matches and misses mix, so whether an entry matches is a
// coin flip to the branch predictor: the match is computed without a
// branch, and the one branch left is the page window's, which is
// predictable.
func (idx *Index) filterInto(page *Page, span format.Span, filter Filter, offset, limit int) {
	for pos := span.Lo; pos < span.Hi; pos++ {
		stats := idx.Stats(pos)
		match := atLeast(stats.ObjectCount, filter.MinCount) & atLeast(stats.TotalBytes, filter.MinBytes)
		if page.Total >= offset && len(page.Positions) < limit && match == 1 {
			page.Positions = append(page.Positions, pos)
		}
		page.Total += int(match)
	}
}

// atLeast returns 1 when v >= floor and 0 otherwise, without a branch.
func atLeast(v, floor uint64) uint64 {
	_, borrow := bits.Sub64(v, floor, 0)

	return borrow ^ 1
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

// AppendTierBreakdown appends TierBreakdown(pos) to dst and returns
// the extended slice; dst is returned unchanged when the index has no
// tier data. Reusing dst across positions avoids an allocation per
// position.
func (idx *Index) AppendTierBreakdown(dst []TierBreakdown, pos uint64) []TierBreakdown {
	if !idx.HasTierData() {
		return dst
	}

	return idx.tierStats.AppendBreakdown(dst, pos)
}

// TiersEqual reports whether two tier breakdowns hold the same tiers with
// the same counts and bytes. Breakdowns come in tier-ID order with zero
// tiers omitted, so a positional comparison is exact.
func TiersEqual(x, y []TierBreakdown) bool {
	return slices.EqualFunc(x, y, func(p, q TierBreakdown) bool {
		return p.TierID == q.TierID && p.ObjectCount == q.ObjectCount && p.Bytes == q.Bytes
	})
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
