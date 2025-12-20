package extsort

import (
	"sync"

	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

// Aggregator accumulates per-prefix statistics in memory.
// It is designed to be flushed when memory usage exceeds a threshold.
//
// The aggregator is NOT safe for concurrent use. For concurrent access,
// use multiple aggregators or external synchronization.
type Aggregator struct {
	// prefixes maps prefix strings to their aggregated statistics.
	prefixes map[string]*PrefixStats

	// statsPool is a pool of PrefixStats structs to reduce allocations.
	statsPool sync.Pool

	// maxDepth limits the maximum prefix depth to track (0 = unlimited).
	maxDepth int

	// objectCount tracks the total number of objects processed.
	objectCount int64

	// bytesProcessed tracks the total bytes processed.
	bytesProcessed int64
}

// NewAggregator creates a new prefix aggregator with the given initial capacity.
// Use maxDepth=0 for unlimited depth.
func NewAggregator(initialCapacity, maxDepth int) *Aggregator {
	if initialCapacity <= 0 {
		initialCapacity = 100000
	}
	return &Aggregator{
		prefixes: make(map[string]*PrefixStats, initialCapacity),
		statsPool: sync.Pool{
			New: func() interface{} {
				return &PrefixStats{}
			},
		},
		maxDepth: maxDepth,
	}
}

// AddObject adds an object's statistics to all its prefix ancestors.
// The key should be the full S3 object key (e.g., "data/2024/01/file.csv").
func (a *Aggregator) AddObject(key string, size uint64, tierID tiers.ID) {
	a.objectCount++
	a.bytesProcessed += int64(size)

	// Add to root prefix (empty string represents the root)
	a.accumulate("", 0, size, tierID)

	// Extract and accumulate all directory prefixes
	depth := uint16(1)
	for i := range len(key) {
		if key[i] == '/' {
			// Check depth limit
			if a.maxDepth > 0 && int(depth) > a.maxDepth {
				break
			}
			// Include the trailing slash in the prefix
			prefix := key[:i+1]
			a.accumulate(prefix, depth, size, tierID)
			depth++
		}
	}
}

// accumulate updates the statistics for a single prefix.
func (a *Aggregator) accumulate(prefix string, depth uint16, size uint64, tierID tiers.ID) {
	stats, ok := a.prefixes[prefix]
	if !ok {
		// Get from pool or allocate new
		poolObj := a.statsPool.Get()
		stats, ok = poolObj.(*PrefixStats)
		if !ok {
			panic("statsPool contained unexpected type")
		}
		stats.Depth = depth
		a.prefixes[prefix] = stats
	}
	stats.Add(size, tierID)
}

// PrefixCount returns the number of unique prefixes currently tracked.
func (a *Aggregator) PrefixCount() int {
	return len(a.prefixes)
}

// ObjectCount returns the total number of objects processed.
func (a *Aggregator) ObjectCount() int64 {
	return a.objectCount
}

// BytesProcessed returns the total bytes processed.
func (a *Aggregator) BytesProcessed() int64 {
	return a.bytesProcessed
}

// EstimatedMemoryUsage returns an approximate memory usage in bytes.
// This is a rough estimate based on prefix count and average prefix length.
func (a *Aggregator) EstimatedMemoryUsage() int64 {
	// Estimate per-prefix overhead:
	// - Map entry: ~48 bytes (key pointer, value pointer, hash bucket)
	// - Average prefix string: ~30 bytes
	// - PrefixStats: ~(2 + 8 + 8 + 12*8 + 12*8) = ~210 bytes
	// Total: ~288 bytes per prefix
	const bytesPerPrefix = 288
	return int64(len(a.prefixes)) * bytesPerPrefix
}

// Drain extracts all prefixes from the aggregator and returns them as PrefixRows.
// The aggregator is cleared and can be reused. The returned slice is sorted
// by prefix in lexicographic order.
//
// This method returns PrefixStats structs to the pool, so the returned
// PrefixRows must be used before the aggregator processes more objects.
func (a *Aggregator) Drain() []*PrefixRow {
	if len(a.prefixes) == 0 {
		return nil
	}

	// Collect all prefixes into a slice
	rows := make([]*PrefixRow, 0, len(a.prefixes))
	for prefix, stats := range a.prefixes {
		row := stats.ToPrefixRow(prefix)
		rows = append(rows, row)

		// Return stats to pool
		stats.Reset()
		a.statsPool.Put(stats)
	}

	// Clear the map (reuse the underlying memory)
	for k := range a.prefixes {
		delete(a.prefixes, k)
	}

	// Reset counters
	a.objectCount = 0
	a.bytesProcessed = 0

	return rows
}

// Clear resets the aggregator, returning all PrefixStats to the pool.
func (a *Aggregator) Clear() {
	for _, stats := range a.prefixes {
		stats.Reset()
		a.statsPool.Put(stats)
	}
	for k := range a.prefixes {
		delete(a.prefixes, k)
	}
	a.objectCount = 0
	a.bytesProcessed = 0
}
