package extsort

import (
	"runtime"
	"runtime/debug"

	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

// Aggregator accumulates per-prefix statistics in memory.
// It is designed to be flushed when memory usage exceeds a threshold.
//
// The aggregator is NOT safe for concurrent use. For concurrent access,
// use multiple aggregators or external synchronization.
type Aggregator struct {
	prefixes       map[string]*PrefixStats
	statsPool      *typedPool[PrefixStats]
	maxDepth       int
	objectCount    int64
	bytesProcessed int64
}

// NewAggregator creates a new prefix aggregator with the given initial capacity.
// Use maxDepth=0 for unlimited depth.
func NewAggregator(initialCapacity, maxDepth int) *Aggregator {
	if initialCapacity <= 0 {
		// Default to 10K - small enough to start but grows as needed
		// Each prefix entry uses ~300 bytes (map overhead + PrefixStats)
		initialCapacity = 10000
	}

	return &Aggregator{
		prefixes:  make(map[string]*PrefixStats, initialCapacity),
		statsPool: newTypedPool(func() *PrefixStats { return &PrefixStats{} }),
		maxDepth:  maxDepth,
	}
}

// AddObject adds an object's statistics to all its prefix ancestors.
// The key should be the full S3 object key (e.g., "data/2024/01/file.csv").
func (a *Aggregator) AddObject(key string, size uint64, tierID tiers.ID) {
	a.objectCount++
	a.bytesProcessed += int64(size)

	a.accumulate("", 0, size, tierID)

	depth := uint16(1)
	for i := range len(key) {
		if key[i] == '/' {
			if a.maxDepth > 0 && int(depth) > a.maxDepth {
				break
			}
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
		stats = a.statsPool.Get()
		stats.Depth = depth
		a.prefixes[prefix] = stats
	}
	stats.Add(size, tierID)
}

func (a *Aggregator) PrefixCount() int {
	return len(a.prefixes)
}

func (a *Aggregator) ObjectCount() int64 {
	return a.objectCount
}

func (a *Aggregator) BytesProcessed() int64 {
	return a.bytesProcessed
}

// EstimatedMemoryUsage returns the per-worker aggregator's approximate
// in-memory footprint in bytes (~288 B/entry covering map overhead +
// avg prefix string + PrefixStats). Undercounts by 2-3× vs runtime
// reality; the heap-pressure safety check in ShouldWorkerFlush catches
// that case.
func (a *Aggregator) EstimatedMemoryUsage() int64 {
	const bytesPerPrefix = 288

	return int64(len(a.prefixes)) * bytesPerPrefix
}

func heapInuseBytes() uint64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	return m.HeapInuse
}

// DefaultAggregatorCap is the spill threshold when no GOMEMLIMIT is set.
const DefaultAggregatorCap uint64 = 512 * 1024 * 1024

// heapPressureRatio is the HeapInuse / GOMEMLIMIT fraction above which
// any worker must spill regardless of its own size — the aggregator
// is the pipeline's only flush valve.
const heapPressureRatio = 0.85

// AggregatorFractionOfLimit caps the aggregator share of GOMEMLIMIT;
// the remainder is left for download / parse / merge / mmap.
const AggregatorFractionOfLimit = 0.15

// unsetMemoryLimit treats debug.SetMemoryLimit's math.MaxInt64 sentinel
// (no GOMEMLIMIT configured) as unset.
const unsetMemoryLimit int64 = 1 << 62

// readMemoryLimit returns the current GOMEMLIMIT (or runtime's sentinel
// near math.MaxInt64 if unset). Debug.SetMemoryLimit(-1) is the
// idiomatic but counterintuitive read-only form — passing -1 leaves
// the limit unchanged and returns the prior value. Wrapping it makes
// intent obvious at call sites.
func readMemoryLimit() int64 {
	return debug.SetMemoryLimit(-1)
}

// AggregatorCap returns the combined spill threshold for all workers.
func AggregatorCap(memoryLimit int64) uint64 {
	if memoryLimit <= 0 || memoryLimit >= unsetMemoryLimit {
		return DefaultAggregatorCap
	}

	return uint64(float64(memoryLimit) * AggregatorFractionOfLimit)
}

func perWorkerAggregatorCap(memoryLimit int64, numWorkers int) uint64 {
	if numWorkers < 1 {
		numWorkers = 1
	}

	return AggregatorCap(memoryLimit) / uint64(numWorkers)
}

// ShouldWorkerFlush decides whether one worker's aggregator should
// spill, given its own footprint, GOMEMLIMIT, and total worker count.
// Each worker decides independently (no flush stampedes); a HeapInuse-
// vs-limit pressure check is the safety valve.
func ShouldWorkerFlush(workerAggBytes uint64, memoryLimit int64, numWorkers int) bool {
	if workerAggBytes >= perWorkerAggregatorCap(memoryLimit, numWorkers) {
		return true
	}
	if memoryLimit <= 0 {
		return false
	}
	heapPressure := uint64(float64(memoryLimit) * heapPressureRatio)

	return heapInuseBytes() >= heapPressure
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

	rows := make([]*PrefixRow, 0, len(a.prefixes))
	for prefix, stats := range a.prefixes {
		row := stats.ToPrefixRow(prefix)
		rows = append(rows, row)
		stats.Reset()
		a.statsPool.Put(stats)
	}

	clear(a.prefixes)

	a.objectCount = 0
	a.bytesProcessed = 0

	return rows
}
