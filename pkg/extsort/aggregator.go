package extsort

import (
	"runtime"
	"sync"

	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

// Aggregator accumulates per-prefix statistics in memory.
// It is designed to be flushed when memory usage exceeds a threshold.
//
// The aggregator is NOT safe for concurrent use. For concurrent access,
// use multiple aggregators or external synchronization.
type Aggregator struct {
	prefixes       map[string]*PrefixStats
	statsPool      sync.Pool
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
		prefixes: make(map[string]*PrefixStats, initialCapacity),
		statsPool: sync.Pool{
			New: func() any {
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

// EstimatedMemoryUsage returns the per-worker aggregator's
// approximate in-memory footprint in bytes — the signal
// ShouldWorkerFlush compares against PerWorkerAggregatorCap.
//
// Estimate per prefix entry (~288 B total):
//   - Map entry: ~48 B (key pointer, value pointer, hash bucket)
//   - Average prefix string: ~30 B
//   - PrefixStats: ~210 B (depth + counts + per-tier arrays)
//
// The estimate tends to undercount by 2–3× vs actual Go-runtime
// usage (map overhead, string fragmentation). That's tolerable
// because the global HeapPressureRatio check in ShouldWorkerFlush
// catches the case where the process as a whole is approaching
// the limit, regardless of any one worker's estimate.
func (a *Aggregator) EstimatedMemoryUsage() int64 {
	const bytesPerPrefix = 288

	return int64(len(a.prefixes)) * bytesPerPrefix
}

// HeapAllocBytes returns the current heap allocation from runtime.
// This provides ground-truth memory usage for making flush decisions.
func HeapAllocBytes() uint64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	return m.HeapAlloc
}

// HeapInuseBytes returns the bytes Go's runtime currently has carved
// out of the OS for heap allocations (in-use spans + spans not yet
// returned). This is a more conservative pressure signal than HeapAlloc
// because it includes recently-freed memory the runtime hasn't given
// back yet — which still counts against GOMEMLIMIT.
func HeapInuseBytes() uint64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	return m.HeapInuse
}

// AbsoluteAggregatorCap is the hard ceiling on the AGGREGATE in-memory
// footprint of all worker aggregators combined before they're forced
// to spill. Each worker's share is this divided by numWorkers (see
// PerWorkerAggregatorCap). The cap bounds the run-file count a build
// generates and keeps per-flush GC pauses tractable.
const AbsoluteAggregatorCap uint64 = 512 * 1024 * 1024

// HeapPressureRatio is the HeapInuse / GOMEMLIMIT fraction above which
// any worker must spill regardless of its own size. Crossing this
// means the rest of the pipeline is approaching the soft memory limit
// and the aggregator is the only structure with a flush valve.
const HeapPressureRatio = 0.85

// AggregatorFractionOfLimit caps the combined aggregator footprint
// at this share of the process memory limit so a multi-GiB
// GOMEMLIMIT doesn't translate into aggregators big enough to dwarf
// every other working set.
const AggregatorFractionOfLimit = 0.15

// AggregatorCap returns the TOTAL spill threshold across all worker
// aggregators combined, given a process memory limit (typically
// sysmem.ApplyMemoryLimit's value). The per-worker share is this
// divided by numWorkers; see PerWorkerAggregatorCap.
//
// The result is the smaller of the absolute cap and
// AggregatorFractionOfLimit × memoryLimit. A zero or negative
// memoryLimit falls back to the absolute cap.
func AggregatorCap(memoryLimit int64) uint64 {
	if memoryLimit <= 0 {
		return AbsoluteAggregatorCap
	}
	fractional := uint64(float64(memoryLimit) * AggregatorFractionOfLimit)
	if fractional < AbsoluteAggregatorCap {
		return fractional
	}

	return AbsoluteAggregatorCap
}

// PerWorkerAggregatorCap returns the spill threshold for ONE
// chunk worker's private aggregator. Equal to the total
// AggregatorCap divided by numWorkers, with a minimum of 1
// worker. Use this when measuring a single worker's footprint
// against its budget.
func PerWorkerAggregatorCap(memoryLimit int64, numWorkers int) uint64 {
	if numWorkers < 1 {
		numWorkers = 1
	}

	return AggregatorCap(memoryLimit) / uint64(numWorkers)
}

// ShouldFlush returns true if the aggregator should spill to disk
// based on either:
//   - its own estimated-bytes exceeding AggregatorCap(memoryLimit), or
//   - overall HeapInuse exceeding HeapPressureRatio × memoryLimit.
//
// aggregatorBytes is the aggregator's best estimate of its own
// in-memory footprint. When memoryLimit is zero/negative only the
// absolute aggregator cap is checked.
//
// Deprecated: this signature treats `aggregatorBytes` as the GLOBAL
// memory snapshot. Multi-worker callers should use ShouldWorkerFlush
// which takes a per-worker budget so one hot worker doesn't trigger
// flushes in all others.
func ShouldFlush(aggregatorBytes uint64, memoryLimit int64) bool {
	if aggregatorBytes >= AggregatorCap(memoryLimit) {
		return true
	}
	if memoryLimit <= 0 {
		return false
	}
	heapPressure := uint64(float64(memoryLimit) * HeapPressureRatio)

	return HeapInuseBytes() >= heapPressure
}

// ShouldWorkerFlush returns true if a single chunk worker's private
// aggregator should spill, given:
//   - workerAggBytes: this worker's own estimated aggregator memory
//   - memoryLimit:    GOMEMLIMIT (or sysmem.ApplyMemoryLimit output)
//   - numWorkers:     total chunkWorker count; the per-worker share
//     of the aggregator budget is AggregatorCap/numWorkers
//
// Each worker decides independently — matches the per-worker
// aggregator invariant (nothing shared, no flush stampedes). A
// process-wide heap pressure check remains as a safety valve: if
// HeapInuse approaches the limit any worker spilling helps.
func ShouldWorkerFlush(workerAggBytes uint64, memoryLimit int64, numWorkers int) bool {
	if workerAggBytes >= PerWorkerAggregatorCap(memoryLimit, numWorkers) {
		return true
	}
	if memoryLimit <= 0 {
		return false
	}
	heapPressure := uint64(float64(memoryLimit) * HeapPressureRatio)

	return HeapInuseBytes() >= heapPressure
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

	for k := range a.prefixes {
		delete(a.prefixes, k)
	}

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
