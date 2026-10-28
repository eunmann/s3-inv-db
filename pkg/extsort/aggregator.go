package extsort

import (
	"runtime/debug"
	"runtime/metrics"
	"strings"
	"unsafe"

	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

// Aggregator accumulates per-prefix statistics in memory.
// It is designed to be flushed when memory usage exceeds a threshold.
//
// The aggregator is NOT safe for concurrent use. For concurrent access,
// use multiple aggregators or external synchronization.
type Aggregator struct {
	// prefixes finds each prefix's slab row, keyed by its own Prefix.
	prefixes map[string]*PrefixRow
	// slabs back every row: the first used of them, in creation order,
	// are what Drain returns, so each must be live. drained is Drain's
	// result slice; both are reused once the caller is done with a
	// Drain, so a worker allocates rows only while its largest spill is
	// still growing.
	slabs   [][]PrefixRow
	drained []*PrefixRow
	// sortScratch is DrainSorted's sort buffer, reused the same way.
	sortScratch []keyedRow
	// path holds the rows of the previous key's prefixes by depth
	// (path[0] is the root's): the next key reuses the ones whose
	// prefix it starts with and looks up only the levels below. Keys
	// in order (as S3 Inventory lists them) share most of their path.
	path     []*PrefixRow
	used     int
	maxDepth int
	// bytes is the heap the rows since the last Drain take: each row,
	// its prefix string and its map entry (EstimatedMemoryUsage).
	bytes    int64
	tierMask uint16
}

// aggregatorEntryBytes is a prefix's heap cost besides its string: its
// PrefixRow and its map entry (key header, value pointer, control byte
// and load-factor slack). Measured: 328 B/prefix for 29 B prefixes and
// 475 B/prefix for 185 B ones, both within 2% of this plus the length.
const aggregatorEntryBytes = int64(unsafe.Sizeof(PrefixRow{})) + 48

// NewAggregator creates a new prefix aggregator with the given initial capacity.
// Use maxDepth=0 for unlimited depth.
func NewAggregator(initialCapacity, maxDepth int) *Aggregator {
	if initialCapacity <= 0 {
		// Default to 10K - small enough to start but grows as needed
		// Each prefix entry uses ~300 bytes (map entry + slab PrefixRow)
		initialCapacity = 10000
	}

	return &Aggregator{
		prefixes: make(map[string]*PrefixRow, initialCapacity),
		maxDepth: maxDepth,
	}
}

// AddObject adds an object's statistics to all its prefix ancestors.
// The key should be the full S3 object key (e.g., "data/2024/01/file.csv").
func (a *Aggregator) AddObject(key string, size uint64, tierID tiers.ID) {
	a.tierMask |= uint16(1) << tierID

	path := a.path
	if len(path) == 0 {
		path = append(path, a.row("", 0))
	}
	path[0].add(size, tierID)
	// Reuse the previous key's rows while this key starts with their
	// prefixes; each level adds one segment, so only it is compared.
	d := 1
	for ; d < len(path); d++ {
		prev, p := len(path[d-1].Prefix), path[d].Prefix
		if len(key) < len(p) || key[prev:len(p)] != p[prev:] {
			break
		}
		path[d].add(size, tierID)
	}
	path = path[:d]

	depth := uint16(d)
	for i := len(path[d-1].Prefix); i < len(key); i++ {
		if key[i] != '/' {
			continue
		}
		if a.maxDepth > 0 && int(depth) > a.maxDepth {
			break
		}
		row := a.row(key[:i+1], depth)
		row.add(size, tierID)
		path = append(path, row)
		depth++
	}
	a.path = path
}

// row returns prefix's row, creating it on first use. A new prefix is
// cloned: the caller's key is usually a substring of a whole inventory
// record, which the map would otherwise keep alive.
func (a *Aggregator) row(prefix string, depth uint16) *PrefixRow {
	row, ok := a.prefixes[prefix]
	if !ok {
		owned := strings.Clone(prefix)
		row = a.newRow()
		row.Prefix, row.Depth = owned, depth
		a.prefixes[owned] = row
		a.bytes += aggregatorEntryBytes + int64(len(owned))
	}

	return row
}

// rowSlabLen is how many rows the aggregator allocates at a time.
const rowSlabLen = 4096

// newRow returns the next zeroed row from the slabs.
func (a *Aggregator) newRow() *PrefixRow {
	slab, i := a.used/rowSlabLen, a.used%rowSlabLen
	if slab == len(a.slabs) {
		a.slabs = append(a.slabs, make([]PrefixRow, rowSlabLen))
	}
	a.used++
	row := &a.slabs[slab][i]
	*row = PrefixRow{}

	return row
}

func (a *Aggregator) PrefixCount() int {
	return len(a.prefixes)
}

// TierMask returns the bitmask of tier IDs this aggregator has seen
// data for. Bit i is set when an object in tier i was added. It is
// accumulated across flushes (Drain does not reset it) so a worker's
// mask reflects its whole lifetime.
func (a *Aggregator) TierMask() uint16 { return a.tierMask }

// PresentTiers returns the tier IDs this aggregator has seen, ascending.
func (a *Aggregator) PresentTiers() []tiers.ID { return tierIDsFromMask(a.tierMask) }

// tierIDsFromMask expands a tier bitmask to an ascending slice of IDs.
func tierIDsFromMask(mask uint16) []tiers.ID {
	ids := make([]tiers.ID, 0, tiers.NumTiers)
	for id := range tiers.NumTiers {
		if mask&(uint16(1)<<id) != 0 {
			ids = append(ids, id)
		}
	}

	return ids
}

// EstimatedMemoryUsage returns the heap the aggregator's rows take
// since the last Drain: each row, its prefix string and its map entry,
// counted as they are created.
func (a *Aggregator) EstimatedMemoryUsage() int64 {
	return a.bytes
}

// heapReadings returns the live heap as of the last GC and HeapInuse
// (live and dead objects plus fragmentation in in-use spans), through
// runtime/metrics, which unlike runtime.ReadMemStats does not stop the
// world. Every ingest worker checks them every few tens of thousands of
// rows.
//
//nolint:gocritic // gocritic wants named returns, nonamedreturns forbids them
func heapReadings() (uint64, uint64) {
	samples := [3]metrics.Sample{
		{Name: "/gc/heap/live:bytes"},
		{Name: "/memory/classes/heap/objects:bytes"},
		{Name: "/memory/classes/heap/unused:bytes"},
	}
	metrics.Read(samples[:])

	return samples[0].Value.Uint64(), samples[1].Value.Uint64() + samples[2].Value.Uint64()
}

// DefaultAggregatorCap is the spill threshold when no GOMEMLIMIT is set.
const DefaultAggregatorCap uint64 = 512 * 1024 * 1024

// Heap pressure makes any worker spill regardless of its own size: the
// aggregator is the pipeline's only flush valve. It is the live heap
// (as of the last GC) reaching liveHeapPressureRatio of GOMEMLIMIT, or,
// as a backstop between GCs, HeapInuse reaching heapInuseBackstopRatio.
// HeapInuse alone counts garbage not yet collected: with GOMEMLIMIT the
// heap grows to the limit before each GC, so it read as pressure once
// the live heap passed ~40% of the limit, and every check spilled.
const (
	liveHeapPressureRatio  = 0.70
	heapInuseBackstopRatio = 0.95
)

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
// Each worker decides independently (no flush stampedes); the safety
// valve is heapUnderPressure: the live heap as of the last GC, with a
// HeapInuse backstop between GCs.
func ShouldWorkerFlush(workerAggBytes uint64, memoryLimit int64, numWorkers int) bool {
	if workerAggBytes >= perWorkerAggregatorCap(memoryLimit, numWorkers) {
		return true
	}

	return heapUnderPressure(memoryLimit)
}

// heapUnderPressure reports whether the heap is under pressure (see
// liveHeapPressureRatio); a limit <= 0 means none is set.
func heapUnderPressure(memoryLimit int64) bool {
	if memoryLimit <= 0 {
		return false
	}
	live, inuse := heapReadings()

	return heapReadingsUnderPressure(live, inuse, memoryLimit)
}

// heapReadingsUnderPressure applies the pressure rule to a live-heap
// and HeapInuse reading against a positive memory limit.
func heapReadingsUnderPressure(live, inuse uint64, memoryLimit int64) bool {
	limit := float64(memoryLimit)

	return live >= uint64(limit*liveHeapPressureRatio) || inuse >= uint64(limit*heapInuseBackstopRatio)
}

// Drain hands over every accumulated row and clears the aggregator for
// reuse. Nothing is copied: the rows and the slice stay valid only until
// the aggregator is next used, when they are recycled.
//
// The rows come in the order their prefixes first appeared, read off the
// slabs rather than the map: keys in order (as S3 Inventory lists them)
// create their prefixes in sorted order, which DrainSorted's sort then
// only has to confirm.
func (a *Aggregator) Drain() []*PrefixRow {
	if a.used == 0 {
		return nil
	}

	rows := a.drained[:0]
	if cap(rows) < a.used {
		rows = make([]*PrefixRow, 0, a.used)
	}
	for s := 0; len(rows) < a.used; s++ {
		slab := a.slabs[s][:min(rowSlabLen, a.used-len(rows))]
		for i := range slab {
			rows = append(rows, &slab[i])
		}
	}
	a.drained = rows

	clear(a.prefixes)
	a.used = 0
	a.bytes = 0
	// The cached path points at rows the next use recycles.
	clear(a.path)
	a.path = a.path[:0]

	return rows
}

// DrainSorted is Drain with the rows sorted by prefix, through a sort
// buffer the aggregator keeps for the next spill.
func (a *Aggregator) DrainSorted() []*PrefixRow {
	rows := a.Drain()
	a.sortScratch = sortPrefixRows(rows, a.sortScratch)

	return rows
}

// Release drops the drained aggregator's slabs, Drain slice, sort
// buffer and map buckets so the GC can reclaim them once the caller is
// done with the last Drain. Call it only on an empty (just drained)
// aggregator.
func (a *Aggregator) Release() {
	a.slabs, a.drained, a.sortScratch, a.path = nil, nil, nil, nil
	a.prefixes = make(map[string]*PrefixRow)
}
