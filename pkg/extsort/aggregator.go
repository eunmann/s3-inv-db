package extsort

import (
	"encoding/binary"
	"math"
	"runtime/debug"
	"runtime/metrics"
	"slices"
	"strings"
	"unsafe"

	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

// Aggregator accumulates per-prefix statistics in memory.
// It is designed to be flushed when memory usage exceeds a threshold.
//
// Its rows are the nodes of a prefix tree: each prefix's node is keyed by
// its parent's node and its segment, the bytes after the parent's prefix
// up to and including the next '/'. A lookup hashes only the segment,
// and Drain hands the rows out sorted by walking the tree.
//
// The aggregator is NOT safe for concurrent use. For concurrent access,
// use multiple aggregators or external synchronization.
type Aggregator struct {
	// children finds a node of the main tree by its key (see nodeKey);
	// keyBuf builds the key to look up.
	children map[string]uint32
	keyBuf   []byte
	// slabs back every node, in creation order (a node's index is its
	// position), and parents holds each node's parent at the same
	// position: noNode for a root, deadNode once mapSegment has merged it
	// into another node. Apart from the nodes, Drain's counting passes
	// read only the parents. segs holds the nodes' keys. All are reused
	// once the caller is done with a Drain, so a worker allocates nodes
	// only while its largest spill is still growing.
	slabs   [][]aggNode
	parents [][]uint32
	segs    segmentArena
	// path holds the nodes of the previous key's prefixes by depth
	// (path[0] is the root): the next key reuses the ones whose prefix
	// it starts with and looks up only the levels below. Keys in order
	// share most of their path; S3 Inventory doesn't promise any order,
	// so this is a saving, never an assumption.
	path []aggPathEntry
	// segments are the nodes since the last Drain by the StartSegment
	// that began them (a chunk each), and lastKey is the previous key of
	// the current one. A segment whose keys come in order needs no map;
	// see StartSegment.
	segments []aggSegment
	lastKey  []byte
	// Drain's output and scratch, and mapSegment's (graft), reused the
	// same way as the slabs.
	drained    []*PrefixRow
	merged     []*PrefixRow
	runs       [][]*PrefixRow
	runNext    []int
	childEnd   []uint32
	kids       []uint32
	stack      []uint32
	siblings   []aggSibling
	graft      []aggGraft
	used, dead int
	// root is the main tree's root node, or noNode.
	root     uint32
	maxDepth int
	// bytes is the heap the nodes since the last Drain take
	// (EstimatedMemoryUsage), and prefixBytes the length of the prefixes
	// Drain writes for the live ones.
	bytes       int64
	prefixBytes int
	tierMask    uint16
}

// aggNode is a prefix's row. Until Drain, the row's Prefix holds only
// the node's segment ("" for a root); Drain writes the whole prefix.
type aggNode struct {
	row PrefixRow
}

const (
	noNode   = math.MaxUint32
	deadNode = math.MaxUint32 - 1
)

// A node's key is its parent's index, little-endian in nodeKeyHeader
// bytes, followed by its segment: one string, so the map's slot is a
// string header and an index, not a struct with a string in it. The
// segments arena stores every non-root node's key, and the node's
// Prefix is the segment within it.
const nodeKeyHeader = 4

// appendNodeKey appends the key of parent's child with segment seg.
func appendNodeKey(dst []byte, parent uint32, seg string) []byte {
	dst = binary.LittleEndian.AppendUint32(dst, parent)

	return append(dst, seg...)
}

// nodeKey rewrites the parent in the key stored before seg, a node's
// segment, and returns that key. Only a node not yet in the map has its
// key rewritten, so no map key changes.
func nodeKey(seg string, parent uint32) string {
	start := unsafe.Add(unsafe.Pointer(unsafe.StringData(seg)), -nodeKeyHeader)
	key := unsafe.Slice((*byte)(start), nodeKeyHeader+len(seg))
	binary.LittleEndian.PutUint32(key, parent)

	return unsafe.String(&key[0], len(key))
}

// aggPathEntry is a node on the cached path and the length of its
// prefix, which ends at that offset of every key it is cached for.
type aggPathEntry struct {
	n   *aggNode
	idx uint32
	end int
}

// aggSegment is a run of nodes from one StartSegment: from start to the
// next segment's start. While its keys come in order its nodes form a
// tree of their own, created in prefix order with no prefix twice; the
// first key out of order maps it, merging those nodes into the main
// tree, and every later node of it is looked up there.
type aggSegment struct {
	start  int
	mapped bool
}

// aggSibling is a node and its segment, for sorting a node's children.
type aggSibling struct {
	seg string
	idx uint32
}

// aggGraft is where mapSegment put one of the segment's nodes and the
// length of its prefix.
type aggGraft struct {
	to        uint32
	prefixLen int
}

// A node's heap cost is aggregatorNodeBytes (the node and its parent's
// index), its key, its prefix
// (which Drain allocates) and, for a node in the main tree's map,
// aggregatorMapEntryBytes: the key and value slot, its control byte and
// the load-factor slack, 35 to 56 B over the map's growth cycle.
// Measured at Drain (nodes, map, keys, prefixes and Drain's output) for
// keys out of order: 352-366 B/prefix against an estimate of 340-343
// for 4-byte segments, 482-496 against 466-477 for 40-byte ones.
const (
	aggregatorNodeBytes     = int64(unsafe.Sizeof(aggNode{}) + unsafe.Sizeof(uint32(0)))
	aggregatorMapEntryBytes = 48
)

// NewAggregator creates a new prefix aggregator with the given initial capacity.
// Use maxDepth=0 for unlimited depth.
func NewAggregator(initialCapacity, maxDepth int) *Aggregator {
	if initialCapacity <= 0 {
		// Default to 10K - small enough to start but grows as needed
		initialCapacity = 10000
	}

	return &Aggregator{
		children: make(map[string]uint32, initialCapacity),
		root:     noNode,
		maxDepth: maxDepth,
	}
}

// AddObject adds an object's statistics to all its prefix ancestors.
// The key should be the full S3 object key (e.g., "data/2024/01/file.csv").
func (a *Aggregator) AddObject(key string, size uint64, tierID tiers.ID) {
	a.tierMask |= uint16(1) << tierID
	ordered := a.inOrder()
	if ordered {
		if key < string(a.lastKey) {
			a.mapSegment()
			ordered = false
		} else {
			a.lastKey = append(a.lastKey[:0], key...)
		}
	}

	path := a.path
	if len(path) == 0 {
		path = append(path, a.rootEntry(ordered))
	}
	path[0].n.row.add(size, tierID)
	// Reuse the previous key's nodes while this key starts with their
	// prefixes; each level adds one segment, so only it is compared.
	d := 1
	for ; d < len(path); d++ {
		start, end := path[d-1].end, path[d].end
		if len(key) < end || key[start:end] != path[d].n.row.Prefix {
			break
		}
		path[d].n.row.add(size, tierID)
	}
	path = path[:d]

	depth := uint16(d)
	start := path[d-1].end
	for i := start; i < len(key); i++ {
		if key[i] != '/' {
			continue
		}
		if a.maxDepth > 0 && int(depth) > a.maxDepth {
			break
		}
		e := a.child(path[len(path)-1].idx, key[start:i+1], i+1, depth, ordered)
		e.n.row.add(size, tierID)
		path = append(path, e)
		start = i + 1
		depth++
	}
	a.path = path
}

// rootEntry returns the root's path entry: the main tree's, created on
// first use, or in an ordered segment a root of the segment's own.
func (a *Aggregator) rootEntry(ordered bool) aggPathEntry {
	if !ordered && a.root != noNode {
		return aggPathEntry{n: a.node(a.root), idx: a.root}
	}
	idx, n := a.newNode(noNode)
	a.bytes += aggregatorNodeBytes
	if !ordered {
		a.root = idx
	}

	return aggPathEntry{n: n, idx: idx}
}

// child returns the entry of parent's child with segment seg, whose
// prefix is prefixLen bytes long, creating the node on first use. A new
// key is copied: the caller's key is usually a substring of a whole
// inventory record, which the node would otherwise keep alive.
func (a *Aggregator) child(parent uint32, seg string, prefixLen int, depth uint16, ordered bool) aggPathEntry {
	a.keyBuf = appendNodeKey(a.keyBuf[:0], parent, seg)
	if !ordered {
		// In an ordered segment a prefix not on the path is new: see
		// StartSegment.
		if idx, ok := a.children[string(a.keyBuf)]; ok {
			return aggPathEntry{n: a.node(idx), idx: idx, end: prefixLen}
		}
	}
	key := a.segs.clone(a.keyBuf)
	idx, n := a.newNode(parent)
	n.row.Prefix, n.row.Depth = key[nodeKeyHeader:], depth
	a.bytes += aggregatorNodeBytes + int64(len(key)+prefixLen)
	a.prefixBytes += prefixLen
	if !ordered {
		a.children[key] = idx
		a.bytes += aggregatorMapEntryBytes
	}

	return aggPathEntry{n: n, idx: idx, end: prefixLen}
}

// rowSlabLen is how many nodes the aggregator allocates at a time.
const rowSlabLen = 4096

// node returns the node at index i.
func (a *Aggregator) node(i uint32) *aggNode {
	return &a.slabs[i/rowSlabLen][i%rowSlabLen]
}

// parent returns the parent index of the node at index i.
func (a *Aggregator) parent(i uint32) uint32 {
	return a.parents[i/rowSlabLen][i%rowSlabLen]
}

// setParent sets the parent index of the node at index i.
func (a *Aggregator) setParent(i, parent uint32) {
	a.parents[i/rowSlabLen][i%rowSlabLen] = parent
}

// newNode returns the next zeroed node from the slabs, with the given
// parent, and its index.
func (a *Aggregator) newNode(parent uint32) (uint32, *aggNode) {
	slab, i := a.used/rowSlabLen, a.used%rowSlabLen
	if slab == len(a.slabs) {
		a.slabs = append(a.slabs, make([]aggNode, rowSlabLen))
		a.parents = append(a.parents, make([]uint32, rowSlabLen))
	}
	idx := uint32(a.used)
	a.used++
	n := &a.slabs[slab][i]
	*n = aggNode{}
	a.parents[slab][i] = parent

	return idx, n
}

// PrefixCount returns the rows the next Drain hands out before merging
// segments: the prefixes seen since the last Drain, counting one seen
// in several ordered segments once per segment.
func (a *Aggregator) PrefixCount() int {
	return a.used - a.dead
}

// StartSegment marks the start of a run of keys that may be in order,
// such as one inventory file's. S3 Inventory doesn't promise any order
// (some inventories have come sorted per file, others not), so nothing
// depends on it: it only decides how fast the keys aggregate. While
// they are, every prefix not on the previous key's path is one the
// segment has not seen: it sorts after every prefix created before it
// (two keys in order bound their prefixes, and a prefix of both is a
// prefix of every key between them, so on the cached path). Its node is
// then created without a map lookup, and the segment's nodes are
// sorted as created. The first key out of order maps the segment, and
// the rest of it aggregates through the map as without segments; the
// result is the same either way. Drain merges the segments, so
// prefixes the segments share come out once.
func (a *Aggregator) StartSegment() {
	a.segments = append(a.segments, aggSegment{start: a.used})
	a.lastKey = a.lastKey[:0]
	clear(a.path)
	a.path = a.path[:0]
}

// inOrder reports whether nodes are being created in an ordered segment.
func (a *Aggregator) inOrder() bool {
	n := len(a.segments)

	return n > 0 && !a.segments[n-1].mapped
}

// mapSegment merges the current segment's tree into the main tree, for
// a key out of order. Its nodes come parents first, so each one's parent
// is placed before it: a node whose prefix the main tree has already is
// merged into that node and dies, any other joins the main tree under
// its parent's place. The cached path moves with its nodes.
func (a *Aggregator) mapSegment() {
	seg := &a.segments[len(a.segments)-1]
	seg.mapped = true
	g := a.graft[:0]
	for i := seg.start; i < a.used; i++ {
		idx := uint32(i)
		n := a.node(idx)
		to, prefixLen := idx, 0
		if p := a.parent(idx); p == noNode {
			if a.root == noNode {
				a.root = idx
			} else {
				to = a.root
			}
		} else {
			parent := g[int(p)-seg.start]
			prefixLen = parent.prefixLen + len(n.row.Prefix)
			a.setParent(idx, parent.to)
			k := nodeKey(n.row.Prefix, parent.to)
			if found, ok := a.children[k]; ok {
				to = found
			} else {
				a.children[k] = idx
				a.bytes += aggregatorMapEntryBytes
			}
		}
		if to != idx {
			a.node(to).row.Merge(&n.row)
			a.setParent(idx, deadNode)
			a.dead++
			a.prefixBytes -= prefixLen
			a.bytes -= int64(prefixLen)
		}
		g = append(g, aggGraft{to: to, prefixLen: prefixLen})
	}
	for d := range a.path {
		e := &a.path[d]
		e.idx = g[int(e.idx)-seg.start].to
		e.n = a.node(e.idx)
	}
	a.graft = g
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

// EstimatedMemoryUsage returns the heap the aggregator's nodes take
// since the last Drain: each node, its key, its map entry if it is
// in the map, and the prefix Drain writes for it, counted as they are
// created.
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

// Drain hands over every accumulated row, sorted by prefix with each
// prefix once, and clears the aggregator for reuse. The rows and the
// slice stay valid only until the aggregator is next used, when they are
// recycled; the prefixes, written into one allocation, stay valid.
//
// The main tree comes out in prefix order by a depth-first walk that
// visits each node's children in segment order. A segment ends in its
// only '/', so no sibling's segment is a prefix of another's: the first
// byte where two differ comes before either ends, and decides the order
// of every prefix below them too. Each ordered segment's nodes are in
// prefix order as created; Drain merges these runs, adding up a prefix
// they share.
func (a *Aggregator) Drain() []*PrefixRow {
	if a.used == 0 {
		return nil
	}

	live := a.used - a.dead
	out := drainBuf{rows: a.drained[:0], arena: make([]byte, 0, a.prefixBytes)}
	if cap(out.rows) < live {
		out.rows = make([]*PrefixRow, 0, live)
	}
	runs := a.runs[:0]
	if a.root != noNode {
		a.walkTree(&out)
		runs = append(runs, out.rows)
	}
	for i, seg := range a.segments {
		end := a.segmentEnd(i)
		if seg.mapped || seg.start == end {
			continue
		}
		begin := len(out.rows)
		for j := seg.start; j < end; j++ {
			out.emit(a, uint32(j))
		}
		runs = append(runs, out.rows[begin:])
	}
	a.drained, a.runs = out.rows, runs
	rows := out.rows
	if len(runs) > 1 {
		rows = a.mergeRuns(runs)
	}
	clear(a.runs)

	clear(a.children)
	a.used, a.dead, a.bytes, a.prefixBytes = 0, 0, 0, 0
	a.root = noNode
	a.segs.reset()
	// The cached path points at nodes the next use recycles.
	clear(a.path)
	a.path = a.path[:0]
	if n := len(a.segments); n > 0 {
		// A segment cut by the Drain goes on in the same mode.
		a.segments = append(a.segments[:0], aggSegment{mapped: a.segments[n-1].mapped})
	}

	return rows
}

// segmentEnd returns the end of segment i's nodes.
func (a *Aggregator) segmentEnd(i int) int {
	if i+1 < len(a.segments) {
		return a.segments[i+1].start
	}

	return a.used
}

// mappedRanges returns the [lo, hi) node ranges of the main tree: the
// nodes before the first segment and those of mapped segments, the
// first range first. It reuses buf.
func (a *Aggregator) mappedRanges(buf [][2]int) [][2]int {
	if len(a.segments) == 0 {
		return append(buf, [2]int{0, a.used})
	}
	buf = append(buf, [2]int{0, a.segments[0].start})
	for i, seg := range a.segments {
		if seg.mapped {
			buf = append(buf, [2]int{seg.start, a.segmentEnd(i)})
		}
	}

	return buf
}

// walkTree emits the main tree's live nodes in prefix order. It groups
// the nodes by parent with a counting sort (a node's index is above its
// parent's), then walks depth first, sorting each node's children by
// segment as it visits it.
func (a *Aggregator) walkTree(out *drainBuf) {
	var rangeBuf [8][2]int
	ranges := a.mappedRanges(rangeBuf[:0])

	// childEnd[p] ends p's children in kids; childEnd[p-1] starts them.
	end := slices.Grow(a.childEnd[:0], a.used)[:a.used]
	clear(end)
	for _, r := range ranges {
		for lo := r[0]; lo < r[1]; {
			parents := a.parentRange(lo, r[1])
			for _, p := range parents {
				if p < deadNode {
					end[p]++
				}
			}
			lo += len(parents)
		}
	}
	total := uint32(0)
	for p := range end {
		total += end[p]
		end[p] = total - end[p]
	}
	kids := slices.Grow(a.kids[:0], int(total))[:total]
	for _, r := range ranges {
		for lo := r[0]; lo < r[1]; {
			parents := a.parentRange(lo, r[1])
			for k, p := range parents {
				if p < deadNode {
					kids[end[p]] = uint32(lo + k)
					end[p]++
				}
			}
			lo += len(parents)
		}
	}
	a.childEnd, a.kids = end, kids

	stack := a.stack[:0]
	stack = append(stack, a.root)
	for len(stack) > 0 {
		i := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		out.emit(a, i)
		begin := uint32(0)
		if i > 0 {
			begin = end[i-1]
		}
		children := kids[begin:end[i]]
		if len(children) > 1 {
			a.sortChildren(children)
		}
		for _, c := range slices.Backward(children) {
			stack = append(stack, c)
		}
	}
	a.stack = stack
}

// parentRange returns the parents of the nodes from index lo, up to hi
// or the end of lo's slab, whichever comes first.
func (a *Aggregator) parentRange(lo, hi int) []uint32 {
	off := lo % rowSlabLen

	return a.parents[lo/rowSlabLen][off:min(rowSlabLen, off+hi-lo)]
}

// sortChildren sorts sibling nodes by segment, comparing copies of the
// segments rather than loading each node per comparison.
func (a *Aggregator) sortChildren(children []uint32) {
	sib := a.siblings[:0]
	for _, c := range children {
		sib = append(sib, aggSibling{seg: a.node(c).row.Prefix, idx: c})
	}
	slices.SortFunc(sib, func(x, y aggSibling) int { return strings.Compare(x.seg, y.seg) })
	for k := range sib {
		children[k] = sib[k].idx
	}
	clear(sib)
	a.siblings = sib
}

// drainBuf collects Drain's rows and writes their prefixes into arena,
// sized to hold them all.
type drainBuf struct {
	rows  []*PrefixRow
	arena []byte
}

// emit replaces node i's segment with its whole prefix, its parent's
// (already written) followed by the segment, and appends its row.
func (b *drainBuf) emit(a *Aggregator, i uint32) {
	n := a.node(i)
	if p := a.parent(i); p != noNode {
		start := len(b.arena)
		b.arena = append(b.arena, a.node(p).row.Prefix...)
		b.arena = append(b.arena, n.row.Prefix...)
		n.row.Prefix = unsafe.String(&b.arena[start], len(b.arena)-start)
	}
	b.rows = append(b.rows, &n.row)
}

// mergeRuns merges sorted runs into one, merging the rows of a prefix
// in several runs into the first.
func (a *Aggregator) mergeRuns(runs [][]*PrefixRow) []*PrefixRow {
	out := a.merged[:0]
	next := slices.Grow(a.runNext[:0], len(runs))[:len(runs)]
	clear(next)
	var h typedMergeHeap
	for i, run := range runs {
		h.push(mergeItem{row: run[0], readerIdx: i})
	}
	for h.Len() > 0 {
		top := h.items[0]
		if n := len(out); n > 0 && out[n-1].Prefix == top.row.Prefix {
			out[n-1].Merge(top.row)
		} else {
			out = append(out, top.row)
		}
		next[top.readerIdx]++
		if next[top.readerIdx] < len(runs[top.readerIdx]) {
			h.items[0].row = runs[top.readerIdx][next[top.readerIdx]]
			h.siftDown(0)
		} else {
			h.pop()
		}
	}
	a.merged, a.runNext = out, next

	return out
}

// Release drops the drained aggregator's slabs, segments, Drain output
// and scratch and map buckets so the GC can reclaim them once the caller
// is done with the last Drain. Call it only on an empty (just drained)
// aggregator.
func (a *Aggregator) Release() {
	a.slabs, a.parents, a.segs, a.path = nil, nil, segmentArena{}, nil
	a.drained, a.merged, a.runs, a.runNext = nil, nil, nil, nil
	a.childEnd, a.kids, a.stack, a.siblings, a.graft = nil, nil, nil, nil, nil
	a.children = make(map[string]uint32)
	a.keyBuf = nil
}

// segmentArena holds the nodes' keys in chunks it reuses after a Drain,
// when the nodes and map keys that point into them are recycled and
// cleared: one allocation per chunk instead of one per prefix.
type segmentArena struct {
	chunks [][]byte
	inUse  int
	free   []byte
}

// segmentArenaChunk is a key chunk's size; a key longer than an eighth
// of it (S3 keys are at most 1024 bytes) gets its own allocation.
const segmentArenaChunk = 64 << 10

func (s *segmentArena) clone(key []byte) string {
	var b []byte
	switch {
	case len(key) > segmentArenaChunk/8:
		b = make([]byte, len(key))
	case len(key) > len(s.free):
		if s.inUse == len(s.chunks) {
			s.chunks = append(s.chunks, make([]byte, segmentArenaChunk))
		}
		s.free = s.chunks[s.inUse]
		s.inUse++

		fallthrough
	default:
		b = s.free[:len(key):len(key)]
		s.free = s.free[len(key):]
	}
	copy(b, key)

	return unsafe.String(unsafe.SliceData(b), len(b))
}

func (s *segmentArena) reset() {
	s.inUse, s.free = 0, nil
}
