package extsort

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
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
// up to and including the next '/'. A lookup hashes the parent's index
// and the segment, and a spill hands the rows out sorted by walking the
// tree, building each prefix from the segments on the way.
//
// The aggregator is NOT safe for concurrent use. For concurrent access,
// use multiple aggregators or external synchronization.
type Aggregator struct {
	// children finds a node of the main tree by its key.
	children nodeTable
	// slabs back every node, in creation order (a node's index is its
	// position), and parents holds each node's parent at the same
	// position: noNode for a root, deadNode once mapSegment has merged it
	// into another node. Apart from the nodes, the walk's counting passes
	// read only the parents. blocks hold the tiers of nodes with more
	// than one, and segs the nodes' keys. None holds a pointer, so the GC
	// never scans them. All are reused after a spill, so a worker
	// allocates them only while its largest spill is still growing.
	slabs   [][]aggNode
	parents [][]uint32
	blocks  [][]tierBlock
	segs    segmentArena
	// path holds the nodes of lastKey's prefixes by depth (path[0] is the
	// root), the previous key's: the next key reuses the ones whose
	// prefix it starts with and looks up only the levels below. Keys in
	// order share most of their path; S3 Inventory doesn't promise any
	// order, so this is a saving, never an assumption. The length of the
	// prefix the next key shares with lastKey decides both which levels
	// it reuses and, in an ordered segment, whether it is in order, so
	// lastKey is kept whether or not the keys are.
	path    []aggPathEntry
	lastKey []byte
	// segments are the nodes since the last spill by the StartSegment
	// that began them (a chunk each). A segment whose keys come in order
	// needs no node table lookup; see StartSegment.
	segments []aggSegment
	// The walk's scratch, reused the same way as the slabs. childEnd is
	// also mapSegment's graft: no walk runs while a segment is mapped.
	childEnd []uint32
	kids     []uint32
	stack    []uint32
	siblings []aggSibling
	prefix   prefixBuilder
	used     int
	dead     int
	// blocksUsed is how many tier blocks the nodes since the last spill
	// have taken.
	blocksUsed int
	// root is the main tree's root node, or noNode.
	root     uint32
	maxDepth int
	// bytes is the heap the nodes since the last spill take, with their
	// keys and tier blocks: EstimatedMemoryUsage adds the node table and
	// the walk's scratch.
	bytes    int64
	tierMask uint16
}

// aggNode is a prefix's statistics and where its key is. Count and bytes
// are its totals. While tiers is 0 they are all in tier; otherwise
// block tiers-1 holds them by tier, and they are always its sums, so a
// node encodes as a row with them (see appendNodeStats). Most prefixes
// hold one or a few objects, most of those in one tier, so most nodes
// never take a block. The key is the parent's index and the segment,
// segLen bytes at seg in segs; a root has none.
type aggNode struct {
	count  uint64
	bytes  uint64
	seg    segRef
	tiers  uint32
	segLen uint32
	depth  uint16
	tier   tiers.ID
}

// tierMask's 16 bits and the run record's mask hold a bit per tier.
const _ uint8 = 16 - uint8(tiers.NumTiers)

const (
	noNode   = math.MaxUint32
	deadNode = math.MaxUint32 - 1
)

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

// A node's heap cost is aggregatorNodeBytes (the node and its parent's
// index), its key and, for a node of the main tree, its share of the
// node table (see nodeSlotBytes); a node with several tiers also takes
// a tier block.
const (
	aggregatorNodeBytes = int64(unsafe.Sizeof(aggNode{}) + unsafe.Sizeof(uint32(0)))
	tierBlockBytes      = int64(unsafe.Sizeof(tierBlock{}))
)

// NewAggregator creates a new prefix aggregator with the given initial capacity.
// Use maxDepth=0 for unlimited depth.
func NewAggregator(initialCapacity, maxDepth int) *Aggregator {
	if initialCapacity <= 0 {
		// Default to 10K - small enough to start but grows as needed
		initialCapacity = 10000
	}

	return &Aggregator{
		children: newNodeTable(initialCapacity),
		root:     noNode,
		maxDepth: maxDepth,
	}
}

// AddObject adds an object's statistics to all its prefix ancestors.
// The key should be the full S3 object key (e.g., "data/2024/01/file.csv").
// The caller must spill once Full reports true.
func (a *Aggregator) AddObject(key string, size uint64, tierID tiers.ID) {
	a.tierMask |= uint16(1) << tierID
	last := unsafe.String(unsafe.SliceData(a.lastKey), len(a.lastKey))
	shared := sharedPrefixLen(key, last)
	ordered := a.inOrder()
	if ordered && shared < len(last) && (shared == len(key) || key[shared] < last[shared]) {
		a.mapSegment()
		ordered = false
	}
	a.lastKey = append(a.lastKey[:shared], key[shared:]...)

	path := a.path
	if len(path) == 0 {
		path = append(path, a.rootEntry(ordered, tierID))
	}
	a.add(path[0].n, size, tierID)
	// The previous key's prefixes no longer than the bytes this key
	// shares with it are this key's too.
	d := 1
	for ; d < len(path) && path[d].end <= shared; d++ {
		a.add(path[d].n, size, tierID)
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
		e := a.child(path[len(path)-1].idx, key[start:i+1], i+1, depth, ordered, tierID)
		a.add(e.n, size, tierID)
		path = append(path, e)
		start = i + 1
		depth++
	}
	a.path = path
}

// add adds an object to node n.
func (a *Aggregator) add(n *aggNode, size uint64, t tiers.ID) {
	if n.tiers == 0 && n.tier == t {
		n.count++
		n.bytes += size

		return
	}
	a.addTier(n, t, 1, size)
}

// addTier adds count objects of size bytes in tier t to node n, giving
// it a tier block when t is a second tier.
func (a *Aggregator) addTier(n *aggNode, t tiers.ID, count, size uint64) {
	if n.tiers == 0 {
		if n.tier == t {
			n.count += count
			n.bytes += size

			return
		}
		a.promote(n)
	}
	b := a.block(n.tiers)
	b.counts[t] += count
	b.bytes[t] += size
	n.count += count
	n.bytes += size
}

// promote gives node n a tier block holding its totals in its one tier,
// so they stay the block's sums.
func (a *Aggregator) promote(n *aggNode) {
	slab, i := a.blocksUsed/tierBlockSlabLen, a.blocksUsed%tierBlockSlabLen
	if slab == len(a.blocks) {
		a.blocks = append(a.blocks, make([]tierBlock, tierBlockSlabLen))
	}
	a.blocksUsed++
	a.bytes += tierBlockBytes
	b := &a.blocks[slab][i]
	*b = tierBlock{}
	b.counts[n.tier], b.bytes[n.tier] = n.count, n.bytes
	n.tiers = uint32(a.blocksUsed)
}

// mergeNode adds src's objects to dst.
func (a *Aggregator) mergeNode(dst, src *aggNode) {
	if src.tiers == 0 {
		a.addTier(dst, src.tier, src.count, src.bytes)

		return
	}
	b := a.block(src.tiers)
	for t := range MaxTiers {
		if b.counts[t]|b.bytes[t] != 0 {
			a.addTier(dst, tiers.ID(t), b.counts[t], b.bytes[t])
		}
	}
}

// tierBlockSlabLen is how many tier blocks the aggregator allocates at a
// time.
const tierBlockSlabLen = 256

// block returns the tier block a node's tiers field names.
func (a *Aggregator) block(ref uint32) *tierBlock {
	i := ref - 1

	return &a.blocks[i/tierBlockSlabLen][i%tierBlockSlabLen]
}

// nodeBlock returns n's tier block, or nil while it has one tier.
func (a *Aggregator) nodeBlock(n *aggNode) *tierBlock {
	if n.tiers == 0 {
		return nil
	}

	return a.block(n.tiers)
}

// rootEntry returns the root's path entry: the main tree's, created on
// first use, or in an ordered segment a root of the segment's own.
func (a *Aggregator) rootEntry(ordered bool, tier tiers.ID) aggPathEntry {
	if !ordered && a.root != noNode {
		return aggPathEntry{n: a.node(a.root), idx: a.root}
	}
	idx, n := a.newNode(noNode, tier)
	a.bytes += aggregatorNodeBytes
	if !ordered {
		a.root = idx
	}

	return aggPathEntry{n: n, idx: idx}
}

// child returns the entry of parent's child with segment seg, whose
// prefix is prefixLen bytes long, creating the node, for an object in
// tier, on first use. A new key is copied: the caller's key is usually
// a substring of a whole inventory record, which the node would
// otherwise keep alive.
func (a *Aggregator) child(parent uint32, seg string, prefixLen int, depth uint16, ordered bool, tier tiers.ID) aggPathEntry {
	// In an ordered segment a prefix not on the path is new: see
	// StartSegment. Otherwise a new key's slot names the node newNode
	// creates next.
	if !ordered {
		if idx, found := a.getOrAddChild(parent, seg, uint32(a.used)); found {
			return aggPathEntry{n: a.node(idx), idx: idx, end: prefixLen}
		}
	}
	idx, n := a.newNode(parent, tier)
	n.seg, n.segLen, n.depth = a.segs.clone(parent, seg), uint32(len(seg)), depth
	a.bytes += aggregatorNodeBytes + int64(nodeKeyHeader+len(seg))

	return aggPathEntry{n: n, idx: idx, end: prefixLen}
}

// getOrAddChild returns the main tree's child of parent with segment seg
// and true, or adds node idx as that child and returns idx and false.
func (a *Aggregator) getOrAddChild(parent uint32, seg string, idx uint32) (uint32, bool) {
	return a.children.getOrAdd(a, a.children.hash(parent, seg), parent, seg, idx)
}

// hasKey reports whether node i is parent's child with segment seg.
func (a *Aggregator) hasKey(i, parent uint32, seg string) bool {
	n := a.node(i)
	if int(n.segLen) != len(seg) {
		return false
	}
	key := a.segs.key(n.seg, n.segLen)

	return binary.LittleEndian.Uint32(key) == parent && string(key[nodeKeyHeader:]) == seg
}

// segment returns node n's segment, "" for a root. It points into the
// arena, which a spill reuses.
func (a *Aggregator) segment(n *aggNode) string {
	if n.segLen == 0 {
		return ""
	}
	seg := a.segs.key(n.seg, n.segLen)[nodeKeyHeader:]

	return unsafe.String(unsafe.SliceData(seg), len(seg))
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

// newNode returns the next node from the slabs, empty in tier, with the
// given parent, and its index.
func (a *Aggregator) newNode(parent uint32, tier tiers.ID) (uint32, *aggNode) {
	slab, i := a.used/rowSlabLen, a.used%rowSlabLen
	if slab == len(a.slabs) {
		a.slabs = append(a.slabs, make([]aggNode, rowSlabLen))
		a.parents = append(a.parents, make([]uint32, rowSlabLen))
	}
	idx := uint32(a.used)
	a.used++
	n := &a.slabs[slab][i]
	*n = aggNode{tier: tier}
	a.parents[slab][i] = parent

	return idx, n
}

// PrefixCount returns the rows the next spill hands out before merging
// segments: the prefixes seen since the last spill, counting one seen
// in several ordered segments once per segment.
func (a *Aggregator) PrefixCount() int {
	return a.used - a.dead
}

// Node indexes and key references are 32 bits. Full reports the
// aggregator full this far short of either limit, more than one key can
// take: aggNodeLimit nodes, or arenaChunkLimit key chunks.
const (
	aggNodeLimit    = deadNode - 1<<20
	arenaChunkLimit = maxArenaChunks - 1<<12
)

// Full reports whether the aggregator is close to the most nodes or key
// bytes it can address, so it must spill before the next object. Only
// a worker with tens of gigabytes to itself gets there.
func (a *Aggregator) Full() bool {
	return a.used >= aggNodeLimit || a.segs.inUse >= arenaChunkLimit
}

// StartSegment marks the start of a run of keys that may be in order,
// such as one inventory file's. S3 Inventory doesn't promise any order
// (some inventories have come sorted per file, others not), so nothing
// depends on it: it only decides how fast the keys aggregate. While
// they are, every prefix not on the previous key's path is one the
// segment has not seen: it sorts after every prefix created before it
// (two keys in order bound their prefixes, and a prefix of both is a
// prefix of every key between them, so on the cached path). Its node is
// then created without a node table lookup, and the segment's nodes
// are sorted as created. The first key out of order maps the segment
// into the main tree, and the rest of it aggregates through the node
// table as without segments; the result is the same either way. A
// spill merges the segments, so prefixes the segments share come out
// once.
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
	// graft[i] is where the segment's i-th node went.
	graft := a.childEnd[:0]
	for i := seg.start; i < a.used; i++ {
		idx := uint32(i)
		n := a.node(idx)
		to := idx
		if p := a.parent(idx); p == noNode {
			if a.root == noNode {
				a.root = idx
			} else {
				to = a.root
			}
		} else {
			parent := graft[int(p)-seg.start]
			a.setParent(idx, parent)
			a.segs.setParent(n.seg, parent)
			to, _ = a.getOrAddChild(parent, a.segment(n), idx)
		}
		if to != idx {
			a.mergeNode(a.node(to), n)
			a.setParent(idx, deadNode)
			a.dead++
		}
		graft = append(graft, to)
	}
	for d := range a.path {
		e := &a.path[d]
		e.idx = graft[int(e.idx)-seg.start]
		e.n = a.node(e.idx)
	}
	a.childEnd = graft
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
// since the last spill, each with its key and any tier block counted as
// it is created, plus the main tree's node table at its allocated size,
// which a spill keeps.
func (a *Aggregator) EstimatedMemoryUsage() int64 {
	return a.bytes + a.children.heapBytes()
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
// prefix once, and clears the aggregator for reuse. The rows are copies
// the caller keeps: a build spills through WriteRun instead, which
// encodes the nodes without building rows.
//
// The main tree comes out in prefix order by a depth-first walk that
// visits each node's children in segment order. A segment ends in its
// only '/', so no sibling's segment is a prefix of another's: the first
// byte where two differ comes before either ends, and decides the order
// of every prefix below them too. Each ordered segment's nodes are in
// prefix order as created. A spill of several of these runs merges
// them, adding up a prefix they share.
func (a *Aggregator) Drain() []*PrefixRow {
	if a.used == 0 {
		return nil
	}
	rows := make([]PrefixRow, 0, a.PrefixCount())
	if _, err := a.mergeRuns(func(r *PrefixRow) error {
		rows = append(rows, *r)

		return nil
	}); err != nil {
		// The runs are the aggregator's own nodes, which read without error.
		panic(fmt.Sprintf("extsort: drain aggregator: %v", err))
	}
	out := make([]*PrefixRow, len(rows))
	for i := range rows {
		out[i] = &rows[i]
	}

	return out
}

// WriteRun writes the rows Drain would hand out to w, byte for byte as
// w.WriteAll(a.Drain()) would, clears the aggregator for reuse and
// returns how many rows it wrote.
func (a *Aggregator) WriteRun(w *CompressedRunWriter) (int, error) {
	return a.writeRun(&w.enc)
}

// writeRun is WriteRun into a run encoder. When the rows come from one
// run, the main tree or one ordered segment, it encodes each straight
// from its node, building the prefix from the segments as it walks
// them; rows from several runs are merged as rows.
func (a *Aggregator) writeRun(e *runEncoder) (int, error) {
	if a.used == 0 {
		return 0, nil
	}
	seg, ok := a.soleRun()
	if !ok {
		return a.mergeRuns(e.write)
	}

	defer a.reset()
	a.prefix.reset()
	n := 0
	if seg < 0 {
		a.startWalk()
		for i, more := a.walkNext(); more; i, more = a.walkNext() {
			if err := a.encodeNode(e, i); err != nil {
				return n, err
			}
			n++
		}

		return n, nil
	}
	for j := a.segments[seg].start; j < a.segmentEnd(seg); j++ {
		if err := a.encodeNode(e, uint32(j)); err != nil {
			return n, err
		}
		n++
	}

	return n, nil
}

// encodeNode writes node i, the next in its run's order, to e.
func (a *Aggregator) encodeNode(e *runEncoder, i uint32) error {
	n := a.node(i)
	shared := a.prefix.next(n.depth, a.segment(n))

	return e.writeNode(shared, a.prefix.cur[shared:], n.depth, n.count, n.bytes, n.tier, a.nodeBlock(n))
}

// mergeRuns merges the runs into rows, adding up a prefix in several,
// passes each to visit in prefix order, clears the aggregator and
// returns how many rows it visited.
func (a *Aggregator) mergeRuns(visit func(*PrefixRow) error) (int, error) {
	defer a.reset()
	merged, err := newMergeIterator(a.runSources())
	if err != nil {
		return 0, fmt.Errorf("merge aggregator runs: %w", err)
	}
	n := 0
	for {
		row, err := merged.Next()
		if errors.Is(err, io.EOF) {
			return n, nil
		}
		if err != nil {
			return n, fmt.Errorf("merge aggregator runs: %w", err)
		}
		if err := visit(row); err != nil {
			return n, err
		}
		n++
	}
}

// runSources returns a cursor over each run: the main tree, if it has a
// root, and every ordered segment with nodes.
func (a *Aggregator) runSources() []runSource {
	var sources []runSource
	segRows := 0
	for i, seg := range a.segments {
		if end := a.segmentEnd(i); !seg.mapped && seg.start < end {
			sources = append(sources, &aggCursor{a: a, next: seg.start, end: end})
			segRows += end - seg.start
		}
	}
	if a.root != noNode {
		a.startWalk()
		tree := &aggCursor{a: a, tree: true, end: a.PrefixCount() - segRows}
		sources = append([]runSource{tree}, sources...)
	}

	return sources
}

// soleRun reports whether every live node is in one run, and which: -1
// for the main tree, or the index of an ordered segment.
func (a *Aggregator) soleRun() (int, bool) {
	which, runs := -1, 0
	if a.root != noNode {
		runs++
	}
	for i, seg := range a.segments {
		if !seg.mapped && seg.start < a.segmentEnd(i) {
			which = i
			runs++
		}
	}

	return which, runs == 1
}

// reset empties the aggregator once its rows are handed over.
func (a *Aggregator) reset() {
	a.children.reset()
	a.used, a.dead, a.blocksUsed, a.bytes = 0, 0, 0, 0
	a.root = noNode
	a.segs.reset()
	// The cached path points at nodes the next use recycles.
	clear(a.path)
	a.path = a.path[:0]
	a.stack = a.stack[:0]
	if n := len(a.segments); n > 0 {
		// A segment cut by the spill goes on in the same mode.
		a.segments = append(a.segments[:0], aggSegment{mapped: a.segments[n-1].mapped})
	}
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

// startWalk readies walkNext to visit the main tree's live nodes in
// prefix order. It groups the nodes by parent with a counting sort (a
// node's index is above its parent's); walkNext then goes depth first,
// sorting each node's children by segment as it visits it.
func (a *Aggregator) startWalk() {
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
	a.stack = append(a.stack[:0], a.root)
}

// walkNext returns the main tree's next node in prefix order, and false
// after the last.
func (a *Aggregator) walkNext() (uint32, bool) {
	top := len(a.stack) - 1
	if top < 0 {
		return 0, false
	}
	i := a.stack[top]
	a.stack = a.stack[:top]
	begin := uint32(0)
	if i > 0 {
		begin = a.childEnd[i-1]
	}
	children := a.kids[begin:a.childEnd[i]]
	if len(children) > 1 {
		a.sortChildren(children)
	}
	for _, c := range slices.Backward(children) {
		a.stack = append(a.stack, c)
	}

	return i, true
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
		sib = append(sib, aggSibling{seg: a.segment(a.node(c)), idx: c})
	}
	slices.SortFunc(sib, func(x, y aggSibling) int { return strings.Compare(x.seg, y.seg) })
	for k := range sib {
		children[k] = sib[k].idx
	}
	clear(sib)
	a.siblings = sib
}

// prefixBuilder builds each prefix of a run from the segments, for nodes
// in a run's order: a depth-first walk with each node's children in
// segment order, or an ordered segment's creation order. Either way a
// node's parent comes before it and every node between them is the
// parent's descendant, so the previous prefix starts with the parent's,
// whose length is still ends[depth-1].
type prefixBuilder struct {
	cur  []byte // the last prefix built
	ends []int  // by depth, the length of the last prefix built at it
}

// reset starts a run.
func (b *prefixBuilder) reset() {
	b.cur, b.ends = b.cur[:0], b.ends[:0]
}

// next makes cur the prefix of the run's next node, at depth with
// segment seg, and returns how many bytes it shares with the previous
// one: its parent's prefix and what seg shares with the rest of the
// previous prefix, a sibling's segment or nothing.
func (b *prefixBuilder) next(depth uint16, seg string) int {
	d := int(depth)
	base := 0
	if d > 0 {
		base = b.ends[d-1]
	}
	rest := b.cur[base:]
	shared := base + sharedPrefixLen(unsafe.String(unsafe.SliceData(rest), len(rest)), seg)
	b.cur = append(b.cur[:base], seg...)
	if len(b.ends) <= d {
		b.ends = slices.Grow(b.ends, d+1-len(b.ends))[:d+1]
	}
	b.ends[d] = len(b.cur)

	return shared
}

// aggCursor reads one of an aggregator's runs as rows, for a merge: the
// main tree's walk, or the nodes of an ordered segment from next to end.
// Its prefixes come from an arena of its own, so a row's stays valid
// after the cursor moves on.
type aggCursor struct {
	a      *Aggregator
	prefix prefixBuilder
	arena  prefixArena
	next   int
	end    int
	read   int
	tree   bool
}

// readNext reads the run's next row into row and returns how many
// bytes its prefix shares with the previous row's.
func (c *aggCursor) readNext(row *PrefixRow) (int, error) {
	var i uint32
	if c.tree {
		var more bool
		if i, more = c.a.walkNext(); !more {
			return 0, io.EOF
		}
	} else {
		if c.next == c.end {
			return 0, io.EOF
		}
		i = uint32(c.next)
		c.next++
	}
	n := c.a.node(i)
	shared := c.prefix.next(n.depth, c.a.segment(n))
	prefix := ""
	if k := len(c.prefix.cur); k > 0 {
		p := c.arena.alloc(k)
		copy(p, c.prefix.cur)
		prefix = unsafe.String(unsafe.SliceData(p), k)
	}
	*row = PrefixRow{Prefix: prefix, Count: n.count, TotalBytes: n.bytes, Depth: n.depth}
	if b := c.a.nodeBlock(n); b != nil {
		row.TierCounts, row.TierBytes = b.counts, b.bytes
	} else {
		row.TierCounts[n.tier], row.TierBytes[n.tier] = n.count, n.bytes
	}
	c.read++

	return shared, nil
}

// Count returns the rows of the run: for the main tree, its live nodes.
func (c *aggCursor) Count() uint64 {
	if c.tree {
		return uint64(c.end)
	}

	return uint64(c.end - c.next + c.read)
}

func (c *aggCursor) ReadCount() uint64 { return uint64(c.read) }

func (c *aggCursor) Close() error { return nil }

// Release drops the spilled aggregator's slabs, segments, scratch and
// node table so the GC can reclaim them. Call it only on an empty (just
// spilled) aggregator.
func (a *Aggregator) Release() {
	a.slabs, a.parents, a.blocks, a.segs, a.path = nil, nil, nil, segmentArena{}, nil
	a.childEnd, a.kids, a.stack, a.siblings, a.prefix = nil, nil, nil, nil, prefixBuilder{}
	a.children.release()
}

// segRef is where a node's key is in a segmentArena: its chunk's index
// above segRefOffsetBits, and its offset in the chunk below.
type segRef uint32

const (
	segRefOffsetBits = 16
	// MaxArenaChunks is how many chunks a segRef can name.
	maxArenaChunks = 1 << (32 - segRefOffsetBits)
)

// segmentArena holds the nodes' keys, each its parent's index and its
// segment (see nodeKeyHeader), in chunks it reuses after a spill, when
// the nodes that point into them are recycled and the node table
// cleared: one allocation per chunk instead of one per prefix, and no
// pointer in a node.
type segmentArena struct {
	chunks [][]byte
	inUse  int // chunks handed out
	cur    int // the chunk keys are being added to: an index plus one, or 0 for none
	off    int // where in cur the next key goes
}

// segmentArenaChunk is a key chunk's size, the most a segRef's offset
// addresses. A key longer than an eighth of it (S3 keys are at most 1024
// bytes) gets a chunk of its own.
const segmentArenaChunk = 1 << segRefOffsetBits

// clone stores the key of parent's child with segment seg and returns
// where it is.
func (s *segmentArena) clone(parent uint32, seg string) segRef {
	n := nodeKeyHeader + len(seg)
	var c, off int
	if n > segmentArenaChunk/8 {
		c = s.acquire(n)
	} else {
		if s.cur == 0 || s.off+n > segmentArenaChunk {
			s.cur, s.off = s.acquire(segmentArenaChunk)+1, 0
		}
		c, off = s.cur-1, s.off
		s.off += n
	}
	b := s.chunks[c][off : off+n]
	binary.LittleEndian.PutUint32(b, parent)
	copy(b[nodeKeyHeader:], seg)

	return segRef(c<<segRefOffsetBits | off)
}

// acquire hands out the next chunk, at least size bytes long, and
// returns its index.
func (s *segmentArena) acquire(size int) int {
	c := s.inUse
	if c == maxArenaChunks {
		panic("extsort: aggregator key arena full; the caller must spill once Full reports true")
	}
	if c == len(s.chunks) {
		s.chunks = append(s.chunks, nil)
	}
	if len(s.chunks[c]) < size {
		s.chunks[c] = make([]byte, max(size, segmentArenaChunk))
	}
	s.inUse++

	return c
}

// key returns the key at ref whose segment is segLen bytes: the parent's
// index, then the segment.
func (s *segmentArena) key(ref segRef, segLen uint32) []byte {
	off := int(ref & (segmentArenaChunk - 1))

	return s.chunks[ref>>segRefOffsetBits][off : off+nodeKeyHeader+int(segLen)]
}

// setParent rewrites the parent index of the key at ref. Only a node not
// yet in the node table has its key rewritten.
func (s *segmentArena) setParent(ref segRef, parent uint32) {
	off := int(ref & (segmentArenaChunk - 1))
	binary.LittleEndian.PutUint32(s.chunks[ref>>segRefOffsetBits][off:], parent)
}

func (s *segmentArena) reset() {
	s.inUse, s.cur, s.off = 0, 0, 0
}
