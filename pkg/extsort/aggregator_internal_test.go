package extsort

import (
	"fmt"
	"maps"
	"math"
	"math/rand/v2"
	"reflect"
	"runtime"
	"runtime/debug"
	"slices"
	"strings"
	"testing"
	"unsafe"

	"github.com/eunmann/s3-inv-db/internal/benchutil"
	"github.com/eunmann/s3-inv-db/pkg/tiers"
	gocmp "github.com/google/go-cmp/cmp"
)

// heapReadings must report what runtime.ReadMemStats calls HeapInuse
// and, right after a GC, the heap that GC found live (HeapAlloc then).
func TestHeapReadingsMatchMemStats(t *testing.T) {
	keep := make([][]byte, 0, 64)
	for range 64 {
		keep = append(keep, make([]byte, 1<<16))
	}
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	live, inuse := heapReadings()
	runtime.KeepAlive(keep)

	if lo, hi := m.HeapInuse*9/10, m.HeapInuse*11/10; inuse < lo || inuse > hi {
		t.Errorf("HeapInuse reading = %d, MemStats.HeapInuse = %d (want within 10%%)", inuse, m.HeapInuse)
	}
	if lo, hi := m.HeapAlloc*9/10, m.HeapAlloc*11/10; live < lo || live > hi {
		t.Errorf("live heap reading = %d, HeapAlloc after GC = %d (want within 10%%)", live, m.HeapAlloc)
	}
}

// Garbage below the HeapInuse backstop is not pressure: with GOMEMLIMIT
// the heap grows toward the limit before each GC, so only the live heap
// or a nearly full HeapInuse may force a spill.
func TestHeapReadingsUnderPressure(t *testing.T) {
	const limit int64 = 1000
	for _, tc := range []struct {
		name        string
		live, inuse uint64
		want        bool
	}{
		{name: "idle", live: 100, inuse: 200, want: false},
		{name: "live 50% inuse 90% garbage", live: 500, inuse: 900, want: false},
		{name: "live just below 70%", live: 699, inuse: 700, want: false},
		{name: "live 70%", live: 700, inuse: 700, want: true},
		{name: "inuse just below 95%", live: 100, inuse: 949, want: false},
		{name: "inuse 95%", live: 100, inuse: 950, want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := heapReadingsUnderPressure(tc.live, tc.inuse, limit); got != tc.want {
				t.Errorf("heapReadingsUnderPressure(live=%d, inuse=%d, limit=%d) = %v, want %v",
					tc.live, tc.inuse, limit, got, tc.want)
			}
		})
	}
}

// EstimatedMemoryUsage counts each new prefix once, with its key (the
// parent's index and the segment), and a tier block for each node that
// comes to hold a second tier, whether from an object or from a
// segment's node merged into it when the segment is mapped; a Drain
// starts it over. The node table counts at its size, which a Drain
// keeps.
func TestAggregatorEstimateCountsEachPrefix(t *testing.T) {
	const node, h, block = aggregatorNodeBytes, nodeKeyHeader, tierBlockBytes
	agg := NewAggregator(0, 0)
	table := int64(tableSlotsFor(10_000)) * nodeSlotBytes
	check := func(step string, want int64) {
		t.Helper()
		if got := agg.EstimatedMemoryUsage(); got != want {
			t.Fatalf("%s: estimate = %d, want %d", step, got, want)
		}
	}
	check("new", table)
	for _, k := range []string{"a/b/x", "a/b/y", "a/c/z", "top"} {
		agg.AddObject(k, 1, tiers.Standard)
	}
	// "" (no key), then "a/", "a/b/", "a/c/".
	want := table + 4*node + 3*(h+2)
	check("table", want)

	agg.AddObject("top2", 1, tiers.GlacierIR)
	// The root's second tier.
	want += block
	check("second tier", want)
	agg.AddObject("top3", 1, tiers.DeepArchive)
	check("third tier", want)

	agg.StartSegment()
	agg.AddObject("a/b/z", 1, tiers.GlacierFR)
	// A root, "a/" and "a/b/" of the segment's own, in no table.
	want += 3*node + 2*(h+2)
	check("ordered segment", want)

	agg.AddObject("a/a/x", 1, tiers.Standard)
	// Mapping merges the three into the main tree's: the root has a
	// block, "a/" and "a/b/" take one. Then "a/a/" is new.
	want += 2*block + node + (h + 2)
	check("mapped segment", want)
	if got := agg.PrefixCount(); got != 5 {
		t.Errorf("PrefixCount = %d, want 5", got)
	}

	agg.Drain()
	check("after Drain", table)
	agg.Release()
	check("after Release", 0)
}

// The estimate counts the node table at the size it grows to, not at the
// size its nodes would need: after a spill that grew it, a smaller one
// still counts the larger table it keeps.
func TestAggregatorEstimateCountsTableGrowth(t *testing.T) {
	agg := NewAggregator(1, 0)
	slots := minTableSlots
	for i := range 1000 {
		agg.AddObject(fmt.Sprintf("p%04d/x", i), 1, tiers.Standard)
		if agg.children.count > maxTableLoad(slots) {
			slots *= 2
		}
		if got := len(agg.children.slots); got != slots {
			t.Fatalf("after %d prefixes: %d slots, want %d", i+1, got, slots)
		}
		if got, want := agg.EstimatedMemoryUsage(), agg.bytes+int64(slots)*nodeSlotBytes; got != want {
			t.Fatalf("after %d prefixes: estimate %d, want %d", i+1, got, want)
		}
	}
	if slots != 2048 {
		t.Fatalf("1000 prefixes grew the table to %d slots, want 2048", slots)
	}
	agg.Drain()
	agg.AddObject("q/x", 1, tiers.Standard)
	if got, want := agg.EstimatedMemoryUsage(), agg.bytes+int64(slots)*nodeSlotBytes; got != want {
		t.Fatalf("after a Drain: estimate %d, want %d", got, want)
	}
}

// Rows recycled after a Drain must start from zero, across more than
// one slab, and a smaller second spill must not see the first's rows.
func TestAggregatorDrainRecyclesRows(t *testing.T) {
	agg := NewAggregator(0, 0)
	first := rowSlabLen + 10
	for i := range first {
		agg.AddObject(fmt.Sprintf("a%d/x", i), 7, tiers.GlacierFR)
	}
	if got := len(agg.Drain()); got != first+1 {
		t.Fatalf("first drain = %d rows, want %d", got, first+1)
	}

	agg.AddObject("b/x", 5, tiers.Standard)
	agg.AddObject("b/y", 6, tiers.Standard)
	rows := agg.Drain()
	if len(rows) != 2 {
		t.Fatalf("second drain = %d rows, want 2", len(rows))
	}
	want := map[string]uint64{"": 11, "b/": 11}
	for _, r := range rows {
		if r.TotalBytes != want[r.Prefix] || r.Count != 2 {
			t.Errorf("row %q = %d objects, %d bytes; want 2, %d", r.Prefix, r.Count, r.TotalBytes, want[r.Prefix])
		}
		if r.TierCounts[tiers.GlacierFR] != 0 || r.TierBytes[tiers.GlacierFR] != 0 {
			t.Errorf("row %q kept tier stats from the previous spill", r.Prefix)
		}
	}
}

// Drain returns the rows sorted by prefix, each once, whatever order the
// keys came in: a segment with a byte below '/' ("a-/") sorts before its
// sibling "a/" and that sibling's children, and keys in reverse order
// across slabs come out in order.
func TestAggregatorDrainIsSorted(t *testing.T) {
	agg := NewAggregator(0, 0)
	for _, k := range []string{"b/x", "a/y/z", "b/w/v", "a-/q", "a/"} {
		agg.AddObject(k, 1, tiers.Standard)
	}
	got := make([]string, 0, 6)
	for _, r := range agg.Drain() {
		got = append(got, r.Prefix)
	}
	if want := []string{"", "a-/", "a/", "a/y/", "b/", "b/w/"}; !slices.Equal(got, want) {
		t.Errorf("Drain order = %q, want %q", got, want)
	}

	n := rowSlabLen + 10
	for i := range n {
		agg.AddObject(fmt.Sprintf("p%05d/x", n-i), 1, tiers.Standard)
	}
	rows := agg.Drain()
	if !slices.IsSortedFunc(rows, func(a, b *PrefixRow) int { return strings.Compare(a.Prefix, b.Prefix) }) {
		t.Error("rows of keys in reverse order are not drained in order")
	}
	if len(rows) != n+1 {
		t.Errorf("drained %d rows, want %d", len(rows), n+1)
	}
}

// A node is 32 bytes with no pointer in it, so the GC never scans the
// slabs, and half a cache line; its parent's index lives in the dense
// parents array, which the walk's counting passes read without touching
// the nodes.
func TestAggregatorNodeIsCompact(t *testing.T) {
	if got := unsafe.Sizeof(aggNode{}); got != 32 {
		t.Errorf("aggNode is %d B, want 32", got)
	}
	typ := reflect.TypeFor[aggNode]()
	for f := range typ.Fields() {
		if f.Type.Kind() < reflect.Bool || f.Type.Kind() > reflect.Uint64 {
			t.Errorf("aggNode.%s is a %s, not an integer", f.Name, f.Type)
		}
	}
}

// Drain reads the parents of the main tree's node ranges slab by slab:
// ranges that start and end inside a slab and cross slab boundaries,
// beside ordered segments whose nodes must stay out of the tree, still
// drain sorted and exact.
func TestAggregatorDrainAcrossSlabs(t *testing.T) {
	const n = 3000
	rng := rand.New(rand.NewPCG(8, 9))
	keyed := func(format string, order []int) []string {
		keys := make([]string, 0, len(order))
		for _, i := range order {
			keys = append(keys, fmt.Sprintf(format, i, i%7))
		}

		return keys
	}
	inOrder := make([]int, n)
	for i := range inOrder {
		inOrder[i] = i
	}
	chunks := [][]string{
		keyed("m%05d/%d/x", rng.Perm(n)), // before any segment: the first range
		keyed("o%05d/%d/y", inOrder),     // ordered: crosses a slab, never mapped
		keyed("m%05d/%d/z", rng.Perm(n)), // mapped: a range inside the slabs
		keyed("s%05d/%d/w", inOrder),     // ordered again
		keyed("q%05d/%d/v", rng.Perm(n)), // mapped, up to the last node
	}
	agg := NewAggregator(0, 0)
	var all []string
	for c, keys := range chunks {
		if c > 0 {
			agg.StartSegment()
		}
		for _, k := range keys {
			agg.AddObject(k, uint64(len(k)+len(all)), tiers.ID(len(all)%3))
			all = append(all, k)
		}
	}
	if agg.used < 4*rowSlabLen {
		t.Fatalf("only %d nodes; the test needs several slabs", agg.used)
	}
	rows := agg.Drain()
	for i := 1; i < len(rows); i++ {
		if rows[i-1].Prefix >= rows[i].Prefix {
			t.Fatalf("drained %q then %q", rows[i-1].Prefix, rows[i].Prefix)
		}
	}
	got := map[string]PrefixRow{}
	mergeRows(got, rows)
	if want := referenceAggregate(all, 0); !maps.Equal(want, got) {
		t.Fatalf("drain differs from a direct count:\n%s", gocmp.Diff(want, got))
	}
}

// A spill forced by heap pressure must leave the rows to the GC: kept
// slabs would hold HeapInuse at the limit and force a spill on every
// later check. Without pressure the slabs stay for the next spill.
func TestFlushAggregatorReleasesRowsUnderHeapPressure(t *testing.T) {
	prev := debug.SetMemoryLimit(math.MaxInt64)
	t.Cleanup(func() { debug.SetMemoryLimit(prev) })

	for _, tc := range []struct {
		name      string
		pressure  bool
		wantSlabs bool
	}{
		{name: "no pressure", pressure: false, wantSlabs: true},
		{name: "pressure", pressure: true, wantSlabs: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := NewPipeline(DefaultConfig(), nil)
			p.tempDir = t.TempDir()
			agg := NewAggregator(0, 0)
			for i := range rowSlabLen + 10 {
				agg.AddObject(fmt.Sprintf("a%d/x", i), 7, tiers.Standard)
			}

			limit := int64(math.MaxInt64)
			if tc.pressure {
				limit = 1 << 20
			}
			debug.SetMemoryLimit(limit)
			err := p.flushAggregator(t.Context(), agg, 0)
			debug.SetMemoryLimit(math.MaxInt64)
			if err != nil {
				t.Fatalf("flushAggregator: %v", err)
			}

			if got := len(agg.slabs) > 0; got != tc.wantSlabs {
				t.Errorf("slabs kept = %v, want %v", got, tc.wantSlabs)
			}
			agg.AddObject("b/x", 5, tiers.Standard)
			rows := agg.Drain()
			if len(rows) != 2 {
				t.Fatalf("drain after flush = %d rows, want 2", len(rows))
			}
		})
	}
}

// The estimate is what the aggregator's heap grows by, for keys out of
// order with short and long segments, with the node table nearly full
// and just doubled: the nodes, their parents, keys and the node table,
// plus the prefixes Drain will write.
func TestAggregatorEstimateTracksHeap(t *testing.T) {
	for _, segLen := range []int{4, 40} {
		// 57,000 nodes fill 65,536 slots to 87%; 115,000 have just
		// doubled them to 262,144.
		for _, n := range []int{57_000, 115_000} {
			objs := make([]estimateObject, n)
			for i, j := range rand.New(rand.NewPCG(1, uint64(n))).Perm(n) {
				objs[i] = estimateObject{key: fmt.Sprintf("%0*d/x", segLen-1, j), size: 1, tier: tiers.Standard}
			}
			checkEstimateTracksHeap(t, fmt.Sprintf("%d-byte segments", segLen), objs)
		}
	}
}

// TestAggregatorBytesPerPrefix logs what a prefix costs the aggregator,
// in heap and in its estimate, for the shapes that decide how often a
// build spills, and checks the estimate tracks the heap for them: keys
// from the realistic benchmark generator, and many directories of 2 or
// 3 objects in different tiers each, the most tiers per prefix a
// common layout has.
func TestAggregatorBytesPerPrefix(t *testing.T) {
	const objects = 150_000
	realistic := make([]estimateObject, 0, objects)
	benchutil.NewGenerator(benchutil.S3RealisticConfig(objects)).Stream(func(o benchutil.FakeObject) {
		realistic = append(realistic, estimateObject{key: o.Key, size: o.Size, tier: o.TierID})
	})
	rng := rand.New(rand.NewPCG(3, 21))
	mixed := make([]estimateObject, 0, objects)
	for dir := 0; len(mixed) < objects; dir++ {
		for f := range 2 + dir%2 {
			mixed = append(mixed, estimateObject{
				key:  fmt.Sprintf("tenant-%03d/upload-%08d/part-%d.bin", dir%500, dir, f),
				size: uint64(rng.IntN(1 << 20)),
				tier: tiers.ID(rng.IntN(MaxTiers)),
			})
		}
	}
	rng.Shuffle(len(mixed), func(i, j int) { mixed[i], mixed[j] = mixed[j], mixed[i] })
	for _, c := range []struct {
		name string
		objs []estimateObject
	}{
		{"realistic", realistic},
		{"mixed tiers, 2-3 objects per directory", mixed},
	} {
		checkEstimateTracksHeap(t, c.name, c.objs)
	}
}

// estimateObject is an object for measuring the aggregator's heap.
type estimateObject struct {
	key  string
	size uint64
	tier tiers.ID
}

// checkEstimateTracksHeap adds objs to a new aggregator, logs per
// prefix how much the live heap grew, the estimate and the tier blocks'
// share of it, and fails unless the estimate is within 5% of the heap's
// growth.
func checkEstimateTracksHeap(t *testing.T, name string, objs []estimateObject) {
	t.Helper()
	// Twice: the first GC moves pooled objects earlier tests left to the
	// victim cache, the second frees them.
	runtime.GC()
	runtime.GC()
	before, _ := heapReadings()
	agg := NewAggregator(0, 0)
	for _, o := range objs {
		agg.AddObject(o.key, o.size, o.tier)
	}
	runtime.GC()
	after, _ := heapReadings()
	measured := int64(after) - int64(before)
	estimate := agg.EstimatedMemoryUsage()
	prefixes := agg.PrefixCount()
	per := func(b int64) float64 { return float64(b) / float64(prefixes) }
	t.Logf("%s: %d objects, %d prefixes: heap %.1f B/prefix, estimate %.1f, of which tier blocks %.1f (%.1f%% of nodes; %d slots)",
		name, len(objs), prefixes, per(measured), per(estimate), per(int64(agg.blocksUsed)*tierBlockBytes),
		100*float64(agg.blocksUsed)/float64(prefixes), len(agg.children.slots))
	runtime.KeepAlive(agg)
	runtime.KeepAlive(objs)
	if lo, hi := measured*95/100, measured*105/100; estimate < lo || estimate > hi {
		t.Errorf("%s: estimate %d without prefixes, heap grew %d (want within 5%%)", name, estimate, measured)
	}
}
