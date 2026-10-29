package extsort

import (
	"fmt"
	"math"
	"runtime"
	"runtime/debug"
	"slices"
	"strings"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/tiers"
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
// parent's index and the segment), the prefix Drain writes and its map
// entry; a segment's nodes add map
// entries when it is mapped and give back the prefixes of those merged
// into the main tree; a Drain starts it over.
func TestAggregatorEstimateCountsEachPrefix(t *testing.T) {
	const node, entry, h = aggregatorNodeBytes, aggregatorMapEntryBytes, nodeKeyHeader
	agg := NewAggregator(0, 0)
	check := func(step string, want int64) {
		t.Helper()
		if got := agg.EstimatedMemoryUsage(); got != want {
			t.Fatalf("%s: estimate = %d, want %d", step, got, want)
		}
	}
	for _, k := range []string{"a/b/x", "a/b/y", "a/c/z", "top"} {
		agg.AddObject(k, 1, tiers.Standard)
	}
	// "" (no key, no map entry), then "a/", "a/b/", "a/c/": key + prefix.
	want := 4*node + 3*entry + (h + 2 + 2) + (h + 2 + 4) + (h + 2 + 4)
	check("map", want)

	agg.StartSegment()
	agg.AddObject("a/b/z", 1, tiers.Standard)
	// A root, "a/" and "a/b/" of the segment's own, in no map.
	want += 3*node + (h + 2 + 2) + (h + 2 + 4)
	check("ordered segment", want)

	agg.AddObject("a/a/x", 1, tiers.Standard)
	// Mapping merges the three into the main tree's, whose prefixes Drain
	// writes once; then "a/a/" is new.
	want += -(2 + 4) + node + entry + (h + 2 + 4)
	check("mapped segment", want)
	if got := agg.PrefixCount(); got != 5 {
		t.Errorf("PrefixCount = %d, want 5", got)
	}

	agg.Drain()
	check("after Drain", 0)
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

// A spill no larger than the last allocates only the block Drain writes
// its prefixes into, whether the keys came through the map or through a
// segment merged into it; an estimate short of the prefixes' length
// would grow that block and allocate again.
func TestAggregatorDrainReusesScratch(t *testing.T) {
	agg := NewAggregator(0, 0)
	keys := make([]string, 0, 600)
	for i := range 300 {
		keys = append(keys, fmt.Sprintf("p%03d/q%d/x", (i*7)%300, i%5), fmt.Sprintf("r/%03d/y", 299-i))
	}
	cycle := func() []*PrefixRow {
		agg.AddObject("p000/q0/x", 1, tiers.Standard)
		agg.StartSegment()
		agg.AddObject("p000/q0/a", 1, tiers.Standard)
		agg.AddObject("p001/q1/a", 1, tiers.Standard)
		for _, k := range keys {
			agg.AddObject(k, 1, tiers.Standard)
		}

		return agg.Drain()
	}
	rows := cycle()
	if !slices.IsSortedFunc(rows, func(a, b *PrefixRow) int { return strings.Compare(a.Prefix, b.Prefix) }) {
		t.Fatal("Drain rows are not in prefix order")
	}
	if allocs := testing.AllocsPerRun(10, func() { cycle() }); allocs != 1 {
		t.Errorf("a spill no larger than the last = %v allocs, want 1", allocs)
	}
}
