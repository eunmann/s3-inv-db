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

// EstimatedMemoryUsage counts each new prefix once, with its length,
// and starts over at a Drain.
func TestAggregatorEstimateCountsEachPrefix(t *testing.T) {
	agg := NewAggregator(0, 0)
	for _, k := range []string{"a/b/x", "a/b/y", "a/c/z", "top"} {
		agg.AddObject(k, 1, tiers.Standard)
	}
	// "", "a/", "a/b/", "a/c/"
	want := 4*aggregatorEntryBytes + int64(len("a/")+len("a/b/")+len("a/c/"))
	if got := agg.EstimatedMemoryUsage(); got != want {
		t.Fatalf("estimate = %d, want %d", got, want)
	}
	agg.Drain()
	if got := agg.EstimatedMemoryUsage(); got != 0 {
		t.Fatalf("estimate after Drain = %d, want 0", got)
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
		if r.TierCount(tiers.GlacierFR) != 0 || r.TierBytes(tiers.GlacierFR) != 0 {
			t.Errorf("row %q kept tier stats from the previous spill", r.Prefix)
		}
	}
}

// Drain returns the rows in the order their prefixes first appeared,
// across slabs, so keys in order hand DrainSorted rows that are
// already sorted.
func TestAggregatorDrainInCreationOrder(t *testing.T) {
	agg := NewAggregator(0, 0)
	agg.AddObject("b/x", 1, tiers.Standard)
	agg.AddObject("a/y/z", 1, tiers.Standard)
	agg.AddObject("b/w/v", 1, tiers.Standard)
	got := make([]string, 0, 5)
	for _, r := range agg.Drain() {
		got = append(got, r.Prefix)
	}
	if want := []string{"", "b/", "a/", "a/y/", "b/w/"}; !slices.Equal(got, want) {
		t.Errorf("Drain order = %q, want %q", got, want)
	}

	keys := make([]string, 0, rowSlabLen+10)
	for i := range rowSlabLen + 10 {
		keys = append(keys, fmt.Sprintf("p%05d/x", i))
	}
	for _, k := range keys {
		agg.AddObject(k, 1, tiers.Standard)
	}
	rows := agg.Drain()
	if !slices.IsSortedFunc(rows, func(a, b *PrefixRow) int { return strings.Compare(a.Prefix, b.Prefix) }) {
		t.Error("rows of keys in order are not drained in order")
	}
	if len(rows) != len(keys)+1 {
		t.Errorf("drained %d rows, want %d", len(rows), len(keys)+1)
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

// DrainSorted hands the rows out in prefix order, and a later spill no
// larger than the biggest so far sorts without allocating.
func TestAggregatorDrainSortedReusesSortBuffer(t *testing.T) {
	agg := NewAggregator(0, 0)
	for i := range 500 {
		agg.AddObject(fmt.Sprintf("p%03d/x", 499-i), 1, tiers.Standard)
	}
	rows := agg.DrainSorted()
	if len(rows) != 501 {
		t.Fatalf("drain = %d rows, want 501", len(rows))
	}
	if !slices.IsSortedFunc(rows, func(a, b *PrefixRow) int { return strings.Compare(a.Prefix, b.Prefix) }) {
		t.Fatal("DrainSorted rows are not in prefix order")
	}
	for _, k := range agg.sortScratch[:cap(agg.sortScratch)] {
		if k.row != nil || k.prefix != "" {
			t.Fatal("sort buffer keeps references to drained rows")
		}
	}

	if allocs := testing.AllocsPerRun(10, func() {
		agg.sortScratch = sortPrefixRows(rows, agg.sortScratch)
	}); allocs != 0 {
		t.Errorf("sorting a spill no larger than the last = %v allocs, want 0", allocs)
	}
}
