package extsort

import (
	"fmt"
	"math"
	"runtime"
	"runtime/debug"
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
		if r.TierCounts[tiers.GlacierFR] != 0 || r.TierBytes[tiers.GlacierFR] != 0 {
			t.Errorf("row %q kept tier stats from the previous spill", r.Prefix)
		}
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
