package extsort

import (
	"fmt"
	"runtime"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

// heapInuseBytes must report what runtime.ReadMemStats calls HeapInuse.
func TestHeapInuseBytesMatchesMemStats(t *testing.T) {
	keep := make([][]byte, 0, 64)
	for range 64 {
		keep = append(keep, make([]byte, 1<<16))
	}
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	got := heapInuseBytes()
	runtime.KeepAlive(keep)

	lo, hi := m.HeapInuse*9/10, m.HeapInuse*11/10
	if got < lo || got > hi {
		t.Errorf("heapInuseBytes = %d, MemStats.HeapInuse = %d (want within 10%%)", got, m.HeapInuse)
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
