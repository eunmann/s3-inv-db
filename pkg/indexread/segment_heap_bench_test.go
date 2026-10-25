package indexread_test

import (
	"fmt"
	"path/filepath"
	"runtime"
	"runtime/metrics"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/extsort"
	"github.com/eunmann/s3-inv-db/pkg/indexread"
	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

// manySegments is the distinct-segment count of the fixture below: one
// per object, as with per-ID folders (uploads/<uuid>/...).
const manySegments = 500_000

// BenchmarkOpenManySegments opens an index whose prefixes are almost
// all distinct segments and reports what an open index holds on the Go
// heap after a GC (retained_heap_B) and how much of the heap the
// collector must scan for pointers (gc_scan_heap_B). Both are what
// every loaded index costs the server's memory limit and every GC cycle.
func BenchmarkOpenManySegments(b *testing.B) {
	dir := filepath.Join(b.TempDir(), "index")
	agg := extsort.NewAggregator(manySegments*3, 0)
	for i := range manySegments {
		agg.AddObject(fmt.Sprintf("uploads/%016x-%08x/data.bin", uint64(i)*0x9e3779b97f4a7c15, i), 1, tiers.Standard)
	}
	rows := agg.Drain()
	extsort.SortPrefixRows(rows)
	builder, err := extsort.NewIndexBuilder(dir, b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	if err := builder.SetPresentTiers(agg.PresentTiers()); err != nil {
		b.Fatal(err)
	}
	for _, r := range rows {
		if err := builder.Add(r); err != nil {
			b.Fatal(err)
		}
	}
	if err := builder.Finalize(); err != nil {
		b.Fatal(err)
	}
	rows, agg = nil, nil //nolint:ineffassign,wastedassign // let the fixture's heap go before measuring

	var retained, scan uint64
	for b.Loop() {
		b.StopTimer()
		runtime.GC()
		before := heapLive()
		b.StartTimer()
		idx, err := indexread.Open(dir)
		if err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
		runtime.GC()
		retained = heapLive() - before
		scan = gcScanHeap()
		if err := idx.Close(); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
	}
	b.ReportMetric(float64(retained), "retained_heap_B")
	b.ReportMetric(float64(scan), "gc_scan_heap_B")
}

func heapLive() uint64 {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	return ms.HeapAlloc
}

func gcScanHeap() uint64 {
	s := []metrics.Sample{{Name: "/gc/scan/heap:bytes"}}
	metrics.Read(s)

	return s[0].Value.Uint64()
}
