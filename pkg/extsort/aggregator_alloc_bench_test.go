package extsort

import (
	"fmt"
	"runtime"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

// diverseKeys returns n realistic S3-style keys with shared parents at low
// depths and unique leaves. Each key is freshly allocated (mirrors CSV parse).
func diverseKeys(n int) []string {
	keys := make([]string, n)
	for i := range keys {
		keys[i] = fmt.Sprintf("tenant-%05d/year=2024/month=%02d/day=%02d/object-%08d.parquet",
			i%1000, (i%12)+1, (i%28)+1, i)
	}
	return keys
}

// BenchmarkAddObjectDiverse measures per-object cost with realistic distinct
// keys. Hot path (after warm-up) should be near-zero alloc because all
// prefixes are cached in the aggregator map.
func BenchmarkAddObjectDiverse(b *testing.B) {
	keys := diverseKeys(10000)
	agg := NewAggregator(len(keys), 0)
	// Warm-up: insert every prefix once so steady-state measures only updates.
	for _, k := range keys {
		agg.AddObject(k, 1024, tiers.Standard)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		agg.AddObject(keys[i%len(keys)], 1024, tiers.Standard)
	}
}

// BenchmarkAddObjectCold measures the cold path: every Add inserts a new
// prefix into the map. This is where allocation and substring-retention
// behavior matter.
func BenchmarkAddObjectCold(b *testing.B) {
	// Pre-generate enough keys for the whole run.
	keys := diverseKeys(b.N)
	agg := NewAggregator(b.N, 0)
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		agg.AddObject(keys[i], 1024, tiers.Standard)
	}
}

// BenchmarkAggregatorRetention measures heap retention after ingesting N
// unique keys with distinct source buffers. Reports HeapAlloc-per-prefix
// so substring-retention shows up as wasted bytes.
func BenchmarkAggregatorRetention(b *testing.B) {
	const N = 200_000
	keys := diverseKeys(N)
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	agg := NewAggregator(N, 0)
	for _, k := range keys {
		agg.AddObject(k, 1024, tiers.Standard)
	}

	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)

	heapDelta := after.HeapAlloc - before.HeapAlloc
	prefixes := agg.PrefixCount()
	b.ReportMetric(float64(heapDelta), "heap_bytes")
	b.ReportMetric(float64(heapDelta)/float64(prefixes), "bytes/prefix")
	b.ReportMetric(float64(prefixes), "prefixes")
	// Keep agg alive so the GC doesn't collect mid-measurement.
	runtime.KeepAlive(agg)
	runtime.KeepAlive(keys)
}

// BenchmarkAggregatorDrain measures Drain throughput on a populated
// aggregator. Includes the implicit sort by SortPrefixRows callers
// typically run afterward.
func BenchmarkAggregatorDrain(b *testing.B) {
	const N = 100_000
	keys := diverseKeys(N)

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		b.StopTimer()
		agg := NewAggregator(N, 0)
		for _, k := range keys {
			agg.AddObject(k, 1024, tiers.Standard)
		}
		b.StartTimer()
		rows := agg.Drain()
		SortPrefixRows(rows)
		runtime.KeepAlive(rows)
	}
}
