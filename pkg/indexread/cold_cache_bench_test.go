package indexread_test

import (
	"fmt"
	"runtime"
	"testing"

	"github.com/eunmann/s3-inv-db/internal/benchutil"
	"github.com/eunmann/s3-inv-db/pkg/extsort"
	"github.com/eunmann/s3-inv-db/pkg/indexread"
)

// BenchmarkIndexOpen_ColdCache measures Open() latency with the OS
// page cache deliberately evicted between iterations. This is the
// realistic case for a fresh process opening a large index that
// hasn't been touched recently. Without eviction, Open() benchmarks
// only the second-call hot path and hides the meaningful cold cost.
//
// Sizes span the regimes where mmap behaviour changes: 100K (small
// enough to fit in L3), 1M (DRAM-resident), 10M (gated behind
// S3INV_LONG_BENCH; will populate ~hundreds of MB of pages).
func BenchmarkIndexOpen_ColdCache(b *testing.B) {
	for _, n := range []int{100_000, 1_000_000} {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			runColdOpenBench(b, n)
		})
	}
	if benchutil.LongBenchEnabled() {
		b.Run("n=10000000", func(b *testing.B) {
			runColdOpenBench(b, 10_000_000)
		})
	}
}

func runColdOpenBench(b *testing.B, n int) {
	b.Helper()
	benchutil.SilenceZerolog(b)
	dir := buildFixtureIndex(b, n)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		b.StopTimer()
		benchutil.DropPageCache(b, dir)
		b.StartTimer()

		idx, err := indexread.Open(dir)
		if err != nil {
			b.Fatalf("Open: %v", err)
		}
		// Touch the count field to force the header-parse cost to bind.
		_ = idx.Count()
		idx.Close()
	}
}

// BenchmarkConcurrentLookup_Scaling measures throughput when N
// goroutines share one open index, varying N. Surfaces serialisation
// in the lookup path (e.g. the per-inventory lock in
// manager.WithIndex closures, if callers use that wrapper). Sister to
// BenchmarkConcurrentLookup but parameterised by worker count.
func BenchmarkConcurrentLookup_Scaling(b *testing.B) {
	benchutil.SilenceZerolog(b)
	dir := buildFixtureIndex(b, 100_000)
	idx, err := indexread.Open(dir)
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	defer idx.Close()

	prefixes := generateLookupPrefixes(100_000)
	for _, conc := range []int{1, 4, 16, 64} {
		if conc > runtime.NumCPU() {
			continue
		}
		b.Run(fmt.Sprintf("workers=%d", conc), func(b *testing.B) {
			b.ReportAllocs()
			b.SetParallelism(conc)
			b.RunParallel(func(pb *testing.PB) {
				i := 0
				for pb.Next() {
					_, _ = idx.Lookup(prefixes[i%len(prefixes)])
					i++
				}
			})
		})
	}
}

// buildFixtureIndex constructs a real index of n synthetic objects
// once per (b, n) pair via b.Cleanup. Multiple bench iterations share
// the same dir.
func buildFixtureIndex(b *testing.B, n int) string {
	b.Helper()
	dir := b.TempDir()

	gen := benchutil.NewGenerator(benchutil.S3RealisticConfig(n))
	objects := gen.Generate()

	agg := extsort.NewAggregator(n, 0)
	for _, obj := range objects {
		agg.AddObject(obj.Key, obj.Size, obj.TierID)
	}
	rows := agg.Drain()
	extsort.SortPrefixRows(rows)

	builder, err := extsort.NewIndexBuilderWithCapacity(dir, "", uint64(len(rows)))
	if err != nil {
		b.Fatalf("NewIndexBuilder: %v", err)
	}
	if err := builder.SetPresentTiers(agg.PresentTiers()); err != nil {
		b.Fatalf("SetPresentTiers: %v", err)
	}
	for _, row := range rows {
		if err := builder.Add(row); err != nil {
			b.Fatalf("Add: %v", err)
		}
	}
	if err := builder.Finalize(); err != nil {
		b.Fatalf("Finalize: %v", err)
	}

	return dir
}

// reopenCold closes idx, evicts dir from the page cache and opens the
// index again. The drop has to follow Close: Linux keeps a page cached
// while any mapping holds it, so dropping under an open index leaves
// every page it already touched warm. Call it with the timer stopped.
func reopenCold(b *testing.B, idx *indexread.Index, dir string) *indexread.Index {
	b.Helper()
	if err := idx.Close(); err != nil {
		b.Fatalf("Close: %v", err)
	}
	benchutil.DropPageCache(b, dir)
	idx, err := indexread.Open(dir)
	if err != nil {
		b.Fatalf("Open: %v", err)
	}

	return idx
}

// generateLookupPrefixes produces a representative set of prefixes a
// browse workload would query — root, top-level dirs, deep paths.
func generateLookupPrefixes(n int) []string {
	gen := benchutil.NewGenerator(benchutil.S3RealisticConfig(n))
	objects := gen.Generate()
	out := make([]string, 0, len(objects))
	seen := map[string]struct{}{}
	for _, o := range objects {
		// Trim to the parent prefix (everything up to the last "/")
		key := o.Key
		i := lastSlash(key)
		if i < 0 {
			continue
		}
		p := key[:i+1]
		if _, ok := seen[p]; ok {
			continue
		}
		seen[p] = struct{}{}
		out = append(out, p)
	}
	if len(out) == 0 {
		out = []string{""}
	}

	return out
}

func lastSlash(s string) int {
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] == '/' {
			return i
		}
	}

	return -1
}
