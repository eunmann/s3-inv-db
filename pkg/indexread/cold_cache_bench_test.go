package indexread_test

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/benchutil"
	"github.com/eunmann/s3-inv-db/pkg/extsort"
	"github.com/eunmann/s3-inv-db/pkg/indexread"
	"github.com/rs/zerolog"
	"golang.org/x/sys/unix"
)

// silenceZerolog disables the global logger for the duration of the
// bench so MPHF / pipeline debug lines don't shred bench output.
func silenceZerolog(b *testing.B) {
	b.Helper()
	prev := zerolog.GlobalLevel()
	zerolog.SetGlobalLevel(zerolog.Disabled)
	b.Cleanup(func() { zerolog.SetGlobalLevel(prev) })
}

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
	if os.Getenv("S3INV_LONG_BENCH") != "" {
		b.Run("n=10000000", func(b *testing.B) {
			runColdOpenBench(b, 10_000_000)
		})
	}
}

func runColdOpenBench(b *testing.B, n int) {
	b.Helper()
	silenceZerolog(b)
	dir := buildFixtureIndex(b, n)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		b.StopTimer()
		dropPageCache(b, dir)
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
	silenceZerolog(b)
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

// BenchmarkMPHFLookup_Latency isolates the per-lookup cost on a hot
// index. Sister to BenchmarkLookup but also reports a cold variant
// that drops the page cache before the loop.
func BenchmarkMPHFLookup_Latency(b *testing.B) {
	silenceZerolog(b)
	for _, n := range []int{100_000, 1_000_000} {
		b.Run(fmt.Sprintf("n=%d/warm", n), func(b *testing.B) {
			dir := buildFixtureIndex(b, n)
			idx, err := indexread.Open(dir)
			if err != nil {
				b.Fatalf("Open: %v", err)
			}
			defer idx.Close()
			prefixes := generateLookupPrefixes(n)
			b.ResetTimer()
			b.ReportAllocs()
			for i := range b.N {
				_, _ = idx.Lookup(prefixes[i%len(prefixes)])
			}
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

	builder, err := extsort.NewIndexBuilderWithCapacity(dir, "", uint64(len(rows)), false)
	if err != nil {
		b.Fatalf("NewIndexBuilder: %v", err)
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

// dropPageCache recursively evicts every regular file under dir from
// the page cache so the next access pays cold-page-fault costs. Walks
// subdirectories (notably tier_stats/) — a flat ReadDir would miss
// most of the index byte volume at 11-tier scale.
func dropPageCache(b *testing.B, dir string) {
	b.Helper()
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		dropFileFromCache(b, path)

		return nil
	})
	if err != nil {
		b.Fatalf("walk: %v", err)
	}
}

func dropFileFromCache(b *testing.B, path string) {
	b.Helper()
	f, err := os.Open(path)
	if err != nil {
		return
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil || st.Size() == 0 {
		return
	}
	// POSIX_FADV_DONTNEED hints the kernel to evict pages.
	_ = unix.Fadvise(int(f.Fd()), 0, st.Size(), unix.FADV_DONTNEED)
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
