package extsort

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eunmann/s3-inv-db/pkg/benchutil"
	"github.com/eunmann/s3-inv-db/pkg/indexread"
	"github.com/rs/zerolog"
)

// BenchmarkBuildHarness runs a full local index build and reports four
// numbers per iteration: ingestion time, peak heap delta during build,
// total bytes-on-disk, and warm post-build Lookup latency. One bench,
// four signals — so each enhancement's effect on the headline metrics
// shows up in a single comparable row.
//
// Sizes default to 500K and 1M (per the user's "long enough to
// simulate reality" guidance); override with S3INV_HARNESS_SIZES
// (comma-separated decimal). 10M auto-enabled when S3INV_LONG_BENCH is
// set.
func BenchmarkBuildHarness(b *testing.B) {
	silenceZerologExtsort(b)
	for _, n := range harnessSizes() {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			runBuildHarness(b, n)
		})
	}
}

func runBuildHarness(b *testing.B, n int) {
	b.Helper()
	gen := benchutil.NewGenerator(benchutil.S3RealisticConfig(n))
	objects := gen.Generate()
	prefixCount := approxPrefixCount(objects)

	b.ReportAllocs()
	b.ResetTimer()

	var (
		peakHeap uint64
		lastSize int64
		lastNs   float64
	)
	for range b.N {
		b.StopTimer()
		dir := b.TempDir()
		runtime.GC()
		var msStart runtime.MemStats
		runtime.ReadMemStats(&msStart)
		samplerStop := make(chan struct{})
		var samplerMax atomic.Uint64
		go heapSampler(samplerStop, &samplerMax)
		b.StartTimer()

		agg := NewAggregator(n/8, 0)
		for _, o := range objects {
			agg.AddObject(o.Key, o.Size, o.TierID)
		}
		rows := agg.Drain()
		SortPrefixRows(rows)

		builder, err := NewIndexBuilderWithCapacity(dir, "", uint64(len(rows)), false)
		if err != nil {
			b.Fatalf("NewIndexBuilderWithCapacity: %v", err)
		}
		for _, row := range rows {
			if err := builder.Add(row); err != nil {
				b.Fatalf("Add: %v", err)
			}
		}
		if err := builder.Finalize(); err != nil {
			b.Fatalf("Finalize: %v", err)
		}

		b.StopTimer()
		close(samplerStop)
		// Give the sampler a tick to drain.
		time.Sleep(2 * time.Millisecond)
		if delta := safeSubHarness(samplerMax.Load(), msStart.HeapAlloc); delta > peakHeap {
			peakHeap = delta
		}
		lastSize = dirBytesHarness(b, dir)
		lastNs = measureLookupHarness(b, dir, objects)
		b.StartTimer()
	}

	b.ReportMetric(float64(lastSize), "disk_B")
	b.ReportMetric(float64(peakHeap), "peak_heap_B")
	b.ReportMetric(lastNs, "lookup_ns")
	if prefixCount > 0 {
		b.ReportMetric(float64(lastSize)/float64(prefixCount), "disk_B/prefix")
		b.ReportMetric(float64(peakHeap)/float64(prefixCount), "peak_heap_B/prefix")
	}
}

// approxPrefixCount runs a throwaway aggregator to count distinct
// prefixes in the synthetic input — used for per-prefix metric
// denominators.
func approxPrefixCount(objects []benchutil.FakeObject) int {
	agg := NewAggregator(len(objects)/8, 0)
	for _, o := range objects {
		agg.AddObject(o.Key, o.Size, o.TierID)
	}

	return agg.PrefixCount()
}

// heapSampler polls runtime memory stats every 5 ms and updates the
// highest HeapAlloc seen into peak. Cheap enough that it doesn't
// move the timing needle on its own. Closing `stop` ends the
// goroutine.
func heapSampler(stop <-chan struct{}, peak *atomic.Uint64) {
	const interval = 5 * time.Millisecond
	t := time.NewTicker(interval)
	defer t.Stop()
	var ms runtime.MemStats
	for {
		select {
		case <-stop:
			return
		case <-t.C:
			runtime.ReadMemStats(&ms)
			cur := ms.HeapAlloc
			for {
				old := peak.Load()
				if cur <= old || peak.CompareAndSwap(old, cur) {
					break
				}
			}
		}
	}
}

func safeSubHarness(a, b uint64) uint64 {
	if a < b {
		return 0
	}

	return a - b
}

func measureLookupHarness(b *testing.B, dir string, objects []benchutil.FakeObject) float64 {
	b.Helper()
	idx, err := indexread.Open(dir)
	if err != nil {
		b.Fatalf("indexread.Open: %v", err)
	}
	defer idx.Close()

	prefixes := harnessPrefixes(objects)
	if len(prefixes) == 0 {
		return 0
	}
	const (
		warmup = 1000
		iters  = 10_000
	)
	for i := range warmup {
		_, _ = idx.Lookup(prefixes[i%len(prefixes)])
	}
	start := time.Now()
	for i := range iters {
		_, _ = idx.Lookup(prefixes[i%len(prefixes)])
	}

	return float64(time.Since(start).Nanoseconds()) / float64(iters)
}

func harnessPrefixes(objects []benchutil.FakeObject) []string {
	seen := make(map[string]struct{}, 1024)
	out := make([]string, 0, 1024)
	for _, o := range objects {
		idx := -1
		for i := len(o.Key) - 1; i >= 0; i-- {
			if o.Key[i] == '/' {
				idx = i

				break
			}
		}
		if idx < 0 {
			continue
		}
		p := o.Key[:idx+1]
		if _, ok := seen[p]; ok {
			continue
		}
		seen[p] = struct{}{}
		out = append(out, p)
		if len(out) >= 1024 {
			break
		}
	}

	return out
}

func dirBytesHarness(b *testing.B, dir string) int64 {
	b.Helper()
	var total int64
	err := filepath.Walk(dir, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			total += info.Size()
		}

		return nil
	})
	if err != nil {
		b.Fatalf("walk: %v", err)
	}

	return total
}

func silenceZerologExtsort(b *testing.B) {
	b.Helper()
	prev := zerolog.GlobalLevel()
	zerolog.SetGlobalLevel(zerolog.Disabled)
	b.Cleanup(func() { zerolog.SetGlobalLevel(prev) })
}

func harnessSizes() []int {
	if env := os.Getenv("S3INV_HARNESS_SIZES"); env != "" {
		return parseHarnessSizes(env)
	}
	if os.Getenv("S3INV_LONG_BENCH") != "" {
		return []int{500_000, 1_000_000, 10_000_000}
	}

	return []int{500_000, 1_000_000}
}

func parseHarnessSizes(env string) []int {
	out := make([]int, 0, 4)
	start := 0
	for i := 0; i <= len(env); i++ {
		if i == len(env) || env[i] == ',' {
			if i > start {
				if n, err := strconv.Atoi(env[start:i]); err == nil && n > 0 {
					out = append(out, n)
				}
			}
			start = i + 1
		}
	}

	return out
}
