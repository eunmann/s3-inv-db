package extsort

import (
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eunmann/s3-inv-db/internal/benchutil"
	"github.com/eunmann/s3-inv-db/pkg/indexread"
	"github.com/rs/zerolog"
)

// BuildHarness — explicit per-(shape × prefix-dictionary × size)
// benchmarks. Each function names the exact knob combination it
// measures, so a `go test -bench` selector picks the comparison the
// caller wants without env-var gymnastics. 500K is the standard size;
// flip to 1M (the *_1M variants) when the smaller run is dominated by
// fixture-setup noise.
//
// The harness streams generated objects directly into the aggregator,
// so peak resident memory is bounded by the aggregator + the run-file
// scratch — *not* by NumObjects × sizeof(FakeObject). At 10M objects
// the slice-materialising path needed >600 MB just for the inputs;
// the streaming path adds zero.

func BenchmarkBuildHarness_Realistic_500K_DictOff(b *testing.B) {
	runShapeHarness(b, benchutil.S3RealisticConfig(500_000), false)
}

func BenchmarkBuildHarness_Realistic_500K_DictOn(b *testing.B) {
	runShapeHarness(b, benchutil.S3RealisticConfig(500_000), true)
}

func BenchmarkBuildHarness_DeepPyramid_500K_DictOff(b *testing.B) {
	runShapeHarness(b, benchutil.S3DeepPyramidConfig(500_000), false)
}

func BenchmarkBuildHarness_DeepPyramid_500K_DictOn(b *testing.B) {
	runShapeHarness(b, benchutil.S3DeepPyramidConfig(500_000), true)
}

const (
	// HarnessPrefixSampleCap caps the prefix sample retained for the
	// post-build Lookup measurement. 1024 is plenty for a meaningful
	// average; larger sets just inflate per-bench RSS.
	harnessPrefixSampleCap = 1024
	// AggregatorChunkRatio sizes the per-worker aggregator hint as
	// NumObjects / aggregatorChunkRatio. Matches the n/8 the old harness used.
	aggregatorChunkRatio = 8
)

// runShapeHarness runs the build harness for one (shape, dict)
// combination. Streams object generation through the aggregator so
// peak resident memory does not scale with NumObjects.
func runShapeHarness(b *testing.B, cfg benchutil.GeneratorConfig, prefixDict bool) {
	b.Helper()
	silenceZerologExtsort(b)

	b.ReportAllocs()
	b.ResetTimer()

	var (
		peakHeap     uint64
		lastSize     int64
		lastNs       float64
		prefixCount  int
		prefixSample []string
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

		// Stream objects through aggregation; capture a bounded prefix
		// sample on the way through for the post-build Lookup measurement
		// so we never need to retain the full object slice.
		agg := NewAggregator(cfg.NumObjects/aggregatorChunkRatio, 0)
		sample := newPrefixSampler(harnessPrefixSampleCap)
		gen := benchutil.NewGenerator(cfg)
		gen.Stream(func(o benchutil.FakeObject) {
			agg.AddObject(o.Key, o.Size, o.TierID)
			sample.observe(o.Key)
		})
		rows := agg.Drain()
		SortPrefixRows(rows)
		prefixCount = agg.PrefixCount()
		prefixSample = sample.prefixes()

		builder, err := NewIndexBuilderWithCapacity(dir, "", uint64(len(rows)))
		if err != nil {
			b.Fatalf("NewIndexBuilderWithCapacity: %v", err)
		}
		if prefixDict {
			if err := builder.SetPrefixDictionary(true); err != nil {
				b.Fatalf("SetPrefixDictionary: %v", err)
			}
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
		lastNs = measureLookupHarnessSampled(b, dir, prefixSample)
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

// prefixSampler keeps a bounded set of distinct directory-prefixes
// observed in a streaming object pass. First-seen wins so the sample
// is deterministic for a given input ordering.
type prefixSampler struct {
	seen  map[string]struct{}
	out   []string
	limit int
}

func newPrefixSampler(limit int) *prefixSampler {
	return &prefixSampler{
		seen:  make(map[string]struct{}, limit),
		out:   make([]string, 0, limit),
		limit: limit,
	}
}

func (s *prefixSampler) observe(key string) {
	if len(s.out) >= s.limit {
		return
	}
	idx := -1
	for i := len(key) - 1; i >= 0; i-- {
		if key[i] == '/' {
			idx = i

			break
		}
	}
	if idx < 0 {
		return
	}
	p := key[:idx+1]
	if _, ok := s.seen[p]; ok {
		return
	}
	s.seen[p] = struct{}{}
	s.out = append(s.out, p)
}

func (s *prefixSampler) prefixes() []string { return s.out }

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

func measureLookupHarnessSampled(b *testing.B, dir string, prefixes []string) float64 {
	b.Helper()
	if len(prefixes) == 0 {
		return 0
	}
	idx, err := indexread.Open(dir)
	if err != nil {
		b.Fatalf("indexread.Open: %v", err)
	}
	defer idx.Close()

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
