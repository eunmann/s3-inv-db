package extsort

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
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
// caller wants.
//
// The harness mirrors Pipeline's bounded-memory invariant: every
// harnessFlushCheckInterval objects, ShouldWorkerFlush decides whether
// the in-memory aggregator has crossed the per-worker cap. When it
// has, the aggregator drains to a compressed run file on disk and is
// reset. After the stream completes, all run files (plus any final
// partial drain) are k-way-merged into the IndexBuilder. This is the
// same sequence Pipeline.Run uses; the harness just runs it
// single-threaded so phase costs stay attributable.
//
// Net effect: the harness scales to the machine. A 10M deep-pyramid
// run spills ~30 run files at ~512 MiB each instead of holding the
// entire 15 GB+ aggregator in RAM.

func BenchmarkBuildHarness_Realistic_500K_DictOff(b *testing.B) {
	runShapeHarness(b, benchutil.S3RealisticConfig(500_000), false)
}

func BenchmarkBuildHarness_Realistic_500K_DictOn(b *testing.B) {
	runShapeHarness(b, benchutil.S3RealisticConfig(500_000), true)
}

func BenchmarkBuildHarness_Realistic_1M_DictOff(b *testing.B) {
	runShapeHarness(b, benchutil.S3RealisticConfig(1_000_000), false)
}

func BenchmarkBuildHarness_Realistic_1M_DictOn(b *testing.B) {
	runShapeHarness(b, benchutil.S3RealisticConfig(1_000_000), true)
}

func BenchmarkBuildHarness_Realistic_10M_DictOff(b *testing.B) {
	runShapeHarness(b, benchutil.S3RealisticConfig(10_000_000), false)
}

func BenchmarkBuildHarness_Realistic_10M_DictOn(b *testing.B) {
	runShapeHarness(b, benchutil.S3RealisticConfig(10_000_000), true)
}

func BenchmarkBuildHarness_DeepPyramid_500K_DictOff(b *testing.B) {
	runShapeHarness(b, benchutil.S3DeepPyramidConfig(500_000), false)
}

func BenchmarkBuildHarness_DeepPyramid_500K_DictOn(b *testing.B) {
	runShapeHarness(b, benchutil.S3DeepPyramidConfig(500_000), true)
}

func BenchmarkBuildHarness_DeepPyramid_1M_DictOff(b *testing.B) {
	runShapeHarness(b, benchutil.S3DeepPyramidConfig(1_000_000), false)
}

func BenchmarkBuildHarness_DeepPyramid_1M_DictOn(b *testing.B) {
	runShapeHarness(b, benchutil.S3DeepPyramidConfig(1_000_000), true)
}

func BenchmarkBuildHarness_DeepPyramid_10M_DictOff(b *testing.B) {
	runShapeHarness(b, benchutil.S3DeepPyramidConfig(10_000_000), false)
}

func BenchmarkBuildHarness_DeepPyramid_10M_DictOn(b *testing.B) {
	runShapeHarness(b, benchutil.S3DeepPyramidConfig(10_000_000), true)
}

const (
	// HarnessPrefixSampleCap caps the prefix sample retained for the
	// post-build Lookup measurement. 1024 is plenty for a meaningful
	// average; larger sets just inflate per-bench RSS.
	harnessPrefixSampleCap = 1024
	// HarnessAggregatorChunkRatio sizes the aggregator's initial map hint as
	// NumObjects / harnessAggregatorChunkRatio. Matches the n/8 the old harness used.
	harnessAggregatorChunkRatio = 8
	// HarnessFlushCheckInterval is the per-object stride between
	// ShouldWorkerFlush checks. ReadMemStats inside ShouldWorkerFlush
	// is not free, so checking every object would distort timing.
	// Pipeline checks per-chunk (typically tens of thousands of rows);
	// 10K mirrors that cadence.
	harnessFlushCheckInterval = 10_000
	// HarnessSingleWorker is the synthetic worker count used to compute
	// the per-worker aggregator cap. The harness is single-threaded by
	// design (phase isolation), so 1 here gives the same cap a real
	// 1-worker pipeline would see.
	harnessSingleWorker = 1
	// HarnessRunBufferSize matches Pipeline's run-file buffer default.
	harnessRunBufferSize = DefaultRunBufferSize
)

// runShapeHarness runs one (shape, dict) build under the same
// bounded-aggregator + spill + merge sequence Pipeline uses.
func runShapeHarness(b *testing.B, cfg benchutil.GeneratorConfig, prefixDict bool) {
	b.Helper()
	silenceZerologExtsort(b)

	b.ReportAllocs()
	b.ResetTimer()

	var (
		peakHeap     uint64
		lastSize     int64
		lastNs       float64
		prefixCount  uint64
		prefixSample []string
	)
	for range b.N {
		b.StopTimer()
		dir := b.TempDir()
		tempDir := b.TempDir()
		runtime.GC()
		var msStart runtime.MemStats
		runtime.ReadMemStats(&msStart)
		samplerStop := make(chan struct{})
		var samplerMax atomic.Uint64
		go heapSampler(samplerStop, &samplerMax)
		b.StartTimer()

		result, err := buildIndexBounded(cfg, dir, tempDir, prefixDict)
		if err != nil {
			b.StopTimer()
			close(samplerStop)
			b.Fatalf("buildIndexBounded: %v", err)
		}
		prefixCount = result.prefixCount
		prefixSample = result.prefixSample

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

// harnessResult bundles the post-build state the bench needs to
// compute metrics.
type harnessResult struct {
	prefixCount  uint64
	prefixSample []string
}

// buildIndexBounded runs the same flush-on-cap → spill → merge →
// IndexBuilder sequence Pipeline uses, but single-threaded so the
// harness can attribute timing to each phase. Every
// harnessFlushCheckInterval streamed objects, ShouldWorkerFlush
// decides whether the in-memory aggregator has crossed the per-worker
// memory cap; when it has, the aggregator drains to a compressed run
// file on disk and is reset. After the stream ends, run files are
// k-way-merged into the IndexBuilder.
func buildIndexBounded(cfg benchutil.GeneratorConfig, outDir, tempDir string, prefixDict bool) (*harnessResult, error) {
	agg := NewAggregator(cfg.NumObjects/harnessAggregatorChunkRatio, 0)
	sample := newPrefixSampler(harnessPrefixSampleCap)
	memLimit := debug.SetMemoryLimit(-1)

	var (
		runFiles []string
		streamed int
	)
	flushIfFull := func() error {
		if !ShouldWorkerFlush(uint64(agg.EstimatedMemoryUsage()), memLimit, harnessSingleWorker) {
			return nil
		}
		path, err := flushAggregatorToRun(agg, tempDir, len(runFiles))
		if err != nil {
			return err
		}
		runFiles = append(runFiles, path)

		return nil
	}

	gen := benchutil.NewGenerator(cfg)
	var streamErr error
	gen.Stream(func(o benchutil.FakeObject) {
		if streamErr != nil {
			return
		}
		agg.AddObject(o.Key, o.Size, o.TierID)
		sample.observe(o.Key)
		streamed++
		if streamed%harnessFlushCheckInterval == 0 {
			if err := flushIfFull(); err != nil {
				streamErr = err
			}
		}
	})
	if streamErr != nil {
		removeFiles(runFiles)

		return nil, streamErr
	}

	// Final drain — if there's anything left, either spill it (when
	// some workers already spilled, so we have multiple sorted runs to
	// merge) or feed it directly into the builder (single-batch case
	// skips the round-trip through a run file).
	finalRows := agg.Drain()
	SortPrefixRows(finalRows)

	prefixCount, err := buildFromRuns(outDir, tempDir, runFiles, finalRows, prefixDict)
	if err != nil {
		removeFiles(runFiles)

		return nil, err
	}
	removeFiles(runFiles)

	return &harnessResult{
		prefixCount:  prefixCount,
		prefixSample: sample.prefixes(),
	}, nil
}

// flushAggregatorToRun drains agg into a compressed run file under
// tempDir and resets the aggregator. Index is the run's sequence
// number; used only to name the file deterministically.
func flushAggregatorToRun(agg *Aggregator, tempDir string, runIdx int) (string, error) {
	rows := agg.Drain()
	SortPrefixRows(rows)
	path := filepath.Join(tempDir, fmt.Sprintf("harness_run_%04d.crun", runIdx))
	writer, err := NewCompressedRunWriter(path, CompressedRunWriterOptions{
		BufferSize:       harnessRunBufferSize,
		CompressionLevel: CompressionFastest,
	})
	if err != nil {
		return "", fmt.Errorf("create run file: %w", err)
	}
	if err := writeAndCloseRun(writer, rows, path); err != nil {
		_ = os.Remove(path)

		return "", err
	}

	return path, nil
}

// buildFromRuns is the merge-and-build phase. Three cases:
//
//  1. No spilled runs: finalRows is the whole dataset; feed it
//     directly into the builder.
//  2. Spilled runs but no final remainder: open a MergeIterator
//     across the run files, feed every yielded row to the builder.
//  3. Spilled runs *plus* a final batch: write the final batch as
//     one more run file and merge the combined set.
func buildFromRuns(outDir, tempDir string, runFiles []string, finalRows []*PrefixRow, prefixDict bool) (uint64, error) {
	builder, err := NewIndexBuilderWithCapacity(outDir, tempDir, 0)
	if err != nil {
		return 0, fmt.Errorf("new builder: %w", err)
	}
	if prefixDict {
		if err := builder.SetPrefixDictionary(true); err != nil {
			return 0, fmt.Errorf("set prefix dictionary: %w", err)
		}
	}

	switch {
	case len(runFiles) == 0:
		for _, row := range finalRows {
			if err := builder.Add(row); err != nil {
				return 0, fmt.Errorf("add row: %w", err)
			}
		}
	default:
		merged := runFiles
		if len(finalRows) > 0 {
			tail, ferr := writeFinalRun(tempDir, finalRows, len(runFiles))
			if ferr != nil {
				return 0, ferr
			}
			merged = append(merged, tail)
		}
		iter, ierr := NewMergeIterator(merged, harnessRunBufferSize)
		if ierr != nil {
			return 0, fmt.Errorf("open merge iterator: %w", ierr)
		}
		defer iter.Close()
		for {
			row, rerr := iter.Next()
			if errors.Is(rerr, io.EOF) {
				break
			}
			if rerr != nil {
				return 0, fmt.Errorf("merge next: %w", rerr)
			}
			if err := builder.Add(row); err != nil {
				return 0, fmt.Errorf("add merged row: %w", err)
			}
		}
	}
	if err := builder.Finalize(); err != nil {
		return 0, fmt.Errorf("finalize: %w", err)
	}

	return builder.Count(), nil
}

func writeFinalRun(tempDir string, rows []*PrefixRow, runIdx int) (string, error) {
	path := filepath.Join(tempDir, fmt.Sprintf("harness_run_%04d.crun", runIdx))
	writer, err := NewCompressedRunWriter(path, CompressedRunWriterOptions{
		BufferSize:       harnessRunBufferSize,
		CompressionLevel: CompressionFastest,
	})
	if err != nil {
		return "", fmt.Errorf("create final run file: %w", err)
	}
	if err := writeAndCloseRun(writer, rows, path); err != nil {
		_ = os.Remove(path)

		return "", err
	}

	return path, nil
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
