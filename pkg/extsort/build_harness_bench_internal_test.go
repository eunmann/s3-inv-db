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

// BuildHarness benchmarks the build phases single-threaded
// (flush-on-cap → spill → k-way-merge).

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
	harnessPrefixSampleCap      = 1024
	harnessAggregatorChunkRatio = 8
	// ShouldWorkerFlush reads MemStats; cadence it to avoid timing skew.
	harnessFlushCheckInterval = 10_000
	harnessSingleWorker       = 1
	harnessRunBufferSize      = DefaultRunBufferSize
)

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

type harnessResult struct {
	prefixCount  uint64
	prefixSample []string
}

// buildIndexBounded streams objects through one aggregator that
// spills on cap, then k-way-merges the spills into IndexBuilder.
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

// prefixSampler retains the first `limit` distinct directory prefixes
// seen during a streaming pass for use as a Lookup fixture.
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

// heapSampler tracks the max HeapAlloc seen until stop closes.
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
