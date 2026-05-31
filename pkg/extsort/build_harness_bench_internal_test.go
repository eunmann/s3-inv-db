package extsort

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"testing"
	"time"

	"github.com/eunmann/s3-inv-db/internal/benchutil"
	"github.com/eunmann/s3-inv-db/pkg/indexread"
)

// BenchmarkBuildHarness sweeps shape × size × prefix-dictionary
// through the single-threaded build phase chain
// (flush-on-cap → spill → k-way-merge → finalize). For the full
// S3-to-index path, see internal/loader BenchmarkPipeline.
func BenchmarkBuildHarness(b *testing.B) {
	sizes := []int{500_000, 1_000_000}
	if benchutil.LongBenchEnabled() {
		sizes = append(sizes, 10_000_000)
	}
	shapes := []struct {
		name string
		cfg  func(int) benchutil.GeneratorConfig
	}{
		{"realistic", benchutil.S3RealisticConfig},
		{"deep_pyramid", benchutil.S3DeepPyramidConfig},
	}
	for _, shape := range shapes {
		for _, n := range sizes {
			for _, dict := range []bool{false, true} {
				name := fmt.Sprintf("shape=%s/n=%d/dict=%v", shape.name, n, dict)
				b.Run(name, func(b *testing.B) {
					runShapeHarness(b, shape.cfg(n), dict)
				})
			}
		}
	}
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
	benchutil.SilenceZerolog(b)

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
		sampler := benchutil.StartHeapPeakSampler()
		b.StartTimer()

		result, err := buildIndexBounded(cfg, dir, tempDir, prefixDict)
		if err != nil {
			b.StopTimer()
			sampler.Stop()
			b.Fatalf("buildIndexBounded: %v", err)
		}
		prefixCount = result.prefixCount
		prefixSample = result.prefixSample

		b.StopTimer()
		samplerMax := sampler.Stop()
		if delta := benchutil.SafeSubU64(samplerMax, msStart.HeapAlloc); delta > peakHeap {
			peakHeap = delta
		}
		lastSize = benchutil.DirBytes(b, dir)
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
