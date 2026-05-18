package loader_test

import (
	"path/filepath"
	"runtime"
	"runtime/debug"
	"testing"
	"time"

	"github.com/eunmann/s3-inv-db/internal/seeder"
	"github.com/eunmann/s3-inv-db/internal/testsupport/miniotest"
	"github.com/eunmann/s3-inv-db/pkg/extsort"
	"github.com/rs/zerolog"
)

// Pipeline integration benchmarks — full S3-to-index path against
// MinIO. Each bench seeds a fresh inventory into a uniquely-named
// bucket, then runs extsort.Pipeline end-to-end (download → parse
// → aggregate → spill → merge → build) and reports build time and
// total on-disk index size.
//
// Naming: `BenchmarkPipeline_<Shape>_<Size>_<DictKnob>`. Three
// dimensions surface here that microbenches can't see — chunk-level
// download parallelism, real CSV parsing cost, and the spill phase
// running on the production code path through chunkWorker.
//
// Requires the docker compose `test` profile (AWS_ENDPOINT_URL_S3
// must point at a reachable MinIO). Run via:
//
//	go test -bench=BenchmarkPipeline -benchtime=1x -count=1 \
//	  -run=^$ ./internal/loader/...
//
// Or via the make target that brings MinIO up around it:
//
//	make docker-bench-pipeline   (not yet defined; see bench README)

const (
	pipelineBenchChunkCount = 8
)

func BenchmarkPipeline_Realistic_500K_DictOff(b *testing.B) {
	runPipelineBench(b, "realistic", 500_000, false, 0)
}

func BenchmarkPipeline_Realistic_500K_DictOn(b *testing.B) {
	runPipelineBench(b, "realistic", 500_000, true, 0)
}

func BenchmarkPipeline_Realistic_1M_DictOff(b *testing.B) {
	runPipelineBench(b, "realistic", 1_000_000, false, 0)
}

func BenchmarkPipeline_Realistic_1M_DictOn(b *testing.B) {
	runPipelineBench(b, "realistic", 1_000_000, true, 0)
}

func BenchmarkPipeline_DeepPyramid_500K_DictOff(b *testing.B) {
	runPipelineBench(b, "deep_pyramid", 500_000, false, 0)
}

func BenchmarkPipeline_DeepPyramid_500K_DictOn(b *testing.B) {
	runPipelineBench(b, "deep_pyramid", 500_000, true, 0)
}

func BenchmarkPipeline_DeepPyramid_1M_DictOff(b *testing.B) {
	runPipelineBench(b, "deep_pyramid", 1_000_000, false, 0)
}

func BenchmarkPipeline_DeepPyramid_1M_DictOn(b *testing.B) {
	runPipelineBench(b, "deep_pyramid", 1_000_000, true, 0)
}

// BenchmarkPipeline_AutoScale_DeepPyramid_1M_Mem* sweeps the
// GOMEMLIMIT budget against the same fixture to show that
// AggregatorCap scales with the configured limit — fewer spills and
// shorter merge phase as the budget grows. All three runs share
// shape, size, and dict setting; only the memory cap varies.
func BenchmarkPipeline_AutoScale_DeepPyramid_1M_Mem2G(b *testing.B) {
	runPipelineBench(b, "deep_pyramid", 1_000_000, true, 2<<30)
}

func BenchmarkPipeline_AutoScale_DeepPyramid_1M_Mem8G(b *testing.B) {
	runPipelineBench(b, "deep_pyramid", 1_000_000, true, 8<<30)
}

func BenchmarkPipeline_AutoScale_DeepPyramid_1M_Mem16G(b *testing.B) {
	runPipelineBench(b, "deep_pyramid", 1_000_000, true, 16<<30)
}

// runPipelineBench is the shared body. MemLimit==0 leaves GOMEMLIMIT
// unchanged (uses whatever the test runtime has, normally unset →
// DefaultAggregatorCap kicks in).
func runPipelineBench(b *testing.B, preset string, numObjects int, prefixDict bool, memLimit int64) {
	b.Helper()
	silenceZerologPipelineBench(b)

	fc := miniotest.FetchClient(b)
	bucket := miniotest.Bucket(b, fc.Raw())

	srcBucket := "bench-src"
	prefix := "bench-inv/"
	stamp := time.Now().UTC().Truncate(time.Minute)

	info, err := seeder.UploadMultiChunkInventory(
		b.Context(), fc.Raw(),
		seeder.Config{
			Target:  seeder.TargetS3,
			Objects: numObjects,
			Preset:  preset,
			Seed:    int64(numObjects ^ pipelineBenchChunkCount),
			Logger:  zerolog.Nop(),
		},
		seeder.S3Config{Bucket: bucket, Prefix: prefix, SrcBucket: srcBucket},
		1, int64(numObjects), stamp, pipelineBenchChunkCount,
	)
	if err != nil {
		b.Fatalf("UploadMultiChunkInventory: %v", err)
	}

	var lastDiskBytes int64
	var lastPeakHeap uint64

	b.ResetTimer()
	for range b.N {
		b.StopTimer()
		outDir := filepath.Join(b.TempDir(), "index")

		// Save + restore GOMEMLIMIT so the bench sweep doesn't leak
		// configured limits into sibling benchmarks.
		var prevLimit int64
		if memLimit > 0 {
			prevLimit = debug.SetMemoryLimit(memLimit)
		}

		runtime.GC()
		var msStart runtime.MemStats
		runtime.ReadMemStats(&msStart)
		peak := atomicHeapSampler(b)

		cfg := extsort.DefaultConfig()
		cfg.PrefixDictionary = prefixDict
		cfg.Observe.OnProgress = func(string, int64, int64) {}
		pipeline := extsort.NewPipeline(cfg, fc)

		b.StartTimer()
		if _, err := pipeline.Run(b.Context(), info.Path, outDir); err != nil {
			b.StopTimer()
			peak.stop()
			if memLimit > 0 {
				debug.SetMemoryLimit(prevLimit)
			}
			b.Fatalf("pipeline.Run: %v", err)
		}
		b.StopTimer()

		samplerMax := peak.stop()
		if memLimit > 0 {
			debug.SetMemoryLimit(prevLimit)
		}
		if delta := safeSubPipelineBench(samplerMax, msStart.HeapAlloc); delta > lastPeakHeap {
			lastPeakHeap = delta
		}
		lastDiskBytes = dirBytesPipelineBench(b, outDir)
		b.StartTimer()
	}

	b.ReportMetric(float64(lastDiskBytes), "disk_B")
	b.ReportMetric(float64(lastPeakHeap), "peak_heap_B")
}
