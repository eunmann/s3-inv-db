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
	"github.com/eunmann/s3-inv-db/pkg/indexread"
	"github.com/rs/zerolog"
)

// Pipeline integration benchmarks — full S3-to-index path against MinIO.
// Requires AWS_ENDPOINT_URL_S3 pointing at a reachable MinIO (docker
// compose `test` profile). Run:
//
//	go test -bench=BenchmarkPipeline -benchtime=1x -count=1 \
//	  -run=^$ ./internal/loader/...

// chunkCountFor returns a chunk count that lets Pipeline's ingest pool
// (sized as min(NumCPU, manifest_files)) saturate the host while
// keeping per-chunk size above minPerChunk.
func chunkCountFor(numObjects int) int {
	const (
		minPerChunk = 50_000
		minChunks   = 4
	)
	byCPU := runtime.NumCPU()
	byFloor := max(numObjects/minPerChunk, minChunks)

	return min(byFloor, byCPU)
}

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

func BenchmarkPipeline_Realistic_10M_DictOff(b *testing.B) {
	runPipelineBench(b, "realistic", 10_000_000, false, 0)
}

func BenchmarkPipeline_Realistic_10M_DictOn(b *testing.B) {
	runPipelineBench(b, "realistic", 10_000_000, true, 0)
}

func BenchmarkPipeline_DeepPyramid_10M_DictOff(b *testing.B) {
	runPipelineBench(b, "deep_pyramid", 10_000_000, false, 0)
}

func BenchmarkPipeline_DeepPyramid_10M_DictOn(b *testing.B) {
	runPipelineBench(b, "deep_pyramid", 10_000_000, true, 0)
}

// BenchmarkPipeline_AutoScale_DeepPyramid_1M_Mem* sweeps GOMEMLIMIT
// against the same fixture; only the memory cap varies.
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
// unchanged.
func runPipelineBench(b *testing.B, preset string, numObjects int, prefixDict bool, memLimit int64) {
	b.Helper()
	silenceZerologPipelineBench(b)

	fc := miniotest.FetchClient(b)
	bucket := miniotest.Bucket(b, fc.Raw())

	srcBucket := "bench-src"
	prefix := "bench-inv/"
	stamp := time.Now().UTC().Truncate(time.Minute)

	chunks := chunkCountFor(numObjects)
	info, err := seeder.UploadMultiChunkInventory(
		b.Context(), fc.Raw(),
		seeder.Config{
			Target:  seeder.TargetS3,
			Objects: numObjects,
			Preset:  preset,
			Seed:    int64(numObjects ^ chunks),
			Logger:  zerolog.Nop(),
		},
		seeder.S3Config{Bucket: bucket, Prefix: prefix, SrcBucket: srcBucket},
		1, int64(numObjects), stamp, chunks,
	)
	if err != nil {
		b.Fatalf("UploadMultiChunkInventory: %v", err)
	}

	var lastDiskBytes int64
	var lastPeakHeap uint64
	var lastPrefixCount uint64
	var lastMaxDepth uint32

	b.ResetTimer()
	for range b.N {
		b.StopTimer()
		outDir := filepath.Join(b.TempDir(), "index")

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
		if idx, err := indexread.Open(outDir); err == nil {
			lastPrefixCount = idx.Count()
			lastMaxDepth = idx.MaxDepth()
			_ = idx.Close()
		}
		b.StartTimer()
	}

	b.ReportMetric(float64(lastDiskBytes), "disk_B")
	b.ReportMetric(float64(lastPeakHeap), "peak_heap_B")
	b.ReportMetric(float64(lastPrefixCount), "prefixes")
	b.ReportMetric(float64(lastMaxDepth), "max_depth")
	b.ReportMetric(float64(chunkCountFor(numObjects)), "chunks")
}
