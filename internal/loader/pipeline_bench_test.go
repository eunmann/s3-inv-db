package loader_test

import (
	"fmt"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"testing"
	"time"

	"github.com/eunmann/s3-inv-db/internal/benchutil"
	"github.com/eunmann/s3-inv-db/internal/miniotest"
	"github.com/eunmann/s3-inv-db/internal/seeder"
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

// pipelineSizes returns the size axis the pipeline benchmark sweeps.
// 10M is gated behind the long-bench env to keep `make test` cheap.
func pipelineSizes() []int {
	sizes := []int{500_000, 1_000_000}
	if benchutil.LongBenchEnabled() {
		sizes = append(sizes, 10_000_000)
	}
	return sizes
}

// BenchmarkPipeline sweeps shape × size × prefix-dictionary against
// the full S3-to-index path.
func BenchmarkPipeline(b *testing.B) {
	for _, shape := range []string{"realistic", "deep_pyramid"} {
		for _, n := range pipelineSizes() {
			for _, dict := range []bool{false, true} {
				name := fmt.Sprintf("shape=%s/n=%d/dict=%v", shape, n, dict)
				b.Run(name, func(b *testing.B) {
					runPipelineBench(b, shape, n, dict, 0)
				})
			}
		}
	}
}

// BenchmarkPipeline_GoMemLimit sweeps GOMEMLIMIT against the same
// deep_pyramid 1M fixture; only the soft GC target varies. NOTE: the
// limit is debug.SetMemoryLimit — a GC pacer hint, not a hard RSS
// cap.
func BenchmarkPipeline_GoMemLimit(b *testing.B) {
	for _, mem := range []int64{2 << 30, 8 << 30, 16 << 30} {
		name := fmt.Sprintf("mem=%dG", mem>>30)
		b.Run(name, func(b *testing.B) {
			runPipelineBench(b, "deep_pyramid", 1_000_000, true, mem)
		})
	}
}

// runPipelineBench is the shared body. MemLimit==0 leaves GOMEMLIMIT
// unchanged.
func runPipelineBench(b *testing.B, preset string, numObjects int, prefixDict bool, memLimit int64) {
	b.Helper()
	benchutil.SilenceZerolog(b)

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
		peak := benchutil.StartHeapPeakSampler()

		cfg := extsort.DefaultConfig()
		cfg.PrefixDictionary = prefixDict
		cfg.Observe.OnProgress = func(string, int64, int64) {}
		pipeline := extsort.NewPipeline(cfg, fc)

		b.StartTimer()
		if _, err := pipeline.Run(b.Context(), info.Path, outDir); err != nil {
			b.StopTimer()
			peak.Stop()
			if memLimit > 0 {
				debug.SetMemoryLimit(prevLimit)
			}
			b.Fatalf("pipeline.Run: %v", err)
		}
		b.StopTimer()

		samplerMax := peak.Stop()
		if memLimit > 0 {
			debug.SetMemoryLimit(prevLimit)
		}
		if delta := benchutil.SafeSubU64(samplerMax, msStart.HeapAlloc); delta > lastPeakHeap {
			lastPeakHeap = delta
		}
		lastDiskBytes = benchutil.DirBytes(b, outDir)
		idx, err := indexread.Open(outDir)
		if err != nil {
			b.Fatalf("indexread.Open: %v", err)
		}
		lastPrefixCount = idx.Count()
		lastMaxDepth = idx.MaxDepth()
		_ = idx.Close()
		b.StartTimer()
	}

	b.ReportMetric(float64(lastDiskBytes), "disk_B")
	b.ReportMetric(float64(lastPeakHeap), "peak_heap_B")
	b.ReportMetric(float64(lastPrefixCount), "prefixes")
	b.ReportMetric(float64(lastMaxDepth), "max_depth")
	b.ReportMetric(float64(chunks), "chunks")
}
