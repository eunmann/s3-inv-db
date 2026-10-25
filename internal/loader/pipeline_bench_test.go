//go:build integration

package loader_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/eunmann/s3-inv-db/internal/benchutil"
	"github.com/eunmann/s3-inv-db/internal/s3test"
	"github.com/eunmann/s3-inv-db/internal/seeder"
	"github.com/eunmann/s3-inv-db/pkg/extsort"
	"github.com/eunmann/s3-inv-db/pkg/indexread"
	"github.com/eunmann/s3-inv-db/pkg/s3fetch"
	"github.com/rs/zerolog"
)

// Pipeline integration benchmarks — full S3-to-index path against the
// local S3 server. Requires AWS_ENDPOINT_URL_S3 pointing at it (docker
// compose `test` profile). Run:
//
//	go test -tags integration -bench=BenchmarkPipeline -benchtime=1x \
//	  -count=1 -run=^$ ./internal/loader/...

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

// BenchmarkPipeline sweeps shape × size against the full S3-to-index path.
// Realistic_sorted is realistic with the keys in order across the
// chunks, as S3 Inventory writes them; the others are in random order.
func BenchmarkPipeline(b *testing.B) {
	for _, shape := range []string{"realistic", "deep_pyramid", "realistic_sorted"} {
		for _, n := range pipelineSizes() {
			name := fmt.Sprintf("shape=%s/n=%d", shape, n)
			b.Run(name, func(b *testing.B) {
				runPipelineBench(b, shape, n, 0)
			})
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
			runPipelineBench(b, "deep_pyramid", 1_000_000, mem)
		})
	}
}

// BenchmarkPipelineSlowS3 runs the build against S3 behind a slower
// link (first-byte latency, a bandwidth cap per GET), with four chunks
// per ingest worker so a worker has a next chunk to fetch while it
// parses one. The local server alone answers instantly and can't show
// the time a build spends waiting on S3.
func BenchmarkPipelineSlowS3(b *testing.B) {
	const (
		latency     = 20 * time.Millisecond
		bytesPerSec = 8 << 20
	)
	fc := s3test.SlowFetchClient(b, latency, bytesPerSec)
	runPipelineBenchWith(b, pipelineBench{
		preset:     "realistic_sorted",
		numObjects: 1_000_000,
		chunks:     4 * runtime.NumCPU(),
		client:     fc,
	})
}

// fixtureInfoKey marks a seeded fixture complete; it holds the
// seeder's InventoryInfo.
const fixtureInfoKey = "fixture.json"

// seededInventory returns the fixture inventory for preset, size and
// chunk count, seeding it into the local S3 server only once per server
// and A/B side: every later run of that side reads the same upload, and
// each side builds from its own seeder's output. Seeding goes straight
// to the local server, so a slow client's link only slows the build's
// reads.
func seededInventory(b *testing.B, preset string, numObjects, chunks int) seeder.InventoryInfo {
	b.Helper()
	raw := s3test.RawClient(b)
	bucket := s3test.FixtureBucket(b, raw, fmt.Sprintf("%s-%d-%d", preset, numObjects, chunks))
	if out, err := raw.GetObject(b.Context(), &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(fixtureInfoKey)}); err == nil {
		var info seeder.InventoryInfo
		decodeErr := json.NewDecoder(out.Body).Decode(&info)
		_ = out.Body.Close()
		if decodeErr == nil {
			return info
		}
	}
	info, err := seeder.UploadMultiChunkInventory(
		b.Context(), raw,
		seeder.Config{
			Target:     seeder.TargetS3,
			Objects:    numObjects,
			Preset:     strings.TrimSuffix(preset, "_sorted"),
			Seed:       int64(numObjects ^ chunks),
			Logger:     zerolog.Nop(),
			SortedKeys: strings.HasSuffix(preset, "_sorted"),
		},
		seeder.S3Config{Bucket: bucket, Prefix: "bench-inv/", SrcBucket: "bench-src"},
		1, int64(numObjects), time.Now().UTC().Truncate(time.Minute), chunks,
	)
	if err != nil {
		b.Fatalf("UploadMultiChunkInventory: %v", err)
	}
	marker, err := json.Marshal(info)
	if err != nil {
		b.Fatal(err)
	}
	if _, err := raw.PutObject(b.Context(), &s3.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(fixtureInfoKey), Body: bytes.NewReader(marker),
	}); err != nil {
		b.Fatalf("put %s: %v", fixtureInfoKey, err)
	}

	return info
}

// pipelineBench is one pipeline benchmark's setup. A zero memLimit
// leaves GOMEMLIMIT unchanged, zero chunks takes chunkCountFor, and a
// nil client is the local S3 server's.
type pipelineBench struct {
	client     *s3fetch.Client
	preset     string
	numObjects int
	memLimit   int64
	chunks     int
}

// runPipelineBench is the shared body. MemLimit==0 leaves GOMEMLIMIT
// unchanged.
func runPipelineBench(b *testing.B, preset string, numObjects int, memLimit int64) {
	b.Helper()
	runPipelineBenchWith(b, pipelineBench{preset: preset, numObjects: numObjects, memLimit: memLimit})
}

func runPipelineBenchWith(b *testing.B, pb pipelineBench) {
	b.Helper()
	benchutil.SilenceZerolog(b)
	preset, numObjects, memLimit := pb.preset, pb.numObjects, pb.memLimit

	fc := pb.client
	if fc == nil {
		fc = s3test.FetchClient(b)
	}
	chunks := pb.chunks
	if chunks == 0 {
		chunks = chunkCountFor(numObjects)
	}
	info := seededInventory(b, preset, numObjects, chunks)

	var lastDiskBytes int64
	var lastPeakHeap uint64
	var lastPrefixCount uint64
	var lastMaxDepth uint32
	var lastRuns, lastRunFiles int

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
		cfg.Observe.OnProgress = func(string, int64, int64) {}
		pipeline := extsort.NewPipeline(cfg, fc)

		b.StartTimer()
		res, err := pipeline.Run(b.Context(), info.Path, outDir)
		if err != nil {
			b.StopTimer()
			peak.Stop()
			if memLimit > 0 {
				debug.SetMemoryLimit(prevLimit)
			}
			b.Fatalf("pipeline.Run: %v", err)
		}
		b.StopTimer()
		lastRuns = res.RunFilesCreated + res.MemoryRuns
		lastRunFiles = res.RunFilesCreated

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
	// Sorted runs the merge read: every spill plus each worker's final
	// state, whether kept in memory or flushed to a run file.
	b.ReportMetric(float64(lastRuns), "runs")
	b.ReportMetric(float64(lastRunFiles), "run_files")
}
