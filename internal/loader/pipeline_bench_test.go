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
	"github.com/eunmann/s3-inv-db/pkg/extsort/events"
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

// chunkCountFor returns the fixture's data file count: up to four per
// ingest worker (the pool is min(NumCPU, data files)), each file at
// least minPerChunk objects, so on a many-core host the floor caps the
// count below four per worker. A real inventory is many data files, more than
// a host has cores, so each worker has more chunks queued: fetchers
// download the next while it parses one, and the order chunks are
// handed out (largest first) shapes the tail. With one chunk per worker
// none of that runs and the slowest chunk is the whole ingest. Four per
// worker keeps a queue behind every worker for most of the ingest,
// while 1M objects still make chunks of tens of thousands of objects,
// big enough that a file's fetch and gzip setup stay small next to its
// parse. Following the core count rather than a fixed file size gives
// the per-PR 1M runs and the 10M scale runs the same queue depth per
// worker wherever the floor does not bind (up to 25 cores for 1M).
func chunkCountFor(numObjects int) int {
	const (
		chunksPerWorker = 4
		minPerChunk     = 10_000
	)

	return max(min(chunksPerWorker*runtime.NumCPU(), numObjects/minPerChunk), 1)
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

// parquetPipelineSizes returns the sizes the Parquet entries run at:
// 1M, and 10M under the long-bench env.
func parquetPipelineSizes() []int {
	sizes := []int{1_000_000}
	if benchutil.LongBenchEnabled() {
		sizes = append(sizes, 10_000_000)
	}
	return sizes
}

// BenchmarkPipeline sweeps shape × size against the full S3-to-index path.
// Realistic and deep_pyramid list keys in random order, which is what to
// expect: S3 Inventory guarantees no order. Realistic_sorted is
// realistic with the keys in order across the chunks, the best case for
// the build's in-order paths. Those are CSV.gz inventories; the
// format=parquet entries build the realistic objects from Snappy
// Parquet files instead, for the cost of one format against the other.
func BenchmarkPipeline(b *testing.B) {
	for _, shape := range []string{"realistic", "deep_pyramid", "realistic_sorted"} {
		for _, n := range pipelineSizes() {
			name := fmt.Sprintf("shape=%s/n=%d", shape, n)
			b.Run(name, func(b *testing.B) {
				runPipelineBench(b, shape, n, 0)
			})
		}
	}
	for _, n := range parquetPipelineSizes() {
		b.Run(fmt.Sprintf("shape=realistic/format=parquet/n=%d", n), func(b *testing.B) {
			runPipelineBenchWith(b, pipelineBench{preset: "realistic", numObjects: n, format: seeder.FormatParquet})
		})
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
// per ingest worker (chunkCountFor) so a worker has a next chunk to
// fetch while it parses one. The local server alone answers instantly
// and can't show the time a build spends waiting on S3.
func BenchmarkPipelineSlowS3(b *testing.B) {
	const (
		latency     = 20 * time.Millisecond
		bytesPerSec = 8 << 20
	)
	fc := s3test.SlowFetchClient(b, latency, bytesPerSec)
	runPipelineBenchWith(b, pipelineBench{
		preset:     "realistic_sorted",
		numObjects: 1_000_000,
		client:     fc,
	})
}

// fixtureInfoKey marks a seeded fixture complete; it holds the
// seeder's InventoryInfo.
const fixtureInfoKey = "fixture.json"

// seededInventory returns the fixture inventory for preset, format,
// size and chunk count, seeding it into the local S3 server only once
// per server, A/B side and seeder output: every later run of that side
// reads the same upload, each side builds from its own seeder's output,
// and the seeder's fingerprint in the bucket name keeps a fixture an
// older seeder wrote from being reused. Seeding goes straight to the
// local server, so a slow client's link only slows the build's reads.
// A Parquet fixture's bucket name says so; a CSV one's is unchanged.
func seededInventory(b *testing.B, preset string, format seeder.Format, numObjects, chunks int) seeder.InventoryInfo {
	b.Helper()
	raw := s3test.RawClient(b)
	cfg := seeder.Config{
		Target:     seeder.TargetS3,
		Objects:    numObjects,
		Preset:     strings.TrimSuffix(preset, "_sorted"),
		Seed:       int64(numObjects ^ chunks),
		Logger:     zerolog.Nop(),
		SortedKeys: strings.HasSuffix(preset, "_sorted"),
		Format:     format,
	}
	seed := int64(numObjects)
	fingerprint, err := seeder.FixtureFingerprint(b.Context(), cfg, seed)
	if err != nil {
		b.Fatalf("FixtureFingerprint: %v", err)
	}
	fixture := preset
	if format == seeder.FormatParquet {
		fixture += "-parquet"
	}
	bucket := s3test.FixtureBucket(b, raw, fmt.Sprintf("%s-%d-%d-%s", fixture, numObjects, chunks, fingerprint))
	if out, err := raw.GetObject(b.Context(), &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(fixtureInfoKey)}); err == nil {
		var info seeder.InventoryInfo
		decodeErr := json.NewDecoder(out.Body).Decode(&info)
		_ = out.Body.Close()
		if decodeErr == nil {
			return info
		}
	}
	info, err := seeder.UploadMultiChunkInventory(
		b.Context(), raw, cfg,
		seeder.S3Config{Bucket: bucket, Prefix: "bench-inv/", SrcBucket: "bench-src"},
		1, seed, time.Now().UTC().Truncate(time.Minute), chunks,
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

// inputBytes returns the size of the inventory's data files: what a
// build downloads.
func inputBytes(b *testing.B, fc *s3fetch.Client, manifestURI string) int64 {
	b.Helper()
	uri, err := s3fetch.ParseS3URI(manifestURI)
	if err != nil {
		b.Fatalf("ParseS3URI: %v", err)
	}
	manifest, err := fc.FetchManifest(b.Context(), uri.Bucket, uri.Key)
	if err != nil {
		b.Fatalf("FetchManifest: %v", err)
	}
	var n int64
	for _, f := range manifest.Files {
		n += f.Size
	}

	return n
}

// pipelineBench is one pipeline benchmark's setup. A zero memLimit
// leaves GOMEMLIMIT unchanged, a nil client is the local S3 server's
// and an empty format is CSV.
type pipelineBench struct {
	client     *s3fetch.Client
	preset     string
	format     seeder.Format
	numObjects int
	memLimit   int64
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
	chunks := chunkCountFor(numObjects)
	info := seededInventory(b, preset, pb.format, numObjects, chunks)
	input := inputBytes(b, fc, info.Path)

	var lastDiskBytes int64
	var lastPeakHeap uint64
	var lastPrefixCount uint64
	var lastMaxDepth uint32
	var lastRuns, lastRunFiles int
	// Proc totals every run's CPU, faults and storage I/O, and wall the
	// runs' time, for the per-op means and the cores they kept busy.
	var proc benchutil.ProcStats
	var wall time.Duration

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
		cfg.Observe.OnProgress = func(events.Progress) {}
		pipeline := extsort.NewPipeline(cfg, fc)

		b.StartTimer()
		before, start := benchutil.ReadProcStats(b), time.Now()
		res, err := pipeline.Run(b.Context(), info.Path, outDir)
		wall += time.Since(start)
		proc = proc.Add(benchutil.ReadProcStats(b).Sub(before))
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

	b.ReportMetric(float64(input), "input_B")
	b.ReportMetric(float64(lastDiskBytes), "disk_B")
	b.ReportMetric(float64(lastPeakHeap), "peak_heap_B")
	b.ReportMetric(float64(lastPrefixCount), "prefixes")
	b.ReportMetric(float64(lastMaxDepth), "max_depth")
	b.ReportMetric(float64(chunks), "chunks")
	// Sorted runs the merge read: every spill plus each worker's final
	// state, whether kept in memory or flushed to a run file.
	b.ReportMetric(float64(lastRuns), "runs")
	b.ReportMetric(float64(lastRunFiles), "run_files")
	proc.Report(b, b.N, wall)
}
