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
	"sync"
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
// ingest worker (the pool is min(GOMAXPROCS, data files)), each file at
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
// It scales with NumCPU, not GOMAXPROCS, so a run with GOMAXPROCS
// lowered (a CPU-quota stand-in) keeps the same inventory and still
// has more files than workers.
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
// Realistic, deep_pyramid and unique_dirs list keys in random order,
// which is what to expect: S3 Inventory guarantees no order.
// Realistic_sorted is realistic with the keys in order across the
// chunks, the best case for the build's in-order paths. Unique_dirs has
// a directory per object, the prefix-heavy case that sizes scratch and
// index disk. Those are CSV.gz inventories; the format=parquet entries
// build the realistic objects from Snappy Parquet files instead, for
// the cost of one format against the other.
func BenchmarkPipeline(b *testing.B) {
	for _, shape := range []string{"realistic", "deep_pyramid", "realistic_sorted", "unique_dirs"} {
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
		var cached seeder.InventoryInfo
		decodeErr := json.NewDecoder(out.Body).Decode(&cached)
		_ = out.Body.Close()
		if decodeErr == nil {
			return cached
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

// phaseClock times a build's phases from its progress events: each
// phase ("downloading" is ingest, "building" the final merge into the
// index builder) and each finalize step runs from its first event to the
// next phase's or step's, or to the build's end. It records only when
// the phase changes, so it allocates a few times per build.
type phaseClock struct {
	spent map[string]time.Duration
	start time.Time
	cur   string
	mu    sync.Mutex
}

func newPhaseClock() *phaseClock {
	return &phaseClock{spent: make(map[string]time.Duration)}
}

// begin starts a build at start.
func (c *phaseClock) begin(start time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cur, c.start = "pipeline", start
}

func (c *phaseClock) observe(ev events.Progress) {
	name := ev.Stage
	if ev.Step != "" {
		name = ev.Stage + ":" + ev.Step
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if name == c.cur || c.cur == "" {
		return
	}
	c.switchTo(name, time.Now())
}

// end closes the build's last phase.
func (c *phaseClock) end() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.switchTo("", time.Now())
}

func (c *phaseClock) switchTo(name string, now time.Time) {
	c.spent[c.cur] += now.Sub(c.start)
	c.cur, c.start = name, now
}

// report reports each phase's mean seconds per op as phase_<name>_s,
// the name lowercased with runs of other characters as underscores.
func (c *phaseClock) report(b *testing.B) {
	b.Helper()
	for name, d := range c.spent {
		var unit strings.Builder
		unit.WriteString("phase_")
		under := false
		for _, r := range strings.ToLower(name) {
			if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
				unit.WriteRune(r)
				under = false
			} else if !under {
				unit.WriteByte('_')
				under = true
			}
		}
		unit.WriteString("_s")
		b.ReportMetric(d.Seconds()/float64(b.N), unit.String())
	}
}

// diskProbe measures a build's disk use when benchutil.DiskPeakEnabled:
// the peak of scratch plus index, and the final merge's run files. Off,
// it does nothing, since its directory walks allocate and would count
// in the build's allocs/op.
type diskProbe struct {
	sampler   *benchutil.DiskPeakSampler
	err       error
	root      string
	finalRuns int64
	taken     bool
}

func startDiskProbe(root string) *diskProbe {
	if !benchutil.DiskPeakEnabled() {
		return &diskProbe{}
	}

	return &diskProbe{root: root, sampler: benchutil.StartDiskPeakSampler(root)}
}

// observe is the build's progress callback. Entering the build phase,
// before the index builder writes anything, the scratch on disk is the
// final merge's run files.
func (d *diskProbe) observe(ev events.Progress) {
	if d.sampler == nil || d.taken || ev.Stage != "building" {
		return
	}
	d.taken = true
	d.finalRuns, d.err = benchutil.DiskUsage(d.root)
}

// stop ends sampling and returns the peak, or the first error.
func (d *diskProbe) stop() (int64, error) {
	if d.sampler == nil {
		return 0, nil
	}
	peak, err := d.sampler.Stop()
	if err == nil {
		err = d.err
	}

	return peak, err
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

	var lastDiskBytes, lastPeakDisk, lastFinalRuns int64
	var lastPeakHeap uint64
	var lastPrefixCount uint64
	var lastMaxDepth uint32
	var lastRuns, lastRunFiles int
	var lastRunRows uint64
	// Proc totals every run's CPU, faults and storage I/O, and wall the
	// runs' time, for the per-op means and the cores they kept busy.
	var proc benchutil.ProcStats
	var wall time.Duration
	phases := newPhaseClock()

	b.ResetTimer()
	for range b.N {
		b.StopTimer()
		// Scratch and the staging index are outDir's siblings, so the
		// disk probe watches its parent.
		buildRoot := b.TempDir()
		outDir := filepath.Join(buildRoot, "index")

		var prevLimit int64
		if memLimit > 0 {
			prevLimit = debug.SetMemoryLimit(memLimit)
		}

		runtime.GC()
		var msStart runtime.MemStats
		runtime.ReadMemStats(&msStart)
		peak := benchutil.StartHeapPeakSampler()
		disk := startDiskProbe(buildRoot)

		cfg := extsort.DefaultConfig()
		cfg.Observe.OnProgress = func(ev events.Progress) {
			disk.observe(ev)
			phases.observe(ev)
		}
		pipeline := extsort.NewPipeline(cfg, fc)

		b.StartTimer()
		before, start := benchutil.ReadProcStats(b), time.Now()
		phases.begin(start)
		res, err := pipeline.Run(b.Context(), info.Path, outDir)
		wall += time.Since(start)
		phases.end()
		proc = proc.Add(benchutil.ReadProcStats(b).Sub(before))
		if err != nil {
			b.StopTimer()
			peak.Stop()
			_, _ = disk.stop()
			if memLimit > 0 {
				debug.SetMemoryLimit(prevLimit)
			}
			b.Fatalf("pipeline.Run: %v", err)
		}
		b.StopTimer()
		lastRuns = res.RunFilesCreated + res.MemoryRuns
		lastRunFiles = res.RunFilesCreated
		lastRunRows = res.MergeInputRows

		samplerMax := peak.Stop()
		peakDisk, err := disk.stop()
		if err != nil {
			b.Fatalf("disk probe: %v", err)
		}
		lastPeakDisk, lastFinalRuns = peakDisk, disk.finalRuns
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
	if benchutil.DiskPeakEnabled() {
		b.ReportMetric(float64(lastPeakDisk), "peak_disk_B")
		b.ReportMetric(float64(lastFinalRuns), "final_runs_B")
	}
	b.ReportMetric(float64(lastPeakHeap), "peak_heap_B")
	b.ReportMetric(float64(lastPrefixCount), "prefixes")
	b.ReportMetric(float64(lastMaxDepth), "max_depth")
	b.ReportMetric(float64(chunks), "chunks")
	// Sorted runs the workers produced: every spill plus each worker's
	// final state, whether kept in memory or flushed to a run file.
	b.ReportMetric(float64(lastRuns), "runs")
	b.ReportMetric(float64(lastRunFiles), "run_files")
	// Rows of those runs, before any disk merge rounds: over prefixes,
	// the worker runs a prefix was in.
	b.ReportMetric(float64(lastRunRows), "run_rows")
	proc.Report(b, b.N, wall)
	phases.report(b)
}
