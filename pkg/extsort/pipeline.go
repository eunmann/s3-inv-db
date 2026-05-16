package extsort

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/eunmann/s3-inv-db/pkg/humanfmt"
	"github.com/eunmann/s3-inv-db/pkg/inventory"
	"github.com/eunmann/s3-inv-db/pkg/memdiag"
	"github.com/eunmann/s3-inv-db/pkg/s3fetch"
	"github.com/eunmann/s3-inv-db/pkg/tiers"
	"github.com/rs/zerolog"
)

// Pipeline orchestrates the external sort build process.
// It streams S3 inventory data, aggregates in bounded memory,
// spills to sorted run files, and merges to build the final index.
type Pipeline struct {
	config    Config
	s3Client  *s3fetch.Client
	tempDir   string
	startTime time.Time

	// runFilesMu guards runFiles + runCount. Both are mutated from N
	// chunkWorker goroutines (each flushing its private aggregator)
	// plus the merge phase which reads runFiles. The mutex is held
	// only across the slice append + counter bump — never across
	// I/O — so contention is negligible.
	runFilesMu sync.Mutex
	runFiles   []string
	runCount   int

	// Progress tracking — atomics because N chunkWorkers update
	// these concurrently as they parse and aggregate.
	chunksProcessed  atomic.Int64
	objectsProcessed atomic.Int64
	bytesProcessed   atomic.Int64
	flushCount       atomic.Int64

	// Memory diagnostics
	memTracker *memdiag.Tracker
}

// Result holds the pipeline execution result.
type Result struct {
	ChunksProcessed  int
	ObjectsProcessed int64
	PrefixCount      uint64
	MaxDepth         uint32
	RunFilesCreated  int
	Duration         time.Duration
}

// setPhase updates both the memory diagnostic tracker and the user's
// progress callback. Single hook so future phases stay consistent. The
// stage transition reports done=0/total=0 — quantitative progress
// within the stage is emitted separately by the stage's own loop.
func (p *Pipeline) setPhase(name string) {
	p.memTracker.SetPhase(name)
	if p.config.OnProgress != nil {
		p.config.OnProgress(name, 0, 0)
	}
}

// reportProgress emits quantitative progress within the current phase.
// Called from ingest after each chunk.
func (p *Pipeline) reportProgress(phase string, done, total int64) {
	if p.config.OnProgress != nil {
		p.config.OnProgress(phase, done, total)
	}
}

// NewPipeline creates a new external sort pipeline.
func NewPipeline(config Config, s3Client *s3fetch.Client) *Pipeline {
	return &Pipeline{
		config:     config,
		s3Client:   s3Client,
		runFiles:   make([]string, 0, 16),
		memTracker: memdiag.NewTracker(memdiag.DefaultConfig()),
	}
}

// Run executes the full pipeline.
func (p *Pipeline) Run(ctx context.Context, manifestURI, outDir string) (*Result, error) {
	p.startTime = time.Now()
	log := zerolog.Ctx(ctx)

	// Start memory diagnostics
	p.memTracker.Start()
	defer p.memTracker.Stop()
	p.setPhase("initializing")

	tempDir := p.config.TempDir
	if tempDir == "" {
		var err error
		tempDir, err = os.MkdirTemp("", "extsort-*")
		if err != nil {
			return nil, fmt.Errorf("create temp dir: %w", err)
		}
	}
	p.tempDir = tempDir

	success := false
	defer func() {
		if !success {
			p.cleanup()
		}
	}()

	memoryLimit := debug.SetMemoryLimit(-1)
	log.Info().
		Str("manifest_uri", manifestURI).
		Int64("gomemlimit_bytes", memoryLimit).
		Str("aggregator_cap", humanfmt.BytesUint64(AggregatorCap(memoryLimit))).
		Msg("pipeline starting")

	p.setPhase("downloading")
	ingestStart := time.Now()
	if err := p.runIngestPhase(ctx, manifestURI); err != nil {
		if errors.Is(err, context.Canceled) {
			log.Warn().Msg("pipeline cancelled during ingest phase")
		}

		return nil, fmt.Errorf("ingest phase: %w", err)
	}
	ingestDuration := time.Since(ingestStart)

	// Force GC after ingest to release aggregator memory
	runtime.GC()
	p.memTracker.LogNow("post_ingest_gc")

	log.Info().
		Int("run_files_count", len(p.runFiles)).
		Str("objects", humanfmt.Count(p.objectsProcessed.Load())).
		Int64("objects_count", p.objectsProcessed.Load()).
		Int64("flushes_count", p.flushCount.Load()).
		Str("duration", humanfmt.Duration(ingestDuration)).
		Dur("duration_ms", ingestDuration).
		Msg("ingest phase complete")

	p.setPhase("building")
	mergeStart := time.Now()
	mergeRes, err := p.runMergeBuildPhase(ctx, outDir)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			log.Warn().Msg("pipeline cancelled during merge phase")
		}

		return nil, fmt.Errorf("merge/build phase: %w", err)
	}
	prefixCount, maxDepth := mergeRes.PrefixCount, mergeRes.MaxDepth
	mergeDuration := time.Since(mergeStart)

	// Force GC after merge
	runtime.GC()
	p.memTracker.LogNow("post_merge_gc")

	log.Info().
		Str("prefixes", humanfmt.CountUint64(prefixCount)).
		Uint64("prefixes_count", prefixCount).
		Uint32("max_depth", maxDepth).
		Str("duration", humanfmt.Duration(mergeDuration)).
		Dur("duration_ms", mergeDuration).
		Msg("merge phase complete")

	p.cleanup()
	success = true

	duration := time.Since(p.startTime)
	log.Info().
		Str("total_duration", humanfmt.Duration(duration)).
		Dur("total_duration_ms", duration).
		Str("objects", humanfmt.Count(p.objectsProcessed.Load())).
		Int64("objects_count", p.objectsProcessed.Load()).
		Str("prefixes", humanfmt.CountUint64(prefixCount)).
		Uint64("prefixes_count", prefixCount).
		Str("throughput", humanfmt.Count(int64(float64(p.objectsProcessed.Load())/duration.Seconds()))+"/s").
		Msg("pipeline complete")

	return &Result{
		ChunksProcessed:  int(p.chunksProcessed.Load()),
		ObjectsProcessed: p.objectsProcessed.Load(),
		PrefixCount:      prefixCount,
		MaxDepth:         maxDepth,
		RunFilesCreated:  len(p.runFiles),
		Duration:         duration,
	}, nil
}

// chunkConfig holds configuration for processing a chunk.
type chunkConfig struct {
	format        s3fetch.InventoryFormat
	keyCol        int
	sizeCol       int
	storageCol    int
	accessTierCol int
	tierMapping   *tiers.Mapping
	fileSize      int64 // Size of the file (used for Parquet)
}

// chunkJob represents a chunk to be processed by a worker.
type chunkJob struct {
	index  int
	bucket string
	key    string
	config chunkConfig
}

// objectRecord holds a single object's data for aggregation.
type objectRecord struct {
	key    string
	size   uint64
	tierID tiers.ID
}

// estimateObjectCount estimates the number of objects in an inventory file
// based on its compressed size and format. This helps pre-size buffers to
// avoid repeated slice growth during parsing.
func estimateObjectCount(fileSize int64, format s3fetch.InventoryFormat) int {
	const (
		minCapacity = 10_000 // Minimum capacity to avoid tiny allocations
		// Upper bound to prevent absurd preallocations even for huge files.
		maxCapacity = 10_000_000

		// CSV inventory: each row is ~100-200 bytes uncompressed.
		// S3 inventory CSVs are gzip-compressed with ~8x ratio.
		// Estimate: fileSize * 8 (decompress) / 150 (avg row size)
		csvBytesPerRecord = 150 / 8 // ~18 bytes compressed per record

		// Parquet inventory: very compact columnar format.
		// Empirically ~40-60 bytes per record including overhead.
		parquetBytesPerRecord = 50
	)

	if fileSize <= 0 {
		return minCapacity
	}

	var estimate int64
	if format == s3fetch.InventoryFormatParquet {
		estimate = fileSize / parquetBytesPerRecord
	} else {
		estimate = fileSize / csvBytesPerRecord
	}

	// Clamp to reasonable range
	if estimate < int64(minCapacity) {
		return minCapacity
	}
	if estimate > maxCapacity {
		return maxCapacity
	}

	return int(estimate)
}

// ingestConfig holds configuration for the ingest phase.
type ingestConfig struct {
	manifest      *s3fetch.Manifest
	format        s3fetch.InventoryFormat
	destBucket    string
	keyCol        int
	sizeCol       int
	storageCol    int
	accessTierCol int
	numWorkers    int
	tierMapping   *tiers.Mapping
}

// runIngestPhase streams S3 inventory and creates sorted run files.
// It uses concurrent workers to download and parse chunks in parallel.
func (p *Pipeline) runIngestPhase(ctx context.Context, manifestURI string) error {
	log := zerolog.Ctx(ctx)

	cfg, err := p.setupIngestConfig(ctx, manifestURI)
	if err != nil {
		return err
	}

	log.Info().
		Str("format", cfg.formatString()).
		Int("chunks_count", len(cfg.manifest.Files)).
		Int("workers_count", cfg.numWorkers).
		Msg("inventory manifest loaded")

	return p.runIngestLoop(ctx, cfg)
}

// setupIngestConfig parses the manifest and computes configuration.
func (p *Pipeline) setupIngestConfig(ctx context.Context, manifestURI string) (*ingestConfig, error) {
	log := zerolog.Ctx(ctx)

	parsed, err := s3fetch.ParseS3URI(manifestURI)
	if err != nil {
		return nil, fmt.Errorf("parse manifest URI: %w", err)
	}

	manifest, err := p.s3Client.FetchManifest(ctx, parsed.Bucket, parsed.Key)
	if err != nil {
		return nil, fmt.Errorf("fetch manifest: %w", err)
	}

	keyCol, err := manifest.KeyColumnIndex()
	if err != nil {
		return nil, fmt.Errorf("get key column: %w", err)
	}
	sizeCol, err := manifest.SizeColumnIndex()
	if err != nil {
		return nil, fmt.Errorf("get size column: %w", err)
	}

	destBucket, err := manifest.GetDestinationBucketName()
	if err != nil {
		return nil, fmt.Errorf("get destination bucket: %w", err)
	}

	// LPT scheduling: sort manifest files largest-first so the tail of the
	// ingest doesn't leave most workers idle behind one big chunk.
	sortFilesLargestFirst(manifest)

	numWorkers := ingestWorkerCount(len(manifest.Files))
	log.Info().
		Int("num_cpu", runtime.NumCPU()).
		Int("manifest_files", len(manifest.Files)).
		Int("ingest_workers", numWorkers).
		Msg("ingest worker count derived from NumCPU")

	return &ingestConfig{
		manifest:      manifest,
		format:        manifest.DetectFormat(),
		destBucket:    destBucket,
		keyCol:        keyCol,
		sizeCol:       sizeCol,
		storageCol:    manifest.StorageClassColumnIndex(),
		accessTierCol: manifest.AccessTierColumnIndex(),
		numWorkers:    numWorkers,
		tierMapping:   tiers.NewMapping(),
	}, nil
}

// ingestWorkerCount returns the worker count for download+parse: the
// smaller of NumCPU and the manifest's chunk count (no point spinning
// up 64 workers for a 3-file manifest). Floored at 2 so even a single
// CPU runs with overlap between download and parse.
func ingestWorkerCount(fileCount int) int {
	const minWorkers = 2
	n := runtime.NumCPU()
	if fileCount > 0 && fileCount < n {
		n = fileCount
	}
	if n < minWorkers {
		n = minWorkers
	}

	return n
}

// sortFilesLargestFirst reorders manifest.Files by Size DESC so that
// large chunks land at the head of the work queue. This is the
// classic Longest-Processing-Time-first heuristic for makespan: the
// last worker to start a job finishes near the median rather than
// being stuck on the only remaining giant chunk.
func sortFilesLargestFirst(manifest *s3fetch.Manifest) {
	if manifest == nil || len(manifest.Files) < 2 {
		return
	}
	files := manifest.Files
	// Insertion sort is fine — manifests have at most a few hundred files.
	for i := 1; i < len(files); i++ {
		j := i
		for j > 0 && files[j-1].Size < files[j].Size {
			files[j-1], files[j] = files[j], files[j-1]
			j--
		}
	}
}

// formatString returns a human-readable format name.
func (c *ingestConfig) formatString() string {
	if c.format == s3fetch.InventoryFormatParquet {
		return "Parquet"
	}

	return "CSV"
}

// runIngestLoop runs the main ingest loop with per-worker
// aggregators. Each chunk worker maintains its own Aggregator and
// flushes directly to its own run files. This eliminates the
// previous single-consumer bottleneck where all AddObject calls
// were funneled onto one goroutine — at N-core ingest, that one
// goroutine was the dominant wall-time cost.
//
// Coordination:
//   - Per-worker state: Aggregator + bytesProcessed counter, all local
//   - Shared state behind p.runFilesMu: runFiles slice + runCount
//     (only mutated at flush time, never on the hot AddObject path)
//   - Atomic counters for cross-worker progress (objectsProcessed,
//     chunksProcessed, bytesProcessed, flushCount)
//   - Errgroup for error propagation; first error cancels the rest
func (p *Pipeline) runIngestLoop(ctx context.Context, cfg *ingestConfig) error {
	log := zerolog.Ctx(ctx)
	totalChunks := len(cfg.manifest.Files)

	jobs := make(chan chunkJob, cfg.numWorkers)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	errCh := make(chan error, cfg.numWorkers)
	progressTicker := p.startIngestProgressLogger(ctx, log, totalChunks)

	for workerID := range cfg.numWorkers {
		wg.Go(func() {
			if err := p.runChunkWorker(ctx, workerID, jobs, totalChunks); err != nil {
				select {
				case errCh <- err:
				default:
				}
				cancel()
			}
		})
	}

	go p.sendIngestJobs(ctx, cfg, jobs)

	wg.Wait()
	close(errCh)
	close(progressTicker)

	for err := range errCh {
		if err != nil {
			return fmt.Errorf("chunk worker: %w", err)
		}
	}

	return nil
}

// startIngestProgressLogger starts a background goroutine that logs
// per-N-chunk progress based on the atomic counters. Returns a
// channel that the caller closes to stop the logger.
func (p *Pipeline) startIngestProgressLogger(ctx context.Context, log *zerolog.Logger, totalChunks int) chan struct{} {
	stop := make(chan struct{})
	progressInterval := max(totalChunks/10, 1)
	go func() {
		var lastLogged int
		const tickInterval = 500 * time.Millisecond
		ticker := time.NewTicker(tickInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-stop:
				return
			case <-ticker.C:
				chunkNum := int(p.chunksProcessed.Load())
				if chunkNum == 0 || chunkNum == lastLogged {
					continue
				}
				p.reportProgress("downloading", int64(chunkNum), int64(totalChunks))
				if chunkNum-lastLogged >= progressInterval || chunkNum == totalChunks {
					p.logIngestProgress(log, chunkNum, totalChunks)
					lastLogged = chunkNum
				}
			}
		}
	}()

	return stop
}

// sendIngestJobs sends chunk jobs to workers.
func (p *Pipeline) sendIngestJobs(ctx context.Context, cfg *ingestConfig, jobs chan<- chunkJob) {
	defer close(jobs)
	for i, file := range cfg.manifest.Files {
		select {
		case <-ctx.Done():
			return
		case jobs <- chunkJob{
			index:  i,
			bucket: cfg.destBucket,
			key:    file.Key,
			config: chunkConfig{
				format:        cfg.format,
				keyCol:        cfg.keyCol,
				sizeCol:       cfg.sizeCol,
				storageCol:    cfg.storageCol,
				accessTierCol: cfg.accessTierCol,
				tierMapping:   cfg.tierMapping,
				fileSize:      file.Size,
			},
		}:
		}
	}
}

// logIngestProgress logs progress information.
func (p *Pipeline) logIngestProgress(log *zerolog.Logger, chunkNum, totalChunks int) {
	elapsed := time.Since(p.startTime)
	avgPerChunk := elapsed / time.Duration(chunkNum)
	remaining := time.Duration(totalChunks-chunkNum) * avgPerChunk
	const percentScale = 100.0
	pct := float64(chunkNum) * percentScale / float64(totalChunks)

	log.Info().
		Int("chunk_num", chunkNum).
		Int("chunks_total", totalChunks).
		Float64("progress_pct", pct).
		Int64("objects_count", p.objectsProcessed.Load()).
		Dur("eta_ms", remaining).
		Msg("ingest progress")
}

// runChunkWorker is the per-worker hot path: own a private Aggregator,
// pull chunks off the jobs channel, parse + AddObject directly into
// the local aggregator (zero contention), flush to a private run file
// when memory pressure hits. At end-of-input (channel closed), flush
// the residual aggregator state to a final run file.
func (p *Pipeline) runChunkWorker(ctx context.Context, workerID int, jobs <-chan chunkJob, totalChunks int) error {
	const initialAggCapacity = 10_000
	agg := NewAggregator(initialAggCapacity, p.config.MaxDepth)
	defer func() {
		if agg.PrefixCount() > 0 {
			if err := p.flushAggregator(ctx, agg); err != nil {
				zerolog.Ctx(ctx).Error().
					Int("worker_id", workerID).
					Err(err).
					Msg("final flush failed")
			}
		}
	}()

	for job := range jobs {
		select {
		case <-ctx.Done():
			return fmt.Errorf("worker %d cancelled: %w", workerID, ctx.Err())
		default:
		}

		// Estimate capacity from file size to reduce slice growth allocations.
		// For compressed CSV: assume ~8x compression, ~100 bytes/record uncompressed.
		// For Parquet: ~50 bytes/record (column-compressed).
		// Minimum 10K to avoid tiny initial allocations.
		capacityHint := estimateObjectCount(job.config.fileSize, job.config.format)

		objects, _, err := p.processChunkToBatch(ctx, job.bucket, job.key, job.config, capacityHint)
		if err != nil {
			return fmt.Errorf("chunk %d: %w", job.index, err)
		}

		var localBytes int64
		for _, obj := range objects {
			agg.AddObject(obj.key, obj.size, obj.tierID)
			localBytes += int64(obj.size)
		}
		p.objectsProcessed.Add(int64(len(objects)))
		p.bytesProcessed.Add(localBytes)
		p.chunksProcessed.Add(1)

		if ShouldFlush(HeapAllocBytes(), debug.SetMemoryLimit(-1)) {
			p.memTracker.LogNow("pre_flush")
			if err := p.flushAggregator(ctx, agg); err != nil {
				return fmt.Errorf("worker %d flush: %w", workerID, err)
			}
			p.memTracker.LogNow("post_flush")
		}
		_ = totalChunks
	}

	return nil
}

// chunkTiming holds timing information for chunk processing.
type chunkTiming struct {
	downloadDuration time.Duration
	parseDuration    time.Duration
	objectCount      int
	totalBytes       int64
}

// sizedReaderAt is an interface for readers that support both ReaderAt and Size.
// This is implemented by tempFileReader from the S3 downloader.
type sizedReaderAt interface {
	io.ReaderAt
	Size() (int64, error)
}

// createInventoryReader creates an appropriate inventory reader based on format.
// For Parquet files, it optimizes by using ReaderAt directly when available
// (from the S3 downloader's temp file), avoiding a second temp file copy.
func createInventoryReader(body io.ReadCloser, key string, cfg chunkConfig) (inventory.InventoryReader, error) {
	if cfg.format == s3fetch.InventoryFormatParquet {
		return createParquetReader(body, cfg.fileSize)
	}

	return createCSVReader(body, key, cfg)
}

// createParquetReader creates a Parquet inventory reader.
// It optimizes by using ReaderAt directly when available to avoid a second temp file.
func createParquetReader(body io.ReadCloser, fileSize int64) (inventory.InventoryReader, error) {
	// Optimization: if the body supports ReaderAt (e.g., tempFileReader from S3 downloader),
	// use it directly to avoid copying to a second temp file.
	if ra, ok := body.(sizedReaderAt); ok {
		size, err := ra.Size()
		if err == nil {
			reader, err := inventory.NewParquetInventoryReaderFromReaderAt(ra, size)
			if err != nil {
				return nil, fmt.Errorf("create parquet reader from readerAt: %w", err)
			}

			return reader, nil
		}
	}
	// Fallback to stream-based reader if ReaderAt not available or Size() failed
	reader, err := inventory.NewParquetInventoryReaderFromStream(body, fileSize)
	if err != nil {
		return nil, fmt.Errorf("create parquet reader from stream: %w", err)
	}

	return reader, nil
}

// createCSVReader creates a CSV inventory reader.
func createCSVReader(body io.ReadCloser, key string, cfg chunkConfig) (inventory.InventoryReader, error) {
	csvCfg := inventory.CSVReaderConfig{
		KeyCol:        cfg.keyCol,
		SizeCol:       cfg.sizeCol,
		StorageCol:    cfg.storageCol,
		AccessTierCol: cfg.accessTierCol,
	}
	reader, err := inventory.NewCSVInventoryReaderFromStream(body, key, csvCfg)
	if err != nil {
		return nil, fmt.Errorf("create csv reader: %w", err)
	}

	return reader, nil
}

// processChunkToBatch downloads and parses a chunk, returning all objects as a batch.
// Uses the S3 Download Manager for parallel range downloads to maximize throughput.
func (p *Pipeline) processChunkToBatch(ctx context.Context, bucket, key string, cfg chunkConfig, capacityHint int) ([]objectRecord, *chunkTiming, error) {
	timing := &chunkTiming{}
	log := zerolog.Ctx(ctx)

	// Download phase using S3 Download Manager (parallel range downloads)
	body, dlResult, err := p.s3Client.DownloadObject(ctx, bucket, key)
	if err != nil {
		return nil, nil, fmt.Errorf("download object: %w", err)
	}
	timing.downloadDuration = dlResult.Duration

	// Log download details
	log.Debug().
		Str("chunk_key", key).
		Str("bytes_downloaded", humanfmt.Bytes(dlResult.BytesDownloaded)).
		Str("download_duration", humanfmt.Duration(dlResult.Duration)).
		Int("concurrency", dlResult.Concurrency).
		Str("part_size", humanfmt.Bytes(dlResult.PartSize)).
		Msg("chunk downloaded")

	// Create reader (parse header/schema)
	parseStart := time.Now()
	reader, err := createInventoryReader(body, key, cfg)
	if err != nil {
		return nil, nil, err
	}
	defer reader.Close()

	objects := make([]objectRecord, 0, capacityHint)

	for {
		select {
		case <-ctx.Done():
			return nil, nil, fmt.Errorf("chunk processing cancelled: %w", ctx.Err())
		default:
		}

		row, err := reader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, nil, fmt.Errorf("read inventory row: %w", err)
		}

		if row.Key == "" {
			continue
		}

		tierID := tiers.Resolve(cfg.tierMapping.FromS3(row.StorageClass, row.AccessTier), row.Size)
		objects = append(objects, objectRecord{
			key:    row.Key,
			size:   row.Size,
			tierID: tierID,
		})
		timing.totalBytes += int64(row.Size)
	}
	timing.parseDuration = time.Since(parseStart)
	timing.objectCount = len(objects)

	// Log chunk timing at debug level
	log.Debug().
		Str("chunk_key", key).
		Int("objects_count", timing.objectCount).
		Dur("download_ms", timing.downloadDuration).
		Dur("parse_ms", timing.parseDuration).
		Msg("chunk processed")

	return objects, timing, nil
}

// flushAggregator drains the aggregator to a sorted run file. Safe
// to call concurrently from multiple worker goroutines — the only
// shared state (p.runFiles + p.runCount) is guarded by p.runFilesMu;
// the actual sort + write is per-goroutine-local until the append.
func (p *Pipeline) flushAggregator(ctx context.Context, agg *Aggregator) error {
	log := zerolog.Ctx(ctx)
	start := time.Now()

	rows := agg.Drain()
	if len(rows) == 0 {
		return nil
	}

	// Use compressed runs if configured (default: true)
	ext := ".bin"
	if p.config.UseCompressedRuns {
		ext = ".crun"
	}
	p.runFilesMu.Lock()
	runIdx := p.runCount
	p.runCount++
	p.runFilesMu.Unlock()
	runPath := filepath.Join(p.tempDir, fmt.Sprintf("run_%04d%s", runIdx, ext))

	// Run file buffer: a fixed 4 MiB is well above the syscall sweet
	// spot for sequential writes and bounded per-worker, so no need to
	// derive it from a fractional memory partition.
	const bufferSize = 4 * 1024 * 1024

	var writeErr error
	if p.config.UseCompressedRuns {
		writer, err := NewCompressedRunWriter(runPath, CompressedRunWriterOptions{
			BufferSize:       int(bufferSize),
			CompressionLevel: CompressionFastest, // Optimize for write speed during ingest
		})
		if err != nil {
			return fmt.Errorf("create compressed run file: %w", err)
		}
		if err := writer.WriteSorted(rows); err != nil {
			writer.Close()
			os.Remove(runPath)

			return fmt.Errorf("write sorted: %w", err)
		}
		writeErr = writer.Close()
	} else {
		writer, err := NewRunFileWriter(runPath, int(bufferSize))
		if err != nil {
			return fmt.Errorf("create run file: %w", err)
		}
		if err := writer.WriteSorted(rows); err != nil {
			writer.Close()
			os.Remove(runPath)

			return fmt.Errorf("write sorted: %w", err)
		}
		writeErr = writer.Close()
	}

	if writeErr != nil {
		os.Remove(runPath)

		return fmt.Errorf("close run file: %w", writeErr)
	}

	p.runFilesMu.Lock()
	p.runFiles = append(p.runFiles, runPath)
	p.runFilesMu.Unlock()
	p.flushCount.Add(1)

	// Log flush with memory stats. Empirically each PrefixStats slot uses
	// ~288 bytes (depth + counts + per-tier arrays) inside the aggregator.
	const bytesPerAggregatorEntry = 288
	aggMemory := int64(len(rows)) * bytesPerAggregatorEntry
	flushDuration := time.Since(start)
	log.Info().
		Int("run_index", runIdx).
		Str("prefixes", humanfmt.Count(int64(len(rows)))).
		Int("prefixes_count", len(rows)).
		Str("aggregator_memory", humanfmt.Bytes(aggMemory)).
		Str("buffer_size", humanfmt.Bytes(bufferSize)).
		Bool("compressed", p.config.UseCompressedRuns).
		Str("duration", humanfmt.Duration(flushDuration)).
		Dur("duration_ms", flushDuration).
		Msg("run file written")

	return nil
}

// mergeBuildResult is the output of runMergeBuildPhase.
type mergeBuildResult struct {
	PrefixCount uint64
	MaxDepth    uint32
}

// runMergeBuildPhase merges run files and builds the index.
func (p *Pipeline) runMergeBuildPhase(ctx context.Context, outDir string) (mergeBuildResult, error) {
	log := zerolog.Ctx(ctx)

	// Check for cancellation before starting
	select {
	case <-ctx.Done():
		return mergeBuildResult{}, fmt.Errorf("merge phase cancelled: %w", ctx.Err())
	default:
	}

	if len(p.runFiles) == 0 {
		log.Info().Msg("no run files to merge, creating empty index")
		builder, err := NewIndexBuilder(outDir, p.config.TempDir, p.config.UseSegmentEncoding)
		if err != nil {
			return mergeBuildResult{}, fmt.Errorf("create index builder: %w", err)
		}
		if err := builder.FinalizeWithContext(ctx); err != nil {
			return mergeBuildResult{}, fmt.Errorf("finalize empty index: %w", err)
		}

		return mergeBuildResult{}, nil
	}

	// Per-reader buffer for the k-way merge. Bounded constant rather
	// than a fraction of a budget — each reader's working set is
	// dominated by its inflight prefix-row decode, not this buffer.
	numRunFiles := len(p.runFiles)
	const perReaderBuffer int64 = 1 * 1024 * 1024

	// Use parallel merge for multiple run files
	numWorkers := p.config.NumMergeWorkers
	if numWorkers <= 0 {
		numWorkers = 1
	}
	maxFanIn := p.config.MaxMergeFanIn
	if maxFanIn <= 1 {
		maxFanIn = 8
	}

	log.Info().
		Int("run_files_count", numRunFiles).
		Int("merge_workers_count", numWorkers).
		Int("max_fan_in", maxFanIn).
		Int64("per_reader_buffer_kb", perReaderBuffer/1024).
		Bool("compressed", p.config.UseCompressedRuns).
		Msg("merge phase starting")

	// Use parallel merger if we have multiple run files
	var finalRunPath string
	var cleanupIntermediates func()

	if numRunFiles > 1 {
		parallelMerger := NewParallelMerger(ParallelMergeConfig{
			NumWorkers:       numWorkers,
			MaxFanIn:         maxFanIn,
			BufferSize:       int(perReaderBuffer),
			TempDir:          p.tempDir,
			UseCompression:   p.config.UseCompressedRuns,
			CompressionLevel: CompressionFastest,
			OnRoundComplete: func(_, remaining int) {
				// Emit progress so the SSE stream isn't silent during
				// long multi-round merges. Done = remaining files
				// reduced from the original; total = numRunFiles.
				p.reportProgress("building", int64(numRunFiles-remaining), int64(numRunFiles))
			},
		})

		var mergeErr error
		finalRunPath, mergeErr = parallelMerger.MergeAll(ctx, p.runFiles)
		if mergeErr != nil {
			return mergeBuildResult{}, fmt.Errorf("parallel merge: %w", mergeErr)
		}

		stats := parallelMerger.Statistics()
		log.Info().
			Int("merge_rounds", stats.Rounds).
			Str("merge_duration", humanfmt.Duration(stats.TotalMergeTime)).
			Str("bytes_written", humanfmt.Bytes(stats.BytesWritten)).
			Msg("parallel merge complete")

		cleanupIntermediates = func() {
			_ = parallelMerger.CleanupIntermediateFiles()
			// Remove original run files
			for _, path := range p.runFiles {
				os.Remove(path)
			}
			// Remove final merged file
			os.Remove(finalRunPath)
		}
	} else {
		// Single run file, no merge needed
		finalRunPath = p.runFiles[0]
		cleanupIntermediates = func() {
			os.Remove(finalRunPath)
		}
	}

	defer cleanupIntermediates()

	// Open final merged run for index building
	reader, err := OpenRunFileAuto(finalRunPath, int(perReaderBuffer))
	if err != nil {
		return mergeBuildResult{}, fmt.Errorf("open merged run: %w", err)
	}

	// Create iterator adapter for index builder
	mergeIter := &singleRunIterator{reader: reader}

	// Use prefix count from run file header to pre-size index builder arrays
	prefixCount := reader.Count()
	log.Debug().
		Uint64("prefix_count", prefixCount).
		Msg("index build starting")

	builder, err := NewIndexBuilderWithCapacity(outDir, p.config.TempDir, prefixCount, p.config.UseSegmentEncoding)
	if err != nil {
		reader.Close()

		return mergeBuildResult{}, fmt.Errorf("create index builder: %w", err)
	}

	if err := builder.AddAllWithContext(ctx, mergeIter); err != nil {
		builder.cleanup()

		return mergeBuildResult{}, fmt.Errorf("build index: %w", err)
	}

	if err := builder.FinalizeWithContext(ctx); err != nil {
		return mergeBuildResult{}, fmt.Errorf("finalize index: %w", err)
	}

	return mergeBuildResult{PrefixCount: builder.Count(), MaxDepth: builder.MaxDepth()}, nil
}

// singleRunIterator wraps a RunReader to implement the iterator interface expected by IndexBuilder.
// Reuses one PrefixRow across all Next() calls — caller must consume the
// returned row before calling Next again. IndexBuilder.Add does this.
type singleRunIterator struct {
	reader RunReader
	row    PrefixRow
}

func (s *singleRunIterator) Next() (*PrefixRow, error) {
	if err := s.reader.ReadInto(&s.row); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, io.EOF
		}

		return nil, fmt.Errorf("read from run: %w", err)
	}

	return &s.row, nil
}

func (s *singleRunIterator) Remaining() uint64 {
	return s.reader.Count() - s.reader.ReadCount()
}

func (s *singleRunIterator) Close() error {
	if err := s.reader.Close(); err != nil {
		return fmt.Errorf("close run reader: %w", err)
	}

	return nil
}

func (s *singleRunIterator) RemoveAll() error {
	if err := s.reader.Remove(); err != nil {
		return fmt.Errorf("remove run file: %w", err)
	}

	return nil
}

// cleanup removes temporary files.
func (p *Pipeline) cleanup() {
	for _, path := range p.runFiles {
		os.Remove(path)
	}
	if p.tempDir != "" && p.config.TempDir == "" {
		// Only remove if we created the temp dir
		os.RemoveAll(p.tempDir)
	}
}
