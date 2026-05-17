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

	"github.com/eunmann/s3-inv-db/internal/memdiag"
	"github.com/eunmann/s3-inv-db/pkg/extsort/events"
	"github.com/eunmann/s3-inv-db/pkg/humanfmt"
	"github.com/eunmann/s3-inv-db/pkg/inventory"
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
	if p.config.Observe.OnProgress != nil {
		p.config.Observe.OnProgress(name, 0, 0)
	}
}

// reportProgress emits quantitative progress within the current phase.
// Called from ingest after each chunk.
func (p *Pipeline) reportProgress(phase string, done, total int64) {
	if p.config.Observe.OnProgress != nil {
		p.config.Observe.OnProgress(phase, done, total)
	}
}

// publish emits an event on the configured bus, if any. Cheap when
// no bus is set (single nil check). Sets the timestamp if missing.
func (p *Pipeline) publish(ev events.Event) {
	if p.config.Observe.EventBus == nil {
		return
	}
	if ev.Time.IsZero() {
		ev.Time = time.Now()
	}
	p.config.Observe.EventBus.Publish(ev)
}

// timedIngestPhase wraps runIngestPhase with start/end events and
// returns the duration. Extracted from Run to keep that function
// under the funlen ceiling now that pub-sub events bloat it.
func (p *Pipeline) timedIngestPhase(ctx context.Context, log *zerolog.Logger, manifestURI string) (time.Duration, error) {
	p.setPhase("downloading")
	p.publish(events.Event{Stage: events.StagePipeline, Type: events.EvtStageStart, Payload: events.StageTiming{Stage: events.StageDownload}})
	start := time.Now()
	if err := p.runIngestPhase(ctx, manifestURI); err != nil {
		if errors.Is(err, context.Canceled) {
			log.Warn().Msg("pipeline cancelled during ingest phase")
		}

		return 0, fmt.Errorf("ingest phase: %w", err)
	}
	d := time.Since(start)
	p.publish(events.Event{
		Stage: events.StagePipeline,
		Type:  events.EvtStageEnd,
		Payload: events.StageTiming{
			Stage:    events.StageDownload,
			Duration: d,
			Rows:     uint64(p.objectsProcessed.Load()),
			Bytes:    uint64(p.bytesProcessed.Load()),
		},
	})

	return d, nil
}

// timedMergeBuildPhase wraps runMergeBuildPhase with start/end
// events; counterpart to timedIngestPhase.
func (p *Pipeline) timedMergeBuildPhase(ctx context.Context, log *zerolog.Logger, outDir string) (mergeBuildResult, time.Duration, error) {
	p.setPhase("building")
	p.publish(events.Event{Stage: events.StagePipeline, Type: events.EvtStageStart, Payload: events.StageTiming{Stage: events.StageMerge}})
	start := time.Now()
	res, err := p.runMergeBuildPhase(ctx, outDir)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			log.Warn().Msg("pipeline cancelled during merge phase")
		}

		return mergeBuildResult{}, 0, fmt.Errorf("merge/build phase: %w", err)
	}
	d := time.Since(start)
	p.publish(events.Event{
		Stage: events.StagePipeline,
		Type:  events.EvtStageEnd,
		Payload: events.StageTiming{
			Stage:    events.StageMerge,
			Duration: d,
			Rows:     res.PrefixCount,
		},
	})

	return res, d, nil
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

	ingestDuration, err := p.timedIngestPhase(ctx, log, manifestURI)
	if err != nil {
		return nil, err
	}

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

	mergeRes, mergeDuration, err := p.timedMergeBuildPhase(ctx, log, outDir)
	if err != nil {
		return nil, err
	}
	prefixCount, maxDepth := mergeRes.PrefixCount, mergeRes.MaxDepth

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

	destBucket, err := manifest.DestinationBucketName()
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
			if err := p.runChunkWorker(ctx, workerID, cfg.numWorkers, jobs, totalChunks); err != nil {
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
// pull chunks off the jobs channel, stream-parse each chunk row-by-row
// directly into the local aggregator (no intermediate slice), flush to
// a private run file when memory pressure hits. At end-of-input
// (channel closed), flush the residual aggregator state to a final
// run file.
func (p *Pipeline) runChunkWorker(ctx context.Context, workerID, numWorkers int, jobs <-chan chunkJob, totalChunks int) error {
	const initialAggCapacity = 10_000
	agg := NewAggregator(initialAggCapacity, p.config.MaxDepth)
	defer func() {
		if agg.PrefixCount() > 0 {
			if err := p.flushAggregator(ctx, agg, workerID); err != nil {
				zerolog.Ctx(ctx).Error().
					Int("worker_id", workerID).
					Err(err).
					Msg("final flush failed")
			}
		}
	}()

	// Track per-worker idle time so E2 utilization is exact rather
	// than inferred from logs.
	publishIdle := func(reason string) {
		p.publish(events.Event{
			Stage:   events.StageAggregator,
			Type:    events.EvtWorkerIdle,
			Payload: events.WorkerState{WorkerID: workerID, Reason: reason},
		})
	}
	publishBusy := func(reason string) {
		p.publish(events.Event{
			Stage:   events.StageAggregator,
			Type:    events.EvtWorkerBusy,
			Payload: events.WorkerState{WorkerID: workerID, Reason: reason},
		})
	}

	for {
		publishIdle("waiting_jobs")
		var job chunkJob
		var ok bool
		select {
		case <-ctx.Done():
			return fmt.Errorf("worker %d cancelled: %w", workerID, ctx.Err())
		case job, ok = <-jobs:
			if !ok {
				return nil
			}
		}
		publishBusy("processing_chunk")

		if err := p.streamChunkIntoAggregator(ctx, job, agg, workerID); err != nil {
			return fmt.Errorf("chunk %d: %w", job.index, err)
		}
		p.chunksProcessed.Add(1)

		if ShouldWorkerFlush(uint64(agg.EstimatedMemoryUsage()), debug.SetMemoryLimit(-1), numWorkers) {
			p.memTracker.LogNow("pre_flush")
			if err := p.flushAggregator(ctx, agg, workerID); err != nil {
				return fmt.Errorf("worker %d flush: %w", workerID, err)
			}
			p.memTracker.LogNow("post_flush")
		}
		_ = totalChunks
	}
}

// streamChunkIntoAggregator downloads one chunk and pumps rows
// directly from the parquet/CSV reader into the worker's aggregator.
// No intermediate slice — the per-chunk objectRecord buffer that the
// old processChunkToBatch path materialised is eliminated, saving
// chunk-size × N-workers of transient heap.
func (p *Pipeline) streamChunkIntoAggregator(ctx context.Context, job chunkJob, agg *Aggregator, workerID int) error {
	log := zerolog.Ctx(ctx)

	body, dlResult, err := p.s3Client.DownloadObject(ctx, job.bucket, job.key)
	if err != nil {
		return fmt.Errorf("download object: %w", err)
	}

	parseStart := time.Now()
	reader, err := createInventoryReader(body, job.key, job.config)
	if err != nil {
		return err
	}
	defer reader.Close()

	const ctxCheckInterval = 4096
	var (
		rowsParsed int64
		bytesAdded int64
		i          int
	)
	for {
		if i%ctxCheckInterval == 0 {
			select {
			case <-ctx.Done():
				return fmt.Errorf("chunk processing cancelled: %w", ctx.Err())
			default:
			}
		}
		i++
		row, err := reader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("read inventory row: %w", err)
		}
		if row.Key == "" {
			continue
		}
		tierID := tiers.Resolve(job.config.tierMapping.FromS3(row.StorageClass, row.AccessTier), row.Size)
		agg.AddObject(row.Key, row.Size, tierID)
		rowsParsed++
		bytesAdded += int64(row.Size)
	}

	p.objectsProcessed.Add(rowsParsed)
	p.bytesProcessed.Add(bytesAdded)

	p.publish(events.Event{
		Stage: events.StageParse,
		Type:  events.EvtBatchCommitted,
		Payload: events.BatchCommitted{
			WorkerID: workerID,
			Rows:     uint64(rowsParsed),
			Bytes:    uint64(bytesAdded),
		},
	})

	log.Debug().
		Str("chunk_key", job.key).
		Int64("objects_count", rowsParsed).
		Str("bytes_downloaded", humanfmt.Bytes(dlResult.BytesDownloaded)).
		Dur("download_ms", dlResult.Duration).
		Dur("parse_ms", time.Since(parseStart)).
		Msg("chunk streamed into aggregator")

	return nil
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

// flushAggregator drains the aggregator to a sorted run file. Safe
// to call concurrently from multiple worker goroutines — the only
// shared state (p.runFiles + p.runCount) is guarded by p.runFilesMu;
// the actual sort + write is per-goroutine-local until the append.
//
// WorkerID is included in spill events so listeners can attribute
// disk I/O to specific workers (utilization analysis).
func (p *Pipeline) flushAggregator(ctx context.Context, agg *Aggregator, workerID int) error {
	log := zerolog.Ctx(ctx)
	start := time.Now()

	rows := agg.Drain()
	if len(rows) == 0 {
		return nil
	}
	p.publish(events.Event{
		Stage:   events.StageSpill,
		Type:    events.EvtSpillStarted,
		Payload: events.WorkerState{WorkerID: workerID, Reason: "spilling"},
	})

	// Use compressed runs if configured (default: true)
	ext := ".bin"
	if p.config.Merge.UseCompressedRuns {
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
	if p.config.Merge.UseCompressedRuns {
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

	// Same estimate as Aggregator.EstimatedMemoryUsage — kept as a
	// log-only post-drain footprint approximation.
	const bytesPerAggregatorEntry = 288
	aggMemory := int64(len(rows)) * bytesPerAggregatorEntry
	flushDuration := time.Since(start)

	p.publish(events.Event{
		Stage: events.StageSpill,
		Type:  events.EvtSpillCompleted,
		Payload: events.SpillCompleted{
			WorkerID:   workerID,
			Rows:       uint64(len(rows)),
			Bytes:      aggMemory,
			Duration:   flushDuration,
			OutputPath: runPath,
		},
	})

	log.Info().
		Int("run_index", runIdx).
		Str("prefixes", humanfmt.Count(int64(len(rows)))).
		Int("prefixes_count", len(rows)).
		Str("aggregator_memory", humanfmt.Bytes(aggMemory)).
		Str("buffer_size", humanfmt.Bytes(bufferSize)).
		Bool("compressed", p.config.Merge.UseCompressedRuns).
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

// runMergePhase returns a streaming RowIterator over the K-way merge
// of p.runFiles, plus a cleanup closure. Single-run case wraps the
// lone run file directly. Multi-run case uses
// ParallelMerger.MergeAllToIterator which avoids writing a final
// merged file to disk (I3 win).
//
//nolint:ireturn // RowIterator is the right shape — callers don't care which variant underneath
func (p *Pipeline) runMergePhase(
	ctx context.Context,
	log *zerolog.Logger,
	numRunFiles, numWorkers, maxFanIn int,
	perReaderBuffer int64,
) (RowIterator, func() error, error) {
	if numRunFiles == 1 {
		reader, err := OpenRunFileAuto(p.runFiles[0], int(perReaderBuffer))
		if err != nil {
			return nil, nil, fmt.Errorf("open single run: %w", err)
		}

		return &singleRunIterator{reader: reader}, func() error {
			if err := reader.Close(); err != nil {
				os.Remove(p.runFiles[0])

				return fmt.Errorf("close single run: %w", err)
			}
			os.Remove(p.runFiles[0])

			return nil
		}, nil
	}
	parallelMerger := NewParallelMerger(ParallelMergeConfig{
		NumWorkers:       numWorkers,
		MaxFanIn:         maxFanIn,
		BufferSize:       int(perReaderBuffer),
		TempDir:          p.tempDir,
		UseCompression:   p.config.Merge.UseCompressedRuns,
		CompressionLevel: CompressionFastest,
		OnRoundComplete: func(round, remaining int) {
			p.reportProgress("building", int64(numRunFiles-remaining), int64(numRunFiles))
			p.publish(events.Event{
				Stage: events.StageMerge,
				Type:  events.EvtRoundCompleted,
				Payload: events.BatchCommitted{
					WorkerID: round,
					Rows:     uint64(numRunFiles - remaining),
				},
			})
		},
	})
	iter, mergerCleanup, err := parallelMerger.MergeAllToIterator(ctx, p.runFiles)
	if err != nil {
		return nil, nil, fmt.Errorf("parallel merge: %w", err)
	}
	stats := parallelMerger.Statistics()
	log.Info().
		Int("merge_rounds", stats.Rounds).
		Str("merge_duration", humanfmt.Duration(stats.TotalMergeTime)).
		Str("bytes_written", humanfmt.Bytes(stats.BytesWritten)).
		Msg("parallel merge → streaming iterator")
	cleanup := func() error {
		err := mergerCleanup()
		for _, path := range p.runFiles {
			os.Remove(path)
		}

		return err
	}

	return iter, cleanup, nil
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
		builder, err := NewIndexBuilder(outDir, p.config.TempDir)
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
	numWorkers := p.config.Merge.NumWorkers
	if numWorkers <= 0 {
		numWorkers = 1
	}
	maxFanIn := p.config.Merge.MaxFanIn
	if maxFanIn <= 1 {
		maxFanIn = 8
	}

	log.Info().
		Int("run_files_count", numRunFiles).
		Int("merge_workers_count", numWorkers).
		Int("max_fan_in", maxFanIn).
		Int64("per_reader_buffer_kb", perReaderBuffer/1024).
		Bool("compressed", p.config.Merge.UseCompressedRuns).
		Msg("merge phase starting")

	mergeIter, cleanupIntermediates, err := p.runMergePhase(ctx, log, numRunFiles, numWorkers, maxFanIn, perReaderBuffer)
	if err != nil {
		return mergeBuildResult{}, err
	}

	defer func() {
		if cleanupErr := cleanupIntermediates(); cleanupErr != nil {
			log.Warn().Err(cleanupErr).Msg("merge intermediates cleanup")
		}
	}()

	// Use prefix count if the iterator can report it (single-run +
	// streaming K-way iterator both implement Remaining()); pass 0
	// when unknown and let the builder grow incrementally.
	prefixCount := iteratorRemaining(mergeIter)
	log.Debug().
		Uint64("prefix_count", prefixCount).
		Msg("index build starting")

	builder, err := NewIndexBuilderWithCapacity(outDir, p.config.TempDir, prefixCount)
	if err != nil {
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

// remainingReporter is implemented by iterators that can report an
// upper-bound row count up front (singleRunIterator from the file
// header, MergeIterator from the sum of underlying readers). Used
// to pre-size the IndexBuilder.
type remainingReporter interface {
	Remaining() uint64
}

// iteratorRemaining returns the iterator's reported remaining count
// if it implements remainingReporter, else 0.
func iteratorRemaining(it RowIterator) uint64 {
	if r, ok := it.(remainingReporter); ok {
		return r.Remaining()
	}

	return 0
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
