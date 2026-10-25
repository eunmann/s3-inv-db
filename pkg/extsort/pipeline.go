package extsort

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/eunmann/s3-inv-db/internal/memdiag"
	"github.com/eunmann/s3-inv-db/pkg/extsort/events"
	"github.com/eunmann/s3-inv-db/pkg/humanfmt"
	"github.com/eunmann/s3-inv-db/pkg/s3fetch"
	"github.com/eunmann/s3-inv-db/pkg/s3inventory"
	"github.com/eunmann/s3-inv-db/pkg/sysmem"
	"github.com/eunmann/s3-inv-db/pkg/tiers"
	"github.com/rs/zerolog"
)

// Pipeline orchestrates the external sort build process.
// It streams S3 inventory data, aggregates in bounded memory,
// spills to sorted run files, and merges to build the final index.
type Pipeline struct {
	config     Config
	startTime  time.Time
	s3Client   *s3fetch.Client
	memTracker *memdiag.Tracker
	tempDir    string
	runFiles   []string
	// memRuns are workers' last aggregator states, sorted and kept in
	// memory for the final merge (see keepResidual).
	memRuns          []*memRun
	runCount         int
	chunksProcessed  atomic.Int64
	objectsProcessed atomic.Int64
	bytesProcessed   atomic.Int64
	flushCount       atomic.Int64
	presentTierMask  atomic.Uint32
	runFilesMu       sync.Mutex
}

// presentTiers expands the tier mask accumulated across all ingest
// workers into the ascending tier-ID list used to size the sparse
// tier-stats rows. Empty when no objects were ingested.
func (p *Pipeline) presentTiers() []tiers.ID {
	return tierIDsFromMask(uint16(p.presentTierMask.Load()))
}

// Result holds the pipeline execution result.
type Result struct {
	ChunksProcessed  int
	ObjectsProcessed int64
	PrefixCount      uint64
	MaxDepth         uint32
	RunFilesCreated  int
	// MemoryRuns counts the workers' final states merged from memory
	// instead of run files.
	MemoryRuns int
	Duration   time.Duration
}

// setPhase notifies the memory tracker and the OnProgress callback
// of a phase transition (done=total=0).
func (p *Pipeline) setPhase(name string) {
	p.memTracker.SetPhase(name)
	if p.config.Observe.OnProgress != nil {
		p.config.Observe.OnProgress(name, 0, 0)
	}
}

// reportProgress emits quantitative progress within the current phase.
func (p *Pipeline) reportProgress(phase string, done, total int64) {
	if p.config.Observe.OnProgress != nil {
		p.config.Observe.OnProgress(phase, done, total)
	}
}

// publish emits ev on the configured bus, if any.
func (p *Pipeline) publish(ev events.Event) {
	if p.config.Observe.EventBus == nil {
		return
	}
	if ev.Time.IsZero() {
		ev.Time = time.Now()
	}
	p.config.Observe.EventBus.Publish(ev)
}

// timedIngestPhase wraps runIngestPhase with start/end events.
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

// timedMergeBuildPhase wraps runMergeBuildPhase with start/end events.
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
			Stage:        events.StageMerge,
			Duration:     d,
			Rows:         res.PrefixCount,
			BytesWritten: res.MergeBytesWritten,
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

	// Start memory diagnostics. Stop intentionally uses its own
	// background context (with a bounded shutdown timeout) so pprof's
	// HTTP drain isn't aborted when the pipeline's ctx is cancelled.
	p.memTracker.Start()
	defer p.memTracker.Stop() //nolint:contextcheck // own shutdown ctx by design
	p.setPhase("initializing")

	if err := reclaimStaleDirs(log, outDir); err != nil {
		return nil, err
	}
	if err := checkOutDir(outDir); err != nil {
		return nil, err
	}

	tempDir := p.config.TempDir
	if tempDir == "" {
		var err error
		tempDir, err = newScratchDir(outDir)
		if err != nil {
			return nil, err
		}
	}
	p.tempDir = tempDir

	// The index is built in a staging sibling and renamed into place
	// only after it is complete, so a failure or cancellation never
	// leaves a partial index at outDir or touches what was there.
	stage, err := newStagingDir(outDir)
	if err != nil {
		p.cleanup(log)

		return nil, err
	}

	success := false
	defer func() {
		if !success {
			p.cleanup(log)
			logCleanupErr(log, "remove staging dir", stage, os.RemoveAll(stage))
		}
	}()

	// Auto-detect memory budget if caller hasn't installed one.
	memoryLimit := readMemoryLimit()
	memSource := "configured"
	if memoryLimit >= unsetMemoryLimit {
		res := sysmem.ApplyMemoryLimit(sysmem.DefaultMemoryLimitFraction)
		memoryLimit = readMemoryLimit()
		memSource = string(res.Source)
	}
	log.Info().
		Str("manifest_uri", manifestURI).
		Int64("memory_limit_bytes", memoryLimit).
		Str("memory_limit_source", memSource).
		Str("aggregator_cap", humanfmt.BytesUint64(AggregatorCap(memoryLimit))).
		Msg("pipeline starting")

	ingestDuration, err := p.timedIngestPhase(ctx, log, manifestURI)
	if err != nil {
		return nil, err
	}

	p.memTracker.LogNow("post_ingest")

	log.Info().
		Int("run_files_count", len(p.runFiles)).
		Str("objects", humanfmt.Count(p.objectsProcessed.Load())).
		Int64("objects_count", p.objectsProcessed.Load()).
		Int64("flushes_count", p.flushCount.Load()).
		Str("duration", humanfmt.Duration(ingestDuration)).
		Dur("duration_ms", ingestDuration).
		Msg("ingest phase complete")

	mergeRes, mergeDuration, err := p.timedMergeBuildPhase(ctx, log, stage)
	if err != nil {
		return nil, err
	}
	if err := publishIndex(log, stage, outDir); err != nil {
		return nil, err
	}
	prefixCount, maxDepth := mergeRes.PrefixCount, mergeRes.MaxDepth

	p.memTracker.LogNow("post_merge")

	log.Info().
		Str("prefixes", humanfmt.CountUint64(prefixCount)).
		Uint64("prefixes_count", prefixCount).
		Uint32("max_depth", maxDepth).
		Str("duration", humanfmt.Duration(mergeDuration)).
		Dur("duration_ms", mergeDuration).
		Msg("merge phase complete")

	p.cleanup(log)
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
		MemoryRuns:       len(p.memRuns),
		Duration:         duration,
	}, nil
}

// chunkConfig holds configuration for processing a chunk.
type chunkConfig struct {
	tierMapping     *tiers.Mapping
	format          s3fetch.InventoryFormat
	keyCol          int
	sizeCol         int
	storageCol      int
	accessTierCol   int
	deleteMarkerCol int
	fileSize        int64
}

// chunkJob represents a chunk to be processed by a worker.
type chunkJob struct {
	config chunkConfig
	bucket string
	key    string
	index  int
}

// fetchedChunk is a chunk downloaded ahead of its parse, or the error
// that stopped the download.
type fetchedChunk struct {
	err    error
	body   *s3fetch.DownloadedObject
	result *s3fetch.DownloadResult
	job    chunkJob
}

// objectRecord holds a single object's data for aggregation.
type objectRecord struct {
	key    string
	size   uint64
	tierID tiers.ID
}

// ingestConfig holds configuration for the ingest phase.
type ingestConfig struct {
	manifest        *s3fetch.Manifest
	tierMapping     *tiers.Mapping
	destBucket      string
	format          s3fetch.InventoryFormat
	keyCol          int
	sizeCol         int
	storageCol      int
	accessTierCol   int
	deleteMarkerCol int
	numWorkers      int
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

// csvColumns are a CSV inventory's column indexes from its manifest's
// schema; -1 for a column it lacks.
type csvColumns struct {
	key, size, storage, accessTier, deleteMarker int
}

// manifestColumns reads the column indexes a CSV inventory's rows need
// from its manifest. A Parquet manifest's fileSchema is a Parquet message,
// not a column list, and its reader takes the columns from each file's
// own schema, so it has none here.
func manifestColumns(manifest *s3fetch.Manifest, format s3fetch.InventoryFormat) (csvColumns, error) {
	if format != s3fetch.InventoryFormatCSV {
		return csvColumns{key: -1, size: -1, storage: -1, accessTier: -1, deleteMarker: -1}, nil
	}
	key, err := manifest.KeyColumnIndex()
	if err != nil {
		return csvColumns{}, fmt.Errorf("get key column: %w", err)
	}
	size, err := manifest.SizeColumnIndex()
	if err != nil {
		return csvColumns{}, fmt.Errorf("get size column: %w", err)
	}

	return csvColumns{
		key:          key,
		size:         size,
		storage:      manifest.StorageClassColumnIndex(),
		accessTier:   manifest.AccessTierColumnIndex(),
		deleteMarker: manifest.DeleteMarkerColumnIndex(),
	}, nil
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

	format, err := manifest.DetectFormat()
	if err != nil {
		return nil, fmt.Errorf("inventory format: %w", err)
	}
	cols, err := manifestColumns(manifest, format)
	if err != nil {
		return nil, err
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
		manifest:        manifest,
		format:          format,
		destBucket:      destBucket,
		keyCol:          cols.key,
		sizeCol:         cols.size,
		storageCol:      cols.storage,
		accessTierCol:   cols.accessTier,
		deleteMarkerCol: cols.deleteMarker,
		numWorkers:      numWorkers,
		tierMapping:     tiers.NewMapping(),
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
	// Unbuffered: a fetcher holding a downloaded chunk waits for a free
	// worker before it downloads another, so at most two chunk files per
	// worker sit in scratch, one parsing and one downloaded or
	// downloading.
	ready := make(chan fetchedChunk)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var fetchers, wg sync.WaitGroup
	errCh := make(chan error, cfg.numWorkers)
	p.startIngestProgressLogger(ctx, log, totalChunks)

	for range cfg.numWorkers {
		fetchers.Go(func() { p.runChunkFetcher(ctx, jobs, ready) })
	}
	go func() {
		fetchers.Wait()
		close(ready)
	}()
	for workerID := range cfg.numWorkers {
		wg.Go(func() {
			if err := p.runChunkWorker(ctx, workerID, cfg.numWorkers, ready); err != nil {
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

	if err := firstCause(errCh); err != nil {
		return fmt.Errorf("chunk worker: %w", err)
	}

	return nil
}

// firstCause picks the error to report from the workers' errors. One
// worker's failure cancels its siblings, which then report
// context.Canceled; joining them all would make every failure look like
// a cancellation (the CLI reports those as "cancelled by user"). The
// first error that is not a cancellation is the cause; a cancellation is
// returned only when every worker was cancelled.
func firstCause(errs <-chan error) error {
	var cancelled error
	for err := range errs {
		switch {
		case err == nil:
		case errors.Is(err, context.Canceled):
			if cancelled == nil {
				cancelled = err
			}
		default:
			return err
		}
	}

	return cancelled
}

// startIngestProgressLogger starts a background goroutine that logs
// per-N-chunk progress based on the atomic counters. Returns a
// channel that the caller closes to stop the logger.
// StartIngestProgressLogger launches a background ticker that emits a
// progress event roughly every 10% of total chunks. The goroutine
// exits when ctx is cancelled — the caller doesn't need a separate
// stop signal because the ingest loop's parent ctx is already cancelled
// on function return (via the deferred cancel paired with the
// errgroup).
func (p *Pipeline) startIngestProgressLogger(ctx context.Context, log *zerolog.Logger, totalChunks int) {
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
				format:          cfg.format,
				keyCol:          cfg.keyCol,
				sizeCol:         cfg.sizeCol,
				storageCol:      cfg.storageCol,
				accessTierCol:   cfg.accessTierCol,
				deleteMarkerCol: cfg.deleteMarkerCol,
				tierMapping:     cfg.tierMapping,
				fileSize:        file.Size,
			},
		}:
		}
	}
}

// runChunkFetcher downloads chunks ahead of the workers, so a worker's
// next chunk downloads while it parses the current one. A downloaded
// chunk that no worker takes before cancellation is removed.
func (p *Pipeline) runChunkFetcher(ctx context.Context, jobs <-chan chunkJob, ready chan<- fetchedChunk) {
	for job := range jobs {
		if ctx.Err() != nil {
			return
		}
		fetched := p.fetchChunk(ctx, job)
		select {
		case ready <- fetched:
		case <-ctx.Done():
			if fetched.body != nil {
				_ = fetched.body.Close()
			}

			return
		}
	}
}

// fetchChunk downloads one chunk to scratch and checks its size against
// the manifest.
func (p *Pipeline) fetchChunk(ctx context.Context, job chunkJob) fetchedChunk {
	body, result, err := p.s3Client.DownloadObjectTo(ctx, job.bucket, job.key, p.tempDir)
	if err != nil {
		return fetchedChunk{job: job, err: fmt.Errorf("download object: %w", err)}
	}
	if job.config.fileSize > 0 && result.BytesDownloaded != job.config.fileSize {
		_ = body.Close()

		return fetchedChunk{job: job, err: fmt.Errorf("%w: %s is %d bytes, the manifest says %d",
			ErrChunkSizeMismatch, job.key, result.BytesDownloaded, job.config.fileSize)}
	}

	return fetchedChunk{job: job, body: body, result: result}
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
// (channel closed), keep the residual aggregator state in memory for
// the final merge, or flush it to a final run file if the heap is
// under pressure.
func (p *Pipeline) runChunkWorker(ctx context.Context, workerID, numWorkers int, ready <-chan fetchedChunk) (err error) {
	const initialAggCapacity = 10_000
	agg := NewAggregator(initialAggCapacity, p.config.MaxDepth)
	defer func() {
		// The residue is kept or spilled only when the worker finished
		// cleanly; after any failure or cancellation the build is
		// abandoned and it would be thrown away. A failed final spill
		// fails the build: dropping it would lose those prefixes.
		if err != nil || agg.PrefixCount() <= 0 {
			return
		}
		if !heapUnderPressure(readMemoryLimit()) {
			p.keepResidual(agg)

			return
		}
		if flushErr := p.flushAggregator(ctx, agg, workerID); flushErr != nil {
			err = fmt.Errorf("worker %d final flush: %w", workerID, flushErr)
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
		var fetched fetchedChunk
		var ok bool
		select {
		case <-ctx.Done():
			return fmt.Errorf("worker %d cancelled: %w", workerID, ctx.Err())
		case fetched, ok = <-ready:
			if !ok {
				return nil
			}
		}
		if fetched.err != nil {
			return fmt.Errorf("chunk %d: %w", fetched.job.index, fetched.err)
		}
		publishBusy("processing_chunk")

		if err := p.streamChunkIntoAggregator(ctx, fetched, agg, workerID, numWorkers); err != nil {
			return fmt.Errorf("chunk %d: %w", fetched.job.index, err)
		}
		p.chunksProcessed.Add(1)

		if ShouldWorkerFlush(uint64(agg.EstimatedMemoryUsage()), readMemoryLimit(), numWorkers) {
			p.memTracker.LogNow("pre_flush")
			if err := p.flushAggregator(ctx, agg, workerID); err != nil {
				return fmt.Errorf("worker %d flush: %w", workerID, err)
			}
			p.memTracker.LogNow("post_flush")
		}
	}
}

// streamChunkIntoAggregator pumps a downloaded chunk's rows directly
// from the parquet/CSV reader into the worker's aggregator. No
// intermediate slice — the per-chunk objectRecord buffer that the old
// processChunkToBatch path materialised is eliminated, saving
// chunk-size × N-workers of transient heap.
func (p *Pipeline) streamChunkIntoAggregator(ctx context.Context, fetched fetchedChunk, agg *Aggregator, workerID, numWorkers int) error {
	log := zerolog.Ctx(ctx)
	job, body, dlResult := fetched.job, fetched.body, fetched.result

	// The Parquet reader reads the file through ReaderAt and never closes
	// it; closing here removes every chunk from scratch once parsed. The
	// CSV reader closes it first, and a second Close is a no-op.
	defer func() { _ = body.Close() }()

	parseStart := time.Now()
	var err error
	var reader s3inventory.Reader
	if job.config.format == s3fetch.InventoryFormatParquet {
		reader, err = openParquetReader(body, job.config.fileSize, p.tempDir)
	} else {
		reader, err = s3inventory.NewCSVReaderFromStream(body, job.key, s3inventory.CSVReaderConfig{
			KeyCol:          job.config.keyCol,
			SizeCol:         job.config.sizeCol,
			StorageCol:      job.config.storageCol,
			AccessTierCol:   job.config.accessTierCol,
			DeleteMarkerCol: job.config.deleteMarkerCol,
		})
	}
	if err != nil {
		return fmt.Errorf("open inventory reader: %w", err)
	}
	defer reader.Close()

	const (
		ctxCheckInterval = 4096
		// The in-chunk spill check lets a big chunk spill before it
		// outgrows the cap; checking every row would only add overhead.
		midChunkFlushInterval = 50_000
	)
	var (
		rowsParsed int64
		bytesAdded int64
		i          int
	)
	memLimit := readMemoryLimit()
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
		tierID := tiers.Resolve(job.config.tierMapping.FromS3(row.StorageClass, row.AccessTier), row.Size)
		agg.AddObject(row.Key, row.Size, tierID)
		rowsParsed++
		bytesAdded += int64(row.Size)
		if rowsParsed%midChunkFlushInterval == 0 &&
			ShouldWorkerFlush(uint64(agg.EstimatedMemoryUsage()), memLimit, numWorkers) {
			if err := p.flushAggregator(ctx, agg, workerID); err != nil {
				return fmt.Errorf("mid-chunk flush: %w", err)
			}
		}
	}

	if a := reader.Anomalies(); a.Total() > 0 {
		if !p.config.AllowAnomalies {
			return fmt.Errorf("%w in %s: %s", ErrInventoryAnomalies, job.key, a)
		}
		log.Warn().
			Str("chunk", job.key).
			Stringer("anomalies", a).
			Msg("inventory chunk: anomalous rows accepted (AllowAnomalies); totals differ from a direct scan")
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

// openParquetReader uses the *DownloadedObject's ReaderAt directly,
// avoiding a second temp-file copy. Falls back to stream-based open
// only when Size() reports an error (in practice impossible — the
// file was just written).
func openParquetReader(obj *s3fetch.DownloadedObject, fileSize int64, tempDir string) (*s3inventory.ParquetReader, error) {
	if size, err := obj.Size(); err == nil {
		r, openErr := s3inventory.NewParquetReaderFromReaderAt(obj, size)
		if openErr != nil {
			return nil, fmt.Errorf("parquet reader from readerAt: %w", openErr)
		}

		return r, nil
	}
	r, err := s3inventory.NewParquetReaderFromStream(obj, fileSize, tempDir)
	if err != nil {
		return nil, fmt.Errorf("parquet reader from stream: %w", err)
	}

	return r, nil
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

	// Fold this worker's tier mask into the pipeline-wide set. The mask
	// survives Drain, so ORing on every flush captures the worker's full
	// lifetime regardless of which flush is the last.
	p.presentTierMask.Or(uint32(agg.TierMask()))

	aggMemory := agg.EstimatedMemoryUsage()
	rows := agg.DrainSorted()
	if len(rows) == 0 {
		return nil
	}
	p.publish(events.Event{
		Stage:   events.StageSpill,
		Type:    events.EvtSpillStarted,
		Payload: events.WorkerState{WorkerID: workerID, Reason: "spilling"},
	})

	p.runFilesMu.Lock()
	runIdx := p.runCount
	p.runCount++
	p.runFilesMu.Unlock()
	runPath := filepath.Join(p.tempDir, fmt.Sprintf("run_%04d.crun", runIdx))

	// Run file buffer: a fixed 4 MiB is well above the syscall sweet
	// spot for sequential writes and bounded per-worker, so no need to
	// derive it from a fractional memory partition.
	const bufferSize = DefaultRunBufferSize

	writer, err := NewCompressedRunWriter(runPath, CompressedRunWriterOptions{
		BufferSize:       int(bufferSize),
		CompressionLevel: CompressionFastest, // Optimize for write speed during ingest
	})
	if err != nil {
		return fmt.Errorf("create run file: %w", err)
	}
	if err := writeAndCloseRun(writer, rows, runPath); err != nil {
		logCleanupErr(log, "remove failed run", runPath, os.Remove(runPath))

		return err
	}

	p.runFilesMu.Lock()
	p.runFiles = append(p.runFiles, runPath)
	p.runFilesMu.Unlock()
	p.flushCount.Add(1)
	// Under heap pressure the recycled rows are what holds the heap up:
	// keeping them would force a spill on every later check.
	if heapUnderPressure(readMemoryLimit()) {
		agg.Release()
	}

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
		Str("duration", humanfmt.Duration(flushDuration)).
		Dur("duration_ms", flushDuration).
		Msg("run file written")

	return nil
}

// keepResidual keeps a finished worker's last aggregator state, sorted,
// for the final merge to read in place: the rows are already in memory,
// and writing them to a run file only to read them back is most of a
// build's spill work when workers rarely spill before their last flush.
// They stay until the merge has read them, which is before the index is
// finalized; under heap pressure the worker spills them instead.
func (p *Pipeline) keepResidual(agg *Aggregator) {
	p.presentTierMask.Or(uint32(agg.TierMask()))
	rows := agg.DrainSorted()
	if len(rows) == 0 {
		return
	}
	p.runFilesMu.Lock()
	p.memRuns = append(p.memRuns, newMemRun(rows))
	p.runFilesMu.Unlock()
}

// mergeBuildResult is the output of runMergeBuildPhase.
type mergeBuildResult struct {
	PrefixCount       uint64
	MaxDepth          uint32
	MergeBytesWritten int64
}

// runMergePhase returns a streaming RowIterator over the K-way merge
// of p.runFiles and p.memRuns, plus a cleanup closure. A lone run,
// in memory or on disk, is wrapped directly. Otherwise the parallel
// merger merges both kinds and writes no final merged file.
func (p *Pipeline) runMergePhase(ctx context.Context, log *zerolog.Logger, numRunFiles, numWorkers, maxFanIn int, perReaderBuffer int64) (RowIterator, func() error, MergeStatistics, error) {
	if numRunFiles == 0 && len(p.memRuns) == 1 {
		run := p.memRuns[0]

		return &singleRunIterator{reader: run}, run.Close, MergeStatistics{}, nil
	}
	if numRunFiles == 1 && len(p.memRuns) == 0 {
		reader, err := OpenCompressedRunFile(p.runFiles[0], int(perReaderBuffer))
		if err != nil {
			return nil, nil, MergeStatistics{}, fmt.Errorf("open single run: %w", err)
		}

		return &singleRunIterator{reader: reader}, func() error {
			closeErr := reader.Close()
			logCleanupErr(log, "remove single run", p.runFiles[0], os.Remove(p.runFiles[0]))
			if closeErr != nil {
				return fmt.Errorf("close single run: %w", closeErr)
			}

			return nil
		}, MergeStatistics{}, nil
	}
	parallelMerger := NewParallelMerger(ParallelMergeConfig{
		NumWorkers:       numWorkers,
		MaxFanIn:         maxFanIn,
		BufferSize:       int(perReaderBuffer),
		TempDir:          p.tempDir,
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
	iter, mergerCleanup, err := parallelMerger.mergeAllToIterator(ctx, p.runFiles, p.memRuns)
	if err != nil {
		return nil, nil, MergeStatistics{}, fmt.Errorf("parallel merge: %w", err)
	}
	stats := parallelMerger.Statistics()
	log.Info().
		Int("merge_rounds", stats.Rounds).
		Int("merge_workers_count", stats.Workers).
		Int("final_fan_in", stats.FinalFanIn).
		Str("merge_duration", humanfmt.Duration(stats.TotalMergeTime)).
		Str("bytes_written", humanfmt.Bytes(stats.BytesWritten)).
		Msg("parallel merge → streaming iterator")
	// The merger owns the run files from here and deletes them.
	return iter, mergerCleanup, stats, nil
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

	if len(p.runFiles) == 0 && len(p.memRuns) == 0 {
		log.Info().Msg("no run files to merge, creating empty index")
		builder, err := NewIndexBuilder(outDir, p.tempDir)
		if err != nil {
			return mergeBuildResult{}, fmt.Errorf("create index builder: %w", err)
		}
		if err := p.buildIndex(ctx, builder, nil); err != nil {
			return mergeBuildResult{}, err
		}

		return mergeBuildResult{}, nil
	}

	// Per-reader buffer for the k-way merge. Bounded constant rather
	// than a fraction of a budget — each reader's working set is
	// dominated by its inflight prefix-row decode, not this buffer.
	numRunFiles := len(p.runFiles)
	const perReaderBuffer int64 = mergeReaderBufferSize

	merge := p.config.Merge.withDefaults()
	numWorkers, maxFanIn := merge.NumWorkers, merge.MaxFanIn

	log.Info().
		Int("run_files_count", numRunFiles).
		Int("memory_runs_count", len(p.memRuns)).
		Int("max_fan_in", maxFanIn).
		Int64("per_reader_buffer_kb", perReaderBuffer/1024).
		Msg("merge phase starting")

	mergeIter, cleanupIntermediates, mergeStats, err := p.runMergePhase(ctx, log, numRunFiles, numWorkers, maxFanIn, perReaderBuffer)
	if err != nil {
		return mergeBuildResult{}, err
	}

	defer func() {
		if cleanupErr := cleanupIntermediates(); cleanupErr != nil {
			log.Warn().Err(cleanupErr).Msg("merge intermediates cleanup")
		}
	}()

	// Upper bound: sums every spilled run's row count, so a prefix
	// touched in N runs is counted N times. IndexBuilder uses it only
	// as a capped hint for the initial build-file size.
	prefixCount := mergeIter.Remaining()
	log.Debug().
		Uint64("merge_iter_remaining", prefixCount).
		Msg("index build starting")

	builder, err := NewIndexBuilderWithCapacity(outDir, p.tempDir, prefixCount)
	if err != nil {
		return mergeBuildResult{}, fmt.Errorf("create index builder: %w", err)
	}

	// Decode the merge a few batches ahead of the builder. Deferred
	// after the cleanup, so it stops before the merge readers close.
	prefetch := newPrefetchIterator(mergeIter)
	defer prefetch.stop()
	// Wrap the prefetched merge so the IndexBuilder consumption emits
	// "building" progress every ~131k rows. Without this the bar stays
	// frozen at the start of the index-build phase (which is the
	// dominant fraction of wall clock for large inventories) until the
	// next merge round fires — sometimes never, if all run files merged
	// in a single round. Power-of-two interval lets the AND short-circuit.
	progressIter := &reportingIterator{
		inner:       prefetch,
		total:       int64(prefixCount),
		onProgress:  func(done, total int64) { p.reportProgress("building", done, total) },
		reportEvery: indexBuildProgressInterval,
	}
	if err := p.buildIndex(ctx, builder, progressIter); err != nil {
		return mergeBuildResult{}, err
	}

	return mergeBuildResult{
		PrefixCount:       builder.Count(),
		MaxDepth:          builder.MaxDepth(),
		MergeBytesWritten: mergeStats.BytesWritten,
	}, nil
}

// buildIndex feeds rows (nil for an empty index) into builder and
// finalizes it. Every failure releases the builder's writers and scratch
// files.
func (p *Pipeline) buildIndex(ctx context.Context, builder *IndexBuilder, rows RowIterator) error {
	if err := p.fillAndFinalize(ctx, builder, rows); err != nil {
		builder.cleanup()

		return err
	}

	return nil
}

func (p *Pipeline) fillAndFinalize(ctx context.Context, builder *IndexBuilder, rows RowIterator) error {
	if err := builder.SetPresentTiers(p.presentTiers()); err != nil {
		return fmt.Errorf("set present tiers: %w", err)
	}
	if rows != nil {
		if err := builder.AddAll(ctx, rows); err != nil {
			return fmt.Errorf("build index: %w", err)
		}
	}
	p.reportProgress("finalizing", 0, 0)
	if err := builder.FinalizeWithContext(ctx); err != nil {
		return fmt.Errorf("finalize index: %w", err)
	}

	return nil
}

// indexBuildProgressInterval is how often the index-build phase emits
// progress updates while consuming the merged iterator. Power of two
// (2^17 = 131072 rows) so the throttle check inside the per-row hot
// loop is a single AND. A 1M-prefix build fires ≈8 updates; a 100M
// build fires ≈800. Cheap either way; perceptible to the user.
const indexBuildProgressInterval int64 = 1 << 17

// reportingIterator wraps a RowIterator and fires onProgress every
// reportEvery successful Next() calls (must be a power of two). Pass-
// through on errors.
type reportingIterator struct {
	inner       RowIterator
	onProgress  func(done, total int64)
	done        int64
	total       int64
	reportEvery int64
}

func (p *reportingIterator) Next() (*PrefixRow, error) {
	row, err := p.inner.Next()
	if row != nil {
		p.done++
		if p.done&(p.reportEvery-1) == 0 {
			p.onProgress(p.done, p.total)
		}
	}
	if err != nil {
		return row, fmt.Errorf("reporting iterator: %w", err)
	}

	return row, nil
}

func (p *reportingIterator) Remaining() uint64 { return p.inner.Remaining() }

// singleRunIterator wraps a CompressedRunReader to implement the iterator interface expected by IndexBuilder.
// Reuses one PrefixRow across all Next() calls — caller must consume the
// returned row before calling Next again. IndexBuilder.Add does this.
type singleRunIterator struct {
	reader runSource
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

// cleanup removes temporary files.
func (p *Pipeline) cleanup(log *zerolog.Logger) {
	for _, path := range p.runFiles {
		logCleanupErr(log, "remove run file", path, os.Remove(path))
	}
	if p.tempDir != "" && p.config.TempDir == "" {
		logCleanupErr(log, "remove temp dir", p.tempDir, os.RemoveAll(p.tempDir))
	}
}
