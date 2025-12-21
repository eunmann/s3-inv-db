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

	"github.com/eunmann/s3-inv-db/pkg/inventory"
	"github.com/eunmann/s3-inv-db/pkg/logging"
	"github.com/eunmann/s3-inv-db/pkg/memdiag"
	"github.com/eunmann/s3-inv-db/pkg/s3fetch"
	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

// Pipeline orchestrates the external sort build process.
// It streams S3 inventory data, aggregates in bounded memory,
// spills to sorted run files, and merges to build the final index.
type Pipeline struct {
	config    Config
	s3Client  *s3fetch.Client
	tempDir   string
	runFiles  []string
	runCount  int
	startTime time.Time

	// Progress tracking
	chunksProcessed  int64
	objectsProcessed int64
	bytesProcessed   int64
	flushCount       int64

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

// NewPipeline creates a new external sort pipeline.
func NewPipeline(config Config, s3Client *s3fetch.Client) *Pipeline {
	// Ensure memory budget is set
	config.EnsureBudget()

	if config.RunFileBufferSize <= 0 {
		config.RunFileBufferSize = 4 * 1024 * 1024 // 4MB default
	}

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
	log := logging.L()

	// Start memory diagnostics
	p.memTracker.Start()
	defer p.memTracker.Stop()
	p.memTracker.SetPhase("init")

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

	// Log memory budget breakdown
	p.config.EnsureBudget()
	aggThreshold := p.config.AggregatorMemoryThreshold()
	log.Info().
		Str("manifest_uri", manifestURI).
		Uint64("total_budget_mb", p.config.MemoryBudget.Total()/(1024*1024)).
		Int64("aggregator_budget_mb", aggThreshold/(1024*1024)).
		Int64("run_buffer_budget_mb", p.config.RunBufferBudget()/(1024*1024)).
		Int64("merge_budget_mb", p.config.MergeBudget()/(1024*1024)).
		Int64("index_budget_mb", p.config.IndexBuildBudget()/(1024*1024)).
		Msg("pipeline starting")

	p.memTracker.SetPhase("ingest")
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
		Int("run_files", len(p.runFiles)).
		Int64("objects", p.objectsProcessed).
		Int64("flushes", p.flushCount).
		Dur("duration_ms", ingestDuration).
		Msg("ingest phase complete")

	p.memTracker.SetPhase("merge")
	mergeStart := time.Now()
	prefixCount, maxDepth, err := p.runMergeBuildPhase(ctx, outDir)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			log.Warn().Msg("pipeline cancelled during merge phase")
		}
		return nil, fmt.Errorf("merge/build phase: %w", err)
	}
	mergeDuration := time.Since(mergeStart)

	// Force GC after merge
	runtime.GC()
	p.memTracker.LogNow("post_merge_gc")

	log.Info().
		Uint64("prefixes", prefixCount).
		Uint32("max_depth", maxDepth).
		Dur("duration_ms", mergeDuration).
		Msg("merge phase complete")

	p.cleanup()
	success = true

	duration := time.Since(p.startTime)
	log.Info().
		Dur("total_duration_ms", duration).
		Int64("objects", p.objectsProcessed).
		Uint64("prefixes", prefixCount).
		Msg("pipeline complete")

	return &Result{
		ChunksProcessed:  int(p.chunksProcessed),
		ObjectsProcessed: p.objectsProcessed,
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

// objectBatch holds a batch of objects to be aggregated.
// Using batches reduces channel overhead compared to sending individual objects.
type objectBatch struct {
	objects []objectRecord
	err     error
}

// objectRecord holds a single object's data for aggregation.
type objectRecord struct {
	key    string
	size   uint64
	tierID tiers.ID
}

// runIngestPhase streams S3 inventory and creates sorted run files.
// It uses concurrent workers to download and parse chunks in parallel.
func (p *Pipeline) runIngestPhase(ctx context.Context, manifestURI string) error {
	log := logging.L()

	bucket, key, err := s3fetch.ParseS3URI(manifestURI)
	if err != nil {
		return fmt.Errorf("parse manifest URI: %w", err)
	}

	manifest, err := p.s3Client.FetchManifest(ctx, bucket, key)
	if err != nil {
		return fmt.Errorf("fetch manifest: %w", err)
	}

	format := manifest.DetectFormat()
	formatStr := "CSV"
	if format == s3fetch.InventoryFormatParquet {
		formatStr = "Parquet"
	}

	numWorkers := p.config.S3DownloadConcurrency
	if numWorkers <= 0 {
		numWorkers = runtime.NumCPU()
	}

	// Coordinate workers with memory budget
	// Each worker can have ~4MB in-flight (batch of ~100K objects * ~40 bytes each)
	// Channel buffer is numWorkers * 2, so max in-flight = numWorkers * 8MB
	const bytesPerWorkerInFlight = 8 * 1024 * 1024 // 8MB per worker
	p.config.EnsureBudget()
	headroom := p.config.MemoryBudget.Total() - p.config.MemoryBudget.AggregatorBudget() -
		p.config.MemoryBudget.RunBufferBudget() - p.config.MemoryBudget.MergeBudget() -
		p.config.MemoryBudget.IndexBuildBudget()

	maxWorkersFromBudget := int(headroom / bytesPerWorkerInFlight)
	if maxWorkersFromBudget < 2 {
		maxWorkersFromBudget = 2 // minimum 2 workers
	}

	originalWorkers := numWorkers
	if numWorkers > maxWorkersFromBudget {
		numWorkers = maxWorkersFromBudget
		log.Warn().
			Int("requested_workers", originalWorkers).
			Int("max_from_budget", maxWorkersFromBudget).
			Int64("headroom_mb", int64(headroom)/(1024*1024)).
			Msg("reducing worker count to fit memory budget")
	}

	log.Info().
		Str("format", formatStr).
		Int("chunks", len(manifest.Files)).
		Int("workers", numWorkers).
		Msg("inventory manifest loaded")

	keyCol, err := manifest.KeyColumnIndex()
	if err != nil {
		return fmt.Errorf("get key column: %w", err)
	}
	sizeCol, err := manifest.SizeColumnIndex()
	if err != nil {
		return fmt.Errorf("get size column: %w", err)
	}
	storageCol := manifest.StorageClassColumnIndex()
	accessTierCol := manifest.AccessTierColumnIndex()

	destBucket, err := manifest.GetDestinationBucketName()
	if err != nil {
		return fmt.Errorf("get destination bucket: %w", err)
	}

	tierMapping := tiers.NewMapping()
	totalChunks := len(manifest.Files)

	// Create job channel - minimal buffer to avoid queuing too many chunks
	// Each chunk downloads ~10-100MB, so we don't want many waiting
	jobs := make(chan chunkJob, numWorkers)

	// Create result channel - minimal buffer to limit memory for batches
	// Each batch holds all objects from a chunk, which can be 50-200MB
	results := make(chan objectBatch, 2)

	// Context for cancellation on error
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Start workers
	var wg sync.WaitGroup
	for range numWorkers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p.chunkWorker(ctx, jobs, results)
		}()
	}

	// Start job sender in background
	go func() {
		defer close(jobs)
		for i, file := range manifest.Files {
			select {
			case <-ctx.Done():
				return
			case jobs <- chunkJob{
				index:  i,
				bucket: destBucket,
				key:    file.Key,
				config: chunkConfig{
					format:        format,
					keyCol:        keyCol,
					sizeCol:       sizeCol,
					storageCol:    storageCol,
					accessTierCol: accessTierCol,
					tierMapping:   tierMapping,
					fileSize:      file.Size,
				},
			}:
			}
		}
	}()

	// Close results channel when all workers done
	go func() {
		wg.Wait()
		close(results)
	}()

	// Aggregation loop - runs in main goroutine
	// Initial capacity of 10K prefixes (~3MB at ~300 bytes/prefix) - map will grow as needed
	agg := NewAggregator(10000, p.config.MaxDepth)
	progressInterval := max(totalChunks/10, 1)
	var firstErr error

	for batch := range results {
		// Check for context cancellation
		select {
		case <-ctx.Done():
			if firstErr == nil {
				firstErr = ctx.Err()
			}
			cancel()
			// Drain remaining results to allow workers to exit
			for range results {
			}
			return firstErr
		default:
		}

		if batch.err != nil {
			if firstErr == nil {
				firstErr = batch.err
				cancel() // Signal workers to stop
			}
			continue
		}

		// Aggregate all objects in this batch
		for _, obj := range batch.objects {
			agg.AddObject(obj.key, obj.size, obj.tierID)
			atomic.AddInt64(&p.objectsProcessed, 1)
			atomic.AddInt64(&p.bytesProcessed, int64(obj.size))
		}

		atomic.AddInt64(&p.chunksProcessed, 1)
		chunkNum := int(atomic.LoadInt64(&p.chunksProcessed))

		// Log progress at intervals
		if chunkNum%progressInterval == 0 || chunkNum == totalChunks {
			elapsed := time.Since(p.startTime)
			avgPerChunk := elapsed / time.Duration(chunkNum)
			remaining := time.Duration(totalChunks-chunkNum) * avgPerChunk
			pct := float64(chunkNum) * 100.0 / float64(totalChunks)

			log.Info().
				Int("chunk", chunkNum).
				Int("total", totalChunks).
				Float64("progress_pct", pct).
				Int64("objects", atomic.LoadInt64(&p.objectsProcessed)).
				Dur("eta_ms", remaining).
				Msg("ingest progress")
		}

		// Flush if memory threshold exceeded
		// Use actual heap measurement instead of estimate for accuracy
		heapThreshold := p.config.MemoryBudget.Total()
		if ShouldFlush(heapThreshold) {
			p.memTracker.LogNow("pre_flush")
			if err := p.flushAggregator(agg); err != nil {
				return fmt.Errorf("flush aggregator: %w", err)
			}
			// Force GC after flush to release memory promptly
			runtime.GC()
			p.memTracker.LogNow("post_flush_gc")
		}
	}

	if firstErr != nil {
		return fmt.Errorf("process chunk: %w", firstErr)
	}

	// Final flush
	if agg.PrefixCount() > 0 {
		if err := p.flushAggregator(agg); err != nil {
			return fmt.Errorf("final flush: %w", err)
		}
	}

	return nil
}

// chunkWorker processes chunks from the jobs channel and sends results to the results channel.
func (p *Pipeline) chunkWorker(ctx context.Context, jobs <-chan chunkJob, results chan<- objectBatch) {
	// Initial capacity for batch buffer - we start small and let it grow.
	// Chunks can have 100K-1M objects, but starting smaller reduces memory
	// spikes when processing small chunks.
	const batchCapacity = 10000

	for job := range jobs {
		select {
		case <-ctx.Done():
			return
		default:
		}

		objects, err := p.processChunkToBatch(ctx, job.bucket, job.key, job.config, batchCapacity)
		if err != nil {
			select {
			case results <- objectBatch{err: fmt.Errorf("chunk %d: %w", job.index, err)}:
			case <-ctx.Done():
			}
			continue
		}

		select {
		case results <- objectBatch{objects: objects}:
		case <-ctx.Done():
			return
		}
	}
}

// processChunkToBatch downloads and parses a chunk, returning all objects as a batch.
func (p *Pipeline) processChunkToBatch(ctx context.Context, bucket, key string, cfg chunkConfig, capacityHint int) ([]objectRecord, error) {
	body, err := p.s3Client.StreamObject(ctx, bucket, key)
	if err != nil {
		return nil, fmt.Errorf("stream object: %w", err)
	}

	var reader inventory.InventoryReader
	if cfg.format == s3fetch.InventoryFormatParquet {
		reader, err = inventory.NewParquetInventoryReaderFromStream(body, cfg.fileSize)
		if err != nil {
			return nil, fmt.Errorf("create parquet reader: %w", err)
		}
	} else {
		csvCfg := inventory.CSVReaderConfig{
			KeyCol:        cfg.keyCol,
			SizeCol:       cfg.sizeCol,
			StorageCol:    cfg.storageCol,
			AccessTierCol: cfg.accessTierCol,
		}
		reader, err = inventory.NewCSVInventoryReaderFromStream(body, key, csvCfg)
		if err != nil {
			return nil, fmt.Errorf("create csv reader: %w", err)
		}
	}
	defer reader.Close()

	objects := make([]objectRecord, 0, capacityHint)

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		row, err := reader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("read inventory row: %w", err)
		}

		if row.Key == "" {
			continue
		}

		tierID := cfg.tierMapping.FromS3(row.StorageClass, row.AccessTier)
		objects = append(objects, objectRecord{
			key:    row.Key,
			size:   row.Size,
			tierID: tierID,
		})
	}

	return objects, nil
}

// flushAggregator drains the aggregator to a sorted run file.
func (p *Pipeline) flushAggregator(agg *Aggregator) error {
	log := logging.L()
	start := time.Now()

	rows := agg.Drain()
	if len(rows) == 0 {
		return nil
	}

	runPath := filepath.Join(p.tempDir, fmt.Sprintf("run_%04d.bin", p.runCount))
	p.runCount++

	// Calculate buffer size from run buffer budget
	// During ingest, we write one run file at a time, so use full run buffer budget
	// Clamp to reasonable range: [64KB, 16MB]
	bufferSize := p.config.RunBufferBudget()
	const minBuffer = 64 * 1024
	const maxBuffer = 16 * 1024 * 1024
	if bufferSize < minBuffer {
		bufferSize = minBuffer
	} else if bufferSize > maxBuffer {
		bufferSize = maxBuffer
	}

	writer, err := NewRunFileWriter(runPath, int(bufferSize))
	if err != nil {
		return fmt.Errorf("create run file: %w", err)
	}

	if err := writer.WriteSorted(rows); err != nil {
		writer.Close()
		os.Remove(runPath)
		return fmt.Errorf("write sorted: %w", err)
	}

	if err := writer.Close(); err != nil {
		os.Remove(runPath)
		return fmt.Errorf("close run file: %w", err)
	}

	p.runFiles = append(p.runFiles, runPath)
	p.flushCount++

	// Log flush with memory stats
	aggMemory := int64(len(rows)) * 288 // Estimated bytes used before flush
	log.Info().
		Int("run_index", p.runCount-1).
		Int("prefixes", len(rows)).
		Int64("aggregator_memory_mb", aggMemory/(1024*1024)).
		Int64("buffer_size_kb", bufferSize/1024).
		Dur("duration_ms", time.Since(start)).
		Msg("run file written")

	return nil
}

// runMergeBuildPhase merges run files and builds the index.
func (p *Pipeline) runMergeBuildPhase(ctx context.Context, outDir string) (uint64, uint32, error) {
	log := logging.L()

	// Check for cancellation before starting
	select {
	case <-ctx.Done():
		return 0, 0, ctx.Err()
	default:
	}

	if len(p.runFiles) == 0 {
		log.Info().Msg("no run files to merge, creating empty index")
		builder, err := NewIndexBuilder(outDir, p.config.TempDir)
		if err != nil {
			return 0, 0, fmt.Errorf("create index builder: %w", err)
		}
		if err := builder.Finalize(); err != nil {
			return 0, 0, fmt.Errorf("finalize empty index: %w", err)
		}
		return 0, 0, nil
	}

	// Calculate per-reader buffer size for k-way merge
	// Divide merge budget by number of run files, clamp to reasonable range
	numRunFiles := len(p.runFiles)
	perReaderBuffer := p.config.MergeBudget() / int64(numRunFiles)
	const minBuffer = 64 * 1024
	const maxBuffer = 8 * 1024 * 1024
	if perReaderBuffer < minBuffer {
		perReaderBuffer = minBuffer
	} else if perReaderBuffer > maxBuffer {
		perReaderBuffer = maxBuffer
	}

	log.Info().
		Int("run_files", numRunFiles).
		Int64("per_reader_buffer_kb", perReaderBuffer/1024).
		Msg("merge phase starting")

	merger, err := NewMergeIterator(p.runFiles, int(perReaderBuffer))
	if err != nil {
		return 0, 0, fmt.Errorf("create merge iterator: %w", err)
	}
	defer func() { _ = merger.RemoveAll() }()

	// Index build budget provides guidance for the streaming phase
	// Memory grows ~75 bytes per prefix during build
	indexBudget := p.config.IndexBuildBudget()
	estimatedMaxPrefixes := indexBudget / 75
	log.Debug().
		Int64("index_budget_mb", indexBudget/(1024*1024)).
		Int64("estimated_max_prefixes", estimatedMaxPrefixes).
		Msg("index build budget")

	builder, err := NewIndexBuilder(outDir, p.config.TempDir)
	if err != nil {
		merger.Close()
		return 0, 0, fmt.Errorf("create index builder: %w", err)
	}

	if err := builder.AddAllWithContext(ctx, merger); err != nil {
		builder.cleanup()
		return 0, 0, fmt.Errorf("build index: %w", err)
	}

	if err := builder.Finalize(); err != nil {
		return 0, 0, fmt.Errorf("finalize index: %w", err)
	}

	return builder.Count(), builder.MaxDepth(), nil
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
