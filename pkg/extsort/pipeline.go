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

	"github.com/eunmann/s3-inv-db/internal/logctx"
	"github.com/eunmann/s3-inv-db/pkg/humanfmt"
	"github.com/eunmann/s3-inv-db/pkg/inventory"
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
	log := logctx.FromContext(ctx)

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

	// Log memory budget breakdown with human-readable formatting
	p.config.EnsureBudget()
	aggThreshold := p.config.AggregatorMemoryThreshold()
	log.Info().
		Str("manifest_uri", manifestURI).
		Str("total_budget", humanfmt.BytesUint64(p.config.MemoryBudget.Total())).
		Str("aggregator_budget", humanfmt.Bytes(aggThreshold)).
		Str("run_buffer_budget", humanfmt.Bytes(p.config.RunBufferBudget())).
		Str("merge_budget", humanfmt.Bytes(p.config.MergeBudget())).
		Str("index_budget", humanfmt.Bytes(p.config.IndexBuildBudget())).
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
		Int("run_files_count", len(p.runFiles)).
		Str("objects", humanfmt.Count(p.objectsProcessed)).
		Int64("objects_count", p.objectsProcessed).
		Int64("flushes_count", p.flushCount).
		Str("duration", humanfmt.Duration(ingestDuration)).
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
		Str("objects", humanfmt.Count(p.objectsProcessed)).
		Int64("objects_count", p.objectsProcessed).
		Str("prefixes", humanfmt.CountUint64(prefixCount)).
		Uint64("prefixes_count", prefixCount).
		Str("throughput", humanfmt.Count(int64(float64(p.objectsProcessed)/duration.Seconds()))+"/s").
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

// estimateObjectCount estimates the number of objects in an inventory file
// based on its compressed size and format. This helps pre-size buffers to
// avoid repeated slice growth during parsing.
func estimateObjectCount(fileSize int64, format s3fetch.InventoryFormat) int {
	const (
		minCapacity = 10000 // Minimum capacity to avoid tiny allocations

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
	if estimate > 10_000_000 { // Cap at 10M to prevent excessive preallocation
		return 10_000_000
	}
	return int(estimate)
}

// runIngestPhase streams S3 inventory and creates sorted run files.
// It uses concurrent workers to download and parse chunks in parallel.
//
//nolint:gocognit,gocyclo,cyclop,funlen,maintidx // Complex pipeline coordination logic
func (p *Pipeline) runIngestPhase(ctx context.Context, manifestURI string) error {
	log := logctx.FromContext(ctx)

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
		Int("chunks_count", len(manifest.Files)).
		Int("workers_count", numWorkers).
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
				firstErr = fmt.Errorf("context cancelled: %w", ctx.Err())
			}
			cancel()
			// Drain remaining results to allow workers to exit
			for range results {
				// Intentionally empty: just consuming remaining items
				continue
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
				Int("chunk_num", chunkNum).
				Int("chunks_total", totalChunks).
				Float64("progress_pct", pct).
				Int64("objects_count", atomic.LoadInt64(&p.objectsProcessed)).
				Dur("eta_ms", remaining).
				Msg("ingest progress")
		}

		// Flush if memory threshold exceeded
		// Use aggregator budget (50% of total) instead of full budget
		// This ensures aggregator memory stays within its allocated fraction
		heapThreshold := p.config.MemoryBudget.AggregatorBudget()
		if ShouldFlush(heapThreshold) {
			p.memTracker.LogNow("pre_flush")
			if err := p.flushAggregator(ctx, agg); err != nil {
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
		if err := p.flushAggregator(ctx, agg); err != nil {
			return fmt.Errorf("final flush: %w", err)
		}
	}

	return nil
}

// chunkWorker processes chunks from the jobs channel and sends results to the results channel.
func (p *Pipeline) chunkWorker(ctx context.Context, jobs <-chan chunkJob, results chan<- objectBatch) {
	for job := range jobs {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Estimate capacity from file size to reduce slice growth allocations.
		// For compressed CSV: assume ~8x compression, ~100 bytes/record uncompressed.
		// For Parquet: ~50 bytes/record (column-compressed).
		// Minimum 10K to avoid tiny initial allocations.
		capacityHint := estimateObjectCount(job.config.fileSize, job.config.format)

		objects, _, err := p.processChunkToBatch(ctx, job.bucket, job.key, job.config, capacityHint)
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
	log := logctx.FromContext(ctx)

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

		tierID := cfg.tierMapping.FromS3(row.StorageClass, row.AccessTier)
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

// flushAggregator drains the aggregator to a sorted run file.
func (p *Pipeline) flushAggregator(ctx context.Context, agg *Aggregator) error {
	log := logctx.FromContext(ctx)
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
	runPath := filepath.Join(p.tempDir, fmt.Sprintf("run_%04d%s", p.runCount, ext))
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

	p.runFiles = append(p.runFiles, runPath)
	p.flushCount++

	// Log flush with memory stats
	aggMemory := int64(len(rows)) * 288 // Estimated bytes used before flush
	flushDuration := time.Since(start)
	log.Info().
		Int("run_index", p.runCount-1).
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

// runMergeBuildPhase merges run files and builds the index.
func (p *Pipeline) runMergeBuildPhase(ctx context.Context, outDir string) (prefixCount uint64, maxDepth uint32, err error) {
	log := logctx.FromContext(ctx)

	// Check for cancellation before starting
	select {
	case <-ctx.Done():
		return 0, 0, fmt.Errorf("merge phase cancelled: %w", ctx.Err())
	default:
	}

	if len(p.runFiles) == 0 {
		log.Info().Msg("no run files to merge, creating empty index")
		builder, err := NewIndexBuilder(outDir, p.config.TempDir)
		if err != nil {
			return 0, 0, fmt.Errorf("create index builder: %w", err)
		}
		if err := builder.FinalizeWithContext(ctx); err != nil {
			return 0, 0, fmt.Errorf("finalize empty index: %w", err)
		}
		return 0, 0, nil
	}

	// Calculate per-reader buffer size for merge
	numRunFiles := len(p.runFiles)
	perReaderBuffer := p.config.MergeBudget() / int64(numRunFiles)
	const minBuffer = 64 * 1024
	const maxBuffer = 8 * 1024 * 1024
	if perReaderBuffer < minBuffer {
		perReaderBuffer = minBuffer
	} else if perReaderBuffer > maxBuffer {
		perReaderBuffer = maxBuffer
	}

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
		})

		var mergeErr error
		finalRunPath, mergeErr = parallelMerger.MergeAll(ctx, p.runFiles)
		if mergeErr != nil {
			return 0, 0, fmt.Errorf("parallel merge: %w", mergeErr)
		}

		rounds, totalTime, bytesWritten := parallelMerger.Statistics()
		log.Info().
			Int("merge_rounds", rounds).
			Str("merge_duration", humanfmt.Duration(totalTime)).
			Str("bytes_written", humanfmt.Bytes(bytesWritten)).
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
		return 0, 0, fmt.Errorf("open merged run: %w", err)
	}

	// Create iterator adapter for index builder
	mergeIter := &singleRunIterator{reader: reader}

	// Use prefix count from run file header to pre-size index builder arrays
	prefixCount = reader.Count()
	log.Debug().
		Uint64("prefix_count", prefixCount).
		Msg("index build starting")

	builder, err := NewIndexBuilderWithCapacity(outDir, p.config.TempDir, prefixCount)
	if err != nil {
		reader.Close()
		return 0, 0, fmt.Errorf("create index builder: %w", err)
	}

	if err := builder.AddAllWithContext(ctx, mergeIter); err != nil {
		builder.cleanup()
		return 0, 0, fmt.Errorf("build index: %w", err)
	}

	if err := builder.FinalizeWithContext(ctx); err != nil {
		return 0, 0, fmt.Errorf("finalize index: %w", err)
	}

	return builder.Count(), builder.MaxDepth(), nil
}

// singleRunIterator wraps a RunReader to implement the iterator interface expected by IndexBuilder.
type singleRunIterator struct {
	reader RunReader
}

func (s *singleRunIterator) Next() (*PrefixRow, error) {
	row, err := s.reader.Read()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, io.EOF
		}
		return nil, fmt.Errorf("read from run: %w", err)
	}
	return row, nil
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
