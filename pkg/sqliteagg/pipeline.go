package sqliteagg

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/eunmann/s3-inv-db/pkg/humanfmt"
	"github.com/eunmann/s3-inv-db/pkg/logging"
	"github.com/eunmann/s3-inv-db/pkg/s3fetch"
	"github.com/eunmann/s3-inv-db/pkg/tiers"
	"github.com/rs/zerolog"
)

// ParsedRow represents a parsed CSV row ready for prefix expansion.
type ParsedRow struct {
	ChunkID string
	Key     string
	Size    uint64
	TierID  tiers.ID
	IsEnd   bool // Sentinel to mark end of chunk
}

// AggDelta represents an aggregation delta for SQLite upsert.
type AggDelta struct {
	ChunkID    string
	Prefix     string
	Depth      int
	TierID     tiers.ID
	Size       uint64
	IsChunkEnd bool // Sentinel to mark all deltas for a chunk are done
}

// AggStats holds accumulated stats for a prefix within a batch.
type AggStats struct {
	Depth      int
	TotalCount int64
	TotalBytes int64
	TierCounts [tiers.NumTiers]int64
	TierBytes  [tiers.NumTiers]int64
}

// Pipeline implements a concurrent aggregation pipeline.
type Pipeline struct {
	client  *s3fetch.Client
	cfg     StreamConfig
	opts    BuildOptions
	agg     *Aggregator
	log     zerolog.Logger
	tierMap *tiers.Mapping

	// Column indices from manifest
	keyCol        int
	sizeCol       int
	storageCol    int
	accessTierCol int
	destBucket    string

	// Channels
	chunksCh <-chan ChunkTask
	rowsCh   chan ParsedRow
	deltasCh chan AggDelta

	// Metrics (only updated AFTER commit)
	objectsProcessed atomic.Int64
	bytesProcessed   atomic.Int64
	prefixesWritten  atomic.Int64
	batchesWritten   atomic.Int64
	totalChunks      int64

	// Progress tracker for chunk completion (updated only after commit)
	progress *logging.ProgressTracker

	// Per-chunk tracking
	chunkStartTimes sync.Map // chunkID -> time.Time
	chunkMetrics    sync.Map // chunkID -> *chunkMetrics

	// Error tracking (hasErr provides race-free early termination check)
	hasErr       atomic.Bool
	firstErr     error
	firstErrOnce sync.Once
}

// chunkMetrics tracks metrics for a single chunk (accumulated during processing).
type chunkMetrics struct {
	objects int64
	bytes   int64
}

// NewPipeline creates a new aggregation pipeline.
func NewPipeline(client *s3fetch.Client, cfg StreamConfig) (*Pipeline, error) {
	cfg.BuildOptions.Validate()

	agg, err := Open(cfg.SQLiteConfig)
	if err != nil {
		return nil, fmt.Errorf("open aggregator: %w", err)
	}

	return &Pipeline{
		client:   client,
		cfg:      cfg,
		opts:     cfg.BuildOptions,
		agg:      agg,
		log:      logging.WithPhase("pipeline"),
		tierMap:  tiers.NewMapping(),
		rowsCh:   make(chan ParsedRow, cfg.BuildOptions.RowChannelBuffer),
		deltasCh: make(chan AggDelta, cfg.BuildOptions.DeltaChannelBuffer),
	}, nil
}

// Close releases resources.
func (p *Pipeline) Close() error {
	if p.agg != nil {
		return p.agg.Close()
	}
	return nil
}

// Run executes the pipeline.
func (p *Pipeline) Run(ctx context.Context) (*StreamResult, error) {
	// Parse manifest URI
	bucket, key, err := s3fetch.ParseS3URI(p.cfg.ManifestURI)
	if err != nil {
		return nil, fmt.Errorf("parse manifest URI: %w", err)
	}

	p.log.Info().
		Str("manifest_uri", p.cfg.ManifestURI).
		Str("db_path", p.cfg.DBPath).
		Int("s3_concurrency", p.opts.S3DownloadConcurrency).
		Int("parse_workers", p.opts.ParseWorkers).
		Int("batch_size", p.opts.SQLiteWriteBatchSize).
		Msg("starting pipelined streaming aggregation")

	// Fetch manifest
	manifest, err := p.client.FetchManifest(ctx, bucket, key)
	if err != nil {
		return nil, fmt.Errorf("fetch manifest: %w", err)
	}

	// Get column indices
	p.keyCol, err = manifest.KeyColumnIndex()
	if err != nil {
		return nil, fmt.Errorf("get key column: %w", err)
	}
	p.sizeCol, err = manifest.SizeColumnIndex()
	if err != nil {
		return nil, fmt.Errorf("get size column: %w", err)
	}
	p.storageCol = manifest.StorageClassColumnIndex()
	p.accessTierCol = manifest.AccessTierColumnIndex()

	p.destBucket, err = manifest.GetDestinationBucketName()
	if err != nil {
		return nil, fmt.Errorf("get destination bucket: %w", err)
	}

	totalChunks := len(manifest.Files)
	p.totalChunks = int64(totalChunks)

	// Initialize progress tracker
	p.progress = logging.NewProgressTracker("pipeline", int64(totalChunks), p.log)

	p.log.Info().
		Int("total_chunks", totalChunks).
		Str("source_bucket", manifest.SourceBucket).
		Msg("processing inventory chunks")

	startTime := time.Now()

	// Create cancellable context for pipeline
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Create chunk channel
	chunksCh := make(chan ChunkTask, p.opts.S3DownloadConcurrency*2)
	p.chunksCh = chunksCh

	// WaitGroups for each stage
	var downloadWg, parseWg sync.WaitGroup

	// Stage 1: Chunk scheduler
	go func() {
		defer close(chunksCh)
		for _, file := range manifest.Files {
			if ctx.Err() != nil {
				return
			}

			// Check if chunk already processed
			done, err := p.agg.ChunkDone(file.Key)
			if err != nil {
				p.setError(fmt.Errorf("check chunk %s: %w", file.Key, err))
				return
			}
			if done {
				p.progress.RecordSkip()
				continue
			}

			select {
			case chunksCh <- ChunkTask{
				ChunkID: file.Key,
				Bucket:  p.destBucket,
				Key:     file.Key,
			}:
			case <-ctx.Done():
				return
			}
		}
	}()

	// Stage 2: Download workers (download + parse CSV -> rowsCh)
	for i := 0; i < p.opts.S3DownloadConcurrency; i++ {
		downloadWg.Add(1)
		go func() {
			defer downloadWg.Done()
			p.downloadWorker(ctx)
		}()
	}

	// Close rowsCh when all download workers are done
	go func() {
		downloadWg.Wait()
		close(p.rowsCh)
	}()

	// Stage 3: Parse workers (prefix expansion -> deltasCh)
	for i := 0; i < p.opts.ParseWorkers; i++ {
		parseWg.Add(1)
		go func() {
			defer parseWg.Done()
			p.parseWorker(ctx)
		}()
	}

	// Close deltasCh when all parse workers are done
	go func() {
		parseWg.Wait()
		close(p.deltasCh)
	}()

	// Stage 4: Single writer (batch deltas -> SQLite)
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		p.writerWorker(ctx)
	}()

	// Wait for writer to finish
	<-writerDone

	elapsed := time.Since(startTime)
	bytesProcessed := p.bytesProcessed.Load()
	objectsProcessed := p.objectsProcessed.Load()
	prefixesWritten := p.prefixesWritten.Load()
	chunksCompleted, chunksSkipped, _ := p.progress.Progress()

	logging.PhaseComplete(p.log, "pipeline", elapsed).
		Int64("chunks_processed", chunksCompleted).
		Int64("chunks_skipped", chunksSkipped).
		Count("objects_processed", objectsProcessed).
		Bytes("bytes_processed", bytesProcessed).
		Count("prefixes_written", prefixesWritten).
		Int64("batches_written", p.batchesWritten.Load()).
		Throughput(bytesProcessed).
		Log("pipelined streaming aggregation complete")

	if p.hasErr.Load() && ctx.Err() == nil {
		return nil, p.firstErr
	}

	return &StreamResult{
		ChunksProcessed:  int(chunksCompleted),
		ChunksSkipped:    int(chunksSkipped),
		TotalChunks:      totalChunks,
		ObjectsProcessed: objectsProcessed,
		BytesProcessed:   bytesProcessed,
	}, nil
}

func (p *Pipeline) setError(err error) {
	p.firstErrOnce.Do(func() {
		p.firstErr = err
		p.hasErr.Store(true)
		p.log.Error().Err(err).Msg("pipeline error")
	})
}

func (p *Pipeline) downloadWorker(ctx context.Context) {
	for chunk := range p.chunksCh {
		if ctx.Err() != nil {
			return
		}
		if p.hasErr.Load() {
			return
		}

		// Log chunk started and record start time
		chunksComplete := p.progress.Completed()
		logging.ChunkStarted(p.log, "pipeline", chunk.ChunkID, chunksComplete, p.totalChunks)
		p.chunkStartTimes.Store(chunk.ChunkID, time.Now())
		p.chunkMetrics.Store(chunk.ChunkID, &chunkMetrics{})

		if err := p.streamChunk(ctx, chunk); err != nil {
			p.setError(fmt.Errorf("stream chunk %s: %w", chunk.ChunkID, err))
			return
		}
	}
}

func (p *Pipeline) streamChunk(ctx context.Context, chunk ChunkTask) error {
	// Stream the chunk from S3
	body, err := p.client.StreamObject(ctx, chunk.Bucket, chunk.Key)
	if err != nil {
		return fmt.Errorf("stream object: %w", err)
	}
	defer body.Close()

	// Decompress if needed
	reader, closeGzip, err := decompressReader(body, chunk.Key)
	if err != nil {
		return err
	}
	if closeGzip != nil {
		defer closeGzip()
	}

	// Create CSV reader
	csvr := newInventoryReader(reader)

	// Parse and send rows
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		fields, err := csvr.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("read CSV row: %w", err)
		}

		// Skip rows with insufficient columns
		if len(fields) <= p.keyCol || len(fields) <= p.sizeCol {
			continue
		}

		objKey := fields[p.keyCol]
		if objKey == "" {
			continue
		}

		sizeStr := strings.TrimSpace(fields[p.sizeCol])
		size, err := strconv.ParseUint(sizeStr, 10, 64)
		if err != nil {
			size = 0
		}

		// Determine tier
		var storageClass, accessTier string
		if p.storageCol >= 0 && len(fields) > p.storageCol {
			storageClass = fields[p.storageCol]
		}
		if p.accessTierCol >= 0 && len(fields) > p.accessTierCol {
			accessTier = fields[p.accessTierCol]
		}
		tierID := p.tierMap.FromS3(storageClass, accessTier)

		select {
		case p.rowsCh <- ParsedRow{
			ChunkID: chunk.ChunkID,
			Key:     objKey,
			Size:    size,
			TierID:  tierID,
		}:
			// Accumulate per-chunk metrics (will be moved to totals on commit)
			if m, ok := p.chunkMetrics.Load(chunk.ChunkID); ok {
				metrics := m.(*chunkMetrics)
				metrics.objects++
				metrics.bytes += int64(size)
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	// Send end-of-chunk marker
	select {
	case p.rowsCh <- ParsedRow{ChunkID: chunk.ChunkID, IsEnd: true}:
	case <-ctx.Done():
		return ctx.Err()
	}

	return nil
}

func (p *Pipeline) parseWorker(ctx context.Context) {
	for row := range p.rowsCh {
		if ctx.Err() != nil {
			return
		}
		if p.hasErr.Load() {
			return
		}

		if row.IsEnd {
			// Forward chunk-end marker
			select {
			case p.deltasCh <- AggDelta{ChunkID: row.ChunkID, IsChunkEnd: true}:
			case <-ctx.Done():
				return
			}
			continue
		}

		// Expand row into prefix deltas
		prefixes := extractPrefixes(row.Key)

		// Root prefix (empty string)
		select {
		case p.deltasCh <- AggDelta{
			ChunkID: row.ChunkID,
			Prefix:  "",
			Depth:   0,
			TierID:  row.TierID,
			Size:    row.Size,
		}:
		case <-ctx.Done():
			return
		}

		// Each directory prefix
		for i, prefix := range prefixes {
			select {
			case p.deltasCh <- AggDelta{
				ChunkID: row.ChunkID,
				Prefix:  prefix,
				Depth:   i + 1,
				TierID:  row.TierID,
				Size:    row.Size,
			}:
			case <-ctx.Done():
				return
			}
		}
	}
}

func (p *Pipeline) writerWorker(ctx context.Context) {
	batch := make(map[string]*AggStats)
	batchSize := 0
	batchBytes := int64(0)
	batchStart := time.Now()
	pendingChunks := make(map[string]int) // chunkID -> pending end markers
	chunkEnds := make(map[string]bool)    // chunks that have received end marker
	timeout := time.Duration(p.opts.SQLiteWriteBatchTimeoutMs) * time.Millisecond
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	lastProgressLog := time.Now()

	flushBatch := func() error {
		if len(batch) == 0 {
			return nil
		}

		// Determine which chunks to mark as done
		chunksToMark := make([]string, 0)
		for chunkID, ends := range pendingChunks {
			if ends <= 0 && chunkEnds[chunkID] {
				chunksToMark = append(chunksToMark, chunkID)
			}
		}

		batchDuration := time.Since(batchStart)
		if err := p.writeBatch(batch, chunksToMark); err != nil {
			return err
		}

		// Process completed chunks: update metrics, record completion, log
		for _, chunkID := range chunksToMark {
			delete(pendingChunks, chunkID)
			delete(chunkEnds, chunkID)

			// Get chunk start time for accurate duration
			var chunkDuration time.Duration
			if startVal, ok := p.chunkStartTimes.LoadAndDelete(chunkID); ok {
				chunkDuration = time.Since(startVal.(time.Time))
			}

			// Get and clear chunk metrics, add to totals
			var objects, bytes int64
			if metricsVal, ok := p.chunkMetrics.LoadAndDelete(chunkID); ok {
				m := metricsVal.(*chunkMetrics)
				objects = m.objects
				bytes = m.bytes
				p.objectsProcessed.Add(objects)
				p.bytesProcessed.Add(bytes)
			}

			// Record completion in progress tracker (updates chunks_complete)
			p.progress.RecordCompletion(chunkDuration)

			// Log chunk completion with accurate metrics
			logging.ChunkComplete(p.log, "pipeline", chunkDuration).
				Str("chunk_id", chunkID).
				Count("objects", objects).
				Bytes("bytes", bytes).
				ProgressFromTracker(p.progress).
				Throughput(bytes).
				Log("chunk committed to SQLite")
		}

		prefixCount := int64(len(batch))
		p.prefixesWritten.Add(prefixCount)
		p.batchesWritten.Add(1)

		// Log batch completion at DEBUG level (not user-facing progress)
		logging.BatchComplete(p.log, "pipeline", batchDuration).
			Count("prefixes", prefixCount).
			Bytes("bytes", batchBytes).
			Int("chunks_marked", len(chunksToMark)).
			Int64("batch_num", p.batchesWritten.Load()).
			Throughput(batchBytes).
			LogDebug("batch written to SQLite")

		// Log overall progress periodically (only when chunks complete)
		if len(chunksToMark) > 0 && time.Since(lastProgressLog) >= 5*time.Second {
			objectsProcessed := p.objectsProcessed.Load()
			bytesProcessed := p.bytesProcessed.Load()
			elapsed := p.progress.Elapsed()

			event := p.log.Info().
				Str("event", "progress").
				Str("phase", "pipeline").
				Float64("progress_pct", p.progress.ProgressPct()).
				Int64("objects_processed", objectsProcessed).
				Int64("bytes_processed", bytesProcessed).
				Int64("prefixes_written", p.prefixesWritten.Load()).
				Dur("elapsed", elapsed)
			if eta := p.progress.ETA(); eta > 0 {
				event = event.Dur("eta", eta)
				if logging.IsPrettyMode() {
					event = event.Str("eta_h", humanfmt.Duration(eta))
				}
			}
			if logging.IsPrettyMode() {
				event = event.
					Str("objects_h", humanfmt.Count(objectsProcessed)).
					Str("bytes_h", humanfmt.Bytes(bytesProcessed)).
					Str("elapsed_h", humanfmt.Duration(elapsed)).
					Str("throughput_h", humanfmt.Throughput(bytesProcessed, elapsed))
			}
			event.Msg("pipeline progress")
			lastProgressLog = time.Now()
		}

		// Clear batch
		clear(batch)
		batchSize = 0
		batchBytes = 0
		batchStart = time.Now()

		return nil
	}

	for {
		timer.Reset(timeout)

		select {
		case delta, ok := <-p.deltasCh:
			if !ok {
				// Channel closed, flush remaining batch
				if err := flushBatch(); err != nil {
					p.setError(err)
				}
				return
			}

			if delta.IsChunkEnd {
				// Mark chunk as having received end
				chunkEnds[delta.ChunkID] = true
				continue
			}

			// Track pending chunks
			if _, exists := pendingChunks[delta.ChunkID]; !exists {
				pendingChunks[delta.ChunkID] = 0
			}

			// Accumulate delta into batch
			stats, exists := batch[delta.Prefix]
			if !exists {
				stats = &AggStats{Depth: delta.Depth}
				batch[delta.Prefix] = stats
			}
			stats.TotalCount++
			stats.TotalBytes += int64(delta.Size)
			stats.TierCounts[delta.TierID]++
			stats.TierBytes[delta.TierID] += int64(delta.Size)
			batchSize++
			batchBytes += int64(delta.Size)

			// Flush if batch is full
			if batchSize >= p.opts.SQLiteWriteBatchSize {
				if err := flushBatch(); err != nil {
					p.setError(err)
					return
				}
			}

		case <-timer.C:
			// Timeout - flush partial batch
			if err := flushBatch(); err != nil {
				p.setError(err)
				return
			}

		case <-ctx.Done():
			return
		}
	}
}

func (p *Pipeline) writeBatch(batch map[string]*AggStats, chunksToMark []string) error {
	if len(batch) == 0 && len(chunksToMark) == 0 {
		return nil
	}

	p.agg.writeMu.Lock()
	defer p.agg.writeMu.Unlock()

	// Begin transaction
	if err := p.agg.BeginChunk(); err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}

	// Build batch upsert
	for prefix, stats := range batch {
		// Build arguments
		args := make([]interface{}, 0, 4+int(tiers.NumTiers)*2)
		args = append(args, prefix, stats.Depth, stats.TotalCount, stats.TotalBytes)

		for i := range tiers.NumTiers {
			args = append(args, stats.TierCounts[i], stats.TierBytes[i])
		}

		_, err := p.agg.upsertStmt.Exec(args...)
		if err != nil {
			p.agg.Rollback()
			return fmt.Errorf("upsert prefix %q: %w", prefix, err)
		}
	}

	// Mark chunks as done
	for _, chunkID := range chunksToMark {
		if err := p.agg.MarkChunkDone(chunkID); err != nil {
			p.agg.Rollback()
			return fmt.Errorf("mark chunk done %s: %w", chunkID, err)
		}
	}

	// Commit
	if err := p.agg.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}

	return nil
}

// StreamFromS3Pipeline runs the pipelined streaming aggregation.
func StreamFromS3Pipeline(ctx context.Context, client *s3fetch.Client, cfg StreamConfig) (*StreamResult, error) {
	p, err := NewPipeline(client, cfg)
	if err != nil {
		return nil, err
	}
	defer p.Close()

	return p.Run(ctx)
}
