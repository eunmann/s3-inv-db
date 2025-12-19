package sqliteagg

import (
	"compress/gzip"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/eunmann/s3-inv-db/pkg/logging"
	"github.com/eunmann/s3-inv-db/pkg/s3fetch"
	"github.com/eunmann/s3-inv-db/pkg/tiers"
	"github.com/rs/zerolog"
)

// ChunkTask represents a chunk to be processed.
type ChunkTask struct {
	ChunkID string
	Bucket  string
	Key     string
}

// RowData represents a parsed inventory row.
type RowData struct {
	Key    string
	Size   uint64
	TierID tiers.ID
}

// PrefixDelta represents an aggregation delta for a single prefix.
type PrefixDelta struct {
	Prefix string
	Depth  int
	TierID tiers.ID
	Size   uint64
}

// ChunkResult contains metrics for a processed chunk.
type ChunkResult struct {
	ChunkID string
	Objects int64
	Bytes   int64
	Err     error
}

// ParallelStreamer handles parallel streaming aggregation from S3.
type ParallelStreamer struct {
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

	// Metrics
	objectsProcessed atomic.Int64
	bytesProcessed   atomic.Int64
	chunksProcessed  atomic.Int64
	chunksSkipped    atomic.Int64
}

// NewParallelStreamer creates a new parallel streamer.
func NewParallelStreamer(client *s3fetch.Client, cfg StreamConfig) (*ParallelStreamer, error) {
	cfg.BuildOptions.Validate()

	agg, err := Open(cfg.SQLiteConfig)
	if err != nil {
		return nil, fmt.Errorf("open aggregator: %w", err)
	}

	return &ParallelStreamer{
		client:  client,
		cfg:     cfg,
		opts:    cfg.BuildOptions,
		agg:     agg,
		log:     logging.WithPhase("parallel_stream"),
		tierMap: tiers.NewMapping(),
	}, nil
}

// Close releases resources.
func (ps *ParallelStreamer) Close() error {
	if ps.agg != nil {
		return ps.agg.Close()
	}
	return nil
}

// Stream performs parallel streaming aggregation.
func (ps *ParallelStreamer) Stream(ctx context.Context) (*StreamResult, error) {
	// Parse manifest URI
	bucket, key, err := s3fetch.ParseS3URI(ps.cfg.ManifestURI)
	if err != nil {
		return nil, fmt.Errorf("parse manifest URI: %w", err)
	}

	ps.log.Info().
		Str("manifest_uri", ps.cfg.ManifestURI).
		Str("db_path", ps.cfg.DBPath).
		Int("s3_concurrency", ps.opts.S3DownloadConcurrency).
		Int("parse_workers", ps.opts.ParseWorkers).
		Msg("starting parallel streaming aggregation")

	// Fetch manifest
	manifest, err := ps.client.FetchManifest(ctx, bucket, key)
	if err != nil {
		return nil, fmt.Errorf("fetch manifest: %w", err)
	}

	// Get column indices
	ps.keyCol, err = manifest.KeyColumnIndex()
	if err != nil {
		return nil, fmt.Errorf("get key column: %w", err)
	}
	ps.sizeCol, err = manifest.SizeColumnIndex()
	if err != nil {
		return nil, fmt.Errorf("get size column: %w", err)
	}
	ps.storageCol = manifest.StorageClassColumnIndex()
	ps.accessTierCol = manifest.AccessTierColumnIndex()

	ps.destBucket, err = manifest.GetDestinationBucketName()
	if err != nil {
		return nil, fmt.Errorf("get destination bucket: %w", err)
	}

	totalChunks := len(manifest.Files)
	ps.log.Info().
		Int("total_chunks", totalChunks).
		Str("source_bucket", manifest.SourceBucket).
		Msg("processing inventory chunks")

	startTime := time.Now()

	// Create chunk channel
	chunksCh := make(chan ChunkTask, ps.opts.S3DownloadConcurrency*2)

	// Results channel for chunk processing
	resultsCh := make(chan ChunkResult, ps.opts.S3DownloadConcurrency*2)

	// Start chunk scheduler goroutine
	go func() {
		defer close(chunksCh)
		for _, file := range manifest.Files {
			if ctx.Err() != nil {
				return
			}

			// Check if chunk already processed
			done, err := ps.agg.ChunkDone(file.Key)
			if err != nil {
				ps.log.Error().Err(err).Str("chunk", file.Key).Msg("failed to check chunk status")
				continue
			}
			if done {
				ps.chunksSkipped.Add(1)
				continue
			}

			select {
			case chunksCh <- ChunkTask{
				ChunkID: file.Key,
				Bucket:  ps.destBucket,
				Key:     file.Key,
			}:
			case <-ctx.Done():
				return
			}
		}
	}()

	// Start download workers
	var wg sync.WaitGroup
	for i := 0; i < ps.opts.S3DownloadConcurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ps.downloadWorker(ctx, chunksCh, resultsCh)
		}()
	}

	// Close results channel when all workers are done
	go func() {
		wg.Wait()
		close(resultsCh)
	}()

	// Collect results
	var firstErr error
	for result := range resultsCh {
		if result.Err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("chunk %s: %w", result.ChunkID, result.Err)
			}
			ps.log.Error().Err(result.Err).Str("chunk", result.ChunkID).Msg("chunk processing failed")
			continue
		}

		ps.chunksProcessed.Add(1)
		ps.objectsProcessed.Add(result.Objects)
		ps.bytesProcessed.Add(result.Bytes)

		ps.log.Debug().
			Str("chunk", result.ChunkID).
			Int64("objects", result.Objects).
			Int64("bytes", result.Bytes).
			Msg("chunk processed")
	}

	elapsed := time.Since(startTime)

	ps.log.Info().
		Int64("chunks_processed", ps.chunksProcessed.Load()).
		Int64("chunks_skipped", ps.chunksSkipped.Load()).
		Int64("objects_processed", ps.objectsProcessed.Load()).
		Int64("bytes_processed", ps.bytesProcessed.Load()).
		Dur("elapsed", elapsed).
		Msg("parallel streaming aggregation complete")

	if firstErr != nil && ctx.Err() == nil {
		return nil, firstErr
	}

	return &StreamResult{
		ChunksProcessed:  int(ps.chunksProcessed.Load()),
		ChunksSkipped:    int(ps.chunksSkipped.Load()),
		TotalChunks:      totalChunks,
		ObjectsProcessed: ps.objectsProcessed.Load(),
		BytesProcessed:   ps.bytesProcessed.Load(),
	}, nil
}

func (ps *ParallelStreamer) downloadWorker(ctx context.Context, chunks <-chan ChunkTask, results chan<- ChunkResult) {
	for chunk := range chunks {
		if ctx.Err() != nil {
			return
		}

		result := ps.processChunk(ctx, chunk)
		select {
		case results <- result:
		case <-ctx.Done():
			return
		}
	}
}

func (ps *ParallelStreamer) processChunk(ctx context.Context, chunk ChunkTask) ChunkResult {
	result := ChunkResult{ChunkID: chunk.ChunkID}

	// Stream the chunk from S3
	body, err := ps.client.StreamObject(ctx, chunk.Bucket, chunk.Key)
	if err != nil {
		result.Err = fmt.Errorf("stream object: %w", err)
		return result
	}
	defer body.Close()

	// Decompress gzip
	var reader io.Reader = body
	if strings.HasSuffix(strings.ToLower(chunk.Key), ".gz") {
		gzr, err := gzip.NewReader(body)
		if err != nil {
			result.Err = fmt.Errorf("create gzip reader: %w", err)
			return result
		}
		defer gzr.Close()
		reader = gzr
	}

	// Create CSV reader
	csvr := csv.NewReader(reader)
	csvr.ReuseRecord = true
	csvr.FieldsPerRecord = -1
	csvr.LazyQuotes = true

	// Parse all rows into memory first
	var rows []RowData
	for {
		if ctx.Err() != nil {
			result.Err = ctx.Err()
			return result
		}

		fields, err := csvr.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			result.Err = fmt.Errorf("read CSV row: %w", err)
			return result
		}

		// Skip rows with insufficient columns
		if len(fields) <= ps.keyCol || len(fields) <= ps.sizeCol {
			continue
		}

		objKey := fields[ps.keyCol]
		if objKey == "" {
			continue
		}

		sizeStr := strings.TrimSpace(fields[ps.sizeCol])
		size, err := strconv.ParseUint(sizeStr, 10, 64)
		if err != nil {
			size = 0
		}

		// Determine tier
		var storageClass, accessTier string
		if ps.storageCol >= 0 && len(fields) > ps.storageCol {
			storageClass = fields[ps.storageCol]
		}
		if ps.accessTierCol >= 0 && len(fields) > ps.accessTierCol {
			accessTier = fields[ps.accessTierCol]
		}
		tierID := ps.tierMap.FromS3(storageClass, accessTier)

		rows = append(rows, RowData{
			Key:    objKey,
			Size:   size,
			TierID: tierID,
		})

		result.Objects++
		result.Bytes += int64(size)
	}

	// Now write to SQLite - this must be serialized
	// We use a mutex in the aggregator to ensure only one transaction at a time
	if err := ps.writeChunkToSQLite(chunk.ChunkID, rows); err != nil {
		result.Err = err
		return result
	}

	return result
}

// writeChunkToSQLite writes a chunk's rows to SQLite.
// This is called from multiple goroutines but internally serialized.
func (ps *ParallelStreamer) writeChunkToSQLite(chunkID string, rows []RowData) error {
	// Lock to ensure only one writer at a time
	ps.agg.writeMu.Lock()
	defer ps.agg.writeMu.Unlock()

	// Begin transaction
	if err := ps.agg.BeginChunk(); err != nil {
		return fmt.Errorf("begin chunk: %w", err)
	}

	// Add all objects
	for _, row := range rows {
		if err := ps.agg.AddObject(row.Key, row.Size, row.TierID); err != nil {
			ps.agg.Rollback()
			return fmt.Errorf("add object: %w", err)
		}
	}

	// Mark chunk as done
	if err := ps.agg.MarkChunkDone(chunkID); err != nil {
		ps.agg.Rollback()
		return fmt.Errorf("mark chunk done: %w", err)
	}

	// Commit
	if err := ps.agg.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}

	return nil
}

// StreamFromS3Parallel streams inventory chunks in parallel from S3 into the SQLite aggregator.
// This function is resumable - it will skip chunks that have already been processed.
func StreamFromS3Parallel(ctx context.Context, client *s3fetch.Client, cfg StreamConfig) (*StreamResult, error) {
	ps, err := NewParallelStreamer(client, cfg)
	if err != nil {
		return nil, err
	}
	defer ps.Close()

	return ps.Stream(ctx)
}
