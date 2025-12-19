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
	"time"

	"github.com/eunmann/s3-inv-db/pkg/logging"
	"github.com/eunmann/s3-inv-db/pkg/s3fetch"
	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

// StreamConfig configures the streaming aggregation.
type StreamConfig struct {
	// ManifestURI is the S3 URI to the manifest.json file.
	ManifestURI string
	// DBPath is the path to the SQLite database file.
	DBPath string
	// SQLiteConfig is the SQLite configuration.
	SQLiteConfig Config
	// BuildOptions controls concurrency and performance settings.
	// If nil or zero-valued, defaults will be used.
	BuildOptions BuildOptions
}

// StreamResult contains the results of streaming aggregation.
type StreamResult struct {
	// ChunksProcessed is the number of chunks processed in this run.
	ChunksProcessed int
	// ChunksSkipped is the number of chunks skipped (already done).
	ChunksSkipped int
	// TotalChunks is the total number of chunks in the manifest.
	TotalChunks int
	// ObjectsProcessed is the total objects processed in this run.
	ObjectsProcessed int64
	// BytesProcessed is the total bytes processed in this run.
	BytesProcessed int64
}

// StreamFromS3 streams inventory chunks directly from S3 into the SQLite aggregator.
// This function is resumable - it will skip chunks that have already been processed.
// When S3DownloadConcurrency > 1, it uses pipelined streaming for better throughput.
func StreamFromS3(ctx context.Context, client *s3fetch.Client, cfg StreamConfig) (*StreamResult, error) {
	cfg.BuildOptions.Validate()

	// Use pipelined streaming when concurrency > 1
	// This gives us parallel S3 downloads + parallel parsing + batched SQLite writes
	if cfg.BuildOptions.S3DownloadConcurrency > 1 {
		return StreamFromS3Pipeline(ctx, client, cfg)
	}

	log := logging.WithPhase("stream_aggregate")

	// Parse manifest URI
	bucket, key, err := s3fetch.ParseS3URI(cfg.ManifestURI)
	if err != nil {
		return nil, fmt.Errorf("parse manifest URI: %w", err)
	}

	log.Info().
		Str("manifest_uri", cfg.ManifestURI).
		Str("db_path", cfg.DBPath).
		Msg("starting streaming aggregation")

	// Fetch manifest
	manifest, err := client.FetchManifest(ctx, bucket, key)
	if err != nil {
		return nil, fmt.Errorf("fetch manifest: %w", err)
	}

	// Get column indices
	keyCol, err := manifest.KeyColumnIndex()
	if err != nil {
		return nil, fmt.Errorf("get key column: %w", err)
	}
	sizeCol, err := manifest.SizeColumnIndex()
	if err != nil {
		return nil, fmt.Errorf("get size column: %w", err)
	}
	storageCol := manifest.StorageClassColumnIndex()
	accessTierCol := manifest.AccessTierColumnIndex()

	// Get destination bucket name
	destBucket, err := manifest.GetDestinationBucketName()
	if err != nil {
		return nil, fmt.Errorf("get destination bucket: %w", err)
	}

	// Open SQLite aggregator
	agg, err := Open(cfg.SQLiteConfig)
	if err != nil {
		return nil, fmt.Errorf("open aggregator: %w", err)
	}
	defer agg.Close()

	result := &StreamResult{
		TotalChunks: len(manifest.Files),
	}

	startTime := time.Now()
	lastLogTime := time.Now()

	log.Info().
		Int("total_chunks", result.TotalChunks).
		Str("source_bucket", manifest.SourceBucket).
		Msg("processing inventory chunks")

	for i, file := range manifest.Files {
		if ctx.Err() != nil {
			return result, ctx.Err()
		}

		chunkID := file.Key

		// Check if chunk already processed
		done, err := agg.ChunkDone(chunkID)
		if err != nil {
			return result, fmt.Errorf("check chunk done: %w", err)
		}
		if done {
			result.ChunksSkipped++
			continue
		}

		// Process chunk
		chunkResult, err := processChunk(ctx, client, agg, destBucket, file.Key, keyCol, sizeCol, storageCol, accessTierCol)
		if err != nil {
			return result, fmt.Errorf("process chunk %s: %w", file.Key, err)
		}

		result.ChunksProcessed++
		result.ObjectsProcessed += chunkResult.objects
		result.BytesProcessed += chunkResult.bytes

		// Log progress every 5 seconds or every chunk
		if time.Since(lastLogTime) >= 5*time.Second {
			log.Info().
				Int("chunks_done", result.ChunksProcessed+result.ChunksSkipped).
				Int("chunks_total", result.TotalChunks).
				Int("chunks_skipped", result.ChunksSkipped).
				Int64("objects_processed", result.ObjectsProcessed).
				Int64("bytes_processed", result.BytesProcessed).
				Dur("elapsed", time.Since(startTime)).
				Msg("aggregation progress")
			lastLogTime = time.Now()
		}

		log.Debug().
			Str("chunk", file.Key).
			Int("chunk_index", i+1).
			Int64("objects", chunkResult.objects).
			Msg("processed chunk")
	}

	log.Info().
		Int("chunks_processed", result.ChunksProcessed).
		Int("chunks_skipped", result.ChunksSkipped).
		Int64("objects_processed", result.ObjectsProcessed).
		Int64("bytes_processed", result.BytesProcessed).
		Dur("elapsed", time.Since(startTime)).
		Msg("streaming aggregation complete")

	return result, nil
}

type chunkResult struct {
	objects int64
	bytes   int64
}

func processChunk(
	ctx context.Context,
	client *s3fetch.Client,
	agg *Aggregator,
	bucket, key string,
	keyCol, sizeCol, storageCol, accessTierCol int,
) (*chunkResult, error) {
	// Stream the chunk from S3
	body, err := client.StreamObject(ctx, bucket, key)
	if err != nil {
		return nil, fmt.Errorf("stream object: %w", err)
	}
	defer body.Close()

	// Decompress gzip
	var reader io.Reader = body
	if strings.HasSuffix(strings.ToLower(key), ".gz") {
		gzr, err := gzip.NewReader(body)
		if err != nil {
			return nil, fmt.Errorf("create gzip reader: %w", err)
		}
		defer gzr.Close()
		reader = gzr
	}

	// Create CSV reader
	csvr := csv.NewReader(reader)
	csvr.ReuseRecord = true
	csvr.FieldsPerRecord = -1
	csvr.LazyQuotes = true

	// Begin transaction for this chunk
	if err := agg.BeginChunk(); err != nil {
		return nil, fmt.Errorf("begin chunk: %w", err)
	}

	// Process rows
	tierMapping := tiers.NewMapping()
	result := &chunkResult{}

	for {
		if ctx.Err() != nil {
			agg.Rollback()
			return nil, ctx.Err()
		}

		fields, err := csvr.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			agg.Rollback()
			return nil, fmt.Errorf("read CSV row: %w", err)
		}

		// Skip rows with insufficient columns
		if len(fields) <= keyCol || len(fields) <= sizeCol {
			continue
		}

		objKey := fields[keyCol]
		if objKey == "" {
			continue
		}

		sizeStr := strings.TrimSpace(fields[sizeCol])
		size, err := strconv.ParseUint(sizeStr, 10, 64)
		if err != nil {
			// Treat invalid size as 0
			size = 0
		}

		// Determine tier
		var storageClass, accessTier string
		if storageCol >= 0 && len(fields) > storageCol {
			storageClass = fields[storageCol]
		}
		if accessTierCol >= 0 && len(fields) > accessTierCol {
			accessTier = fields[accessTierCol]
		}
		tierID := tierMapping.FromS3(storageClass, accessTier)

		// Add object to aggregator
		if err := agg.AddObject(objKey, size, tierID); err != nil {
			agg.Rollback()
			return nil, fmt.Errorf("add object: %w", err)
		}

		result.objects++
		result.bytes += int64(size)
	}

	// Mark chunk as done and commit
	if err := agg.MarkChunkDone(key); err != nil {
		agg.Rollback()
		return nil, fmt.Errorf("mark chunk done: %w", err)
	}

	if err := agg.Commit(); err != nil {
		return nil, fmt.Errorf("commit: %w", err)
	}

	return result, nil
}

// AllChunksProcessed returns true if all chunks in the manifest have been processed.
func AllChunksProcessed(ctx context.Context, client *s3fetch.Client, manifestURI string, agg *Aggregator) (bool, error) {
	// Parse manifest URI
	bucket, key, err := s3fetch.ParseS3URI(manifestURI)
	if err != nil {
		return false, fmt.Errorf("parse manifest URI: %w", err)
	}

	// Fetch manifest
	manifest, err := client.FetchManifest(ctx, bucket, key)
	if err != nil {
		return false, fmt.Errorf("fetch manifest: %w", err)
	}

	// Check each chunk
	for _, file := range manifest.Files {
		done, err := agg.ChunkDone(file.Key)
		if err != nil {
			return false, fmt.Errorf("check chunk %s: %w", file.Key, err)
		}
		if !done {
			return false, nil
		}
	}

	return true, nil
}
