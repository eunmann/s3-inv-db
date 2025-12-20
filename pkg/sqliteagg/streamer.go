package sqliteagg

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/eunmann/s3-inv-db/pkg/humanfmt"
	"github.com/eunmann/s3-inv-db/pkg/logging"
	"github.com/eunmann/s3-inv-db/pkg/s3fetch"
	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

// StreamResult contains the results of streaming aggregation.
type StreamResult struct {
	// ChunksProcessed is the number of chunks processed.
	ChunksProcessed int
	// TotalChunks is the total number of chunks in the manifest.
	TotalChunks int
	// ObjectsProcessed is the total objects processed.
	ObjectsProcessed int64
	// BytesProcessed is the total bytes processed.
	BytesProcessed int64
}

// MemoryStreamConfig configures the fast in-memory streaming aggregation.
type MemoryStreamConfig struct {
	// ManifestURI is the S3 URI to the manifest.json file.
	ManifestURI string
	// DBPath is the path to the SQLite database file (for final write).
	DBPath string
	// MemoryAggregatorConfig is the configuration for the memory aggregator.
	MemoryConfig MemoryAggregatorConfig
	// S3DownloadConcurrency is the number of concurrent S3 downloads.
	// Default: 1 (sequential processing).
	S3DownloadConcurrency int
}

// StreamFromS3Memory streams inventory chunks from S3 using in-memory aggregation.
// This is faster than the standard StreamFromS3 for one-shot builds.
// All data is accumulated in memory, then written to SQLite at the end.
func StreamFromS3Memory(ctx context.Context, client *s3fetch.Client, cfg MemoryStreamConfig) (*StreamResult, error) {
	log := logging.WithPhase("stream_memory")

	// Parse manifest URI
	bucket, key, err := s3fetch.ParseS3URI(cfg.ManifestURI)
	if err != nil {
		return nil, fmt.Errorf("parse manifest URI: %w", err)
	}

	log.Info().
		Str("manifest_uri", cfg.ManifestURI).
		Str("db_path", cfg.DBPath).
		Msg("starting in-memory streaming aggregation")

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

	// Create memory aggregator
	if cfg.MemoryConfig.DBPath == "" {
		cfg.MemoryConfig.DBPath = cfg.DBPath
	}
	agg := NewMemoryAggregator(cfg.MemoryConfig)

	result := &StreamResult{
		TotalChunks: len(manifest.Files),
	}

	startTime := time.Now()
	lastLogTime := time.Now()
	tierMapping := tiers.NewMapping()

	// Create progress tracker
	progress := logging.NewProgressTracker("aggregate", int64(result.TotalChunks), log)

	log.Info().
		Int("total_chunks", result.TotalChunks).
		Str("source_bucket", manifest.SourceBucket).
		Msg("processing inventory chunks (in-memory mode)")

	for _, file := range manifest.Files {
		if ctx.Err() != nil {
			return result, ctx.Err()
		}

		chunkID := file.Key
		chunkStart := time.Now()

		// Log chunk started
		logging.ChunkStarted(log, "aggregate", chunkID, progress.Completed(), int64(result.TotalChunks))

		// Stream the chunk from S3
		body, err := client.StreamObject(ctx, destBucket, file.Key)
		if err != nil {
			return result, fmt.Errorf("stream object %s: %w", file.Key, err)
		}

		// Decompress if needed
		reader, closeGzip, err := decompressReader(body, file.Key)
		if err != nil {
			body.Close()
			return result, err
		}

		// Create CSV reader
		csvr := newInventoryReader(reader)

		var chunkObjects int64
		var chunkBytes int64

		// Process rows
		for {
			if ctx.Err() != nil {
				if closeGzip != nil {
					closeGzip()
				}
				body.Close()
				return result, ctx.Err()
			}

			fields, err := csvr.Read()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				if closeGzip != nil {
					closeGzip()
				}
				body.Close()
				return result, fmt.Errorf("read CSV row: %w", err)
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

			// Add object to memory aggregator
			agg.AddObject(objKey, size, tierID)

			chunkObjects++
			chunkBytes += int64(size)
		}

		if closeGzip != nil {
			closeGzip()
		}
		body.Close()

		chunkDuration := time.Since(chunkStart)
		result.ChunksProcessed++
		result.ObjectsProcessed += chunkObjects
		result.BytesProcessed += chunkBytes
		progress.RecordCompletion(chunkDuration)

		// Log chunk completion
		logging.ChunkComplete(log, "aggregate", chunkDuration).
			Str("chunk_id", chunkID).
			Count("objects", chunkObjects).
			Bytes("bytes", chunkBytes).
			ProgressFromTracker(progress).
			Throughput(chunkBytes).
			Log("chunk processed")

		// Log overall progress every 5 seconds
		if time.Since(lastLogTime) >= 5*time.Second {
			elapsed := time.Since(startTime)
			event := log.Info().
				Str("event", "progress").
				Int("chunks_done", result.ChunksProcessed).
				Int("chunks_total", result.TotalChunks).
				Int64("objects_processed", result.ObjectsProcessed).
				Int64("bytes_processed", result.BytesProcessed).
				Int("prefix_count", agg.PrefixCount()).
				Int64("memory_mb", agg.MemoryUsageEstimate()/(1024*1024)).
				Float64("progress_pct", progress.ProgressPct()).
				Dur("elapsed", elapsed)
			if eta := progress.ETA(); eta > 0 {
				event = event.Dur("eta", eta)
				if logging.IsPrettyMode() {
					event = event.Str("eta_h", humanfmt.Duration(eta))
				}
			}
			if logging.IsPrettyMode() {
				event = event.
					Str("objects_h", humanfmt.Count(result.ObjectsProcessed)).
					Str("bytes_h", humanfmt.Bytes(result.BytesProcessed)).
					Str("elapsed_h", humanfmt.Duration(elapsed)).
					Str("throughput_h", humanfmt.Throughput(result.BytesProcessed, elapsed))
			}
			event.Msg("aggregation progress (in-memory)")
			lastLogTime = time.Now()
		}
	}

	// Finalize: write all data to SQLite
	log.Info().
		Int("prefix_count", agg.PrefixCount()).
		Int64("memory_mb", agg.MemoryUsageEstimate()/(1024*1024)).
		Msg("finalizing in-memory aggregation to SQLite")

	if err := agg.Finalize(); err != nil {
		return result, fmt.Errorf("finalize aggregator: %w", err)
	}

	// Clear memory
	agg.Clear()

	elapsed := time.Since(startTime)
	logging.PhaseComplete(log, "aggregate", elapsed).
		Int("chunks_processed", result.ChunksProcessed).
		Count("objects_processed", result.ObjectsProcessed).
		Bytes("bytes_processed", result.BytesProcessed).
		Throughput(result.BytesProcessed).
		Log("in-memory streaming aggregation complete")

	return result, nil
}

