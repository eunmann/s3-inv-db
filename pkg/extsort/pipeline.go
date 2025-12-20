package extsort

import (
	"compress/gzip"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/eunmann/s3-inv-db/pkg/logging"
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
	if config.MemoryThreshold <= 0 {
		config.MemoryThreshold = 256 * 1024 * 1024 // 256MB default
	}
	if config.RunFileBufferSize <= 0 {
		config.RunFileBufferSize = 4 * 1024 * 1024 // 4MB default
	}

	return &Pipeline{
		config:   config,
		s3Client: s3Client,
		runFiles: make([]string, 0, 16),
	}
}

// Run executes the full pipeline.
func (p *Pipeline) Run(ctx context.Context, manifestURI, outDir string) (*Result, error) {
	p.startTime = time.Now()
	log := logging.L()

	// Create temp directory for run files
	tempDir := p.config.TempDir
	if tempDir == "" {
		var err error
		tempDir, err = os.MkdirTemp("", "extsort-*")
		if err != nil {
			return nil, fmt.Errorf("create temp dir: %w", err)
		}
	}
	p.tempDir = tempDir

	// Ensure cleanup on failure
	success := false
	defer func() {
		if !success {
			p.cleanup()
		}
	}()

	log.Info().
		Str("manifest_uri", manifestURI).
		Str("temp_dir", tempDir).
		Int64("memory_threshold_mb", p.config.MemoryThreshold/(1024*1024)).
		Msg("starting external sort pipeline")

	// Stage A+B: Stream, aggregate, and spill to run files
	if err := p.runIngestPhase(ctx, manifestURI); err != nil {
		return nil, fmt.Errorf("ingest phase: %w", err)
	}

	log.Info().
		Int("run_files", len(p.runFiles)).
		Int64("objects_processed", p.objectsProcessed).
		Int64("flushes", p.flushCount).
		Msg("ingest phase complete")

	// Stage C+D: Merge run files and build index
	prefixCount, maxDepth, err := p.runMergeBuildPhase(ctx, outDir)
	if err != nil {
		return nil, fmt.Errorf("merge/build phase: %w", err)
	}

	// Cleanup temp files
	p.cleanup()
	success = true

	duration := time.Since(p.startTime)
	log.Info().
		Dur("duration", duration).
		Int64("objects_processed", p.objectsProcessed).
		Uint64("prefix_count", prefixCount).
		Uint32("max_depth", maxDepth).
		Msg("external sort pipeline complete")

	return &Result{
		ChunksProcessed:  int(p.chunksProcessed),
		ObjectsProcessed: p.objectsProcessed,
		PrefixCount:      prefixCount,
		MaxDepth:         maxDepth,
		RunFilesCreated:  len(p.runFiles),
		Duration:         duration,
	}, nil
}

// runIngestPhase streams S3 inventory and creates sorted run files.
func (p *Pipeline) runIngestPhase(ctx context.Context, manifestURI string) error {
	log := logging.L()

	// Parse manifest URI
	bucket, key, err := s3fetch.ParseS3URI(manifestURI)
	if err != nil {
		return fmt.Errorf("parse manifest URI: %w", err)
	}

	// Fetch manifest
	manifest, err := p.s3Client.FetchManifest(ctx, bucket, key)
	if err != nil {
		return fmt.Errorf("fetch manifest: %w", err)
	}

	// Get column indices
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

	// Get destination bucket name
	destBucket, err := manifest.GetDestinationBucketName()
	if err != nil {
		return fmt.Errorf("get destination bucket: %w", err)
	}

	// Create aggregator
	agg := NewAggregator(100000, p.config.MaxDepth)
	tierMapping := tiers.NewMapping()

	// Process each chunk
	for i, file := range manifest.Files {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		log.Debug().
			Int("chunk", i+1).
			Int("total_chunks", len(manifest.Files)).
			Str("key", file.Key).
			Msg("processing inventory chunk")

		// Stream chunk and aggregate
		chunkCfg := chunkConfig{
			keyCol:        keyCol,
			sizeCol:       sizeCol,
			storageCol:    storageCol,
			accessTierCol: accessTierCol,
			tierMapping:   tierMapping,
		}
		if err := p.processChunk(ctx, destBucket, file.Key, agg, chunkCfg); err != nil {
			return fmt.Errorf("process chunk %d: %w", i, err)
		}

		atomic.AddInt64(&p.chunksProcessed, 1)

		// Check memory threshold and flush if needed
		if agg.EstimatedMemoryUsage() >= p.config.MemoryThreshold {
			if err := p.flushAggregator(agg); err != nil {
				return fmt.Errorf("flush aggregator: %w", err)
			}
		}
	}

	// Flush any remaining data
	if agg.PrefixCount() > 0 {
		if err := p.flushAggregator(agg); err != nil {
			return fmt.Errorf("final flush: %w", err)
		}
	}

	return nil
}

// chunkConfig holds configuration for processing a chunk.
type chunkConfig struct {
	keyCol        int
	sizeCol       int
	storageCol    int
	accessTierCol int
	tierMapping   *tiers.Mapping
}

// processChunk processes a single inventory CSV chunk.
func (p *Pipeline) processChunk(ctx context.Context, bucket, key string, agg *Aggregator, cfg chunkConfig) error {
	// Stream object from S3
	body, err := p.s3Client.StreamObject(ctx, bucket, key)
	if err != nil {
		return fmt.Errorf("stream object: %w", err)
	}
	defer body.Close()

	// Decompress if needed
	reader, closeGzip, err := decompressReader(body, key)
	if err != nil {
		return fmt.Errorf("decompress: %w", err)
	}
	if closeGzip != nil {
		defer closeGzip()
	}

	// Create CSV reader
	csvr := newInventoryReader(reader)

	// Process each row
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		fields, err := csvr.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("read CSV row: %w", err)
		}

		// Skip rows with insufficient columns
		if len(fields) <= cfg.keyCol || len(fields) <= cfg.sizeCol {
			continue
		}

		objKey := fields[cfg.keyCol]
		if objKey == "" {
			continue
		}

		sizeStr := strings.TrimSpace(fields[cfg.sizeCol])
		size, err := strconv.ParseUint(sizeStr, 10, 64)
		if err != nil {
			size = 0
		}

		// Determine tier
		var storageClass, accessTier string
		if cfg.storageCol >= 0 && len(fields) > cfg.storageCol {
			storageClass = fields[cfg.storageCol]
		}
		if cfg.accessTierCol >= 0 && len(fields) > cfg.accessTierCol {
			accessTier = fields[cfg.accessTierCol]
		}
		tierID := cfg.tierMapping.FromS3(storageClass, accessTier)

		// Add to aggregator
		agg.AddObject(objKey, size, tierID)

		atomic.AddInt64(&p.objectsProcessed, 1)
		atomic.AddInt64(&p.bytesProcessed, int64(size))
	}

	return nil
}

// newInventoryReader creates a csv.Reader configured for S3 inventory files.
func newInventoryReader(r io.Reader) *csv.Reader {
	csvr := csv.NewReader(r)
	csvr.ReuseRecord = true
	csvr.FieldsPerRecord = -1
	csvr.LazyQuotes = true
	return csvr
}

// decompressReader wraps a reader with gzip decompression if the key ends in .gz.
func decompressReader(r io.Reader, key string) (io.Reader, func() error, error) {
	if !strings.HasSuffix(strings.ToLower(key), ".gz") {
		return r, nil, nil
	}

	gzr, err := gzip.NewReader(r)
	if err != nil {
		return nil, nil, fmt.Errorf("create gzip reader: %w", err)
	}
	return gzr, gzr.Close, nil
}

// flushAggregator drains the aggregator to a sorted run file.
func (p *Pipeline) flushAggregator(agg *Aggregator) error {
	log := logging.L()
	start := time.Now()

	// Drain to slice
	rows := agg.Drain()
	if len(rows) == 0 {
		return nil
	}

	// Create run file
	runPath := filepath.Join(p.tempDir, fmt.Sprintf("run_%04d.bin", p.runCount))
	p.runCount++

	writer, err := NewRunFileWriter(runPath, p.config.RunFileBufferSize)
	if err != nil {
		return fmt.Errorf("create run file: %w", err)
	}

	// Sort and write
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

	log.Debug().
		Str("path", runPath).
		Int("prefix_count", len(rows)).
		Dur("duration", time.Since(start)).
		Msg("flushed run file")

	return nil
}

// runMergeBuildPhase merges run files and builds the index.
func (p *Pipeline) runMergeBuildPhase(ctx context.Context, outDir string) (uint64, uint32, error) {
	log := logging.L()

	if len(p.runFiles) == 0 {
		// No data - create empty index
		builder, err := NewIndexBuilder(outDir)
		if err != nil {
			return 0, 0, fmt.Errorf("create index builder: %w", err)
		}
		if err := builder.Finalize(); err != nil {
			return 0, 0, fmt.Errorf("finalize empty index: %w", err)
		}
		return 0, 0, nil
	}

	log.Info().
		Int("run_files", len(p.runFiles)).
		Msg("starting merge phase")

	// Create merge iterator
	merger, err := NewMergeIterator(p.runFiles, p.config.RunFileBufferSize)
	if err != nil {
		return 0, 0, fmt.Errorf("create merge iterator: %w", err)
	}
	defer merger.RemoveAll() // Clean up run files after merge

	// Create index builder
	builder, err := NewIndexBuilder(outDir)
	if err != nil {
		merger.Close()
		return 0, 0, fmt.Errorf("create index builder: %w", err)
	}

	// Stream merged data to index builder
	if err := builder.AddAll(merger); err != nil {
		return 0, 0, fmt.Errorf("build index: %w", err)
	}

	// Finalize index
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
