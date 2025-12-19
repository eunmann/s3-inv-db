// Package indexbuild constructs S3 inventory indexes from SQLite aggregated data.
package indexbuild

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/eunmann/s3-inv-db/pkg/format"
	"github.com/eunmann/s3-inv-db/pkg/logging"
	"github.com/eunmann/s3-inv-db/pkg/sqliteagg"
	"github.com/eunmann/s3-inv-db/pkg/triebuild"
)

// SQLiteConfig holds configuration for building from a SQLite aggregator.
type SQLiteConfig struct {
	// OutDir is the output directory for index files.
	OutDir string
	// DBPath is the path to the SQLite prefix aggregation database.
	DBPath string
	// SQLiteCfg is the SQLite configuration.
	SQLiteCfg sqliteagg.Config
	// BuildOptions controls concurrency and performance settings.
	BuildOptions sqliteagg.BuildOptions
}

// BuildFromSQLite builds an index from a pre-populated SQLite prefix aggregation database.
// The database must have been populated using sqliteagg.StreamFromS3 or equivalent.
// This always rebuilds from scratch - any existing output directory is replaced.
func BuildFromSQLite(cfg SQLiteConfig) error {
	cfg.BuildOptions.Validate()
	log := logging.L()
	buildStart := time.Now()

	if cfg.OutDir == "" {
		return fmt.Errorf("output directory required")
	}
	if cfg.DBPath == "" {
		return fmt.Errorf("database path required")
	}

	// Remove any existing output directory to ensure clean build
	if err := os.RemoveAll(cfg.OutDir); err != nil {
		return fmt.Errorf("remove existing output dir: %w", err)
	}

	// Open SQLite aggregator
	agg, err := sqliteagg.Open(cfg.SQLiteCfg)
	if err != nil {
		return fmt.Errorf("open SQLite aggregator: %w", err)
	}
	defer agg.Close()

	// Build trie from SQLite
	result, err := sqliteagg.BuildTrieFromSQLite(agg)
	if err != nil {
		return fmt.Errorf("build trie from SQLite: %w", err)
	}

	if len(result.Nodes) == 0 {
		return fmt.Errorf("no nodes built from SQLite database")
	}

	// Create temp output directory
	tmpOutDir := cfg.OutDir + ".tmp"
	if err := os.MkdirAll(tmpOutDir, 0755); err != nil {
		return fmt.Errorf("create temp output dir: %w", err)
	}

	// Track whether we successfully renamed - only cleanup on failure
	renamed := false
	defer func() {
		if !renamed {
			os.RemoveAll(tmpOutDir)
		}
	}()

	// Write all index files in parallel
	if err := writeIndexFilesParallel(tmpOutDir, result, cfg.BuildOptions.FileWriteConcurrency); err != nil {
		return err
	}

	// Write manifest with checksums (must be after all files are written)
	if err := format.WriteManifest(tmpOutDir, uint64(len(result.Nodes)), result.MaxDepth); err != nil {
		return fmt.Errorf("write manifest: %w", err)
	}

	// Sync directory to ensure all files are persisted
	if err := format.SyncDir(tmpOutDir); err != nil {
		return fmt.Errorf("sync output dir: %w", err)
	}

	// Atomic move to final location
	if err := os.RemoveAll(cfg.OutDir); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove old output dir: %w", err)
	}
	if err := os.Rename(tmpOutDir, cfg.OutDir); err != nil {
		return fmt.Errorf("rename output dir: %w", err)
	}
	renamed = true

	// Best-effort sync of parent directory to persist the rename
	parentDir := filepath.Dir(cfg.OutDir)
	_ = format.SyncDir(parentDir)

	logging.PhaseComplete(log.With().Str("phase", "build_index").Logger(), "build_index", time.Since(buildStart)).
		Str("output_dir", cfg.OutDir).
		Int("node_count", len(result.Nodes)).
		Log("index build from SQLite complete")

	return nil
}

// writeTask represents a file writing task.
type writeTask struct {
	name string
	fn   func() (int64, error) // returns bytes written
}

// writeIndexFilesParallel writes all index files using a worker pool.
func writeIndexFilesParallel(outDir string, result *triebuild.Result, concurrency int) error {
	if concurrency <= 0 {
		concurrency = 4
	}

	log := logging.WithPhase("write_index")
	start := time.Now()

	log.Info().
		Str("output_dir", outDir).
		Int("node_count", len(result.Nodes)).
		Int("concurrency", concurrency).
		Msg("writing index files in parallel")

	// Create tasks for all file writes
	tasks := []writeTask{
		// Columnar arrays
		{"subtree_end.u64", func() (int64, error) {
			return writeU64Array(outDir, "subtree_end.u64", result.Nodes, func(n triebuild.Node) uint64 { return n.SubtreeEnd })
		}},
		{"object_count.u64", func() (int64, error) {
			return writeU64Array(outDir, "object_count.u64", result.Nodes, func(n triebuild.Node) uint64 { return n.ObjectCount })
		}},
		{"total_bytes.u64", func() (int64, error) {
			return writeU64Array(outDir, "total_bytes.u64", result.Nodes, func(n triebuild.Node) uint64 { return n.TotalBytes })
		}},
		{"depth.u32", func() (int64, error) {
			return writeU32Array(outDir, "depth.u32", result.Nodes, func(n triebuild.Node) uint32 { return n.Depth })
		}},
		{"max_depth_in_subtree.u32", func() (int64, error) {
			return writeU32Array(outDir, "max_depth_in_subtree.u32", result.Nodes, func(n triebuild.Node) uint32 { return n.MaxDepthInSubtree })
		}},
		// Depth index
		{"depth_index", func() (int64, error) { return writeDepthIndex(outDir, result) }},
		// MPHF
		{"mphf", func() (int64, error) { return writeMPHF(outDir, result) }},
	}

	// Add tier stats if tracking enabled
	if result.TrackTiers && len(result.PresentTiers) > 0 {
		tasks = append(tasks, writeTask{"tier_stats", func() (int64, error) { return writeTierStats(outDir, result) }})
	}

	// Create task channel
	taskCh := make(chan writeTask, len(tasks))
	for _, t := range tasks {
		taskCh <- t
	}
	close(taskCh)

	// Error collection
	var (
		errMu    sync.Mutex
		firstErr error
	)

	// Track total bytes written
	var totalBytes int64
	var bytesMu sync.Mutex

	// Start workers
	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for task := range taskCh {
				taskStart := time.Now()
				bytesWritten, err := task.fn()
				if err != nil {
					errMu.Lock()
					if firstErr == nil {
						firstErr = fmt.Errorf("%s: %w", task.name, err)
					}
					errMu.Unlock()
					return
				}

				// Log file creation
				taskDuration := time.Since(taskStart)
				logging.FileCreated(log, "write_index", taskDuration).
					Str("file", task.name).
					Bytes("bytes", bytesWritten).
					Throughput(bytesWritten).
					Log("index file written")

				bytesMu.Lock()
				totalBytes += bytesWritten
				bytesMu.Unlock()
			}
		}()
	}

	wg.Wait()

	if firstErr != nil {
		return firstErr
	}

	elapsed := time.Since(start)
	logging.PhaseComplete(log, "write_index", elapsed).
		Int("files_written", len(tasks)).
		Bytes("total_bytes", totalBytes).
		Throughput(totalBytes).
		Log("index files written")

	return nil
}

func writeU64Array(outDir, name string, nodes []triebuild.Node, getter func(triebuild.Node) uint64) (int64, error) {
	path := filepath.Join(outDir, name)
	writer, err := format.NewArrayWriter(path, 8)
	if err != nil {
		return 0, fmt.Errorf("create writer: %w", err)
	}
	for i, node := range nodes {
		if err := writer.WriteU64(getter(node)); err != nil {
			writer.Close()
			return 0, fmt.Errorf("write node %d: %w", i, err)
		}
	}
	if err := writer.Close(); err != nil {
		return 0, fmt.Errorf("close writer: %w", err)
	}
	// Calculate bytes written: header (16 bytes) + data (8 bytes per node)
	bytesWritten := int64(16 + len(nodes)*8)
	return bytesWritten, nil
}

func writeU32Array(outDir, name string, nodes []triebuild.Node, getter func(triebuild.Node) uint32) (int64, error) {
	path := filepath.Join(outDir, name)
	writer, err := format.NewArrayWriter(path, 4)
	if err != nil {
		return 0, fmt.Errorf("create writer: %w", err)
	}
	for i, node := range nodes {
		if err := writer.WriteU32(getter(node)); err != nil {
			writer.Close()
			return 0, fmt.Errorf("write node %d: %w", i, err)
		}
	}
	if err := writer.Close(); err != nil {
		return 0, fmt.Errorf("close writer: %w", err)
	}
	// Calculate bytes written: header (16 bytes) + data (4 bytes per node)
	bytesWritten := int64(16 + len(nodes)*4)
	return bytesWritten, nil
}

func writeDepthIndex(outDir string, result *triebuild.Result) (int64, error) {
	builder := format.NewDepthIndexBuilder()

	for _, node := range result.Nodes {
		builder.Add(node.Pos, node.Depth)
	}

	if err := builder.Build(outDir); err != nil {
		return 0, fmt.Errorf("build depth index: %w", err)
	}

	// Estimate bytes: header + depth entries
	bytesWritten := int64(16 + len(result.Nodes)*4)
	return bytesWritten, nil
}

func writeMPHF(outDir string, result *triebuild.Result) (int64, error) {
	builder := format.NewMPHFBuilder()

	for _, node := range result.Nodes {
		builder.Add(node.Prefix, node.Pos)
	}

	if err := builder.Build(outDir); err != nil {
		return 0, fmt.Errorf("build MPHF: %w", err)
	}

	// Get file size for bytes written
	bytesWritten, _ := getFileSize(filepath.Join(outDir, "mphf.bin"))
	// Also add keys file size
	keysSize, _ := getFileSize(filepath.Join(outDir, "mphf_keys.bin"))
	bytesWritten += keysSize

	return bytesWritten, nil
}

func writeTierStats(outDir string, result *triebuild.Result) (int64, error) {
	if !result.TrackTiers || len(result.PresentTiers) == 0 {
		return 0, nil
	}

	tierWriter, err := format.NewTierStatsWriter(outDir)
	if err != nil {
		return 0, fmt.Errorf("create tier stats writer: %w", err)
	}
	if err := tierWriter.Write(result); err != nil {
		return 0, err
	}

	// Sum up all tier stats file sizes
	var bytesWritten int64
	for _, tier := range result.PresentTiers {
		countSize, _ := getFileSize(filepath.Join(outDir, fmt.Sprintf("tier_%d_count.u64", tier)))
		bytesSize, _ := getFileSize(filepath.Join(outDir, fmt.Sprintf("tier_%d_bytes.u64", tier)))
		bytesWritten += countSize + bytesSize
	}

	return bytesWritten, nil
}

// getFileSize returns the size of a file in bytes.
func getFileSize(path string) (int64, error) {
	info, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

