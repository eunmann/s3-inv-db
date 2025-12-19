// Package indexbuild constructs S3 inventory indexes from CSV files.
package indexbuild

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/eunmann/s3-inv-db/pkg/extsort"
	"github.com/eunmann/s3-inv-db/pkg/format"
	"github.com/eunmann/s3-inv-db/pkg/inventory"
	"github.com/eunmann/s3-inv-db/pkg/s3fetch"
	"github.com/eunmann/s3-inv-db/pkg/triebuild"
)

// Config holds configuration for index building.
type Config struct {
	// OutDir is the output directory for index files.
	OutDir string
	// TmpDir is the temporary directory for sort runs.
	TmpDir string
	// ChunkSize is the maximum number of records per sort chunk.
	ChunkSize int
	// TrackTiers enables per-tier byte and count tracking.
	TrackTiers bool
}

// DefaultConfig returns a default configuration.
func DefaultConfig() Config {
	return Config{
		ChunkSize: 1_000_000,
	}
}

// S3Config holds configuration for building from S3 inventory.
type S3Config struct {
	// Config is the base build configuration.
	Config
	// ManifestURI is the S3 URI to the manifest.json file (s3://bucket/path/manifest.json).
	ManifestURI string
	// DownloadConcurrency is the number of parallel downloads (default: 4).
	DownloadConcurrency int
	// KeepDownloads if true, don't delete downloaded files after building.
	KeepDownloads bool
}

// BuildFromS3 fetches inventory files from S3 and builds an index.
func BuildFromS3(ctx context.Context, cfg S3Config) error {
	if cfg.ManifestURI == "" {
		return fmt.Errorf("manifest URI required")
	}
	if cfg.OutDir == "" {
		return fmt.Errorf("output directory required")
	}
	if cfg.TmpDir == "" {
		return fmt.Errorf("temp directory required")
	}

	// Create S3 client
	client, err := s3fetch.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("create S3 client: %w", err)
	}

	// Setup download directory
	downloadDir := filepath.Join(cfg.TmpDir, "downloads")

	fetcher := s3fetch.NewFetcher(client, s3fetch.FetchConfig{
		ManifestURI: cfg.ManifestURI,
		DownloadDir: downloadDir,
		Concurrency: cfg.DownloadConcurrency,
		KeepFiles:   cfg.KeepDownloads,
	})
	defer fetcher.Cleanup()

	// Fetch manifest and download files
	result, err := fetcher.Fetch(ctx)
	if err != nil {
		return fmt.Errorf("fetch inventory: %w", err)
	}

	// Build index using downloaded files with schema from manifest
	schema := SchemaConfig{
		KeyCol:        result.KeyColumn,
		SizeCol:       result.SizeColumn,
		StorageCol:    result.StorageClassColumn,
		AccessTierCol: result.AccessTierColumn,
	}
	return BuildWithSchema(ctx, cfg.Config, result.LocalFiles, schema)
}

// SchemaConfig holds column configuration for headerless inventory files.
type SchemaConfig struct {
	KeyCol        int
	SizeCol       int
	StorageCol    int  // -1 if not available
	AccessTierCol int  // -1 if not available
}

// BuildWithSchema constructs an index from inventory files using pre-known column indices.
// This is used for AWS S3 inventory files which have no header row.
func BuildWithSchema(ctx context.Context, cfg Config, inventoryFiles []string, schema SchemaConfig) error {
	if cfg.OutDir == "" {
		return fmt.Errorf("output directory required")
	}
	if cfg.TmpDir == "" {
		return fmt.Errorf("temp directory required")
	}
	if cfg.ChunkSize <= 0 {
		cfg.ChunkSize = 1_000_000
	}

	// Create temp output directory
	tmpOutDir := cfg.OutDir + ".tmp"
	if err := os.MkdirAll(tmpOutDir, 0755); err != nil {
		return fmt.Errorf("create temp output dir: %w", err)
	}
	defer os.RemoveAll(tmpOutDir)

	// Step 1: External sort all inventory files
	sorter := extsort.NewSorter(extsort.Config{
		MaxRecordsPerChunk: cfg.ChunkSize,
		TmpDir:             cfg.TmpDir,
	})
	defer sorter.Cleanup()

	for _, path := range inventoryFiles {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		opts := inventory.SchemaOptions{
			KeyCol:        schema.KeyCol,
			SizeCol:       schema.SizeCol,
			StorageCol:    schema.StorageCol,
			AccessTierCol: schema.AccessTierCol,
			TrackTiers:    cfg.TrackTiers,
		}
		reader, err := inventory.OpenFileWithSchemaOptions(path, opts)
		if err != nil {
			return fmt.Errorf("open inventory file %s: %w", path, err)
		}

		if err := sorter.AddRecords(ctx, reader); err != nil {
			reader.Close()
			return fmt.Errorf("process inventory file %s: %w", path, err)
		}

		reader.Close()
	}

	// Step 2: Merge and build trie
	iter, err := sorter.Merge(ctx)
	if err != nil {
		return fmt.Errorf("merge sorted runs: %w", err)
	}
	defer iter.Close()

	var builder *triebuild.Builder
	if cfg.TrackTiers {
		builder = triebuild.NewWithTiers()
	} else {
		builder = triebuild.New()
	}
	trieResult, err := builder.Build(iter)
	if err != nil {
		return fmt.Errorf("build trie: %w", err)
	}

	if len(trieResult.Nodes) == 0 {
		return fmt.Errorf("no nodes built from inventory")
	}

	// Step 3: Write columnar arrays
	if err := writeColumnarArrays(tmpOutDir, trieResult); err != nil {
		return fmt.Errorf("write columnar arrays: %w", err)
	}

	// Step 4: Write tier stats (if tracking enabled)
	if cfg.TrackTiers {
		tierWriter, err := format.NewTierStatsWriter(tmpOutDir)
		if err != nil {
			return fmt.Errorf("create tier stats writer: %w", err)
		}
		if err := tierWriter.Write(trieResult); err != nil {
			return fmt.Errorf("write tier stats: %w", err)
		}
	}

	// Step 5: Build depth index
	if err := writeDepthIndex(tmpOutDir, trieResult); err != nil {
		return fmt.Errorf("write depth index: %w", err)
	}

	// Step 6: Build MPHF
	if err := writeMPHF(tmpOutDir, trieResult); err != nil {
		return fmt.Errorf("write MPHF: %w", err)
	}

	// Step 7: Write manifest with checksums
	if err := format.WriteManifest(tmpOutDir, uint64(len(trieResult.Nodes)), trieResult.MaxDepth); err != nil {
		return fmt.Errorf("write manifest: %w", err)
	}

	// Step 8: Sync directory to ensure all files are persisted
	if err := format.SyncDir(tmpOutDir); err != nil {
		return fmt.Errorf("sync output dir: %w", err)
	}

	// Step 9: Atomic move to final location
	if err := os.RemoveAll(cfg.OutDir); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove old output dir: %w", err)
	}
	if err := os.Rename(tmpOutDir, cfg.OutDir); err != nil {
		return fmt.Errorf("rename output dir: %w", err)
	}

	// Final sync of parent directory to persist the rename
	parentDir := filepath.Dir(cfg.OutDir)
	_ = format.SyncDir(parentDir)

	return nil
}

// Build constructs an index from the given inventory files.
func Build(ctx context.Context, cfg Config, inventoryFiles []string) error {
	if cfg.OutDir == "" {
		return fmt.Errorf("output directory required")
	}
	if cfg.TmpDir == "" {
		return fmt.Errorf("temp directory required")
	}
	if cfg.ChunkSize <= 0 {
		cfg.ChunkSize = 1_000_000
	}

	// Create temp output directory
	tmpOutDir := cfg.OutDir + ".tmp"
	if err := os.MkdirAll(tmpOutDir, 0755); err != nil {
		return fmt.Errorf("create temp output dir: %w", err)
	}
	defer os.RemoveAll(tmpOutDir)

	// Step 1: External sort all inventory files
	sorter := extsort.NewSorter(extsort.Config{
		MaxRecordsPerChunk: cfg.ChunkSize,
		TmpDir:             cfg.TmpDir,
	})
	defer sorter.Cleanup()

	for _, path := range inventoryFiles {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		opts := inventory.OpenOptions{TrackTiers: cfg.TrackTiers}
		reader, err := inventory.OpenFileWithOptions(path, opts)
		if err != nil {
			return fmt.Errorf("open inventory file %s: %w", path, err)
		}

		if err := sorter.AddRecords(ctx, reader); err != nil {
			reader.Close()
			return fmt.Errorf("process inventory file %s: %w", path, err)
		}

		reader.Close()
	}

	// Step 2: Merge and build trie
	iter, err := sorter.Merge(ctx)
	if err != nil {
		return fmt.Errorf("merge sorted runs: %w", err)
	}
	defer iter.Close()

	var builder *triebuild.Builder
	if cfg.TrackTiers {
		builder = triebuild.NewWithTiers()
	} else {
		builder = triebuild.New()
	}
	result, err := builder.Build(iter)
	if err != nil {
		return fmt.Errorf("build trie: %w", err)
	}

	if len(result.Nodes) == 0 {
		return fmt.Errorf("no nodes built from inventory")
	}

	// Step 3: Write columnar arrays
	if err := writeColumnarArrays(tmpOutDir, result); err != nil {
		return fmt.Errorf("write columnar arrays: %w", err)
	}

	// Step 4: Write tier stats (if tracking enabled)
	if cfg.TrackTiers {
		tierWriter, err := format.NewTierStatsWriter(tmpOutDir)
		if err != nil {
			return fmt.Errorf("create tier stats writer: %w", err)
		}
		if err := tierWriter.Write(result); err != nil {
			return fmt.Errorf("write tier stats: %w", err)
		}
	}

	// Step 5: Build depth index
	if err := writeDepthIndex(tmpOutDir, result); err != nil {
		return fmt.Errorf("write depth index: %w", err)
	}

	// Step 6: Build MPHF
	if err := writeMPHF(tmpOutDir, result); err != nil {
		return fmt.Errorf("write MPHF: %w", err)
	}

	// Step 7: Write manifest with checksums
	if err := format.WriteManifest(tmpOutDir, uint64(len(result.Nodes)), result.MaxDepth); err != nil {
		return fmt.Errorf("write manifest: %w", err)
	}

	// Step 8: Sync directory to ensure all files are persisted
	if err := format.SyncDir(tmpOutDir); err != nil {
		return fmt.Errorf("sync output dir: %w", err)
	}

	// Step 9: Atomic move to final location
	if err := os.RemoveAll(cfg.OutDir); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove old output dir: %w", err)
	}
	if err := os.Rename(tmpOutDir, cfg.OutDir); err != nil {
		return fmt.Errorf("rename output dir: %w", err)
	}

	// Final sync of parent directory to persist the rename
	parentDir := filepath.Dir(cfg.OutDir)
	_ = format.SyncDir(parentDir) // Best-effort durability, non-fatal if it fails

	return nil
}

func writeColumnarArrays(outDir string, result *triebuild.Result) error {
	n := len(result.Nodes)
	use64 := n > 1<<32

	// subtree_end
	subtreeEndPath := filepath.Join(outDir, "subtree_end.u64")
	subtreeWriter, err := format.NewArrayWriter(subtreeEndPath, 8)
	if err != nil {
		return err
	}
	for _, node := range result.Nodes {
		if err := subtreeWriter.WriteU64(node.SubtreeEnd); err != nil {
			subtreeWriter.Close()
			return err
		}
	}
	if err := subtreeWriter.Close(); err != nil {
		return err
	}

	// depth
	depthPath := filepath.Join(outDir, "depth.u32")
	depthWriter, err := format.NewArrayWriter(depthPath, 4)
	if err != nil {
		return err
	}
	for _, node := range result.Nodes {
		if err := depthWriter.WriteU32(node.Depth); err != nil {
			depthWriter.Close()
			return err
		}
	}
	if err := depthWriter.Close(); err != nil {
		return err
	}

	// object_count
	countPath := filepath.Join(outDir, "object_count.u64")
	countWriter, err := format.NewArrayWriter(countPath, 8)
	if err != nil {
		return err
	}
	for _, node := range result.Nodes {
		if err := countWriter.WriteU64(node.ObjectCount); err != nil {
			countWriter.Close()
			return err
		}
	}
	if err := countWriter.Close(); err != nil {
		return err
	}

	// total_bytes
	bytesPath := filepath.Join(outDir, "total_bytes.u64")
	bytesWriter, err := format.NewArrayWriter(bytesPath, 8)
	if err != nil {
		return err
	}
	for _, node := range result.Nodes {
		if err := bytesWriter.WriteU64(node.TotalBytes); err != nil {
			bytesWriter.Close()
			return err
		}
	}
	if err := bytesWriter.Close(); err != nil {
		return err
	}

	// max_depth_in_subtree
	maxDepthPath := filepath.Join(outDir, "max_depth_in_subtree.u32")
	maxDepthWriter, err := format.NewArrayWriter(maxDepthPath, 4)
	if err != nil {
		return err
	}
	for _, node := range result.Nodes {
		if err := maxDepthWriter.WriteU32(node.MaxDepthInSubtree); err != nil {
			maxDepthWriter.Close()
			return err
		}
	}
	if err := maxDepthWriter.Close(); err != nil {
		return err
	}

	_ = use64 // For future optimization
	return nil
}

func writeDepthIndex(outDir string, result *triebuild.Result) error {
	builder := format.NewDepthIndexBuilder()

	for _, node := range result.Nodes {
		builder.Add(node.Pos, node.Depth)
	}

	return builder.Build(outDir)
}

func writeMPHF(outDir string, result *triebuild.Result) error {
	builder := format.NewMPHFBuilder()

	for _, node := range result.Nodes {
		builder.Add(node.Prefix, node.Pos)
	}

	return builder.Build(outDir)
}
